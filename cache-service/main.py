# C:\Users\user\Desktop\TechStats\cache-service\main.py
import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
import signal
import sys

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import structlog

from config import settings
from app.middleware import RequestLoggingMiddleware, ResponseTimeMiddleware, RateLimitMiddleware
from app.routers import cache, health, metrics, admin, cluster
from app.cache_manager import CacheManager
from app.cleanup_scheduler import CleanupScheduler
from app.cluster_manager import ClusterManager
from app.metrics import setup_metrics, metrics_router

# Настройка логирования
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Обработка сигналов
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Запуск
    logger.info(
        "Starting Cache Service",
        version=settings.version,
        environment=settings.environment,
        backend=settings.cache_backend,
        node_id=settings.node_id
    )
    
    # Инициализация менеджера кэша
    cache_manager = CacheManager()
    await cache_manager.initialize()
    app.state.cache_manager = cache_manager
    logger.info("Cache manager initialized", backend=settings.cache_backend)
    
    # Инициализация планировщика очистки
    cleanup_scheduler = CleanupScheduler(cache_manager)
    await cleanup_scheduler.start()
    app.state.cleanup_scheduler = cleanup_scheduler
    logger.info("Cleanup scheduler started")
    
    # Инициализация кластера (если включено)
    cluster_manager = None
    if settings.enable_clustering:
        cluster_manager = ClusterManager()
        await cluster_manager.initialize()
        app.state.cluster_manager = cluster_manager
        logger.info("Cluster manager initialized", nodes=len(settings.cluster_nodes))
    
    # Инициализация метрик
    if settings.enable_prometheus:
        setup_metrics(cache_manager)
        logger.info("Metrics initialized")
    
    # Предварительное тестирование
    await perform_health_check(cache_manager)
    
    yield
    
    # Завершение работы
    logger.info("Shutting down Cache Service")
    
    if cluster_manager:
        await cluster_manager.shutdown()
    
    await cleanup_scheduler.stop()
    await cache_manager.shutdown()


async def perform_health_check(cache_manager: CacheManager):
    """Предварительная проверка здоровья"""
    try:
        # Тест записи и чтения
        test_key = f"health_check:{settings.node_id}:{time.time()}"
        test_value = {"status": "healthy", "timestamp": time.time()}
        
        await cache_manager.set(test_key, test_value, ttl=10)
        retrieved = await cache_manager.get(test_key)
        
        if retrieved and retrieved.get("status") == "healthy":
            logger.info("Health check passed", key=test_key)
        else:
            logger.error("Health check failed", key=test_key)
            raise RuntimeError("Cache health check failed")
            
        # Удаление тестового ключа
        await cache_manager.delete(test_key)
        
    except Exception as e:
        logger.error("Health check error", error=str(e))
        raise


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="Централизованный сервис кэширования для TechStats",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Настройка middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ResponseTimeMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"] if settings.debug else ["techstats.com", "*.techstats.com"]
)

# Подключение роутеров
app.include_router(cache.router, prefix="/api/v1", tags=["cache"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])
app.include_router(cluster.router, prefix="/api/v1/cluster", tags=["cluster"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])


@app.get("/")
async def root():
    """Корневой endpoint"""
    cache_manager = app.state.cache_manager
    
    return {
        "service": settings.app_name,
        "version": settings.version,
        "status": "operational",
        "node_id": settings.node_id,
        "cache_backend": settings.cache_backend.value,
        "cache_strategy": settings.cache_strategy.value,
        "cache_stats": await cache_manager.get_stats(),
        "cluster_enabled": settings.enable_clustering,
        "cluster_nodes": settings.cluster_nodes if settings.enable_clustering else []
    }


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Глобальный обработчик исключений"""
    logger.error(
        "Unhandled exception",
        path=request.url.path,
        method=request.method,
        error=str(exc),
        exc_info=True
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error_id": str(time.time()),
            "node_id": settings.node_id
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.port,
        workers=settings.workers,
        reload=settings.debug,
        log_level=settings.log_level
    )