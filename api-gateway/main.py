# C:\Users\user\Desktop\TechStats\api-gateway\main.py
import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from prometheus_client import Counter, Histogram, generate_latest
import structlog
import uvicorn

from config import settings
from app.middleware import (
    RequestLoggingMiddleware,
    ResponseTimeMiddleware,
    AuthenticationMiddleware,
    ServiceHealthMiddleware
)
from app.routers import (
    vacancy_router,
    analyzer_router,
    cache_router,
    websocket_router,
    health_router
)
from app.rate_limiting import rate_limiter
from app.cache import cache_manager
from app.metrics import setup_metrics, metrics_router
from app.websocket_manager import WebSocketManager

# Настройка логирования
logger = structlog.get_logger()

# Метрики
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
REQUEST_LATENCY = Histogram('http_request_duration_seconds', 'HTTP request latency', ['method', 'endpoint'])


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info("Starting API Gateway", version="1.0.0", environment=settings.environment)
    
    # Инициализация Redis
    await cache_manager.init_redis()
    logger.info("Redis connected")
    
    # Инициализация rate limiter
    await rate_limiter.init_redis()
    logger.info("Rate limiter initialized")
    
    # Проверка доступности сервисов
    await check_services_health()
    
    yield
    
    # Завершение работы
    logger.info("Shutting down API Gateway")
    await cache_manager.close()
    await rate_limiter.close()


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    description="API Gateway для микросервисной архитектуры TechStats",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Настройка middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ResponseTimeMiddleware)
app.add_middleware(AuthenticationMiddleware)
app.add_middleware(ServiceHealthMiddleware)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"] if settings.debug else ["techstats.com", "*.techstats.com"]
)

# Настройка rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Подключение роутеров
app.include_router(health_router, prefix="/api/v1", tags=["health"])
app.include_router(vacancy_router, prefix="/api/v1", tags=["vacancy"])
app.include_router(analyzer_router, prefix="/api/v1", tags=["analyzer"])
app.include_router(cache_router, prefix="/api/v1", tags=["cache"])
app.include_router(websocket_router, prefix="/api/v1", tags=["websocket"])
app.include_router(metrics_router, prefix="/api/v1", tags=["metrics"])

# WebSocket менеджер
websocket_manager = WebSocketManager()


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware для сбора метрик"""
    start_time = time.time()
    method = request.method
    endpoint = request.url.path
    
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as e:
        status_code = 500
        response = JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
    
    request_duration = time.time() - start_time
    
    # Обновление метрик
    REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status_code).inc()
    REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(request_duration)
    
    return response


async def check_services_health():
    """Проверка доступности всех сервисов"""
    services = {
        "vacancy": settings.vacancy_service_url,
        "analyzer": settings.analyzer_service_url,
        "cache": settings.cache_service_url,
        "websocket": settings.websocket_service_url,
    }
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in services.items():
            try:
                response = await client.get(f"{url}/health")
                if response.status_code == 200:
                    logger.info(f"Service {name} is healthy", url=url)
                else:
                    logger.warning(f"Service {name} returned {response.status_code}", url=url)
            except Exception as e:
                logger.error(f"Service {name} is unavailable", url=url, error=str(e))


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": settings.app_name,
        "version": "1.0.0",
        "status": "operational",
        "services": {
            "vacancy": settings.vacancy_service_url,
            "analyzer": settings.analyzer_service_url,
            "cache": settings.cache_service_url,
            "websocket": settings.websocket_service_url,
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="debug" if settings.debug else "info"
    )