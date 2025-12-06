# C:\Users\user\Desktop\TechStats\vacancy-service\main.py
import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, List

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import structlog
import uvicorn
from prometheus_client import Counter, Histogram, generate_latest

from config import settings
from app.middleware import RequestLoggingMiddleware, ResponseTimeMiddleware
from app.routers import vacancies, health, metrics
from app.cache import cache_manager
from app.hh_client import HHClient
from app.rate_limiter import RateLimiter

# Настройка логирования
logger = structlog.get_logger()

# Метрики
VACANCY_REQUESTS = Counter(
    'vacancy_requests_total',
    'Total vacancy requests',
    ['method', 'endpoint', 'status']
)
VACANCY_LATENCY = Histogram(
    'vacancy_request_duration_seconds',
    'Vacancy request latency',
    ['method', 'endpoint']
)
HH_API_CALLS = Counter(
    'hh_api_calls_total',
    'Total HH API calls',
    ['endpoint', 'status']
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info(
        "Starting Vacancy Service",
        version=settings.version,
        environment=settings.environment
    )
    
    # Инициализация Redis
    await cache_manager.init_redis()
    logger.info("Redis connected")
    
    # Инициализация HH клиента
    hh_client = HHClient()
    await hh_client.initialize()
    app.state.hh_client = hh_client
    logger.info("HH Client initialized")
    
    # Инициализация rate limiter
    rate_limiter = RateLimiter()
    await rate_limiter.initialize()
    app.state.rate_limiter = rate_limiter
    logger.info("Rate limiter initialized")
    
    yield
    
    # Завершение работы
    logger.info("Shutting down Vacancy Service")
    await cache_manager.close()
    await hh_client.close()
    await rate_limiter.close()


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="Сервис для работы с вакансиями HH.ru",
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
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"] if settings.debug else ["techstats.com", "*.techstats.com"]
)

# Подключение роутеров
app.include_router(vacancies.router, prefix="/api/v1", tags=["vacancies"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])


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
    VACANCY_REQUESTS.labels(method=method, endpoint=endpoint, status=status_code).inc()
    VACANCY_LATENCY.labels(method=method, endpoint=endpoint).observe(request_duration)
    
    return response


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": settings.app_name,
        "version": settings.version,
        "status": "operational",
        "hh_api_url": settings.hh_api_base_url,
        "cache_ttl_hours": settings.cache_ttl_hours
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.port,
        workers=settings.workers,
        reload=settings.debug,
        log_level=settings.log_level
    )