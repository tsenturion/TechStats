# C:\Users\user\Desktop\TechStats\analyzer-service\main.py
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi_health import health as healthcheck_route
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
import structlog
import uvicorn
from prometheus_client import Counter, Histogram, Gauge

from config import settings
from app.middleware import RequestLoggingMiddleware, ResponseTimeMiddleware
from app.routers import analyze, health as health_router, metrics, patterns, stats
from app.cache import cache_manager
from app.analyzer import TextAnalyzer, PatternMatcher
from app.tech_patterns import TechPatternsLoader
from shared.http_client import build_async_client
from shared.observability import setup_correlation_middleware, setup_prometheus_instrumentation

# Настройка логирования
logger = structlog.get_logger()

# Метрики
ANALYSIS_REQUESTS = Counter(
    'analysis_requests_total',
    'Total analysis requests',
    ['type', 'status']
)
ANALYSIS_LATENCY = Histogram(
    'analysis_duration_seconds',
    'Analysis processing latency',
    ['type']
)
TECHNOLOGY_MATCHES = Counter(
    'technology_matches_total',
    'Total technology matches found',
    ['technology']
)
ACTIVE_ANALYSES = Gauge(
    'active_analyses',
    'Number of active analysis processes'
)
VACANCIES_PROCESSED = Counter(
    'vacancies_processed_total',
    'Total vacancies processed'
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info(
        "Starting Analyzer Service",
        version=settings.version,
        environment=settings.environment
    )
    
    # Инициализация Redis
    await cache_manager.init_redis()
    logger.info("Redis connected")
    
    # Загрузка паттернов технологий
    patterns_loader = TechPatternsLoader()
    await patterns_loader.load_patterns()
    app.state.patterns_loader = patterns_loader
    logger.info("Technology patterns loaded", count=len(patterns_loader.get_all_patterns()))
    
    # Инициализация анализатора текста
    text_analyzer = TextAnalyzer()
    await text_analyzer.initialize()
    app.state.text_analyzer = text_analyzer
    logger.info("Text analyzer initialized")
    
    # Инициализация pattern matcher
    pattern_matcher = PatternMatcher(text_analyzer, patterns_loader)
    app.state.pattern_matcher = pattern_matcher
    logger.info("Pattern matcher initialized")
    
    # Инициализация HTTP клиента для vacancy service
    vacancy_client = build_async_client(
        base_url=settings.vacancy_service_url,
        timeout=settings.request_timeout,
        headers={"User-Agent": f"TechStats Analyzer/{settings.version}"}
    )
    app.state.vacancy_client = vacancy_client
    logger.info("Vacancy service client initialized")
    
    yield
    
    # Завершение работы
    logger.info("Shutting down Analyzer Service")
    await cache_manager.close()
    await vacancy_client.aclose()


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="Сервис для анализа текстов вакансий на наличие технологий",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

setup_correlation_middleware(app)
setup_prometheus_instrumentation(app, expose=False)

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

limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=settings.redis_url,
    default_limits=["600/minute"],
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Подключение роутеров
app.include_router(analyze.router, prefix="/api/v1", tags=["analysis"])
app.include_router(patterns.router, prefix="/api/v1", tags=["patterns"])
app.include_router(stats.router, prefix="/api/v1", tags=["statistics"])
app.include_router(health_router.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])


async def _redis_ready() -> bool:
    return bool(cache_manager.redis_client)


app.add_api_route(
    "/api/v1/health/ready",
    healthcheck_route([_redis_ready]),
    tags=["health"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware для сбора метрик"""
    start_time = time.time()
    endpoint = request.url.path
    
    try:
        response = await call_next(request)
        status_code = response.status_code
        ANALYSIS_REQUESTS.labels(type=endpoint.split('/')[-1], status=status_code).inc()
    except Exception:
        status_code = 500
        response = JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
        ANALYSIS_REQUESTS.labels(type=endpoint.split('/')[-1], status=status_code).inc()
    
    request_duration = time.time() - start_time
    ANALYSIS_LATENCY.labels(type=endpoint.split('/')[-1]).observe(request_duration)
    
    return response


@app.get("/")
async def root():
    """Корневой endpoint"""
    patterns_loader = app.state.patterns_loader
    
    return {
        "service": settings.app_name,
        "version": settings.version,
        "status": "operational",
        "vacancy_service_url": settings.vacancy_service_url,
        "patterns_loaded": len(patterns_loader.get_all_patterns()),
        "settings": {
            "max_workers": settings.max_workers,
            "batch_size": settings.batch_size,
            "enable_stemming": settings.enable_stemming,
            "enable_lemmatization": settings.enable_lemmatization
        }
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
