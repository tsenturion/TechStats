# C:\Users\user\Desktop\TechStats\websocket-service\main.py
import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, List
import signal
import sys

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import structlog
import uvicorn
from prometheus_client import Counter, Histogram, Gauge, generate_latest

from config import settings
from app.middleware import RequestLoggingMiddleware, WebSocketMiddleware
from app.routers import websocket_router, health, metrics, admin
from app.connection_manager import ConnectionManager
from app.session_store import SessionStore
from app.analysis_proxy import AnalysisProxy
from app.metrics import setup_metrics

# Настройка логирования
logger = structlog.get_logger()

# Метрики
WEBSOCKET_CONNECTIONS = Counter(
    'websocket_connections_total',
    'Total WebSocket connections',
    ['status']
)
WEBSOCKET_MESSAGES = Counter(
    'websocket_messages_total',
    'Total WebSocket messages',
    ['direction']
)
ACTIVE_CONNECTIONS = Gauge(
    'websocket_active_connections',
    'Number of active WebSocket connections'
)
MESSAGE_PROCESSING_TIME = Histogram(
    'websocket_message_processing_seconds',
    'WebSocket message processing time',
    ['message_type']
)
ANALYSIS_PROGRESS_UPDATES = Counter(
    'analysis_progress_updates_total',
    'Total analysis progress updates'
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    logger.info(
        "Starting WebSocket Service",
        version=settings.version,
        environment=settings.environment
    )
    
    # Инициализация Redis для сессий
    redis_client = redis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=False
    )
    app.state.redis_client = redis_client
    logger.info("Redis connected")
    
    # Инициализация менеджера соединений
    connection_manager = ConnectionManager()
    app.state.connection_manager = connection_manager
    logger.info("Connection manager initialized")
    
    # Инициализация хранилища сессий
    session_store = SessionStore(redis_client)
    await session_store.initialize()
    app.state.session_store = session_store
    logger.info("Session store initialized")
    
    # Инициализация HTTP клиентов для других сервисов
    analyzer_client = httpx.AsyncClient(
        base_url=settings.analyzer_service_url,
        timeout=30.0,
        headers={"User-Agent": f"TechStats WebSocket/{settings.version}"}
    )
    app.state.analyzer_client = analyzer_client
    
    vacancy_client = httpx.AsyncClient(
        base_url=settings.vacancy_service_url,
        timeout=30.0,
        headers={"User-Agent": f"TechStats WebSocket/{settings.version}"}
    )
    app.state.vacancy_client = vacancy_client
    
    cache_client = httpx.AsyncClient(
        base_url=settings.cache_service_url,
        timeout=10.0,
        headers={"User-Agent": f"TechStats WebSocket/{settings.version}"}
    )
    app.state.cache_client = cache_client
    
    logger.info("HTTP clients initialized")
    
    # Инициализация Analysis Proxy
    analysis_proxy = AnalysisProxy(
        analyzer_client=analyzer_client,
        vacancy_client=vacancy_client,
        cache_client=cache_client,
        session_store=session_store
    )
    app.state.analysis_proxy = analysis_proxy
    logger.info("Analysis proxy initialized")
    
    # Запуск фоновых задач
    cleanup_task = asyncio.create_task(
        periodic_cleanup(app.state.session_store, app.state.connection_manager)
    )
    app.state.cleanup_task = cleanup_task
    logger.info("Background tasks started")
    
    # Обработка сигналов для graceful shutdown
    def handle_shutdown(signum, frame):
        logger.info("Received shutdown signal")
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    yield
    
    # Завершение работы
    logger.info("Shutting down WebSocket Service")
    
    # Отмена фоновых задач
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    
    # Закрытие соединений
    await connection_manager.disconnect_all()
    
    # Закрытие HTTP клиентов
    await analyzer_client.aclose()
    await vacancy_client.aclose()
    await cache_client.aclose()
    
    # Закрытие Redis
    await redis_client.close()
    
    logger.info("WebSocket Service shutdown complete")


async def periodic_cleanup(session_store, connection_manager):
    """Периодическая очистка устаревших сессий"""
    while True:
        try:
            await asyncio.sleep(settings.cleanup_interval_seconds)
            
            # Очистка устаревших сессий
            cleaned = await session_store.cleanup_expired_sessions()
            if cleaned > 0:
                logger.debug("Cleaned expired sessions", count=cleaned)
            
            # Очистка неактивных соединений
            await connection_manager.cleanup_inactive_connections()
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Cleanup error", error=str(e))


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="WebSocket сервис для передачи прогресса анализа в реальном времени",
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
app.add_middleware(WebSocketMiddleware)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"] if settings.debug else ["techstats.com", "*.techstats.com"]
)

# Подключение роутеров
app.include_router(websocket_router.router, prefix="/api/v1", tags=["websocket"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])


@app.get("/")
async def root():
    """Корневой endpoint"""
    connection_manager = app.state.connection_manager
    
    return {
        "service": settings.app_name,
        "version": settings.version,
        "status": "operational",
        "connections": {
            "active": connection_manager.active_connections_count(),
            "total_accepted": connection_manager.total_connections_accepted(),
            "total_rejected": connection_manager.total_connections_rejected()
        },
        "services": {
            "analyzer": settings.analyzer_service_url,
            "vacancy": settings.vacancy_service_url,
            "cache": settings.cache_service_url
        },
        "settings": {
            "max_connections": settings.max_total_connections,
            "ping_interval": settings.websocket_ping_interval,
            "session_ttl": settings.session_ttl_seconds
        }
    }


@app.get("/stats/connections")
async def get_connection_stats():
    """Получение статистики соединений"""
    connection_manager = app.state.connection_manager
    
    return {
        "active_connections": connection_manager.active_connections_count(),
        "total_accepted": connection_manager.total_connections_accepted(),
        "total_rejected": connection_manager.total_connections_rejected(),
        "connection_stats": connection_manager.get_connection_stats(),
        "ip_limits": connection_manager.get_ip_limits(),
        "message_stats": connection_manager.get_message_stats()
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Основной WebSocket endpoint для всех соединений
    """
    connection_manager = app.state.connection_manager
    
    try:
        # Принятие соединения
        await connection_manager.connect(websocket)
        WEBSOCKET_CONNECTIONS.labels(status="connected").inc()
        ACTIVE_CONNECTIONS.inc()
        
        # Получение информации о соединении
        client_ip = websocket.client.host if websocket.client else "unknown"
        connection_id = connection_manager.get_connection_id(websocket)
        
        logger.info(
            "WebSocket connected",
            connection_id=connection_id,
            client_ip=client_ip,
            active_connections=connection_manager.active_connections_count()
        )
        
        try:
            while True:
                # Ожидание сообщения от клиента
                try:
                    data = await asyncio.wait_for(
                        websocket.receive_json(),
                        timeout=settings.connection_timeout
                    )
                except asyncio.TimeoutError:
                    # Отправляем ping для проверки соединения
                    try:
                        await websocket.send_json({"type": "ping", "timestamp": time.time()})
                        continue
                    except:
                        # Соединение разорвано
                        break
                
                WEBSOCKET_MESSAGES.labels(direction="inbound").inc()
                
                # Обработка сообщения
                start_time = time.time()
                await process_websocket_message(websocket, data)
                processing_time = time.time() - start_time
                
                MESSAGE_PROCESSING_TIME.labels(
                    message_type=data.get("type", "unknown")
                ).observe(processing_time)
                
        except WebSocketDisconnect:
            logger.info(
                "WebSocket disconnected by client",
                connection_id=connection_id,
                client_ip=client_ip
            )
            
        except Exception as e:
            logger.error(
                "WebSocket error",
                connection_id=connection_id,
                client_ip=client_ip,
                error=str(e)
            )
            
            try:
                await websocket.send_json({
                    "type": "error",
                    "message": f"Internal server error: {str(e)}"
                })
            except:
                pass
            
    except Exception as e:
        logger.error("WebSocket connection failed", error=str(e))
        WEBSOCKET_CONNECTIONS.labels(status="rejected").inc()
        
    finally:
        # Закрытие соединения
        connection_manager.disconnect(websocket)
        ACTIVE_CONNECTIONS.dec()
        
        logger.info(
            "WebSocket connection closed",
            active_connections=connection_manager.active_connections_count()
        )


async def process_websocket_message(websocket: WebSocket, data: Dict[str, Any]):
    """Обработка входящего WebSocket сообщения"""
    message_type = data.get("type", "unknown")
    
    if message_type == "analyze":
        # Запуск анализа через proxy
        analysis_proxy = websocket.app.state.analysis_proxy
        await analysis_proxy.start_analysis(websocket, data)
        
    elif message_type == "subscribe":
        # Подписка на обновления
        topic = data.get("topic")
        if topic:
            connection_manager = websocket.app.state.connection_manager
            await connection_manager.subscribe(websocket, topic)
            await websocket.send_json({
                "type": "subscribed",
                "topic": topic,
                "timestamp": time.time()
            })
            
    elif message_type == "unsubscribe":
        # Отписка от обновлений
        topic = data.get("topic")
        if topic:
            connection_manager = websocket.app.state.connection_manager
            await connection_manager.unsubscribe(websocket, topic)
            await websocket.send_json({
                "type": "unsubscribed",
                "topic": topic,
                "timestamp": time.time()
            })
            
    elif message_type == "ping":
        # Ответ на ping
        await websocket.send_json({
            "type": "pong",
            "timestamp": time.time(),
            "original_timestamp": data.get("timestamp")
        })
        
    elif message_type == "get_session":
        # Получение информации о сессии
        session_id = data.get("session_id")
        if session_id:
            session_store = websocket.app.state.session_store
            session = await session_store.get_session(session_id)
            await websocket.send_json({
                "type": "session_info",
                "session_id": session_id,
                "session": session
            })
            
    else:
        # Неизвестный тип сообщения
        await websocket.send_json({
            "type": "error",
            "message": f"Unknown message type: {message_type}",
            "supported_types": ["analyze", "subscribe", "unsubscribe", "ping", "get_session"]
        })


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.port,
        workers=settings.workers,
        reload=settings.debug,
        log_level=settings.log_level,
        ws_ping_interval=settings.websocket_ping_interval,
        ws_ping_timeout=settings.websocket_ping_timeout
    )