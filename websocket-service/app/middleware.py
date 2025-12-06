# C:\Users\user\Desktop\TechStats\websocket-service\app\middleware.py
import time
import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import structlog

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware для логирования HTTP запросов"""
    
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        start_time = time.time()
        
        # Логирование начала запроса
        logger.info(
            "WebSocket service HTTP request started",
            request_id=request_id,
            method=request.method,
            url=str(request.url),
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent")
        )
        
        try:
            response = await call_next(request)
            process_time = time.time() - start_time
            
            # Логирование завершения запроса
            logger.info(
                "WebSocket service HTTP request completed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                status_code=response.status_code,
                process_time=process_time
            )
            
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = str(process_time)
            
            return response
            
        except Exception as e:
            process_time = time.time() - start_time
            
            logger.error(
                "WebSocket service HTTP request failed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                error=str(e),
                process_time=process_time
            )
            
            raise


class WebSocketMiddleware:
    """Middleware для WebSocket соединений"""
    
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] == "websocket":
            # Логирование подключения WebSocket
            client = scope.get("client")
            client_ip = client[0] if client else "unknown"
            
            logger.info(
                "WebSocket connection attempt",
                path=scope.get("path", ""),
                client_ip=client_ip
            )
        
        await self.app(scope, receive, send)