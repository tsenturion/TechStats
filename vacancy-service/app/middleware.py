# C:\Users\user\Desktop\TechStats\vacancy-service\app\middleware.py
import time
import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import structlog

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware для логирования запросов"""
    
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        start_time = time.time()
        
        # Логирование начала запроса
        logger.info(
            "Vacancy service request started",
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
                "Vacancy service request completed",
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
                "Vacancy service request failed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                error=str(e),
                process_time=process_time
            )
            
            raise


class ResponseTimeMiddleware(BaseHTTPMiddleware):
    """Middleware для измерения времени ответа"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        
        response.headers["X-Response-Time"] = str(process_time)
        
        # Логирование медленных запросов
        if process_time > 2.0:  # Более 2 секунд
            logger.warning(
                "Slow request detected",
                url=str(request.url),
                method=request.method,
                process_time=process_time
            )
        
        return response