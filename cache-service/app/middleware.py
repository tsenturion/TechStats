# C:\Users\user\Desktop\TechStats\cache-service\app\middleware.py
import time
import uuid
from typing import Dict, Any
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import structlog

from config import settings

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware для логирования запросов"""
    
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        start_time = time.time()
        
        # Логирование начала запроса
        logger.info(
            "Cache service request started",
            request_id=request_id,
            method=request.method,
            url=str(request.url),
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            node_id=settings.node_id
        )
        
        try:
            response = await call_next(request)
            process_time = time.time() - start_time
            
            # Логирование завершения запроса
            logger.info(
                "Cache service request completed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                status_code=response.status_code,
                process_time=process_time,
                node_id=settings.node_id
            )
            
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time"] = str(process_time)
            response.headers["X-Node-ID"] = settings.node_id
            
            return response
            
        except Exception as e:
            process_time = time.time() - start_time
            
            logger.error(
                "Cache service request failed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                error=str(e),
                process_time=process_time,
                node_id=settings.node_id,
                exc_info=True
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
        if process_time > 1.0:  # Более 1 секунды
            logger.warning(
                "Slow cache request detected",
                url=str(request.url),
                method=request.method,
                process_time=process_time,
                node_id=settings.node_id
            )
        
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware для rate limiting"""
    
    def __init__(self, app):
        super().__init__(app)
        self.rate_limits: Dict[str, Dict[str, Any]] = {}
    
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "unknown"
        current_minute = int(time.time() / 60)
        current_hour = int(time.time() / 3600)
        
        # Ключи для rate limiting
        minute_key = f"{client_ip}:{current_minute}"
        hour_key = f"{client_ip}:{current_hour}"
        
        # Инициализация счетчиков
        if minute_key not in self.rate_limits:
            self.rate_limits[minute_key] = {"count": 0, "expires": current_minute + 1}
        
        if hour_key not in self.rate_limits:
            self.rate_limits[hour_key] = {"count": 0, "expires": current_hour + 1}
        
        # Проверка лимитов
        if (self.rate_limits[minute_key]["count"] >= settings.api_rate_limit_per_minute or
            self.rate_limits[hour_key]["count"] >= settings.api_rate_limit_per_hour):
            
            logger.warning(
                "Rate limit exceeded",
                client_ip=client_ip,
                minute_count=self.rate_limits[minute_key]["count"],
                hour_count=self.rate_limits[hour_key]["count"],
                node_id=settings.node_id
            )
            
            return Response(
                status_code=429,
                content="Rate limit exceeded. Please try again later.",
                headers={
                    "X-RateLimit-Limit-Minute": str(settings.api_rate_limit_per_minute),
                    "X-RateLimit-Limit-Hour": str(settings.api_rate_limit_per_hour),
                    "Retry-After": "60"
                }
            )
        
        # Увеличение счетчиков
        self.rate_limits[minute_key]["count"] += 1
        self.rate_limits[hour_key]["count"] += 1
        
        # Очистка устаревших записей
        self._cleanup_rate_limits(current_minute, current_hour)
        
        # Добавление заголовков
        response = await call_next(request)
        response.headers["X-RateLimit-Minute-Remaining"] = str(
            settings.api_rate_limit_per_minute - self.rate_limits[minute_key]["count"]
        )
        response.headers["X-RateLimit-Hour-Remaining"] = str(
            settings.api_rate_limit_per_hour - self.rate_limits[hour_key]["count"]
        )
        
        return response
    
    def _cleanup_rate_limits(self, current_minute: int, current_hour: int):
        """Очистка устаревших записей rate limiting"""
        keys_to_delete = []
        
        for key, data in self.rate_limits.items():
            if ":" in key:
                timestamp = int(key.split(":")[1])
                
                # Удаляем записи старше 2 часов для минутных и 24 часов для часовых
                if ("minute" in key and timestamp < current_minute - 120) or \
                   ("hour" in key and timestamp < current_hour - 24):
                    keys_to_delete.append(key)
        
        for key in keys_to_delete:
            del self.rate_limits[key]