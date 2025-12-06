# C:\Users\user\Desktop\TechStats\api-gateway\app\rate_limiting.py
import time
from typing import Optional
import redis.asyncio as redis
from fastapi import Request, HTTPException
import structlog

from config import settings

logger = structlog.get_logger()


class RateLimiter:
    """Класс для rate limiting на основе Redis"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.rate_limit_per_minute = settings.rate_limit_per_minute
        self.rate_limit_per_hour = settings.rate_limit_per_hour
    
    async def init_redis(self):
        """Инициализация Redis"""
        self.redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
    
    async def check_rate_limit(self, key: str, limit: int, window: int) -> bool:
        """Проверка превышения лимита запросов"""
        if not self.redis_client:
            return True
        
        try:
            current = await self.redis_client.get(key)
            current_count = int(current) if current else 0
            
            if current_count >= limit:
                return False
            
            # Увеличение счетчика
            pipeline = self.redis_client.pipeline()
            pipeline.incr(key)
            pipeline.expire(key, window)
            await pipeline.execute()
            
            return True
            
        except Exception as e:
            logger.error("Rate limiting error", error=str(e))
            return True  # В случае ошибки Redis пропускаем запрос
    
    async def is_rate_limited(self, request: Request) -> bool:
        """Проверка rate limiting для запроса"""
        client_ip = request.client.host if request.client else "unknown"
        path = request.url.path
        
        # Ключи для Redis
        minute_key = f"rate_limit:minute:{client_ip}:{path}"
        hour_key = f"rate_limit:hour:{client_ip}"
        
        # Проверка лимитов
        minute_allowed = await self.check_rate_limit(minute_key, self.rate_limit_per_minute, 60)
        hour_allowed = await self.check_rate_limit(hour_key, self.rate_limit_per_hour, 3600)
        
        return not (minute_allowed and hour_allowed)
    
    async def get_rate_limit_info(self, request: Request) -> dict:
        """Получение информации о rate limiting"""
        client_ip = request.client.host if request.client else "unknown"
        
        if not self.redis_client:
            return {"limited": False, "remaining": float("inf")}
        
        try:
            minute_key = f"rate_limit:minute:{client_ip}:{request.url.path}"
            hour_key = f"rate_limit:hour:{client_ip}"
            
            minute_count = await self.redis_client.get(minute_key) or "0"
            hour_count = await self.redis_client.get(hour_key) or "0"
            
            return {
                "limited": False,
                "minute": {
                    "used": int(minute_count),
                    "limit": self.rate_limit_per_minute,
                    "remaining": max(0, self.rate_limit_per_minute - int(minute_count))
                },
                "hour": {
                    "used": int(hour_count),
                    "limit": self.rate_limit_per_hour,
                    "remaining": max(0, self.rate_limit_per_hour - int(hour_count))
                }
            }
            
        except Exception as e:
            logger.error("Failed to get rate limit info", error=str(e))
            return {"limited": False, "remaining": float("inf")}
    
    async def close(self):
        """Закрытие соединения с Redis"""
        if self.redis_client:
            await self.redis_client.close()


# Глобальный экземпляр rate limiter
rate_limiter = RateLimiter()


async def rate_limit(request: Request):
    """Dependency для rate limiting"""
    if await rate_limiter.is_rate_limited(request):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later.",
            headers={
                "X-RateLimit-Limit": str(rate_limiter.rate_limit_per_minute),
                "X-RateLimit-Remaining": "0",
                "Retry-After": "60"
            }
        )