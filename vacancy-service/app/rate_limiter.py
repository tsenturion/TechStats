# C:\Users\user\Desktop\TechStats\vacancy-service\app\rate_limiter.py
import time
import asyncio
from typing import Optional, Dict, Any
import redis.asyncio as redis
import structlog

from config import settings

logger = structlog.get_logger()


class RateLimiter:
    """Rate limiter для управления запросами к HH API"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.local_limits: Dict[str, Any] = {}
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """Инициализация rate limiter"""
        self.redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        
        # Инициализация локальных лимитов
        self.local_limits = {
            "second": {"count": 0, "timestamp": time.time()},
            "minute": {"count": 0, "timestamp": time.time()},
            "hour": {"count": 0, "timestamp": time.time()}
        }
    
    async def can_make_request(self) -> bool:
        """Проверка возможности сделать запрос"""
        async with self.lock:
            current_time = time.time()
            
            # Проверка лимита в секунду (7 запросов/сек)
            if current_time - self.local_limits["second"]["timestamp"] < 1:
                if self.local_limits["second"]["count"] >= settings.hh_rate_limit_per_second:
                    return False
            else:
                self.local_limits["second"] = {"count": 0, "timestamp": current_time}
            
            # Увеличение счетчиков
            self.local_limits["second"]["count"] += 1
            self.local_limits["minute"]["count"] += 1
            self.local_limits["hour"]["count"] += 1
            
            # Сброс счетчиков по истечении времени
            if current_time - self.local_limits["minute"]["timestamp"] > 60:
                self.local_limits["minute"] = {"count": 0, "timestamp": current_time}
            
            if current_time - self.local_limits["hour"]["timestamp"] > 3600:
                self.local_limits["hour"] = {"count": 0, "timestamp": current_time}
            
            # Проверка дневного лимита в Redis
            if self.redis_client:
                day_key = f"hh_rate_limit:day:{datetime.now().strftime('%Y-%m-%d')}"
                day_count = await self.redis_client.get(day_key)
                
                if day_count and int(day_count) >= settings.hh_rate_limit_per_day:
                    logger.warning("Daily HH API rate limit exceeded")
                    return False
            
            return True
    
    async def increment_daily_counter(self):
        """Увеличение дневного счетчика"""
        if self.redis_client:
            day_key = f"hh_rate_limit:day:{datetime.now().strftime('%Y-%m-%d')}"
            pipeline = self.redis_client.pipeline()
            pipeline.incr(day_key)
            pipeline.expire(day_key, 86400)  # 24 часа
            await pipeline.execute()
    
    async def get_rate_limit_stats(self) -> Dict[str, Any]:
        """Получение статистики по rate limiting"""
        stats = {
            "local": {
                "second": self.local_limits["second"]["count"],
                "minute": self.local_limits["minute"]["count"],
                "hour": self.local_limits["hour"]["count"]
            },
            "limits": {
                "per_second": settings.hh_rate_limit_per_second,
                "per_day": settings.hh_rate_limit_per_day
            }
        }
        
        if self.redis_client:
            day_key = f"hh_rate_limit:day:{datetime.now().strftime('%Y-%m-%d')}"
            day_count = await self.redis_client.get(day_key)
            stats["daily"] = int(day_count) if day_count else 0
            
        return stats
    
    async def close(self):
        """Закрытие соединения с Redis"""
        if self.redis_client:
            await self.redis_client.close()