import time
import asyncio
import json
from typing import Optional, Dict, Any
from datetime import datetime
import redis.asyncio as redis
import structlog

from config import settings
from shared.runtime_settings import RUNTIME_SETTINGS_KEY, build_effective_runtime_settings

logger = structlog.get_logger()


class RateLimiter:
    """Rate limiter для управления запросами к HH API"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.local_limits: Dict[str, Any] = {}
        self.lock = asyncio.Lock()
        self._runtime_cache: Dict[str, Any] = build_effective_runtime_settings()
        self._runtime_cache_loaded_at: float = 0.0
        
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
            runtime_settings = await self._load_runtime_settings()
            per_second_limit = int(runtime_settings.get("hh_rate_limit_per_second", settings.hh_rate_limit_per_second))
            per_day_limit = int(runtime_settings.get("hh_rate_limit_per_day", settings.hh_rate_limit_per_day))
            
            # Проверка лимита в секунду (7 запросов/сек)
            if current_time - self.local_limits["second"]["timestamp"] < 1:
                if self.local_limits["second"]["count"] >= per_second_limit:
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
                
                if day_count and int(day_count) >= per_day_limit:
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
        runtime_settings = await self._load_runtime_settings()
        per_second_limit = int(runtime_settings.get("hh_rate_limit_per_second", settings.hh_rate_limit_per_second))
        per_day_limit = int(runtime_settings.get("hh_rate_limit_per_day", settings.hh_rate_limit_per_day))

        stats = {
            "local": {
                "second": self.local_limits["second"]["count"],
                "minute": self.local_limits["minute"]["count"],
                "hour": self.local_limits["hour"]["count"]
            },
            "limits": {
                "per_second": per_second_limit,
                "per_day": per_day_limit
            }
        }
        
        if self.redis_client:
            day_key = f"hh_rate_limit:day:{datetime.now().strftime('%Y-%m-%d')}"
            day_count = await self.redis_client.get(day_key)
            stats["daily"] = int(day_count) if day_count else 0
            
        return stats

    async def _load_runtime_settings(self) -> Dict[str, Any]:
        now = time.time()
        if (now - self._runtime_cache_loaded_at) < 2.0:
            return self._runtime_cache

        if not self.redis_client:
            self._runtime_cache = build_effective_runtime_settings()
            self._runtime_cache_loaded_at = now
            return self._runtime_cache

        try:
            raw = await self.redis_client.get(RUNTIME_SETTINGS_KEY)
            if raw:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    self._runtime_cache = build_effective_runtime_settings(parsed)
                else:
                    self._runtime_cache = build_effective_runtime_settings()
            else:
                self._runtime_cache = build_effective_runtime_settings()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load runtime settings in vacancy rate limiter", error=str(exc))
            self._runtime_cache = build_effective_runtime_settings()

        self._runtime_cache_loaded_at = now
        return self._runtime_cache
    
    async def close(self):
        """Закрытие соединения с Redis"""
        if self.redis_client:
            await self.redis_client.close()
