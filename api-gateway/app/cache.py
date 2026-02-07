import hashlib
import json
from functools import wraps
from typing import Any, Optional

import redis.asyncio as redis
import structlog
from fastapi import Request

from config import settings

logger = structlog.get_logger()


class CacheManager:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None

    async def init_redis(self) -> None:
        self.redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=False,
        )
        await self.redis_client.ping()

    async def get(self, key: str) -> Optional[Any]:
        if not self.redis_client:
            return None
        try:
            payload = await self.redis_client.get(key)
            if payload is None:
                return None
            return json.loads(payload)
        except Exception as exc:
            logger.warning("Gateway cache get failed", key=key, error=str(exc))
            return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        if not self.redis_client:
            return False
        try:
            await self.redis_client.set(key, json.dumps(value, ensure_ascii=False), ex=ttl)
            return True
        except Exception as exc:
            logger.warning("Gateway cache set failed", key=key, error=str(exc))
            return False

    async def delete(self, key: str) -> bool:
        if not self.redis_client:
            return False
        try:
            return bool(await self.redis_client.delete(key))
        except Exception as exc:
            logger.warning("Gateway cache delete failed", key=key, error=str(exc))
            return False

    async def clear(self, pattern: str = "gateway:*") -> int:
        if not self.redis_client:
            return 0
        keys = []
        async for key in self.redis_client.scan_iter(match=pattern):
            keys.append(key)
        if not keys:
            return 0
        return int(await self.redis_client.delete(*keys))

    async def get_stats(self) -> dict:
        if not self.redis_client:
            return {"connected": False}
        info = await self.redis_client.info()
        return {
            "connected": True,
            "redis_version": info.get("redis_version"),
            "used_memory_human": info.get("used_memory_human"),
            "hits": info.get("keyspace_hits", 0),
            "misses": info.get("keyspace_misses", 0),
        }

    async def close(self) -> None:
        if self.redis_client:
            await self.redis_client.close()


cache_manager = CacheManager()


def _make_cache_key(request: Request, prefix: str = "gateway") -> str:
    params = sorted(request.query_params.items())
    payload = {
        "path": request.url.path,
        "method": request.method,
        "params": params,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


async def get_cached_response(cache_key: str):
    return await cache_manager.get(cache_key)


def cache_response(ttl: int = 300):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request: Optional[Request] = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None:
                return await func(*args, **kwargs)

            cache_key = _make_cache_key(request)
            cached = await cache_manager.get(cache_key)
            if cached is not None:
                return cached

            response = await func(*args, **kwargs)
            await cache_manager.set(cache_key, response, ttl=ttl)
            return response

        return wrapper

    return decorator

