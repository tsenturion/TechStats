import hashlib
import json
from functools import wraps
from typing import Any, Optional

from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache as fastapi_cache
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

    async def set(self, key: str, value: Any, ttl: Optional[int] = 300) -> bool:
        if not self.redis_client:
            return False
        try:
            payload = json.dumps(value, ensure_ascii=False)
            if ttl is None:
                await self.redis_client.set(key, payload)
            else:
                await self.redis_client.set(key, payload, ex=ttl)
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


def _fastapi_cache_key_builder(func, namespace: str = "", *, request: Request = None, **kwargs) -> str:
    if request is not None:
        prefix = namespace or "gateway"
        return _make_cache_key(request, prefix=prefix)

    payload = {"func": f"{func.__module__}.{func.__name__}", "kwargs": kwargs}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    prefix = namespace or "gateway"
    return f"{prefix}:{digest}"


def _resolve_request_from_call(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Request | None:
    candidate = kwargs.get("request")
    if isinstance(candidate, Request):
        return candidate

    for item in args:
        if isinstance(item, Request):
            return item
    return None


def _fastapi_cache_ready() -> bool:
    return getattr(FastAPICache, "_backend", None) is not None and getattr(FastAPICache, "_prefix", None) is not None


def cache_response(ttl: int = 300):
    native_decorator = fastapi_cache(expire=ttl, key_builder=_fastapi_cache_key_builder, namespace="gateway")

    def decorator(func):
        cached_func = native_decorator(func)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            if _fastapi_cache_ready():
                return await cached_func(*args, **kwargs)

            request = _resolve_request_from_call(args, kwargs)
            if request is None:
                return await func(*args, **kwargs)

            cache_key = _make_cache_key(request)
            cached_payload = await cache_manager.get(cache_key)
            if cached_payload is not None:
                return cached_payload

            result = await func(*args, **kwargs)
            await cache_manager.set(cache_key, result, ttl=ttl)
            return result

        return wrapper

    return decorator
