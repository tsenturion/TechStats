from pathlib import Path
import json
import sys

import pytest
from starlette.requests import Request

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.cache import CacheManager, _make_cache_key, cache_manager, cache_response


class DummyRedis:
    def __init__(self):
        self.storage = {}
        self.deleted = []
        self.info_payload = {
            "redis_version": "7.2.0",
            "used_memory_human": "1M",
            "keyspace_hits": 7,
            "keyspace_misses": 3,
        }

    async def ping(self):
        return True

    async def get(self, key):
        return self.storage.get(key)

    async def set(self, key, value, ex=None):
        self.storage[key] = value
        return True

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.storage:
                removed += 1
                del self.storage[key]
        self.deleted.extend(keys)
        return removed

    async def scan_iter(self, match="*"):
        prefix = match.replace("*", "")
        for key in list(self.storage.keys()):
            if not prefix or str(key).startswith(prefix):
                yield key

    async def info(self):
        return self.info_payload

    async def close(self):
        return True


def _request(path="/api/v1/demo", query_string="a=1&b=2"):
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query_string.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_cache_manager_get_set_delete_cycle():
    manager = CacheManager()
    manager.redis_client = DummyRedis()

    ok = await manager.set("k1", {"x": 1}, ttl=30)
    assert ok is True
    assert await manager.get("k1") == {"x": 1}

    deleted = await manager.delete("k1")
    assert deleted is True
    assert await manager.get("k1") is None


@pytest.mark.asyncio
async def test_cache_manager_handles_invalid_json_gracefully():
    manager = CacheManager()
    redis_client = DummyRedis()
    redis_client.storage["broken"] = "{bad json"
    manager.redis_client = redis_client

    assert await manager.get("broken") is None


@pytest.mark.asyncio
async def test_cache_manager_clear_and_stats():
    manager = CacheManager()
    redis_client = DummyRedis()
    redis_client.storage.update({"gateway:1": json.dumps({"a": 1}), "gateway:2": json.dumps({"b": 2}), "other:1": "{}"})
    manager.redis_client = redis_client

    cleared = await manager.clear(pattern="gateway:*")
    assert cleared == 2

    stats = await manager.get_stats()
    assert stats["connected"] is True
    assert stats["hits"] == 7
    assert stats["misses"] == 3


def test_make_cache_key_is_stable_for_query_param_order():
    request_a = _request(query_string="b=2&a=1")
    request_b = _request(query_string="a=1&b=2")

    assert _make_cache_key(request_a) == _make_cache_key(request_b)


@pytest.mark.asyncio
async def test_cache_response_decorator_uses_cached_value(monkeypatch):
    call_counter = {"handler": 0}

    async def fake_get(_key):
        return {"from_cache": True}

    async def fake_set(_key, _value, ttl=300):
        return True

    monkeypatch.setattr(cache_manager, "get", fake_get)
    monkeypatch.setattr(cache_manager, "set", fake_set)

    @cache_response(ttl=10)
    async def handler(request):
        call_counter["handler"] += 1
        return {"from_handler": True}

    result = await handler(_request())
    assert result == {"from_cache": True}
    assert call_counter["handler"] == 0


@pytest.mark.asyncio
async def test_cache_response_decorator_stores_fresh_value(monkeypatch):
    stored = {}

    async def fake_get(_key):
        return None

    async def fake_set(key, value, ttl=300):
        stored["key"] = key
        stored["value"] = value
        stored["ttl"] = ttl
        return True

    monkeypatch.setattr(cache_manager, "get", fake_get)
    monkeypatch.setattr(cache_manager, "set", fake_set)

    @cache_response(ttl=25)
    async def handler(request):
        return {"fresh": 1}

    result = await handler(_request(path="/api/v1/fresh"))
    assert result == {"fresh": 1}
    assert stored["ttl"] == 25
    assert stored["value"] == {"fresh": 1}
