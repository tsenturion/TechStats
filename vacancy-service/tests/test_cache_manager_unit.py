from pathlib import Path
import json
import sys

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.cache import CacheManager


class DummyPipeline:
    def __init__(self):
        self.commands = []

    def set(self, key, value, ex=None):
        self.commands.append((key, value, ex))
        return self

    async def execute(self):
        return [True for _ in self.commands]


class DummyRedis:
    def __init__(self):
        self.storage = {}
        self.info_payload = {
            "redis_version": "7.2.0",
            "used_memory_human": "1M",
            "keyspace_hits": 5,
            "keyspace_misses": 2,
        }
        self.pipeline_instance = DummyPipeline()

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
        return removed

    async def mget(self, keys):
        return [self.storage.get(key) for key in keys]

    def pipeline(self):
        return self.pipeline_instance

    async def keys(self, pattern):
        prefix = pattern.replace("*", "")
        return [key for key in self.storage.keys() if str(key).startswith(prefix)]

    async def info(self):
        return self.info_payload

    async def dbsize(self):
        return len(self.storage)

    async def close(self):
        return True


def _manager_with_redis():
    manager = CacheManager()
    manager.redis_client = DummyRedis()
    return manager


def test_generate_key_hashes_long_payload():
    manager = CacheManager()
    long_query = "x" * 500
    key = manager._generate_key("vacancies:search", query=long_query, page=0)
    assert key.startswith("vacancies:search:hash:")


@pytest.mark.asyncio
async def test_cache_search_and_get_vacancy_flow():
    manager = _manager_with_redis()

    search_data = {"items": [{"id": "1"}], "found": 1, "pages": 1}
    ok = await manager.cache_search_results("python", 113, 0, 50, "name", search_data)
    assert ok is True
    cached_search = await manager.search_vacancies_cache("python", 113, 0, 50, "name")
    assert cached_search["found"] == 1

    vacancy_data = {"id": "1", "name": "Python Developer"}
    assert await manager.cache_vacancy("1", vacancy_data) is True
    assert await manager.get_vacancy_cache("1") == vacancy_data


@pytest.mark.asyncio
async def test_batch_cache_get_and_set():
    manager = _manager_with_redis()
    manager.redis_client.storage["vacancy:1"] = json.dumps({"id": "1"})

    batch = await manager.get_vacancies_batch_cache(["1", "2"])
    assert batch["1"]["id"] == "1"
    assert batch["2"] is None

    ok = await manager.cache_vacancies_batch([{"id": "2", "name": "N2"}])
    assert ok is True
    assert manager.redis_client.pipeline_instance.commands


@pytest.mark.asyncio
async def test_cache_stats_and_clear_cache():
    manager = _manager_with_redis()
    manager.redis_client.storage.update(
        {
            "vacancies:search:1": "{}",
            "vacancy:1": "{}",
            "vacancy:2": "{}",
        }
    )

    stats = await manager.get_cache_stats()
    assert stats["total_keys"] == 3
    assert stats["search_keys"] == 1
    assert stats["vacancy_keys"] == 2
    assert stats["hit_rate"] > 0

    cleared = await manager.clear_cache("vacancy:*")
    assert cleared == 2
