
import pytest

from app.cache import CacheManager


class DummyPipeline:
    def __init__(self, storage):
        self.storage = storage
        self.commands = []

    def set(self, key, value, ex=None):
        self.commands.append((key, value, ex))
        self.storage[key] = value
        return self

    async def execute(self):
        return [True for _ in self.commands]


class DummyRedis:
    def __init__(self):
        self.storage = {}
        self.info_payload = {
            "redis_version": "7.2.0",
            "used_memory_human": "2M",
            "keyspace_hits": 10,
            "keyspace_misses": 2,
        }
        self.pipeline_instance = DummyPipeline(self.storage)

    async def get(self, key):
        return self.storage.get(key)

    async def set(self, key, value, ex=None):
        self.storage[key] = value
        return True

    async def mget(self, keys):
        return [self.storage.get(key) for key in keys]

    def pipeline(self):
        return self.pipeline_instance

    async def keys(self, pattern):
        prefix = pattern.replace("*", "")
        return [key for key in self.storage.keys() if str(key).startswith(prefix)]

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.storage:
                removed += 1
                del self.storage[key]
        return removed

    async def info(self):
        return self.info_payload

    async def dbsize(self):
        return len(self.storage)

    async def close(self):
        return True


def _manager():
    manager = CacheManager()
    manager.redis_client = DummyRedis()
    return manager


def test_generate_keys_are_stable():
    manager = CacheManager()
    analysis_key = manager._generate_analysis_key(["2", "1"], "python", True)
    analysis_key_same = manager._generate_analysis_key(["1", "2"], "python", True)
    vacancy_key = manager._generate_vacancy_analysis_key("1", "python", False)

    assert analysis_key == analysis_key_same
    assert vacancy_key == f"vacancy_analysis:{manager.ANALYSIS_CACHE_SCHEMA_VERSION}:1:python:False"


@pytest.mark.asyncio
async def test_basic_get_set_and_analysis_result_cache():
    manager = _manager()
    assert await manager.set("x", {"a": 1}, ttl=30) is True
    assert await manager.get("x") == {"a": 1}

    result = {"total_vacancies": 2, "tech_vacancies": 1}
    assert await manager.cache_analysis_result(["1", "2"], "python", True, result) is True
    cached = await manager.get_analysis_result(["1", "2"], "python", True)
    assert cached == result


@pytest.mark.asyncio
async def test_vacancy_and_batch_analysis_cache_flows():
    manager = _manager()
    vacancy_result = {"vacancy_id": "1", "has_technology": True}
    assert await manager.cache_vacancy_analysis("1", "python", True, vacancy_result) is True
    assert await manager.get_vacancy_analysis("1", "python", True) == vacancy_result

    assert await manager.cache_batch_analysis(
        [
            {"vacancy_id": "1", "has_technology": True},
            {"vacancy_id": "2", "has_technology": False},
        ],
        technology="python",
        exact_search=True,
    )
    batch = await manager.get_batch_analysis(["1", "2", "3"], "python", True)
    assert batch["1"]["vacancy_id"] == "1"
    assert batch["2"]["vacancy_id"] == "2"
    assert batch["3"] is None


@pytest.mark.asyncio
async def test_cache_stats_and_clear():
    manager = _manager()
    manager.redis_client.storage.update(
        {
            "analysis:abc": "{}",
            "vacancy_analysis:1:python:True": "{}",
            "tech_patterns:compiled": "{}",
        }
    )

    stats = await manager.get_cache_stats()
    assert stats["total_keys"] == 3
    assert stats["analysis_keys"] == 1
    assert stats["vacancy_analysis_keys"] == 1
    assert stats["pattern_keys"] == 1
    assert stats["hit_rate"] > 0

    cleared = await manager.clear_analysis_cache("analysis:*")
    assert cleared == 1
