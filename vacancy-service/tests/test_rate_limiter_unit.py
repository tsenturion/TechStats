import time

import pytest

from app.rate_limiter import RateLimiter


class DummyPipeline:
    def __init__(self):
        self.commands = []

    def incr(self, key):
        self.commands.append(("incr", key))
        return self

    def expire(self, key, ttl):
        self.commands.append(("expire", key, ttl))
        return self

    async def execute(self):
        return [1, True]


class DummyRedis:
    def __init__(self, daily_value=None):
        self.daily_value = daily_value
        self.pipeline_instance = DummyPipeline()

    async def get(self, _key):
        return self.daily_value

    def pipeline(self):
        return self.pipeline_instance


def _prepare_limiter() -> RateLimiter:
    limiter = RateLimiter()
    now = time.time()
    limiter.local_limits = {
        "second": {"count": 0, "timestamp": now},
        "minute": {"count": 0, "timestamp": now},
        "hour": {"count": 0, "timestamp": now},
    }
    return limiter


@pytest.mark.asyncio
async def test_can_make_request_allows_within_runtime_limits(monkeypatch):
    limiter = _prepare_limiter()

    async def fake_runtime_settings():
        return {"hh_rate_limit_per_second": 3, "hh_rate_limit_per_day": 100}

    monkeypatch.setattr(limiter, "_load_runtime_settings", fake_runtime_settings)

    allowed = await limiter.can_make_request()
    assert allowed is True
    assert limiter.local_limits["second"]["count"] == 1


@pytest.mark.asyncio
async def test_can_make_request_blocks_when_per_second_exceeded(monkeypatch):
    limiter = _prepare_limiter()
    limiter.local_limits["second"]["count"] = 3

    async def fake_runtime_settings():
        return {"hh_rate_limit_per_second": 3, "hh_rate_limit_per_day": 100}

    monkeypatch.setattr(limiter, "_load_runtime_settings", fake_runtime_settings)

    allowed = await limiter.can_make_request()
    assert allowed is False


@pytest.mark.asyncio
async def test_can_make_request_blocks_when_daily_limit_exceeded(monkeypatch):
    limiter = _prepare_limiter()
    limiter.redis_client = DummyRedis(daily_value="10")

    async def fake_runtime_settings():
        return {"hh_rate_limit_per_second": 10, "hh_rate_limit_per_day": 10}

    monkeypatch.setattr(limiter, "_load_runtime_settings", fake_runtime_settings)

    allowed = await limiter.can_make_request()
    assert allowed is False


@pytest.mark.asyncio
async def test_increment_daily_counter_uses_pipeline():
    limiter = _prepare_limiter()
    limiter.redis_client = DummyRedis(daily_value=None)

    await limiter.increment_daily_counter()

    assert limiter.redis_client.pipeline_instance.commands
    assert limiter.redis_client.pipeline_instance.commands[0][0] == "incr"
    assert limiter.redis_client.pipeline_instance.commands[1][0] == "expire"


@pytest.mark.asyncio
async def test_get_rate_limit_stats_returns_runtime_limits(monkeypatch):
    limiter = _prepare_limiter()
    limiter.redis_client = DummyRedis(daily_value="7")
    limiter.local_limits["second"]["count"] = 2

    async def fake_runtime_settings():
        return {"hh_rate_limit_per_second": 9, "hh_rate_limit_per_day": 123}

    monkeypatch.setattr(limiter, "_load_runtime_settings", fake_runtime_settings)

    stats = await limiter.get_rate_limit_stats()

    assert stats["limits"]["per_second"] == 9
    assert stats["limits"]["per_day"] == 123
    assert stats["daily"] == 7
    assert stats["local"]["second"] == 2
