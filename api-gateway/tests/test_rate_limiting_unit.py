from pathlib import Path
import sys

import pytest
from fastapi import HTTPException
from starlette.requests import Request

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.rate_limiting import RateLimiter, rate_limit, rate_limiter


class DummyPipeline:
    def __init__(self):
        self.commands = []

    def incr(self, key):
        self.commands.append(("incr", key))
        return self

    def expire(self, key, window):
        self.commands.append(("expire", key, window))
        return self

    async def execute(self):
        return [1, True]


class DummyRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.pipeline_instance = DummyPipeline()

    async def get(self, key):
        return self.values.get(key)

    def pipeline(self):
        return self.pipeline_instance

    async def close(self):
        return True


def _request(path="/api/v1/analyze"):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": [],
            "client": ("10.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


@pytest.mark.asyncio
async def test_check_rate_limit_allows_when_redis_unavailable():
    limiter = RateLimiter()
    limiter.redis_client = None
    assert await limiter.check_rate_limit("k", 1, 60) is True


@pytest.mark.asyncio
async def test_check_rate_limit_blocks_when_limit_reached():
    limiter = RateLimiter()
    limiter.redis_client = DummyRedis({"limit-key": "5"})
    assert await limiter.check_rate_limit("limit-key", 5, 60) is False


@pytest.mark.asyncio
async def test_check_rate_limit_increments_counter_when_allowed():
    limiter = RateLimiter()
    redis_client = DummyRedis({"limit-key": "2"})
    limiter.redis_client = redis_client

    assert await limiter.check_rate_limit("limit-key", 5, 60) is True
    assert redis_client.pipeline_instance.commands[0] == ("incr", "limit-key")
    assert redis_client.pipeline_instance.commands[1] == ("expire", "limit-key", 60)


@pytest.mark.asyncio
async def test_is_rate_limited_checks_both_windows(monkeypatch):
    limiter = RateLimiter()
    calls = []

    async def fake_check(key, limit, window):
        calls.append((key, limit, window))
        return len(calls) == 1

    monkeypatch.setattr(limiter, "check_rate_limit", fake_check)
    limited = await limiter.is_rate_limited(_request("/api/v1/vacancies/search"))
    assert limited is True
    assert len(calls) == 2
    assert "rate_limit:minute" in calls[0][0]
    assert "rate_limit:hour" in calls[1][0]


@pytest.mark.asyncio
async def test_get_rate_limit_info_returns_structured_payload():
    limiter = RateLimiter()
    limiter.redis_client = DummyRedis(
        {
            "rate_limit:minute:10.0.0.1:/api/v1/analyze": "3",
            "rate_limit:hour:10.0.0.1": "11",
        }
    )
    limiter.rate_limit_per_minute = 10
    limiter.rate_limit_per_hour = 100

    info = await limiter.get_rate_limit_info(_request("/api/v1/analyze"))
    assert info["minute"]["used"] == 3
    assert info["minute"]["remaining"] == 7
    assert info["hour"]["used"] == 11
    assert info["hour"]["remaining"] == 89


@pytest.mark.asyncio
async def test_rate_limit_dependency_raises_when_limited(monkeypatch):
    async def fake_is_rate_limited(_request):
        return True

    monkeypatch.setattr(rate_limiter, "is_rate_limited", fake_is_rate_limited)

    with pytest.raises(HTTPException) as exc:
        await rate_limit(_request("/api/v1/analyze"))
    assert exc.value.status_code == 429
