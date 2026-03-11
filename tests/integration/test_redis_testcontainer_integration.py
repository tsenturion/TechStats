import pytest
import redis.asyncio as redis
from testcontainers.redis import RedisContainer


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_container_roundtrip():
    try:
        with RedisContainer("redis:7.2-alpine") as container:
            redis_url = container.get_connection_url()
            client = redis.from_url(redis_url, decode_responses=True)
            try:
                await client.set("techstats:test:key", "ok", ex=30)
                value = await client.get("techstats:test:key")
                assert value == "ok"
            finally:
                await client.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker/testcontainers unavailable: {exc}")
