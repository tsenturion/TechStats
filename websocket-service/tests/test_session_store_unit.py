import json
import time

import pytest

from app.session_store import SessionStore


class DummyRedis:
    def __init__(self):
        self.kv = {}
        self.ttl_map = {}
        self.sets = {}

    async def ping(self):
        return True

    async def setex(self, key, ttl, value):
        self.kv[key] = value
        self.ttl_map[key] = int(ttl)
        return True

    async def get(self, key):
        return self.kv.get(key)

    async def ttl(self, key):
        return self.ttl_map.get(key, -2)

    async def delete(self, key):
        self.kv.pop(key, None)
        self.ttl_map.pop(key, None)
        return 1

    async def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)
        return 1

    async def srem(self, key, value):
        self.sets.setdefault(key, set()).discard(value)
        return 1

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def scard(self, key):
        return len(self.sets.get(key, set()))

    async def expire(self, key, ttl):
        self.ttl_map[key] = int(ttl)
        return True


@pytest.mark.asyncio
async def test_create_get_and_update_session():
    redis = DummyRedis()
    store = SessionStore(redis)
    await store.initialize()

    session_id = await store.create_session({"vacancy_title": "Python", "technology": "python"}, ttl=100)
    assert session_id.startswith("session_")
    assert session_id in redis.sets["sessions:active"]

    loaded = await store.get_session(session_id)
    assert loaded["id"] == session_id
    assert loaded["status"] == "created"

    updated = await store.update_progress(session_id, progress=40, stage="analyzing", message="step")
    assert updated is True
    reloaded = await store.get_session(session_id)
    assert reloaded["progress"] == 40
    assert reloaded["stage"] == "analyzing"


@pytest.mark.asyncio
async def test_complete_and_fail_session_move_between_sets():
    redis = DummyRedis()
    store = SessionStore(redis)
    session_id = await store.create_session({"vacancy_title": "Python", "technology": "python"})

    completed = await store.complete_session(session_id, {"ok": True})
    assert completed is True
    assert session_id not in redis.sets["sessions:active"]
    assert session_id in redis.sets["sessions:completed"]

    failed_session_id = await store.create_session({"vacancy_title": "Java", "technology": "java"})
    failed = await store.fail_session(failed_session_id, "boom", {"type": "RuntimeError"})
    assert failed is True
    assert failed_session_id not in redis.sets["sessions:active"]
    assert failed_session_id in redis.sets["sessions:failed"]


@pytest.mark.asyncio
async def test_cleanup_expired_sessions_and_search():
    redis = DummyRedis()
    store = SessionStore(redis)

    active = await store.create_session({"vacancy_title": "Python", "technology": "python"})
    done = await store.create_session({"vacancy_title": "React", "technology": "react"})
    await store.complete_session(done, {"ok": True})

    # Expire completed session to validate cleanup.
    redis.ttl_map[f"session:{done}"] = 0
    cleaned = await store.cleanup_expired_sessions()
    assert cleaned == 1

    raw = json.loads(redis.kv[f"session:{active}"])
    raw["progress"] = 60
    raw["stage"] = "analyzing"
    raw["status"] = "created"
    raw["created_at"] = time.time()
    redis.kv[f"session:{active}"] = json.dumps(raw, ensure_ascii=False)

    sessions = await store.search_sessions({"stage": "analyzing", "min_progress": 50}, limit=10)
    assert len(sessions) == 1
    assert sessions[0]["id"] == active
