import time

import pytest
from fastapi import HTTPException

from app.connection_manager import ConnectionManager
from config import settings


class DummyURL:
    def __init__(self, path="/ws"):
        self.path = path


class DummyClient:
    def __init__(self, host="127.0.0.1"):
        self.host = host


class DummyWebSocket:
    def __init__(self, host="127.0.0.1", fail_send=False):
        self.client = DummyClient(host)
        self.headers = {"user-agent": "pytest"}
        self.url = DummyURL("/ws")
        self.fail_send = fail_send
        self.accepted = False
        self.closed = False
        self.sent = []

    async def accept(self):
        self.accepted = True

    async def send_json(self, payload):
        if self.fail_send:
            raise RuntimeError("send error")
        self.sent.append(payload)

    async def close(self, code=1000, reason=""):
        self.closed = True
        self.close_args = (code, reason)


@pytest.mark.asyncio
async def test_connect_subscribe_send_and_disconnect():
    manager = ConnectionManager()
    ws = DummyWebSocket()

    connection_id = await manager.connect(ws)
    assert ws.accepted is True
    assert manager.active_connections_count() == 1

    assert await manager.subscribe(ws, "topic-a") is True
    assert await manager.send_message(ws, {"type": "ping"}) is True
    assert ws.sent == [{"type": "ping"}]

    history = manager.get_message_history(connection_id)
    assert history[-1]["message"]["type"] == "ping"

    assert await manager.unsubscribe(ws, "topic-a") is True
    await manager.disconnect(ws)
    assert manager.active_connections_count() == 0


@pytest.mark.asyncio
async def test_connect_rejects_when_per_ip_limit_exceeded(monkeypatch):
    original_limit = settings.max_connections_per_ip
    settings.max_connections_per_ip = 1
    try:
        manager = ConnectionManager()
        first = DummyWebSocket(host="10.1.1.1")
        second = DummyWebSocket(host="10.1.1.1")

        await manager.connect(first)
        with pytest.raises(HTTPException) as exc:
            await manager.connect(second)
        assert exc.value.status_code == 429
    finally:
        settings.max_connections_per_ip = original_limit


@pytest.mark.asyncio
async def test_send_message_failure_updates_error_stats():
    manager = ConnectionManager()
    ws = DummyWebSocket(fail_send=True)
    await manager.connect(ws)

    success = await manager.send_message(ws, {"type": "x"})
    assert success is False
    assert manager.stats["errors"] == 1


@pytest.mark.asyncio
async def test_cleanup_inactive_connections_closes_stale():
    manager = ConnectionManager()
    ws = DummyWebSocket()
    connection_id = await manager.connect(ws)
    manager.connection_info[connection_id]["last_activity"] = time.time() - (settings.connection_timeout + 10)

    await manager.cleanup_inactive_connections()
    assert ws.closed is True
    assert manager.active_connections_count() == 0
