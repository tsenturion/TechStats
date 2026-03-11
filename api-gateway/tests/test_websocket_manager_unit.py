from pathlib import Path
import sys

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.websocket_manager import WebSocketManager


class DummyWebSocket:
    def __init__(self, fail_send=False):
        self.fail_send = fail_send
        self.sent = []
        self.closed = False

    async def send_json(self, payload):
        if self.fail_send:
            raise RuntimeError("send failed")
        self.sent.append(payload)

    async def close(self, code=1000, reason=""):
        self.closed = True
        self.close_args = (code, reason)


@pytest.mark.asyncio
async def test_connect_disconnect_and_snapshot():
    manager = WebSocketManager()
    ws = DummyWebSocket()

    connection_id = await manager.connect(ws)
    assert connection_id.startswith("gw_")
    assert manager.active_connections_count() == 1

    snapshot = manager.snapshot()
    assert snapshot["active_connections"] == 1
    assert "timestamp" in snapshot

    await manager.disconnect(ws)
    assert manager.active_connections_count() == 0


@pytest.mark.asyncio
async def test_broadcast_handles_send_errors():
    manager = WebSocketManager()
    good = DummyWebSocket()
    bad = DummyWebSocket(fail_send=True)
    await manager.connect(good)
    await manager.connect(bad)

    await manager.broadcast({"type": "hello"})
    assert good.sent == [{"type": "hello"}]
    assert bad.sent == []


@pytest.mark.asyncio
async def test_disconnect_cleans_all_connections():
    manager = WebSocketManager()
    sockets = [DummyWebSocket(), DummyWebSocket()]
    for ws in sockets:
        await manager.connect(ws)

    assert manager.active_connections_count() == 2
    for ws in sockets:
        await manager.disconnect(ws)
    assert manager.active_connections_count() == 0
