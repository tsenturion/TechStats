import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.routers.admin import router as admin_router


class DummyWebSocket:
    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)


class DummyConnectionManager:
    def __init__(self):
        self.connection_info = {
            "conn_1": {"client_ip": "127.0.0.1", "subscriptions": set(), "connected_at": 1, "last_activity": 1}
        }
        self.active_connections = {"conn_1": DummyWebSocket()}
        self.disconnected = []

    def get_connection_stats(self):
        return {"active_connections": 1}

    def active_connections_count(self):
        return 1

    def total_connections_accepted(self):
        return 3

    def total_connections_rejected(self):
        return 0

    def get_connection_info(self, connection_id):
        return self.connection_info.get(connection_id)

    def get_message_history(self, connection_id, limit=20):
        return [{"direction": "out", "message": {"id": connection_id}}]

    async def disconnect(self, websocket):
        self.disconnected.append(websocket)
        return True

    async def cleanup_inactive_connections(self):
        return True


class DummySessionStore:
    async def search_sessions(self, query, limit=50):
        return [
            {"id": "s1", "status": "completed", "created_at": 1, "progress": 100.0},
            {"id": "s2", "status": "failed", "created_at": 2, "progress": 100.0},
        ]

    async def get_session_stats(self):
        return {"active": 1, "completed": 5, "failed": 2}

    async def cleanup_expired_sessions(self):
        return 2


class DummyAnalysisProxy:
    async def cleanup_cancelled_analyses(self):
        return 1

    async def get_active_analysis_count(self):
        return 3


def _client():
    app = FastAPI()
    app.include_router(admin_router, prefix="/api/v1/admin")
    app.state.connection_manager = DummyConnectionManager()
    app.state.session_store = DummySessionStore()
    app.state.analysis_proxy = DummyAnalysisProxy()
    return TestClient(app)


def _admin_headers():
    return {"Authorization": "Bearer admin_secret_token"}


def test_admin_connections_endpoints():
    client = _client()

    unauthorized = client.get("/api/v1/admin/connections")
    assert unauthorized.status_code in {401, 403}

    stats = client.get("/api/v1/admin/connections", headers=_admin_headers())
    assert stats.status_code == 200
    assert stats.json()["active_count"] == 1

    detailed = client.get("/api/v1/admin/connections?detailed=true", headers=_admin_headers())
    assert detailed.status_code == 200
    assert detailed.json()["total"] == 1


def test_admin_connection_details_and_disconnect():
    client = _client()

    details = client.get("/api/v1/admin/connections/conn_1?include_history=true", headers=_admin_headers())
    assert details.status_code == 200
    assert "message_history" in details.json()

    missing = client.get("/api/v1/admin/connections/missing", headers=_admin_headers())
    assert missing.status_code == 404

    disconnect = client.delete("/api/v1/admin/connections/conn_1", headers=_admin_headers())
    assert disconnect.status_code == 200
    assert disconnect.json()["success"] is True


def test_admin_sessions_stats_and_cleanup(monkeypatch):
    client = _client()

    sessions = client.get("/api/v1/admin/sessions?status=completed", headers=_admin_headers())
    assert sessions.status_code == 200
    assert sessions.json()["total_found"] >= 1

    stats = client.get("/api/v1/admin/sessions/stats?hours=24", headers=_admin_headers())
    assert stats.status_code == 200
    assert "recent_period" in stats.json()

    cleanup = client.post("/api/v1/admin/system/cleanup", params={"cleanup_type": "all"}, headers=_admin_headers())
    assert cleanup.status_code == 200
    assert cleanup.json()["success"] is True


def test_admin_system_info(monkeypatch):
    class FakeProcess:
        def memory_info(self):
            return SimpleNamespace(rss=4 * 1024 * 1024)

        def cpu_percent(self):
            return 1.0

        def num_threads(self):
            return 2

    fake_psutil = SimpleNamespace(Process=lambda: FakeProcess())
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil)

    client = _client()
    response = client.get("/api/v1/admin/system/info", headers=_admin_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload["analyses"]["active"] == 3
    assert payload["system"]["process"]["memory_usage_mb"] > 0
