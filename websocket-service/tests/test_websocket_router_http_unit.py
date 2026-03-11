from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.routers.websocket_router import router as websocket_router


class DummySessionStore:
    def __init__(self):
        self.sessions = {"s1": {"id": "s1", "status": "created"}}

    async def get_active_sessions(self, limit=20, offset=0):
        return list(self.sessions.values())[offset : offset + limit]

    async def get_session(self, session_id):
        return self.sessions.get(session_id)

    async def delete_session(self, session_id):
        self.sessions.pop(session_id, None)
        return True


class DummyAnalysisProxy:
    def __init__(self):
        self.cancelled = []

    async def cancel_analysis(self, session_id):
        self.cancelled.append(session_id)
        return True


class DummyConnectionManager:
    def get_connection_stats(self):
        return {"active_connections": 2}

    def active_connections_count(self):
        return 2

    def total_connections_accepted(self):
        return 5

    def total_connections_rejected(self):
        return 1

    async def broadcast_to_topic(self, topic, message):
        return [("conn1", True), ("conn2", False)]

    async def broadcast(self, message, exclude=None):
        return [("conn1", True)]


def _client():
    app = FastAPI()
    app.include_router(websocket_router, prefix="/api/v1")
    app.state.session_store = DummySessionStore()
    app.state.analysis_proxy = DummyAnalysisProxy()
    app.state.connection_manager = DummyConnectionManager()
    return TestClient(app), app


def test_sessions_endpoints_and_cancel_delete():
    client, app = _client()

    list_response = client.get("/api/v1/ws/sessions")
    assert list_response.status_code == 200
    assert list_response.json()["total"] == 1

    one = client.get("/api/v1/ws/sessions/s1")
    assert one.status_code == 200

    missing = client.get("/api/v1/ws/sessions/missing")
    assert missing.status_code == 404

    cancel = client.post("/api/v1/ws/sessions/s1/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["success"] is True

    delete = client.delete("/api/v1/ws/sessions/s1")
    assert delete.status_code == 200


def test_connections_and_broadcast_endpoints():
    client, _ = _client()

    connections = client.get("/api/v1/ws/connections")
    assert connections.status_code == 200
    assert connections.json()["active_count"] == 2

    missing_message = client.post("/api/v1/ws/broadcast", json={})
    assert missing_message.status_code == 400

    topic_broadcast = client.post(
        "/api/v1/ws/broadcast",
        json={"message": {"type": "notice"}, "topic": "notifications"},
    )
    assert topic_broadcast.status_code == 200
    assert topic_broadcast.json()["stats"]["total_recipients"] == 2

    direct_broadcast = client.post("/api/v1/ws/broadcast", json={"message": {"type": "global"}})
    assert direct_broadcast.status_code == 200
    assert direct_broadcast.json()["stats"]["successful"] == 1
