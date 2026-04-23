import json
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

import app.routers.websocket as websocket_module
from app.routers.websocket import router as websocket_router
from app.security import UserRole, create_access_token


class DummyBackendWebSocket:
    def __init__(self):
        self.sent_payloads = []
        self.messages = [
            json.dumps({"type": "progress", "stage": "analyzing", "message": "step 1"}),
            json.dumps({"type": "progress", "stage": "completed", "message": "done"}),
        ]

    async def send(self, payload):
        self.sent_payloads.append(json.loads(payload))

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        return json.dumps({"type": "error", "message": "done"})


class DummyConnectContext:
    def __init__(self):
        self.backend = DummyBackendWebSocket()

    async def __aenter__(self):
        return self.backend

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _app():
    app = FastAPI()
    app.include_router(websocket_router, prefix="/api/v1")
    return app


def test_websocket_analyze_requires_authentication():
    client = TestClient(_app())
    with client.websocket_connect("/api/v1/ws/analyze") as ws:
        payload = ws.receive_json()
        assert payload["type"] == "error"
        assert "Authentication required" in payload["message"]


def test_websocket_analyze_proxies_messages(monkeypatch):
    async def fake_runtime_settings():
        return {
            "analysis_max_pages_hard_limit": 5,
            "analysis_per_page_hard_limit": 20,
            "search_default_max_pages": 1,
            "search_default_per_page": 10,
            "search_default_exact": True,
            "search_default_area": 113,
            "analysis_default_use_cache": True,
            "gateway_analyzer_request_delay_ms": 0,
        }

    dummy_context = DummyConnectContext()
    captured_connect_kwargs = {}

    def _fake_connect(*args, **kwargs):
        captured_connect_kwargs.update(kwargs)
        return dummy_context

    monkeypatch.setattr(websocket_module, "get_runtime_settings_effective", fake_runtime_settings)
    monkeypatch.setattr(websocket_module.websockets, "connect", _fake_connect)

    token = create_access_token(username="user", role=UserRole.user, expires_minutes=30)
    client = TestClient(_app())
    with client.websocket_connect(f"/api/v1/ws/analyze?access_token={token}") as ws:
        ws.send_json({"vacancy_title": "Python Developer", "technology": "python"})
        first = ws.receive_json()
        second = ws.receive_json()
        assert first["stage"] == "analyzing"
        assert second["stage"] == "completed"

    assert dummy_context.backend.sent_payloads
    assert dummy_context.backend.sent_payloads[0]["technology"] == "python"
    assert int(captured_connect_kwargs.get("max_size", 0)) > 1_000_000
