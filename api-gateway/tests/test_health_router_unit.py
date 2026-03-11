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

import app.routers.health as health_module
from app.routers.health import router as health_router


class DummyElapsed:
    @staticmethod
    def total_seconds():
        return 0.01


class DummyResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.elapsed = DummyElapsed()


class DummyAsyncClient:
    def __init__(self, *args, **kwargs):
        self._mapping = {
            "vacancy-service": 200,
            "analyzer-service": 200,
            "cache-service": 503,
            "websocket-service": 200,
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url):
        for service, code in self._mapping.items():
            if service in url:
                return DummyResponse(code)
        raise RuntimeError("unknown service")


def create_test_app():
    app = FastAPI()
    app.include_router(health_router, prefix="/api/v1")
    return app


def test_health_endpoint_returns_basic_payload():
    client = TestClient(create_test_app())
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "api-gateway"
    assert payload["status"] == "healthy"
    assert "timestamp" in payload


def test_services_health_aggregates_downstream_statuses(monkeypatch):
    monkeypatch.setattr(health_module.httpx, "AsyncClient", DummyAsyncClient)
    client = TestClient(create_test_app())
    response = client.get("/api/v1/health/services")
    assert response.status_code == 200
    services = response.json()["services"]
    assert services["vacancy"]["healthy"] is True
    assert services["analyzer"]["healthy"] is True
    assert services["cache"]["healthy"] is False
