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

from app.routers.health import router as health_router


class DummyElapsed:
    @staticmethod
    def total_seconds():
        return 0.02


class DummyResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.elapsed = DummyElapsed()


class DummyRedis:
    async def ping(self):
        return True


class DummyHttpClient:
    def __init__(self, status_code=200):
        self.status_code = status_code

    async def get(self, _path):
        return DummyResponse(self.status_code)


class DummyConnectionManager:
    def active_connections_count(self):
        return 2

    def total_connections_accepted(self):
        return 5

    def total_connections_rejected(self):
        return 1


class DummySessionStore:
    async def get_session_stats(self):
        return {"active": 1, "completed": 2, "failed": 0}


def _app(analyzer_status=200, vacancy_status=200):
    app = FastAPI()
    app.include_router(health_router, prefix="/api/v1")
    app.state.redis_client = DummyRedis()
    app.state.analyzer_client = DummyHttpClient(analyzer_status)
    app.state.vacancy_client = DummyHttpClient(vacancy_status)
    app.state.connection_manager = DummyConnectionManager()
    app.state.session_store = DummySessionStore()
    return app


def test_health_router_healthy_path():
    client = TestClient(_app(200, 200))
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"
    assert payload["checks"]["redis"]["status"] == "healthy"
    assert payload["checks"]["connection_manager"]["active_connections"] == 2


def test_health_router_degraded_when_dependency_non_200():
    client = TestClient(_app(analyzer_status=503, vacancy_status=200))
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["analyzer_service"]["status"] == "degraded"
