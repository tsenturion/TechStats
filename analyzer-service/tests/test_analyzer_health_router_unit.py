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


class DummyVacancyClient:
    def __init__(self, status_code=200):
        self.status_code = status_code

    async def get(self, _path):
        return DummyResponse(self.status_code)


class DummyPatternsLoader:
    def __init__(self, count=2):
        self.count = count

    def get_all_patterns(self):
        return {str(index): {"name": f"Tech {index}"} for index in range(self.count)}


class DummyRedis:
    async def ping(self):
        return True


def _app(vacancy_status=200, patterns_count=2):
    app = FastAPI()
    app.include_router(health_router, prefix="/api/v1")
    app.state.vacancy_client = DummyVacancyClient(vacancy_status)
    app.state.patterns_loader = DummyPatternsLoader(patterns_count)
    return app


def test_health_router_reports_healthy(monkeypatch):
    async def fake_cache_stats():
        return {"connected": True}

    monkeypatch.setattr(health_module.cache_manager, "get_cache_stats", fake_cache_stats)
    monkeypatch.setattr(health_module.cache_manager, "redis_client", DummyRedis())

    client = TestClient(_app(vacancy_status=200, patterns_count=3))
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"
    assert payload["checks"]["vacancy_service"]["status"] == "healthy"
    assert payload["checks"]["nlp_tools"]["patterns_loaded"] == 3


def test_health_router_reports_degraded_when_vacancy_unavailable(monkeypatch):
    async def fake_cache_stats():
        return {"connected": True}

    monkeypatch.setattr(health_module.cache_manager, "get_cache_stats", fake_cache_stats)
    monkeypatch.setattr(health_module.cache_manager, "redis_client", DummyRedis())

    client = TestClient(_app(vacancy_status=503, patterns_count=1))
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["vacancy_service"]["status"] == "degraded"


def test_health_router_reports_unhealthy_when_patterns_fail(monkeypatch):
    async def fake_cache_stats():
        return {"connected": False}

    class BrokenPatternsLoader:
        def get_all_patterns(self):
            raise RuntimeError("patterns error")

    monkeypatch.setattr(health_module.cache_manager, "get_cache_stats", fake_cache_stats)
    monkeypatch.setattr(health_module.cache_manager, "redis_client", None)

    app = FastAPI()
    app.include_router(health_router, prefix="/api/v1")
    app.state.vacancy_client = DummyVacancyClient(200)
    app.state.patterns_loader = BrokenPatternsLoader()
    client = TestClient(app)

    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "unhealthy"
    assert payload["checks"]["nlp_tools"]["status"] == "unhealthy"
