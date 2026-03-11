from pathlib import Path
import sys

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import app.routers.websocket as websocket_module
from app.routers.websocket import check_service_health


class DummyElapsed:
    @staticmethod
    def total_seconds():
        return 0.03


class DummyResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.elapsed = DummyElapsed()


class HealthyClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, _url):
        return DummyResponse(200)


class UnhealthyClient(HealthyClient):
    async def get(self, _url):
        raise RuntimeError("connection failed")


@pytest.mark.asyncio
async def test_check_service_health_returns_healthy(monkeypatch):
    monkeypatch.setattr(websocket_module.httpx, "AsyncClient", HealthyClient)
    result = await check_service_health("http://service")
    assert result["status"] == "healthy"
    assert result["status_code"] == 200


@pytest.mark.asyncio
async def test_check_service_health_returns_unavailable(monkeypatch):
    monkeypatch.setattr(websocket_module.httpx, "AsyncClient", UnhealthyClient)
    result = await check_service_health("http://service")
    assert result["status"] == "unavailable"
    assert "error" in result
