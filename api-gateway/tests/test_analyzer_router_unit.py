from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.middleware import AuthenticationMiddleware
import app.routers.analyzer as analyzer_module
from app.routers.analyzer import router as analyzer_router
from app.security import UserRole, create_access_token


class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = "dummy-text"

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "http://analyzer")
            response = httpx.Response(self.status_code, json=self._payload, request=request)
            raise httpx.HTTPStatusError("boom", request=request, response=response)
        return None

    def json(self):
        return self._payload


class DummyAsyncClient:
    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.get("timeout")
        self.last_post = None
        self.last_get = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, params=None, timeout=None):
        self.last_post = {"url": url, "json": json, "params": params, "timeout": timeout}
        return DummyResponse(200, {"ok": True, "echo": json})

    async def get(self, url):
        self.last_get = {"url": url}
        if "/analysis/missing/results" in url:
            return DummyResponse(404, {"detail": "missing"})
        return DummyResponse(200, {"analysis_id": "a1", "status": "completed"})


def create_test_app():
    app = FastAPI()
    app.add_middleware(AuthenticationMiddleware)
    app.include_router(analyzer_router, prefix="/api/v1")
    return app


def _auth_header():
    token = create_access_token(username="tester", role=UserRole.user, expires_minutes=30)
    return {"Authorization": f"Bearer {token}"}


def test_analyze_requires_required_fields():
    client = TestClient(create_test_app())
    response = client.post("/api/v1/analyze", json={"vacancy_title": "Python"}, headers=_auth_header())
    assert response.status_code == 400
    assert "Missing required field: technology" in response.json()["detail"]


def test_analyze_success_applies_runtime_limits(monkeypatch):
    async def fake_runtime_settings():
        return {
            "analysis_max_pages_hard_limit": 2,
            "analysis_per_page_hard_limit": 5,
            "gateway_analyzer_request_timeout_sec": 9,
            "gateway_analyzer_request_delay_ms": 0,
            "search_default_max_pages": 1,
            "search_default_per_page": 3,
            "analysis_default_use_cache": True,
            "search_default_exact": True,
            "search_default_area": 113,
        }

    monkeypatch.setattr(analyzer_module, "get_runtime_settings_effective", fake_runtime_settings)
    monkeypatch.setattr(analyzer_module.httpx, "AsyncClient", DummyAsyncClient)

    client = TestClient(create_test_app())
    response = client.post(
        "/api/v1/analyze",
        json={
            "vacancy_title": "Python Developer",
            "technology": "python",
            "max_pages": 999,
            "per_page": 999,
        },
        headers=_auth_header(),
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["echo"]["max_pages"] == 2
    assert payload["echo"]["per_page"] == 5


def test_analyze_timeout_returns_504(monkeypatch):
    class TimeoutClient(DummyAsyncClient):
        async def post(self, url, json=None, params=None, timeout=None):
            raise httpx.TimeoutException("timeout")

    async def fake_runtime_settings():
        return {"gateway_analyzer_request_timeout_sec": 1, "gateway_analyzer_request_delay_ms": 0}

    monkeypatch.setattr(analyzer_module, "get_runtime_settings_effective", fake_runtime_settings)
    monkeypatch.setattr(analyzer_module.httpx, "AsyncClient", TimeoutClient)

    client = TestClient(create_test_app())
    response = client.post(
        "/api/v1/analyze",
        json={"vacancy_title": "Python Developer", "technology": "python"},
        headers=_auth_header(),
    )
    assert response.status_code == 504


def test_get_analysis_results_from_cache(monkeypatch):
    async def fake_get(_key):
        return {"cached": True}

    async def fake_runtime_settings():
        return {"gateway_analyzer_request_timeout_sec": 10}

    monkeypatch.setattr(analyzer_module.cache_manager, "get", fake_get)
    monkeypatch.setattr(analyzer_module, "get_runtime_settings_effective", fake_runtime_settings)

    client = TestClient(create_test_app())
    response = client.get("/api/v1/analysis/results/abc")
    assert response.status_code == 200
    assert response.json() == {"cached": True}


def test_get_analysis_results_returns_404_on_not_found(monkeypatch):
    async def fake_get(_key):
        return None

    async def fake_set(_key, _value, ttl=300):
        return True

    async def fake_runtime_settings():
        return {"gateway_analyzer_request_timeout_sec": 10}

    class NotFoundClient(DummyAsyncClient):
        async def get(self, url):
            return DummyResponse(404, {"detail": "not found"})

    monkeypatch.setattr(analyzer_module.cache_manager, "get", fake_get)
    monkeypatch.setattr(analyzer_module.cache_manager, "set", fake_set)
    monkeypatch.setattr(analyzer_module, "get_runtime_settings_effective", fake_runtime_settings)
    monkeypatch.setattr(analyzer_module.httpx, "AsyncClient", NotFoundClient)

    client = TestClient(create_test_app())
    response = client.get("/api/v1/analysis/results/missing")
    assert response.status_code == 404
