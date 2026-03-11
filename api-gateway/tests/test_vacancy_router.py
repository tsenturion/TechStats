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
if "app" in sys.modules and not hasattr(sys.modules["app"], "__path__"):
    sys.modules.pop("app", None)

from app.middleware import AuthenticationMiddleware
import app.routers.vacancy as vacancy_module
from app.routers.vacancy import router as vacancy_router
from app.security import UserRole, create_access_token


def create_test_app():
    app = FastAPI()
    app.add_middleware(AuthenticationMiddleware)
    app.include_router(vacancy_router, prefix="/api/v1")
    return app


def _auth_header():
    token = create_access_token(username="tester", role=UserRole.user, expires_minutes=30)
    return {"Authorization": f"Bearer {token}"}


def test_vacancy_search_requires_authentication():
    client = TestClient(create_test_app())
    response = client.get("/api/v1/vacancies/search", params={"query": "python"})
    assert response.status_code == 401


def test_vacancy_search_applies_runtime_limits(monkeypatch):
    captured = {}

    class DummyResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"items": [{"id": "1", "name": "Vacancy"}], "found": 1, "pages": 1, "source": "hh_api"}

    class DummyAsyncClient:
        def __init__(self, *args, **kwargs):
            captured["timeout"] = kwargs.get("timeout")

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None):
            captured["url"] = url
            captured["params"] = params
            return DummyResponse()

    async def fake_runtime_settings():
        return {
            "search_per_page_hard_limit": 3,
            "search_max_pages_hard_limit": 2,
            "gateway_vacancy_request_timeout_sec": 5,
            "gateway_vacancy_request_delay_ms": 0,
        }

    monkeypatch.setattr(vacancy_module.httpx, "AsyncClient", DummyAsyncClient)
    monkeypatch.setattr(vacancy_module, "get_runtime_settings_effective", fake_runtime_settings)

    client = TestClient(create_test_app())
    response = client.get(
        "/api/v1/vacancies/search",
        params={"query": "python", "page": 99, "per_page": 100, "area": 113, "exact_search": True},
        headers=_auth_header(),
    )

    assert response.status_code == 200
    assert captured["params"]["per_page"] == 3
    assert captured["params"]["page"] == 1
    assert captured["params"]["area"] == 113
    assert captured["timeout"] == 5


def test_get_vacancy_maps_404(monkeypatch):
    class NotFoundClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None):
            request = httpx.Request("GET", url)
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

    async def fake_runtime_settings():
        return {"gateway_vacancy_request_timeout_sec": 5, "gateway_vacancy_request_delay_ms": 0}

    async def fake_cached(_cache_key):
        return None

    monkeypatch.setattr(vacancy_module.httpx, "AsyncClient", NotFoundClient)
    monkeypatch.setattr(vacancy_module, "get_runtime_settings_effective", fake_runtime_settings)
    monkeypatch.setattr(vacancy_module, "get_cached_response", fake_cached)

    client = TestClient(create_test_app())
    response = client.get("/api/v1/vacancies/404", headers=_auth_header())
    assert response.status_code == 404


def test_get_vacancies_batch_merges_cache_and_remote(monkeypatch):
    captured = {}

    class DummyResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"vacancies": [{"id": "2", "name": "From API"}]}

    class DummyAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json=None, **kwargs):
            captured["url"] = url
            captured["json"] = json
            return DummyResponse()

    async def fake_runtime_settings():
        return {
            "vacancy_batch_max_ids": 10,
            "gateway_vacancy_request_timeout_sec": 5,
            "gateway_vacancy_request_delay_ms": 0,
        }

    async def fake_cached_response(cache_key):
        if cache_key.endswith(":1"):
            return {"id": "1", "name": "Cached"}
        return None

    async def fake_cache_set(_key, _value, ttl=3600):
        return True

    monkeypatch.setattr(vacancy_module.httpx, "AsyncClient", DummyAsyncClient)
    monkeypatch.setattr(vacancy_module, "get_runtime_settings_effective", fake_runtime_settings)
    monkeypatch.setattr(vacancy_module, "get_cached_response", fake_cached_response)
    monkeypatch.setattr(vacancy_module.cache_manager, "set", fake_cache_set)

    client = TestClient(create_test_app())
    response = client.get(
        "/api/v1/vacancies/batch",
        params=[("vacancy_ids", "1"), ("vacancy_ids", "2")],
        headers=_auth_header(),
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["vacancies"]) == 2
    assert captured["json"]["vacancy_ids"] == ["2"]
