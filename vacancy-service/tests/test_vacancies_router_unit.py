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

import app.routers.vacancies as vacancies_module
from app.routers.vacancies import (
    get_hh_client,
    get_rate_limiter,
    router as vacancies_router,
)


class FakeRateLimiter:
    def __init__(self, can_request=True):
        self.can_request = can_request
        self.incremented = 0

    async def can_make_request(self):
        return self.can_request

    async def increment_daily_counter(self):
        self.incremented += 1

    async def get_rate_limit_stats(self):
        return {"local": {"second": 1}, "limits": {"per_second": 7}}


class FakeHHClient:
    def __init__(self):
        self.search_calls = []
        self.vacancy_calls = []
        self.batch_calls = []

    async def search_vacancies(self, **kwargs):
        self.search_calls.append(kwargs)
        return {"items": [{"id": "1", "name": "Python Developer"}], "found": 1, "pages": 1}

    async def get_vacancy(self, vacancy_id):
        self.vacancy_calls.append(vacancy_id)
        return {"id": vacancy_id, "name": "Vacancy"}

    async def get_vacancies_batch(self, ids):
        self.batch_calls.append(list(ids))
        return [{"id": value, "name": f"Vacancy {value}"} for value in ids]

    async def get_areas(self):
        return [{"id": "113", "name": "Russia"}]

    async def get_metro(self, city_id):
        return [{"id": city_id, "name": "Station"}]

    async def get_industries(self):
        return [{"id": "1", "name": "IT"}]

    async def get_professional_roles(self):
        return [{"id": "dev", "name": "Developer"}]


def _build_app(hh_client=None, rate_limiter=None):
    app = FastAPI()
    app.include_router(vacancies_router, prefix="/api/v1")
    hh = hh_client or FakeHHClient()
    limiter = rate_limiter or FakeRateLimiter()
    app.dependency_overrides[get_hh_client] = lambda: hh
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    return app, hh, limiter


def test_search_uses_cache_hit(monkeypatch):
    captured = {}

    async def fake_cache_search(query, area, page, per_page, search_field):
        captured["query"] = query
        return {"items": [{"id": "cached"}], "found": 1, "pages": 1}

    monkeypatch.setattr(vacancies_module.cache_manager, "search_vacancies_cache", fake_cache_search)

    app, _, _ = _build_app()
    client = TestClient(app)
    response = client.get("/api/v1/search", params={"query": "Python"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cache"
    assert payload["cached"] is True
    assert captured["query"] == '"Python"'


def test_search_from_hh_api_and_cache_store(monkeypatch):
    cache_calls = {}

    async def fake_cache_search(*args, **kwargs):
        return None

    async def fake_cache_store(query, area, page, per_page, search_field, results):
        cache_calls["query"] = query
        cache_calls["results"] = results
        return True

    monkeypatch.setattr(vacancies_module.cache_manager, "search_vacancies_cache", fake_cache_search)
    monkeypatch.setattr(vacancies_module.cache_manager, "cache_search_results", fake_cache_store)

    app, hh_client, limiter = _build_app()
    client = TestClient(app)
    response = client.get(
        "/api/v1/search",
        params={"query": "Python", "page": 0, "per_page": 10, "exact_search": True},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "hh_api"
    assert payload["cached"] is False
    assert hh_client.search_calls
    assert limiter.incremented == 1
    assert cache_calls["query"] == '"Python"'


def test_search_non_exact_name_uses_default_hh_field_and_title_includes_filter(monkeypatch):
    async def fake_cache_search(*args, **kwargs):
        return None

    monkeypatch.setattr(vacancies_module.cache_manager, "search_vacancies_cache", fake_cache_search)

    class FilterAwareHHClient(FakeHHClient):
        async def search_vacancies(self, **kwargs):
            self.search_calls.append(kwargs)
            return {
                "items": [
                    {"id": "1", "name": "Инженер-химия"},
                    {"id": "2", "name": "Менеджер нефтехимия"},
                    {"id": "3", "name": "Бухгалтер"},
                ],
                "found": 3,
                "pages": 1,
            }

    app, hh_client, _ = _build_app(hh_client=FilterAwareHHClient())
    client = TestClient(app)

    response = client.get(
        "/api/v1/search",
        params={"query": "химия", "exact_search": False},
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["search_params"]["title_contains_mode"] is True
    assert payload["search_params"]["effective_search_field"] == "name"
    assert hh_client.search_calls
    assert hh_client.search_calls[0]["search_field"] == "name"
    names = [item["name"] for item in payload["items"]]
    assert names == ["Инженер-химия", "Менеджер нефтехимия"]


def test_search_non_exact_name_multiword_does_not_overfilter_results(monkeypatch):
    async def fake_cache_search(*args, **kwargs):
        return None

    monkeypatch.setattr(vacancies_module.cache_manager, "search_vacancies_cache", fake_cache_search)

    class TokenAwareHHClient(FakeHHClient):
        async def search_vacancies(self, **kwargs):
            self.search_calls.append(kwargs)
            return {
                "items": [
                    {"id": "1", "name": "Senior QA Python Engineer"},
                    {"id": "2", "name": "Python Developer"},
                    {"id": "3", "name": "QA engineer (Python)"},
                    {"id": "4", "name": "QA Java Engineer"},
                ],
                "found": 4,
                "pages": 1,
            }

    app, _, _ = _build_app(hh_client=TokenAwareHHClient())
    client = TestClient(app)
    response = client.get(
        "/api/v1/search",
        params={"query": "Python qa", "exact_search": False},
    )
    assert response.status_code == 200
    payload = response.json()
    names = [item["name"] for item in payload["items"]]
    assert names == [
        "Senior QA Python Engineer",
        "Python Developer",
        "QA engineer (Python)",
        "QA Java Engineer",
    ]


def test_search_non_exact_name_uses_dedicated_cache_field(monkeypatch):
    captured = {}

    async def fake_cache_search(query, area, page, per_page, search_field):
        captured["query"] = query
        captured["search_field"] = search_field
        return {"items": [], "found": 0, "pages": 0}

    monkeypatch.setattr(vacancies_module.cache_manager, "search_vacancies_cache", fake_cache_search)

    app, _, _ = _build_app()
    client = TestClient(app)
    response = client.get("/api/v1/search", params={"query": "химия", "exact_search": False})

    assert response.status_code == 200
    assert captured["query"] == "химия"
    assert captured["search_field"] == "default_title_contains"


def test_search_rate_limited(monkeypatch):
    async def fake_cache_search(*args, **kwargs):
        return None

    monkeypatch.setattr(vacancies_module.cache_manager, "search_vacancies_cache", fake_cache_search)
    app, _, _ = _build_app(rate_limiter=FakeRateLimiter(can_request=False))
    client = TestClient(app)
    response = client.get("/api/v1/search", params={"query": "Python"})
    assert response.status_code == 429


def test_get_vacancy_cache_hit(monkeypatch):
    async def fake_get_vacancy_cache(vacancy_id):
        assert vacancy_id == "123"
        return {"id": "123", "name": "Cached vacancy"}

    monkeypatch.setattr(vacancies_module.cache_manager, "get_vacancy_cache", fake_get_vacancy_cache)
    app, _, _ = _build_app()
    client = TestClient(app)
    response = client.get("/api/v1/vacancies/123")
    assert response.status_code == 200
    assert response.json()["source"] == "cache"


def test_get_vacancy_maps_hh_404(monkeypatch):
    async def fake_get_vacancy_cache(_vacancy_id):
        return None

    class NotFoundHHClient(FakeHHClient):
        async def get_vacancy(self, vacancy_id):
            request = httpx.Request("GET", f"https://api.hh.ru/vacancies/{vacancy_id}")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

    monkeypatch.setattr(vacancies_module.cache_manager, "get_vacancy_cache", fake_get_vacancy_cache)
    app, _, _ = _build_app(hh_client=NotFoundHHClient())
    client = TestClient(app)
    response = client.get("/api/v1/vacancies/404")
    assert response.status_code == 404


def test_batch_returns_cache_only(monkeypatch):
    async def fake_batch_cache(vacancy_ids):
        return {vacancy_id: {"id": vacancy_id, "name": f"Cached {vacancy_id}"} for vacancy_id in vacancy_ids}

    monkeypatch.setattr(vacancies_module.cache_manager, "get_vacancies_batch_cache", fake_batch_cache)
    app, hh_client, _ = _build_app()
    client = TestClient(app)
    response = client.post("/api/v1/vacancies/batch", json={"vacancy_ids": ["1", "2"]})
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cache"
    assert len(payload["vacancies"]) == 2
    assert hh_client.batch_calls == []


def test_batch_fetches_missing_and_caches(monkeypatch):
    cached = {"1": {"id": "1", "name": "Cached 1"}, "2": None}
    captured = {}

    async def fake_batch_cache(_vacancy_ids):
        return cached

    async def fake_cache_batch(vacancies):
        captured["vacancies"] = vacancies
        return True

    monkeypatch.setattr(vacancies_module.cache_manager, "get_vacancies_batch_cache", fake_batch_cache)
    monkeypatch.setattr(vacancies_module.cache_manager, "cache_vacancies_batch", fake_cache_batch)

    app, hh_client, limiter = _build_app()
    client = TestClient(app)
    response = client.post("/api/v1/vacancies/batch", json={"vacancy_ids": ["1", "2"]})
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "mixed"
    assert payload["cache_stats"]["hits"] == 1
    assert payload["cache_stats"]["misses"] == 1
    assert hh_client.batch_calls == [["2"]]
    assert limiter.incremented == 1
    assert captured["vacancies"][0]["id"] == "2"


def test_reference_endpoints(monkeypatch):
    app, _, _ = _build_app()
    client = TestClient(app)

    areas = client.get("/api/v1/areas")
    metro = client.get("/api/v1/metro/1")
    industries = client.get("/api/v1/industries")
    roles = client.get("/api/v1/professional-roles")
    rate_stats = client.get("/api/v1/rate-limit/stats")

    assert areas.status_code == 200
    assert metro.status_code == 200
    assert industries.status_code == 200
    assert roles.status_code == 200
    assert rate_stats.status_code == 200
