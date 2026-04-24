import time

import httpx
import pytest

from config import settings
from app.hh_client import HHClient, HHVacancySearchForbiddenError


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _disable_auth(monkeypatch):
    monkeypatch.setattr(settings, "hh_api_access_token", "")
    monkeypatch.setattr(settings, "hh_api_client_id", "")
    monkeypatch.setattr(settings, "hh_api_client_secret", "")


@pytest.mark.asyncio
async def test_search_vacancies_builds_expected_params(monkeypatch):
    client = HHClient()
    captured = {}

    async def fake_make_request(method, endpoint, params=None, json_data=None):
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = params
        return DummyResponse({"items": [], "found": 0, "pages": 0})

    monkeypatch.setattr(client, "make_request", fake_make_request)

    payload = await client.search_vacancies(query="Python Developer", area=113, page=1, per_page=20, search_field="name")
    assert payload["found"] == 0
    assert captured["method"] == "GET"
    assert captured["endpoint"] == "/vacancies"
    assert captured["params"]["text"] == "Python Developer"
    assert captured["params"]["page"] == 1
    assert captured["params"]["per_page"] == 20
    assert captured["params"]["search_field"] == "name"


@pytest.mark.asyncio
async def test_search_vacancies_omits_search_field_when_not_provided(monkeypatch):
    client = HHClient()
    captured = {}

    async def fake_make_request(method, endpoint, params=None, json_data=None):
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = params
        return DummyResponse({"items": [], "found": 0, "pages": 0})

    monkeypatch.setattr(client, "make_request", fake_make_request)

    payload = await client.search_vacancies(
        query="chemistry",
        area=113,
        page=0,
        per_page=10,
        search_field=None,
    )
    assert payload["pages"] == 0
    assert captured["method"] == "GET"
    assert captured["endpoint"] == "/vacancies"
    assert captured["params"]["text"] == "chemistry"
    assert "search_field" not in captured["params"]


@pytest.mark.asyncio
async def test_search_vacancies_passes_date_range_filters(monkeypatch):
    client = HHClient()
    captured = {}

    async def fake_make_request(method, endpoint, params=None, json_data=None):
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = params
        return DummyResponse({"items": [], "found": 0, "pages": 0})

    monkeypatch.setattr(client, "make_request", fake_make_request)

    await client.search_vacancies(
        query="Python",
        area=113,
        page=0,
        per_page=50,
        search_field="name",
        date_from="2026-03-01T00:00:00Z",
        date_to="2026-03-10T23:59:59Z",
    )

    assert captured["method"] == "GET"
    assert captured["endpoint"] == "/vacancies"
    assert captured["params"]["date_from"] == "2026-03-01T00:00:00Z"
    assert captured["params"]["date_to"] == "2026-03-10T23:59:59Z"


@pytest.mark.asyncio
async def test_make_request_maps_vacancies_403_to_domain_error(monkeypatch):
    client = HHClient()
    _disable_auth(monkeypatch)

    async def fake_rate_limit():
        return None

    class FakeHTTPClient:
        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: A002
            request = httpx.Request(method, f"https://api.hh.ru{url}", params=params)
            return httpx.Response(
                403,
                request=request,
                headers={"server": "ddos-guard", "x-request-id": "req-123"},
                json={"errors": [{"type": "forbidden"}]},
            )

    monkeypatch.setattr(client, "_rate_limit", fake_rate_limit)
    client.client = FakeHTTPClient()

    with pytest.raises(HHVacancySearchForbiddenError):
        await client.make_request("GET", "/vacancies", params={"text": "devops"})


@pytest.mark.asyncio
async def test_make_request_uses_client_credentials_token(monkeypatch):
    client = HHClient()
    captured = {"token_requests": 0, "auth_headers": []}

    monkeypatch.setattr(settings, "hh_api_access_token", "")
    monkeypatch.setattr(settings, "hh_api_client_id", "cid")
    monkeypatch.setattr(settings, "hh_api_client_secret", "secret")

    async def fake_rate_limit():
        return None

    class FakeHTTPClient:
        async def post(self, url, data=None, params=None):
            captured["token_requests"] += 1
            request = httpx.Request("POST", f"https://api.hh.ru{url}", params=params)
            return httpx.Response(
                200,
                request=request,
                json={"access_token": "oauth-token-1", "token_type": "bearer", "expires_in": 3600},
            )

        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: A002
            captured["auth_headers"].append((headers or {}).get("Authorization"))
            request = httpx.Request(method, f"https://api.hh.ru{url}", params=params)
            return httpx.Response(200, request=request, json={"items": []})

    monkeypatch.setattr(client, "_rate_limit", fake_rate_limit)
    client.client = FakeHTTPClient()

    response_1 = await client.make_request("GET", "/areas")
    response_2 = await client.make_request("GET", "/industries")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert captured["token_requests"] == 1
    assert captured["auth_headers"] == ["Bearer oauth-token-1", "Bearer oauth-token-1"]


@pytest.mark.asyncio
async def test_make_request_prefers_static_access_token_over_client_credentials(monkeypatch):
    client = HHClient()
    captured = {"token_requests": 0, "auth_headers": []}

    monkeypatch.setattr(settings, "hh_api_access_token", "static-token")
    monkeypatch.setattr(settings, "hh_api_client_id", "cid")
    monkeypatch.setattr(settings, "hh_api_client_secret", "secret")

    async def fake_rate_limit():
        return None

    class FakeHTTPClient:
        async def post(self, url, data=None, params=None):
            captured["token_requests"] += 1
            request = httpx.Request("POST", f"https://api.hh.ru{url}", params=params)
            return httpx.Response(
                200,
                request=request,
                json={"access_token": "oauth-token-1", "token_type": "bearer", "expires_in": 3600},
            )

        async def request(self, method, url, params=None, json=None, headers=None):  # noqa: A002
            captured["auth_headers"].append((headers or {}).get("Authorization"))
            request = httpx.Request(method, f"https://api.hh.ru{url}", params=params)
            return httpx.Response(200, request=request, json={"items": []})

    monkeypatch.setattr(client, "_rate_limit", fake_rate_limit)
    client.client = FakeHTTPClient()

    response = await client.make_request("GET", "/areas")

    assert response.status_code == 200
    assert captured["token_requests"] == 0
    assert captured["auth_headers"] == ["Bearer static-token"]


@pytest.mark.asyncio
async def test_get_vacancies_batch_ignores_failed_items(monkeypatch):
    client = HHClient()

    async def fake_get_vacancy(vacancy_id):
        if vacancy_id == "2":
            raise RuntimeError("boom")
        return {"id": vacancy_id}

    monkeypatch.setattr(client, "get_vacancy", fake_get_vacancy)
    results = await client.get_vacancies_batch(["1", "2", "3"])
    assert [item["id"] for item in results] == ["1", "3"]


@pytest.mark.asyncio
async def test_reference_lookup_methods_delegate_to_make_request(monkeypatch):
    client = HHClient()
    called = []

    async def fake_make_request(method, endpoint, params=None, json_data=None):
        called.append((method, endpoint))
        return DummyResponse([{"id": endpoint}])

    monkeypatch.setattr(client, "make_request", fake_make_request)

    areas = await client.get_areas()
    metro = await client.get_metro(1)
    industries = await client.get_industries()
    roles = await client.get_professional_roles()

    assert areas and metro and industries and roles
    assert ("GET", "/areas") in called
    assert ("GET", "/metro/1") in called
    assert ("GET", "/industries") in called
    assert ("GET", "/professional_roles") in called


@pytest.mark.asyncio
async def test_rate_limit_waits_when_requests_are_too_frequent(monkeypatch):
    client = HHClient()
    client.last_request_time = time.time()
    slept = {"duration": 0.0}

    async def fake_sleep(duration):
        slept["duration"] = duration

    monkeypatch.setattr("app.hh_client.asyncio.sleep", fake_sleep)
    await client._rate_limit()
    assert slept["duration"] >= 0
