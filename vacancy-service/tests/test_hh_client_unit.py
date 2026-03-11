import time

import pytest

from app.hh_client import HHClient


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


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
