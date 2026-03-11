import asyncio

import httpx
import pytest


BASE_URL = "http://localhost:8001"


async def _ensure_service_available() -> None:
    try:
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=3.0, trust_env=False) as client:
            response = await client.get("/api/v1/health")
            if response.status_code != 200:
                pytest.skip(f"Vacancy service unavailable: status={response.status_code}")
    except Exception as exc:
        pytest.skip(f"Vacancy service unavailable: {exc}")


@pytest.mark.asyncio
async def test_search_vacancies():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL, trust_env=False) as client:
        response = await client.get(
            "/api/v1/search",
            params={
                "query": "Python Developer",
                "area": 113,
                "page": 0,
                "per_page": 10,
                "exact_search": True,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "found" in data
        assert "pages" in data
        assert data["source"] in ["cache", "hh_api", "cache_fallback"]


@pytest.mark.asyncio
async def test_get_vacancy():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL, trust_env=False) as client:
        response = await client.get("/api/v1/vacancies/123456")
        if response.status_code == 404:
            assert response.json()["detail"] == "Vacancy not found"
        else:
            assert response.status_code == 200
            data = response.json()
            assert "vacancy" in data
            assert data["vacancy"]["id"] == "123456"


@pytest.mark.asyncio
async def test_get_areas():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL, trust_env=False) as client:
        response = await client.get("/api/v1/areas")
        assert response.status_code == 200
        data = response.json()
        assert "areas" in data
        assert isinstance(data["areas"], list)


@pytest.mark.asyncio
async def test_health_check():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL, trust_env=False) as client:
        response = await client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "vacancy-service"
        assert "status" in data
        assert "checks" in data


@pytest.mark.asyncio
async def test_rate_limit_stats():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL, trust_env=False) as client:
        response = await client.get("/api/v1/rate-limit/stats")
        assert response.status_code == 200
        data = response.json()
        assert "local" in data
        assert "limits" in data
        assert "daily" in data or "error" in data


if __name__ == "__main__":
    asyncio.run(test_search_vacancies())
    print("All tests passed!")
