import asyncio

import httpx
import pytest


BASE_URL = "http://localhost:8002"


async def _ensure_service_available() -> None:
    try:
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=3.0) as client:
            response = await client.get("/api/v1/health")
            if response.status_code != 200:
                pytest.skip(f"Analyzer service unavailable: status={response.status_code}")
    except Exception as exc:
        pytest.skip(f"Analyzer service unavailable: {exc}")


@pytest.mark.asyncio
async def test_analyze_vacancies():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.post(
            "/api/v1/analyze",
            json={
                "vacancy_title": "Python Developer",
                "technology": "Python",
                "exact_search": True,
                "area": 113,
                "max_pages": 1,
                "per_page": 10,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "total_vacancies" in data
        assert "tech_vacancies" in data
        assert "tech_percentage" in data
        assert "vacancies_with_tech" in data
        assert "request_stats" in data


@pytest.mark.asyncio
async def test_get_patterns():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.get("/api/v1/patterns")
        assert response.status_code == 200
        data = response.json()
        assert "total_patterns" in data
        assert "categories" in data
        assert "patterns" in data
        assert isinstance(data["patterns"], list)


@pytest.mark.asyncio
async def test_analyze_text():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.post(
            "/api/v1/analyze/text",
            json={
                "text": "We are looking for a Python developer with Django experience",
                "technology": "Python",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "text_preview" in data
        assert "analysis" in data
        assert "found" in data["analysis"]


@pytest.mark.asyncio
async def test_stats():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.get("/api/v1/stats/summary")
        assert response.status_code == 200
        data = response.json()
        assert "time_range" in data
        assert "total_analyses" in data
        assert "total_vacancies_processed" in data


@pytest.mark.asyncio
async def test_health():
    await _ensure_service_available()
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "analyzer-service"
        assert "status" in data
        assert "checks" in data


if __name__ == "__main__":
    asyncio.run(test_analyze_vacancies())
    asyncio.run(test_get_patterns())
    asyncio.run(test_analyze_text())
    asyncio.run(test_stats())
    asyncio.run(test_health())
    print("All analyzer tests passed!")

