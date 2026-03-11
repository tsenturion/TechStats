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

import app.routers.stats as stats_module
from app.routers.stats import router as stats_router


def _app():
    app = FastAPI()
    app.include_router(stats_router, prefix="/api/v1")
    return app


def test_stats_summary_and_filter(monkeypatch):
    async def fake_summary(hours=24):
        return {
            "total_analyses": 2,
            "total_vacancies_processed": 30,
            "total_technologies_found": 6,
            "avg_processing_time_seconds": 1.2,
            "cache_hit_rate": 70.0,
            "records": [
                {
                    "analysis_timestamp": 1_700_000_000,
                    "technology": "python",
                    "vacancy_title": "Python Developer",
                    "total_vacancies": 20,
                    "tech_vacancies": 5,
                    "tech_percentage": 25.0,
                },
                {
                    "analysis_timestamp": 1_700_000_100,
                    "technology": "react",
                    "vacancy_title": "Frontend Engineer",
                    "total_vacancies": 10,
                    "tech_vacancies": 1,
                    "tech_percentage": 10.0,
                },
            ],
        }

    monkeypatch.setattr(stats_module.analysis_store, "summary", fake_summary)
    client = TestClient(_app())
    response = client.get("/api/v1/stats/summary", params={"hours": 24, "technology": "python"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_analyses"] == 2
    assert payload["filtered"]["technology"] == "python"
    assert payload["by_technology"][0]["technology"] in {"python", "react"}


def test_technology_comparison_and_limits(monkeypatch):
    async def fake_get_technology_stats(technology, days=30):
        value = {"python": 20, "react": 10}.get(technology, 1)
        return {
            "technology": technology,
            "total_mentions": value,
            "trend_percentage": float(value),
            "daily_stats": [{"date": "2026-03-01", "mentions": value, "vacancies": value, "percentage": 10.0}],
        }

    monkeypatch.setattr(stats_module, "get_technology_stats", fake_get_technology_stats)
    client = TestClient(_app())

    limited = client.get("/api/v1/stats/comparison", params=[("technologies", str(i)) for i in range(11)])
    assert limited.status_code == 400

    ok = client.get("/api/v1/stats/comparison", params=[("technologies", "python"), ("technologies", "react")])
    assert ok.status_code == 200
    payload = ok.json()
    assert payload["summary"]["most_popular"] == "python"
    assert len(payload["comparison_data"]) == 2


def test_cache_and_performance_stats(monkeypatch):
    async def fake_cache_stats():
        return {"connected": True, "hits": 10}

    async def fake_summary(hours=24):
        return {
            "total_analyses": 4,
            "total_vacancies_processed": 200,
            "total_technologies_found": 40,
            "avg_processing_time_seconds": 0.5,
            "cache_hit_rate": 80.0,
            "records": [],
        }

    monkeypatch.setattr(stats_module.cache_manager, "get_cache_stats", fake_cache_stats)
    monkeypatch.setattr(stats_module.analysis_store, "summary", fake_summary)

    client = TestClient(_app())
    cache_response = client.get("/api/v1/stats/cache")
    assert cache_response.status_code == 200
    assert cache_response.json()["connected"] is True

    perf_response = client.get("/api/v1/stats/performance", params={"hours": 24})
    assert perf_response.status_code == 200
    assert perf_response.json()["requests"]["total"] == 4
    assert perf_response.json()["cache_performance"]["hit_rate"] == 80.0
