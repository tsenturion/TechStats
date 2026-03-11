import time

import pytest

from app.analysis_store import AnalysisStore


@pytest.mark.asyncio
async def test_add_record_trims_to_5000_items():
    store = AnalysisStore()
    for index in range(5005):
        await store.add_record({"analysis_timestamp": float(index), "total_vacancies": 1, "tech_vacancies": 0})

    records = await store.list_records()
    assert len(records) == 5000
    assert records[0]["analysis_timestamp"] == 5.0


@pytest.mark.asyncio
async def test_list_records_since_timestamp_filters():
    store = AnalysisStore()
    now = time.time()
    await store.add_record({"analysis_timestamp": now - 100, "total_vacancies": 10, "tech_vacancies": 2})
    await store.add_record({"analysis_timestamp": now - 10, "total_vacancies": 5, "tech_vacancies": 1})

    recent = await store.list_records(since_ts=now - 60)
    assert len(recent) == 1
    assert recent[0]["total_vacancies"] == 5


@pytest.mark.asyncio
async def test_summary_aggregates_core_metrics():
    store = AnalysisStore()
    now = time.time()
    await store.add_record(
        {
            "analysis_timestamp": now - 100,
            "total_vacancies": 20,
            "tech_vacancies": 4,
            "request_stats": {"processing_time": 2.0, "cache_hit_rate": 50},
        }
    )
    await store.add_record(
        {
            "analysis_timestamp": now - 50,
            "total_vacancies": 10,
            "tech_vacancies": 1,
            "request_stats": {"processing_time": 1.0, "cache_hit_rate": 100},
        }
    )

    summary = await store.summary(hours=1)
    assert summary["total_analyses"] == 2
    assert summary["total_vacancies_processed"] == 30
    assert summary["total_technologies_found"] == 5
    assert summary["avg_processing_time_seconds"] == 1.5
    assert summary["cache_hit_rate"] == 75.0
