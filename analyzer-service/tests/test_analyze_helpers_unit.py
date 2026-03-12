import pytest
import httpx
from fastapi import HTTPException

from app.routers.analyze import (
    _format_exception_message,
    _calculate_duplicate_metrics,
    _fetch_detailed_vacancies,
    _fetch_vacancy_ids,
    _is_complete_cached_result,
    analysis_tasks,
    execute_async_analysis,
)


class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = "error"

    def json(self):
        return self._payload


class DummyVacancyClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    async def get(self, path, params=None):
        page = int(params.get("page", 0))
        self.calls.append((path, params))
        return self.pages[page]


class DummyBatchVacancyClient:
    def __init__(self, payload_by_chunk, payload_by_id=None):
        self.payload_by_chunk = payload_by_chunk
        self.payload_by_id = payload_by_id or {}
        self.calls = []

    async def post(self, path, json=None, params=None, timeout=None):
        chunk = tuple(json.get("vacancy_ids", []))
        self.calls.append((path, chunk, params, timeout))
        payload = self.payload_by_chunk[chunk]
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, DummyResponse):
            return payload
        return DummyResponse(200, payload)

    async def get(self, path, params=None, timeout=None):
        vacancy_id = str(path).rstrip("/").split("/")[-1]
        self.calls.append((path, ("single", vacancy_id), params, timeout))
        payload = self.payload_by_id.get(vacancy_id)
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, DummyResponse):
            return payload
        if payload is None:
            return DummyResponse(404, {"detail": "not found"})
        return DummyResponse(200, payload)


@pytest.mark.asyncio
async def test_fetch_vacancy_ids_raises_on_first_page_error():
    client = DummyVacancyClient([DummyResponse(500, {})])

    with pytest.raises(HTTPException) as exc:
        await _fetch_vacancy_ids(
            vacancy_client=client,
            search_query="python",
            area=113,
            per_page=10,
            exact_search=True,
            use_cache=True,
            max_pages=1,
        )

    assert exc.value.status_code == 500


@pytest.mark.asyncio
async def test_fetch_vacancy_ids_collects_and_deduplicates_ids():
    client = DummyVacancyClient(
        [
            DummyResponse(
                200,
                {
                    "items": [{"id": "1"}, {"id": "2"}],
                    "pages": 3,
                },
            ),
            DummyResponse(200, {"items": [{"id": "2"}, {"id": "3"}]}),
            DummyResponse(200, {"items": [{"id": "4"}, {"id": "1"}]}),
        ]
    )

    ids = await _fetch_vacancy_ids(
        vacancy_client=client,
        search_query="python",
        area=113,
        per_page=10,
        exact_search=False,
        use_cache=False,
        max_pages=3,
    )

    assert ids == ["1", "2", "3", "4"]
    assert len(client.calls) == 3


@pytest.mark.asyncio
async def test_fetch_vacancy_ids_respects_max_pages_cap():
    client = DummyVacancyClient(
        [
            DummyResponse(
                200,
                {
                    "items": [{"id": "1"}],
                    "pages": 10,
                },
            ),
            DummyResponse(200, {"items": [{"id": "2"}]}),
            DummyResponse(200, {"items": [{"id": "3"}]}),
            DummyResponse(200, {"items": [{"id": "4"}]}),
        ]
    )

    ids = await _fetch_vacancy_ids(
        vacancy_client=client,
        search_query="python",
        area=113,
        per_page=10,
        exact_search=True,
        use_cache=True,
        max_pages=2,
    )

    assert ids == ["1", "2"]
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_fetch_detailed_vacancies_chunks_and_preserves_order():
    client = DummyBatchVacancyClient(
        {
            ("1", "2"): {"vacancies": [{"id": "2"}, {"id": "1"}], "cache_stats": {"hits": 1, "misses": 1}},
            ("3", "4"): {"vacancies": [{"id": "4"}, {"id": "3"}], "cache_stats": {"hits": 0, "misses": 2}},
        }
    )

    result = await _fetch_detailed_vacancies(
        vacancy_client=client,
        vacancy_ids=["1", "2", "3", "4"],
        use_cache=True,
        batch_chunk_size=2,
    )

    assert [item["id"] for item in result["vacancies"]] == ["1", "2", "3", "4"]
    assert result["cache_stats"] == {"hits": 1, "misses": 3}
    assert result["missing_ids"] == []
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_fetch_detailed_vacancies_skips_failed_chunks(monkeypatch):
    from config import settings

    monkeypatch.setattr(settings, "max_retries", 1)
    client = DummyBatchVacancyClient(
        {
            ("1", "2"): {"vacancies": [{"id": "1"}, {"id": "2"}], "cache_stats": {"hits": 0, "misses": 2}},
            ("3", "4"): DummyResponse(400, {"detail": "upstream failure"}),
        }
    )

    result = await _fetch_detailed_vacancies(
        vacancy_client=client,
        vacancy_ids=["1", "2", "3", "4"],
        use_cache=True,
        batch_chunk_size=2,
    )

    assert [item["id"] for item in result["vacancies"]] == ["1", "2"]
    assert result["missing_ids"] == ["3", "4"]
    assert len(result["failed_chunks"]) == 1
    assert result["failed_chunks"][0]["status_code"] == 400


@pytest.mark.asyncio
async def test_fetch_detailed_vacancies_splits_retryable_failed_chunk_and_recovers():
    timeout_exc = httpx.ReadTimeout("timeout", request=httpx.Request("POST", "http://vacancy-service/api/v1/vacancies/batch"))
    client = DummyBatchVacancyClient(
        {
            ("1", "2", "3", "4"): timeout_exc,
            ("1", "2"): {"vacancies": [{"id": "1"}, {"id": "2"}], "cache_stats": {"hits": 0, "misses": 2}},
            ("3", "4"): DummyResponse(502, {"detail": "temporary upstream failure"}),
            ("3",): {"vacancies": [{"id": "3"}], "cache_stats": {"hits": 0, "misses": 1}},
            ("4",): timeout_exc,
        }
    )

    result = await _fetch_detailed_vacancies(
        vacancy_client=client,
        vacancy_ids=["1", "2", "3", "4"],
        use_cache=True,
        batch_chunk_size=4,
        request_retry_attempts=1,
        chunk_hard_timeout_sec=1,
    )

    assert [item["id"] for item in result["vacancies"]] == ["1", "2", "3"]
    assert result["missing_ids"] == ["4"]
    assert len(result["failed_chunks"]) == 1
    assert result["failed_chunks"][0]["vacancy_ids"] == ["4"]


@pytest.mark.asyncio
async def test_fetch_detailed_vacancies_recovers_missing_ids_from_partial_batch_payload():
    client = DummyBatchVacancyClient(
        {
            ("1", "2", "3", "4"): {"vacancies": [{"id": "1"}, {"id": "2"}], "cache_stats": {"hits": 0, "misses": 4}},
            ("3",): {"vacancies": [{"id": "3"}], "cache_stats": {"hits": 0, "misses": 1}},
            ("4",): {"vacancies": [], "cache_stats": {"hits": 0, "misses": 1}},
        },
        payload_by_id={
            "4": {"vacancy": {"id": "4"}},
        },
    )

    result = await _fetch_detailed_vacancies(
        vacancy_client=client,
        vacancy_ids=["1", "2", "3", "4"],
        use_cache=True,
        batch_chunk_size=4,
        request_retry_attempts=1,
        chunk_hard_timeout_sec=1,
    )

    assert [item["id"] for item in result["vacancies"]] == ["1", "2", "3", "4"]
    assert result["missing_ids"] == []
    assert result["failed_chunks"] == []


@pytest.mark.asyncio
async def test_fetch_detailed_vacancies_emits_progress_with_loaded_counts():
    client = DummyBatchVacancyClient(
        {
            ("1", "2"): {"vacancies": [{"id": "1"}, {"id": "2"}], "cache_stats": {"hits": 0, "misses": 2}},
            ("3", "4"): {"vacancies": [{"id": "3"}, {"id": "4"}], "cache_stats": {"hits": 0, "misses": 2}},
        }
    )
    events = []

    async def _progress(payload):
        events.append(payload)

    result = await _fetch_detailed_vacancies(
        vacancy_client=client,
        vacancy_ids=["1", "2", "3", "4"],
        use_cache=True,
        batch_chunk_size=2,
        progress_callback=_progress,
    )

    assert [item["id"] for item in result["vacancies"]] == ["1", "2", "3", "4"]
    assert len(events) == 2
    assert events[0]["stage"] == "fetching_details"
    assert events[0]["processed"] == 2
    assert events[0]["total"] == 4
    assert events[1]["processed"] == 4
    assert events[1]["total"] == 4
    assert "4/4" in events[1]["message"]


def test_is_complete_cached_result_requires_with_and_without_lists():
    complete = {
        "total_vacancies": 3,
        "requested_vacancies": 3,
        "duplicate_vacancies_count": 1,
        "vacancies_with_tech": [{"id": "1", "text_match_count": 1, "key_skills_match_count": 0}],
        "vacancies_without_tech": [{"id": "2"}, {"id": "3"}],
        "unprocessed_vacancy_ids": [],
    }
    incomplete = {
        "total_vacancies": 3,
        "duplicate_vacancies_count": 1,
        "vacancies_with_tech": [{"id": "1"}],
        "vacancies_without_tech": [{"id": "2"}, {"id": "3"}],
    }

    assert _is_complete_cached_result(complete) is True
    assert _is_complete_cached_result(incomplete) is False


def test_format_exception_message_uses_http_exception_detail():
    exc = HTTPException(status_code=502, detail="Vacancy service error: upstream 502")
    assert _format_exception_message(exc) == "Vacancy service error: upstream 502"


@pytest.mark.asyncio
async def test_execute_async_analysis_preserves_http_exception_detail(monkeypatch):
    task_id = "task-test-http-exception"
    analysis_tasks[task_id] = {
        "status": "pending",
        "created_at": 0,
        "updated_at": 0,
        "progress": 0,
        "stage": "pending",
        "message": "",
        "processed": 0,
        "total": 0,
        "found_with_tech": 0,
        "request": {},
        "result": None,
        "error": None,
    }

    async def _raise_http_exception(*args, **kwargs):
        raise HTTPException(status_code=502, detail="Vacancy service error: upstream 502")

    monkeypatch.setattr("app.routers.analyze._perform_analysis", _raise_http_exception)

    await execute_async_analysis(task_id, {}, pattern_matcher=None, vacancy_client=None)

    task = analysis_tasks[task_id]
    assert task["status"] == "failed"
    assert "Vacancy service error: upstream 502" in task["error"]
    assert "Vacancy service error: upstream 502" in task["message"]
    analysis_tasks.pop(task_id, None)


def test_is_complete_cached_result_requires_duplicate_count_field():
    incomplete = {
        "total_vacancies": 2,
        "vacancies_with_tech": [{"id": "1", "text_match_count": 1, "key_skills_match_count": 0}],
        "vacancies_without_tech": [{"id": "2"}],
    }
    assert _is_complete_cached_result(incomplete) is False


def test_is_complete_cached_result_rejects_unprocessed_or_requested_mismatch():
    with_unprocessed = {
        "total_vacancies": 2,
        "requested_vacancies": 3,
        "duplicate_vacancies_count": 0,
        "vacancies_with_tech": [{"id": "1", "text_match_count": 1, "key_skills_match_count": 0}],
        "vacancies_without_tech": [{"id": "2"}],
        "unprocessed_vacancy_ids": ["3"],
    }
    with_requested_mismatch = {
        "total_vacancies": 2,
        "requested_vacancies": 1,
        "duplicate_vacancies_count": 0,
        "vacancies_with_tech": [{"id": "1", "text_match_count": 1, "key_skills_match_count": 0}],
        "vacancies_without_tech": [{"id": "2"}],
        "unprocessed_vacancy_ids": [],
    }

    assert _is_complete_cached_result(with_unprocessed) is False
    assert _is_complete_cached_result(with_requested_mismatch) is False


def test_calculate_duplicate_metrics_by_employer_name_title_and_description():
    vacancies = [
        {
            "id": "1",
            "name": "Python Developer",
            "description": "<p>Stack: Python, PostgreSQL</p>",
            "employer": {"name": "ACME"},
        },
        {
            "id": "2",
            "name": " python developer ",
            "description": "Stack: Python, PostgreSQL",
            "employer": {"name": " acme "},
        },
        {
            "id": "3",
            "name": "Python Developer",
            "description": "Stack: Python, Redis",
            "employer": {"name": "ACME"},
        },
        {
            "id": "4",
            "name": "Python Developer",
            "description": "",
            "employer": {"name": "ACME"},
        },
    ]

    metrics = _calculate_duplicate_metrics(vacancies)

    assert metrics["duplicate_vacancies_count"] == 2
    assert metrics["duplicate_groups_count"] == 1
    assert metrics["duplicate_extra_count"] == 1
    assert metrics["duplicate_vacancy_ids"] == ["1", "2"]
    assert metrics["duplicate_group_size_by_id"]["1"] == 2
    assert metrics["duplicate_group_size_by_id"]["2"] == 2


def test_calculate_duplicate_metrics_returns_zero_when_no_duplicates():
    vacancies = [
        {
            "id": "1",
            "name": "Python Developer",
            "description": "Stack: Python",
            "employer": {"name": "ACME"},
        },
        {
            "id": "2",
            "name": "Python Engineer",
            "description": "Stack: Python",
            "employer": {"name": "ACME"},
        },
    ]

    metrics = _calculate_duplicate_metrics(vacancies)

    assert metrics["duplicate_vacancies_count"] == 0
    assert metrics["duplicate_groups_count"] == 0
    assert metrics["duplicate_extra_count"] == 0
    assert metrics["duplicate_vacancy_ids"] == []
