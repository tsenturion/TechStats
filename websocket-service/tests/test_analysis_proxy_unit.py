import asyncio
from types import SimpleNamespace

import httpx
import pytest

from app.analysis_proxy import AnalysisProxy


def test_estimate_total_for_analysis_respects_found_pages_and_caps():
    search_data = {
        "found": 150,
        "pages": 10,
    }

    total = AnalysisProxy._estimate_total_for_analysis(
        search_data=search_data,
        max_pages=2,
        per_page=50,
        first_page_count=50,
    )

    assert total == 100


def test_estimate_total_for_analysis_not_less_than_first_page_count():
    search_data = {
        "found": 2,
        "pages": 1,
    }

    total = AnalysisProxy._estimate_total_for_analysis(
        search_data=search_data,
        max_pages=1,
        per_page=50,
        first_page_count=20,
    )

    assert total == 20


def test_estimate_total_for_analysis_handles_bad_input_types():
    total = AnalysisProxy._estimate_total_for_analysis(
        search_data={"found": "bad", "pages": None},
        max_pages="x",
        per_page="y",
        first_page_count=5,
    )

    assert total == 5


def test_format_exception_message_returns_fallback_for_empty_exception_text():
    class EmptyMessageError(Exception):
        def __str__(self) -> str:
            return ""

    message = AnalysisProxy._format_exception_message(EmptyMessageError())
    assert "empty error message" in message


def test_format_exception_message_for_timeout_contains_request_url():
    request = httpx.Request("POST", "http://analyzer-service/api/v1/analyze")
    exc = httpx.ReadTimeout("", request=request)

    message = AnalysisProxy._format_exception_message(exc)
    assert "Request timeout while calling" in message
    assert "/api/v1/analyze" in message


class _DummyWebSocket:
    def __init__(self):
        self.messages = []

    async def send_json(self, payload):
        self.messages.append(payload)


class _DummyConnectionManager:
    def __init__(self):
        self.activity_calls = 0

    async def update_activity(self, websocket, received=False):  # noqa: ARG002
        self.activity_calls += 1


class _DummyWebSocketWithApp(_DummyWebSocket):
    def __init__(self):
        super().__init__()
        self.app = SimpleNamespace(state=SimpleNamespace(connection_manager=_DummyConnectionManager()))


class _DummySessionStore:
    def __init__(self):
        self.progress_calls = []
        self.complete_payload = None
        self.fail_payload = None

    async def update_progress(self, session_id, progress, stage, message, metadata=None):
        self.progress_calls.append(
            {
                "session_id": session_id,
                "progress": progress,
                "stage": stage,
                "message": message,
                "metadata": metadata or {},
            }
        )

    async def complete_session(self, session_id, result):
        self.complete_payload = {"session_id": session_id, "result": result}

    async def fail_session(self, session_id, error, metadata=None):
        self.fail_payload = {
            "session_id": session_id,
            "error": error,
            "metadata": metadata or {},
        }


class _FakeVacancyClient:
    async def get(self, path, params=None, timeout=None):
        assert path == "/api/v1/search"
        request = httpx.Request("GET", f"http://vacancy-service{path}")
        payload = {
            "items": [
                {"id": "1"},
                {"id": "2"},
                {"id": "3"},
                {"id": "4"},
                {"id": "5"},
            ],
            "found": 5,
            "pages": 1,
        }
        return httpx.Response(200, json=payload, request=request)


class _FakeAnalyzerTimeoutClient:
    def __init__(self):
        self.task_id = "task-timeout"
        self.status_calls = 0

    async def post(self, path, json=None, params=None, timeout=None):
        assert path == "/api/v1/analyze/async"
        request = httpx.Request("POST", f"http://analyzer-service{path}")
        return httpx.Response(200, json={"task_id": self.task_id}, request=request)

    async def get(self, path, params=None, timeout=None):  # noqa: ARG002
        self.status_calls += 1
        if path == f"/api/v1/analyze/async/{self.task_id}/status":
            if self.status_calls == 1:
                request = httpx.Request("GET", f"http://analyzer-service{path}")
                return httpx.Response(
                    200,
                    json={
                        "task_id": self.task_id,
                        "status": "processing",
                        "progress": 20,
                        "stage": "analyzing",
                        "message": "Обработано вакансий: 1/5",
                        "processed": 1,
                        "total": 5,
                        "found_with_tech": 0,
                    },
                    request=request,
                )
            await asyncio.sleep(0.03)
            request = httpx.Request("GET", f"http://analyzer-service{path}")
            raise httpx.ReadTimeout("", request=request)

        request = httpx.Request("GET", f"http://analyzer-service{path}")
        return httpx.Response(404, json={"detail": "not found"}, request=request)


class _FakeAnalyzerSlowSuccessClient:
    def __init__(self):
        self.task_id = "task-success"
        self.status_calls = 0

    async def post(self, path, json=None, params=None, timeout=None):  # noqa: ARG002
        assert path == "/api/v1/analyze/async"
        request = httpx.Request("POST", f"http://analyzer-service{path}")
        return httpx.Response(200, json={"task_id": self.task_id}, request=request)

    async def get(self, path, params=None, timeout=None):  # noqa: ARG002
        request = httpx.Request("GET", f"http://analyzer-service{path}")
        if path == f"/api/v1/analyze/async/{self.task_id}/status":
            self.status_calls += 1
            if self.status_calls <= 4:
                return httpx.Response(
                    200,
                    json={
                        "task_id": self.task_id,
                        "status": "processing",
                        "progress": 50,
                        "stage": "analyzing",
                        "message": "Обработано вакансий: 1/1",
                        "processed": 1,
                        "total": 1,
                        "found_with_tech": 0,
                    },
                    request=request,
                )
            return httpx.Response(
                200,
                json={
                    "task_id": self.task_id,
                    "status": "completed",
                    "progress": 100,
                    "stage": "completed",
                    "message": "Анализ завершен!",
                    "processed": 1,
                    "total": 1,
                    "found_with_tech": 1,
                },
                request=request,
            )

        if path == f"/api/v1/analyze/async/{self.task_id}/result":
            return httpx.Response(
                200,
                json={
                    "task_id": self.task_id,
                    "status": "completed",
                    "processed_at": 1.0,
                    "result": {
                        "vacancy_title": "Python Developer",
                        "technology": "python",
                        "total_vacancies": 1,
                        "tech_vacancies": 1,
                        "tech_percentage": 100.0,
                        "vacancies_with_tech": [{"id": "1", "name": "Python Dev", "url": "https://hh.ru/vacancy/1"}],
                        "vacancies_without_tech": [],
                    },
                },
                request=request,
            )

        return httpx.Response(
            404,
            json={"detail": "not found"},
            request=request,
        )


class _FakeAnalyzerNeverCompletesClient:
    def __init__(self):
        self.task_id = "task-never-completes"

    async def post(self, path, json=None, params=None, timeout=None):  # noqa: ARG002
        assert path == "/api/v1/analyze/async"
        request = httpx.Request("POST", f"http://analyzer-service{path}")
        return httpx.Response(200, json={"task_id": self.task_id}, request=request)

    async def get(self, path, params=None, timeout=None):  # noqa: ARG002
        request = httpx.Request("GET", f"http://analyzer-service{path}")
        if path == f"/api/v1/analyze/async/{self.task_id}/status":
            return httpx.Response(
                200,
                json={
                    "task_id": self.task_id,
                    "status": "processing",
                    "progress": 10,
                    "stage": "fetching_details",
                    "message": "Загружаем детальную информацию о вакансиях... 0/5",
                    "processed": 0,
                    "total": 5,
                    "found_with_tech": 0,
                },
                request=request,
            )
        return httpx.Response(
            404,
            json={"detail": "not found"},
            request=request,
        )


class _FakeVacancyClientSingle:
    async def get(self, path, params=None, timeout=None):  # noqa: ARG002
        assert path == "/api/v1/search"
        request = httpx.Request("GET", f"http://vacancy-service{path}")
        payload = {"items": [{"id": "1"}], "found": 1, "pages": 1}
        return httpx.Response(200, json=payload, request=request)


@pytest.mark.asyncio
async def test_execute_analysis_progress_never_exceeds_total_before_timeout_error():
    session_store = _DummySessionStore()
    websocket = _DummyWebSocket()
    proxy = AnalysisProxy(
        analyzer_client=_FakeAnalyzerTimeoutClient(),
        vacancy_client=_FakeVacancyClient(),
        cache_client=None,
        session_store=session_store,
    )

    with pytest.raises(httpx.ReadTimeout):
        await proxy._execute_analysis_with_progress(
            websocket=websocket,
            session_id="s1",
            vacancy_title="Python Developer",
            technology="python",
            exact_search=False,
            area=113,
            max_pages=1,
            per_page=50,
            use_cache=True,
            runtime_settings={
                "live_progress_update_interval_sec": 0.01,
                "live_progress_batch_size": 2,
            },
        )

    analyzing_messages = [
        msg
        for msg in websocket.messages
        if msg.get("type") == "progress"
        and msg.get("stage") == "analyzing"
        and "Обработано вакансий" in msg.get("message", "")
    ]
    assert analyzing_messages
    for message in analyzing_messages:
        text = str(message.get("message", ""))
        if "Обработано вакансий:" not in text:
            continue
        marker = text.split("Обработано вакансий:", 1)[1].strip().split(" ", 1)[0]
        processed_text, total_text = marker.split("/", 1)
        assert int(processed_text) <= int(total_text)

    error_messages = [msg for msg in websocket.messages if msg.get("type") == "error"]
    assert error_messages
    assert "Analysis failed: Request timeout while calling" in error_messages[-1]["message"]
    assert session_store.fail_payload["error"]


@pytest.mark.asyncio
async def test_send_progress_and_error_touch_connection_activity():
    proxy = AnalysisProxy(
        analyzer_client=None,
        vacancy_client=None,
        cache_client=None,
        session_store=_DummySessionStore(),
    )
    websocket = _DummyWebSocketWithApp()

    await proxy._send_progress(
        websocket=websocket,
        stage="analyzing",
        message="step",
        progress=50,
        session_id="session-test",
    )
    await proxy._send_error(
        websocket=websocket,
        error_message="boom",
        session_id="session-test",
    )

    assert websocket.app.state.connection_manager.activity_calls == 2


@pytest.mark.asyncio
async def test_execute_analysis_sends_keepalive_when_progress_stalls():
    session_store = _DummySessionStore()
    websocket = _DummyWebSocket()
    proxy = AnalysisProxy(
        analyzer_client=_FakeAnalyzerSlowSuccessClient(),
        vacancy_client=_FakeVacancyClientSingle(),
        cache_client=None,
        session_store=session_store,
    )

    await proxy._execute_analysis_with_progress(
        websocket=websocket,
        session_id="s2",
        vacancy_title="Python Developer",
        technology="python",
        exact_search=False,
        area=113,
        max_pages=1,
        per_page=50,
        use_cache=True,
        runtime_settings={
            "live_progress_update_interval_sec": 0.01,
            "live_progress_keepalive_interval_sec": 0.1,
            "live_progress_batch_size": 1,
        },
    )

    analyzing_messages = [
        msg
        for msg in websocket.messages
        if msg.get("type") == "progress"
        and msg.get("stage") == "analyzing"
        and bool(msg.get("metadata", {}).get("keepalive"))
    ]
    assert analyzing_messages
    assert all("Обработано вакансий:" in str(msg.get("message", "")) for msg in analyzing_messages)

    completed_messages = [
        msg
        for msg in websocket.messages
        if msg.get("type") == "progress" and msg.get("stage") == "completed"
    ]
    assert completed_messages
    completed_metadata = completed_messages[-1].get("metadata", {})
    assert completed_metadata.get("result_truncated") is True
    assert completed_metadata.get("session_result_available") is True
    assert isinstance(completed_metadata.get("result"), dict)
    assert "vacancies_with_tech" not in completed_metadata.get("result", {})
    assert "vacancies_without_tech" not in completed_metadata.get("result", {})


@pytest.mark.asyncio
async def test_execute_analysis_uses_total_timeout_separately_from_request_timeout():
    session_store = _DummySessionStore()
    websocket = _DummyWebSocket()
    proxy = AnalysisProxy(
        analyzer_client=_FakeAnalyzerNeverCompletesClient(),
        vacancy_client=_FakeVacancyClient(),
        cache_client=None,
        session_store=session_store,
    )

    with pytest.raises(TimeoutError) as exc_info:
        await proxy._execute_analysis_with_progress(
            websocket=websocket,
            session_id="s3",
            vacancy_title="Python Developer",
            technology="python",
            exact_search=False,
            area=113,
            max_pages=1,
            per_page=50,
            use_cache=True,
            runtime_settings={
                "live_progress_update_interval_sec": 0.01,
                "live_progress_keepalive_interval_sec": 0.05,
                "live_analyzer_request_timeout_sec": 30,
                "live_analyzer_total_timeout_sec": 1,
            },
        )

    assert "Analyzer async task timeout after 1s" in str(exc_info.value)
    assert session_store.fail_payload is not None
    assert "Analyzer async task timeout after 1s" in session_store.fail_payload["error"]
