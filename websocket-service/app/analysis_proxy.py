# C:\Users\user\Desktop\TechStats\websocket-service\app\analysis_proxy.py
import asyncio
from contextlib import suppress
import json
import time
from typing import Dict, Any, Optional
import structlog
import httpx
from fastapi import WebSocket

from config import settings
from app.session_store import SessionStore
from shared.runtime_settings import RUNTIME_SETTINGS_KEY, build_effective_runtime_settings

logger = structlog.get_logger()


class AnalysisProxy:
    """Прокси для управления анализом через WebSocket"""
    
    def __init__(
        self,
        analyzer_client: httpx.AsyncClient,
        vacancy_client: httpx.AsyncClient,
        cache_client: httpx.AsyncClient,
        session_store: SessionStore
    ):
        self.analyzer_client = analyzer_client
        self.vacancy_client = vacancy_client
        self.cache_client = cache_client
        self.session_store = session_store
        
        # Активные задачи анализа
        self.active_analyses: Dict[str, asyncio.Task] = {}

    async def _load_runtime_settings(self) -> Dict[str, Any]:
        try:
            raw = await self.session_store.redis.get(RUNTIME_SETTINGS_KEY)
            if not raw:
                return build_effective_runtime_settings()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return build_effective_runtime_settings(parsed)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load runtime settings for websocket analysis", error=str(exc))
        return build_effective_runtime_settings()

    @staticmethod
    def _estimate_total_for_analysis(search_data: Dict[str, Any], max_pages: int, per_page: int, first_page_count: int) -> int:
        """Оценка итогового количества вакансий, которое будет обработано analyzer-service."""
        try:
            found = int(search_data.get("found", first_page_count))
        except (TypeError, ValueError):
            found = first_page_count

        try:
            pages = int(search_data.get("pages", 1))
        except (TypeError, ValueError):
            pages = 1

        try:
            safe_per_page = max(1, int(per_page or 1))
        except (TypeError, ValueError):
            safe_per_page = 1

        try:
            safe_max_pages = max(1, int(max_pages or 1))
        except (TypeError, ValueError):
            safe_max_pages = 1
        safe_pages = max(1, pages)

        effective_pages = min(safe_max_pages, safe_pages)
        capped_total = min(found, effective_pages * safe_per_page)

        return max(first_page_count, capped_total)

    @staticmethod
    def _normalize_timeout_seconds(raw_value: Any, fallback: float) -> float:
        try:
            timeout = float(raw_value)
            if timeout > 0:
                return timeout
        except (TypeError, ValueError):
            pass
        return float(fallback)

    @staticmethod
    def _response_body_preview(response: httpx.Response, max_len: int = 400) -> str:
        try:
            body = response.text.strip()
        except Exception:  # noqa: BLE001
            body = ""

        if not body:
            return "<empty body>"
        if len(body) <= max_len:
            return body
        return f"{body[:max_len]}..."

    @staticmethod
    def _format_exception_message(exc: Exception) -> str:
        if isinstance(exc, httpx.TimeoutException):
            request = getattr(exc, "request", None)
            if request and getattr(request, "url", None):
                return f"Request timeout while calling {request.url}"
            return "Request timeout while waiting for upstream service response"

        if isinstance(exc, httpx.HTTPStatusError):
            response = getattr(exc, "response", None)
            request = getattr(exc, "request", None)
            status = response.status_code if response else "unknown"
            path = request.url if request and getattr(request, "url", None) else "unknown URL"
            if response is not None:
                return f"Upstream HTTP {status} from {path}: {AnalysisProxy._response_body_preview(response)}"
            return f"Upstream HTTP {status} from {path}"

        if isinstance(exc, httpx.RequestError):
            request = getattr(exc, "request", None)
            if request and getattr(request, "url", None):
                return f"Request error while calling {request.url}: {type(exc).__name__}"
            return f"Request error: {type(exc).__name__}"

        text = str(exc).strip()
        if text:
            return text
        return f"{type(exc).__name__} (empty error message)"
        
    async def start_analysis(self, websocket: WebSocket, request_data: Dict[str, Any]):
        """Запуск анализа с отправкой прогресса через WebSocket"""
        # Валидация запроса
        required_fields = ["vacancy_title", "technology"]
        for field in required_fields:
            if field not in request_data:
                await self._send_error(websocket, f"Missing required field: {field}")
                return
        
        runtime_settings = await self._load_runtime_settings()

        vacancy_title = request_data["vacancy_title"]
        technology = request_data["technology"]
        exact_search = request_data.get("exact_search", runtime_settings.get("search_default_exact", True))
        area = request_data.get("area", runtime_settings.get("search_default_area", 113))

        max_pages_limit = int(runtime_settings.get("analysis_max_pages_hard_limit", 20))
        per_page_limit = int(runtime_settings.get("analysis_per_page_hard_limit", 100))
        max_pages = int(request_data.get("max_pages", runtime_settings.get("search_default_max_pages", 3)))
        per_page = int(request_data.get("per_page", runtime_settings.get("search_default_per_page", 50)))
        max_pages = max(1, min(max_pages, max_pages_limit))
        per_page = max(1, min(per_page, per_page_limit))

        use_cache = request_data.get("use_cache", runtime_settings.get("analysis_default_use_cache", True))
        
        # Создание сессии
        session_data = {
            "vacancy_title": vacancy_title,
            "technology": technology,
            "exact_search": exact_search,
            "area": area,
            "max_pages": max_pages,
            "per_page": per_page,
            "use_cache": use_cache,
            "request_data": request_data,
            "connection_id": None,
            "started_at": time.time()
        }
        
        try:
            # Получение connection_id
            connection_manager = websocket.app.state.connection_manager
            connection_id = connection_manager.get_connection_id(websocket)
            
            if connection_id:
                session_data["connection_id"] = connection_id
            
            # Создание сессии
            session_id = await self.session_store.create_session(session_data)
            
            # Отправка информации о начале анализа
            await self._send_progress(
                websocket,
                stage="initializing",
                message="Инициализация анализа...",
                progress=0,
                session_id=session_id
            )
            
            # Запуск асинхронного анализа
            analysis_task = asyncio.create_task(
                self._execute_analysis_with_progress(
                    websocket,
                    session_id,
                    vacancy_title,
                    technology,
                    exact_search,
                    area,
                    max_pages,
                    per_page,
                    use_cache,
                    runtime_settings,
                )
            )
            
            # Сохранение задачи
            self.active_analyses[session_id] = analysis_task
            
            # Ожидание завершения задачи
            try:
                await analysis_task
            except asyncio.CancelledError:
                logger.warning("Analysis task cancelled", session_id=session_id)
            except Exception as e:
                logger.error(
                    "Analysis task failed",
                    session_id=session_id,
                    error=self._format_exception_message(e),
                    error_type=type(e).__name__,
                )
            finally:
                # Удаление задачи из активных
                if session_id in self.active_analyses:
                    del self.active_analyses[session_id]
            
        except Exception as e:
            logger.error("Failed to start analysis", error=str(e))
            await self._send_error(websocket, f"Failed to start analysis: {str(e)}")
    
    async def _execute_analysis_with_progress(
        self,
        websocket: WebSocket,
        session_id: str,
        vacancy_title: str,
        technology: str,
        exact_search: bool,
        area: int,
        max_pages: int,
        per_page: int,
        use_cache: bool,
        runtime_settings: Dict[str, Any],
    ):
        """Выполнение анализа с отправкой прогресса"""
        analysis_request_task: Optional[asyncio.Task] = None
        try:
            vacancy_timeout = self._normalize_timeout_seconds(
                runtime_settings.get("live_vacancy_request_timeout_sec"),
                fallback=30.0,
            )
            analyzer_request_timeout = self._normalize_timeout_seconds(
                runtime_settings.get("live_analyzer_request_timeout_sec"),
                fallback=180.0,
            )
            analyzer_total_timeout = self._normalize_timeout_seconds(
                runtime_settings.get("live_analyzer_total_timeout_sec"),
                fallback=max(900.0, analyzer_request_timeout * 5.0),
            )

            # Этап 1: Получение списка вакансий
            await self.session_store.update_progress(
                session_id,
                progress=10,
                stage="fetching_vacancies",
                message="Получаем список вакансий..."
            )
            
            await self._send_progress(
                websocket,
                stage="fetching_vacancies",
                message="Получаем список вакансий...",
                progress=10,
                session_id=session_id
            )
            
            # Поиск вакансий через vacancy service
            # vacancy-service applies exact-search quoting internally.
            search_query = vacancy_title
            
            search_response = await self.vacancy_client.get(
                "/api/v1/search",
                params={
                    "query": search_query,
                    "area": area,
                    "page": 0,
                    "per_page": per_page,
                    "search_field": "name",
                    "exact_search": exact_search,
                    "use_cache": use_cache
                },
                timeout=vacancy_timeout,
            )
            
            if search_response.status_code != 200:
                raise RuntimeError(
                    f"Vacancy search failed with HTTP {search_response.status_code}: "
                    f"{self._response_body_preview(search_response)}"
                )
            
            search_data = search_response.json()
            vacancies = search_data.get("items", [])
            
            if not vacancies:
                await self._send_progress(
                    websocket,
                    stage="completed",
                    message="Вакансии не найдены",
                    progress=100,
                    session_id=session_id
                )
                
                await self.session_store.complete_session(
                    session_id,
                    {
                        "total_vacancies": 0,
                        "tech_vacancies": 0,
                        "tech_percentage": 0,
                        "vacancies_with_tech": [],
                        "message": "Вакансии не найдены"
                    }
                )
                return
            
            total_vacancies = self._estimate_total_for_analysis(
                search_data=search_data,
                max_pages=max_pages,
                per_page=per_page,
                first_page_count=len(vacancies),
            )
            total_cap = int(runtime_settings.get("live_max_total_vacancies", 2000))
            total_vacancies = min(total_vacancies, total_cap)
            
            # Отправка информации о найденных вакансиях
            await self.session_store.update_progress(
                session_id,
                progress=20,
                stage="vacancies_found",
                message=f"Найдено {total_vacancies} вакансий",
                metadata={"found": total_vacancies, "pages": search_data.get("pages", 1)}
            )
            
            await self._send_progress(
                websocket,
                stage="vacancies_found",
                message=f"Найдено {total_vacancies} вакансий",
                progress=20,
                session_id=session_id,
                metadata={
                    "found": total_vacancies,
                    "pages": search_data.get("pages", 1),
                    "found_all_pages": search_data.get("found", total_vacancies),
                    "source": search_data.get("source", "unknown")
                }
            )
            
            # Этап 2: Получение детальной информации о вакансиях
            await self.session_store.update_progress(
                session_id,
                progress=30,
                stage="fetching_details",
                message="Загружаем детальную информацию о вакансиях..."
            )
            
            await self._send_progress(
                websocket,
                stage="fetching_details",
                message="Загружаем детальную информацию о вакансиях...",
                progress=30,
                session_id=session_id
            )

            analysis_start_response = await self.analyzer_client.post(
                "/api/v1/analyze/async",
                json={
                    "vacancy_title": vacancy_title,
                    "technology": technology,
                    "exact_search": exact_search,
                    "area": area,
                    "max_pages": max_pages,
                    "per_page": per_page,
                    "use_cache": use_cache,
                },
                timeout=analyzer_request_timeout,
            )

            if analysis_start_response.status_code != 200:
                raise RuntimeError(
                    f"Analyzer async start failed with HTTP {analysis_start_response.status_code}: "
                    f"{self._response_body_preview(analysis_start_response)}"
                )

            analysis_start_data = analysis_start_response.json()
            analyzer_task_id = str(analysis_start_data.get("task_id", "")).strip()
            if not analyzer_task_id:
                raise RuntimeError("Analyzer async start response does not include task_id")
             
            # Этап 3: Анализ вакансий
            await self.session_store.update_progress(
                session_id,
                progress=40,
                stage="analyzing",
                message="Анализируем вакансии на наличие технологии..."
            )
            
            await self._send_progress(
                websocket,
                stage="analyzing",
                message="Анализируем вакансии на наличие технологии...",
                progress=40,
                session_id=session_id,
                metadata={
                    "total": total_vacancies,
                    "processed": 0,
                    "found_with_tech": 0,
                    "analyzer_task_id": analyzer_task_id,
                }
            )
             
            # Реальный прогресс: polling async task в analyzer-service
            processed = 0
            progress_update_interval = float(runtime_settings.get("live_progress_update_interval_sec", settings.progress_update_interval))
            keepalive_interval_sec = max(0.1, float(runtime_settings.get("live_progress_keepalive_interval_sec", 5.0)))
            poll_interval_sec = max(0.1, progress_update_interval)
            status_request_timeout = self._normalize_timeout_seconds(
                runtime_settings.get("live_analyzer_status_request_timeout_sec"),
                fallback=min(30.0, analyzer_request_timeout),
            )
            wait_started_at = time.monotonic()
            last_progress_sent_at = 0.0
            last_status_fingerprint = None
            found_with_tech_hint = 0
            status_total_hint = total_vacancies
            analyzer_status = "pending"
            last_known_stage = "pending"
            last_known_total = max(1, int(status_total_hint or total_vacancies or 1))

            while True:
                elapsed = time.monotonic() - wait_started_at
                if elapsed > analyzer_total_timeout:
                    raise TimeoutError(
                        "Analyzer async task timeout after "
                        f"{int(analyzer_total_timeout)}s "
                        f"(last stage: {last_known_stage}, processed: {processed}/{last_known_total})"
                    )

                status_response = await self.analyzer_client.get(
                    f"/api/v1/analyze/async/{analyzer_task_id}/status",
                    timeout=status_request_timeout,
                )
                if status_response.status_code != 200:
                    raise RuntimeError(
                        f"Analyzer async status failed with HTTP {status_response.status_code}: "
                        f"{self._response_body_preview(status_response)}"
                    )

                raw_status_data = status_response.json()
                status_data = raw_status_data if isinstance(raw_status_data, dict) else {}
                analyzer_status = str(status_data.get("status", "processing")).strip().lower()
                stage = str(status_data.get("stage", "analyzing") or "analyzing")
                last_known_stage = stage

                raw_total = status_data.get("total")
                try:
                    parsed_total = int(raw_total)
                    if parsed_total > 0:
                        status_total_hint = parsed_total
                except Exception:  # noqa: BLE001
                    pass

                normalized_total = max(1, int(status_total_hint or total_vacancies or 1))
                last_known_total = normalized_total

                raw_processed = status_data.get("processed")
                try:
                    parsed_processed = int(raw_processed)
                    processed = max(0, min(parsed_processed, normalized_total))
                except Exception:  # noqa: BLE001
                    processed = max(0, min(processed, normalized_total))

                raw_found_with_tech = status_data.get("found_with_tech")
                try:
                    found_with_tech_hint = max(0, int(raw_found_with_tech))
                except Exception:  # noqa: BLE001
                    pass

                message = str(status_data.get("message", "") or "").strip()
                if not message:
                    message = f"Обработано вакансий: {processed}/{normalized_total}"

                raw_internal_progress = status_data.get("progress")
                try:
                    internal_progress = float(raw_internal_progress)
                except Exception:  # noqa: BLE001
                    internal_progress = (processed / normalized_total) * 100
                internal_progress = max(0.0, min(100.0, internal_progress))
                progress = 40 + (50 * internal_progress / 100.0)

                now = time.monotonic()
                status_fingerprint = (
                    analyzer_status,
                    stage,
                    message,
                    processed,
                    normalized_total,
                    found_with_tech_hint,
                    round(progress, 3),
                )
                should_send_keepalive = (now - last_progress_sent_at) >= keepalive_interval_sec
                should_send_update = status_fingerprint != last_status_fingerprint or should_send_keepalive

                if should_send_update:
                    metadata = {
                        "processed": processed,
                        "total": normalized_total,
                        "found_with_tech": found_with_tech_hint,
                        "keepalive": bool(status_fingerprint == last_status_fingerprint),
                        "analyzer_request_in_flight": analyzer_status not in {"completed", "failed"},
                        "analyzer_status": analyzer_status,
                        "analyzer_stage": stage,
                        "analyzer_task_id": analyzer_task_id,
                    }

                    await self.session_store.update_progress(
                        session_id,
                        progress=progress,
                        stage="analyzing",
                        message=message,
                        metadata=metadata,
                    )

                    await self._send_progress(
                        websocket,
                        stage="analyzing",
                        message=message,
                        progress=progress,
                        session_id=session_id,
                        metadata=metadata,
                    )
                    last_progress_sent_at = now
                    last_status_fingerprint = status_fingerprint

                if analyzer_status in {"completed", "failed"}:
                    break

                await asyncio.sleep(poll_interval_sec)

            if analyzer_status == "failed":
                failed_result_response = await self.analyzer_client.get(
                    f"/api/v1/analyze/async/{analyzer_task_id}/result",
                    timeout=status_request_timeout,
                )
                raise RuntimeError(
                    f"Analyzer async task failed: "
                    f"{self._response_body_preview(failed_result_response)}"
                )

            while True:
                result_response = await self.analyzer_client.get(
                    f"/api/v1/analyze/async/{analyzer_task_id}/result",
                    timeout=status_request_timeout,
                )
                if result_response.status_code == 202:
                    await asyncio.sleep(poll_interval_sec)
                    continue
                if result_response.status_code != 200:
                    raise RuntimeError(
                        f"Analyzer async result failed with HTTP {result_response.status_code}: "
                        f"{self._response_body_preview(result_response)}"
                    )
                result_payload = result_response.json()
                if isinstance(result_payload, dict) and isinstance(result_payload.get("result"), dict):
                    result = result_payload["result"]
                else:
                    result = result_payload
                break

            result["analysis_timestamp"] = time.time()

            result_total = int(result.get("total_vacancies", status_total_hint or total_vacancies) or 0)
            result_total = max(result_total, processed, total_vacancies)

            await self.session_store.update_progress(
                session_id,
                progress=90,
                stage="analyzing",
                message=f"Обработано вакансий: {result_total}/{result_total} (ответ сервиса анализа получен)",
                metadata={
                    "processed": result_total,
                    "total": result_total,
                    "found_with_tech": result.get("tech_vacancies", found_with_tech_hint),
                    "analyzer_response_received": True,
                    "analyzer_task_id": analyzer_task_id,
                }
            )
            await self._send_progress(
                websocket,
                stage="analyzing",
                message=f"Обработано вакансий: {result_total}/{result_total} (ответ сервиса анализа получен)",
                progress=90,
                session_id=session_id,
                metadata={
                    "processed": result_total,
                    "total": result_total,
                    "found_with_tech": result.get("tech_vacancies", found_with_tech_hint),
                    "analyzer_response_received": True,
                    "analyzer_task_id": analyzer_task_id,
                }
            )
            
            # Этап 4: Завершение анализа
            await self.session_store.update_progress(
                session_id,
                progress=95,
                stage="finalizing",
                message="Формирование результатов..."
            )
            
            await self._send_progress(
                websocket,
                stage="finalizing",
                message="Формирование результатов...",
                progress=95,
                session_id=session_id
            )
            
            # Завершение сессии
            await self.session_store.complete_session(session_id, result)
            
            # Отправка финального результата
            await self._send_progress(
                websocket,
                stage="completed",
                message="Анализ завершен!",
                progress=100,
                session_id=session_id,
                metadata={"result": result}
            )
            
            logger.info(
                "Analysis completed",
                session_id=session_id,
                total_vacancies=result.get("total_vacancies", 0),
                tech_vacancies=result.get("tech_vacancies", 0),
                tech_percentage=result.get("tech_percentage", 0),
            )
            
        except Exception as e:
            if analysis_request_task:
                if not analysis_request_task.done():
                    analysis_request_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await analysis_request_task
                else:
                    with suppress(Exception):  # noqa: BLE001
                        analysis_request_task.result()

            error_message = self._format_exception_message(e)
            logger.error(
                "Analysis execution failed",
                session_id=session_id,
                error=error_message,
                error_type=type(e).__name__,
            )
            
            await self.session_store.fail_session(
                session_id,
                error_message,
                {"error_type": type(e).__name__}
            )
            
            await self._send_error(
                websocket,
                f"Analysis failed: {error_message}",
                session_id=session_id
            )
            
            raise
    
    async def _send_progress(
        self,
        websocket: WebSocket,
        stage: str,
        message: str,
        progress: float,
        session_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Отправка прогресса анализа"""
        message_data = {
            "type": "progress",
            "session_id": session_id,
            "stage": stage,
            "message": message,
            "progress": progress,
            "timestamp": time.time()
        }
        
        if metadata:
            message_data["metadata"] = metadata
        
        try:
            await websocket.send_json(message_data)
            await self._touch_ws_activity(websocket)
        except Exception as e:
            logger.error("Failed to send progress", session_id=session_id, error=str(e))
    
    async def _send_error(
        self,
        websocket: WebSocket,
        error_message: str,
        session_id: Optional[str] = None
    ):
        """Отправка сообщения об ошибке"""
        error_data = {
            "type": "error",
            "message": error_message,
            "timestamp": time.time()
        }
        
        if session_id:
            error_data["session_id"] = session_id
        
        try:
            await websocket.send_json(error_data)
            await self._touch_ws_activity(websocket)
        except Exception as e:
            logger.error("Failed to send error", error=str(e))

    async def _touch_ws_activity(self, websocket: WebSocket) -> None:
        app = getattr(websocket, "app", None)
        state = getattr(app, "state", None) if app else None
        connection_manager = getattr(state, "connection_manager", None)
        if connection_manager is None:
            return
        try:
            await connection_manager.update_activity(websocket)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to update websocket activity", error=str(exc))
    
    async def cancel_analysis(self, session_id: str):
        """Отмена анализа"""
        if session_id in self.active_analyses:
            task = self.active_analyses[session_id]
            task.cancel()
            
            try:
                await task
            except asyncio.CancelledError:
                pass
            
            # Обновление статуса сессии
            await self.session_store.update_session(
                session_id,
                {
                    "status": "cancelled",
                    "cancelled_at": time.time(),
                    "progress": 100.0,
                    "stage": "cancelled"
                }
            )
            
            logger.info("Analysis cancelled", session_id=session_id)
            
            return True
        
        return False
    
    async def get_active_analysis_count(self) -> int:
        """Получение количества активных анализов"""
        return len(self.active_analyses)
    
    async def cleanup_cancelled_analyses(self):
        """Очистка отмененных анализов"""
        cancelled_tasks = []
        
        for session_id, task in self.active_analyses.items():
            if task.done():
                cancelled_tasks.append(session_id)
        
        for session_id in cancelled_tasks:
            del self.active_analyses[session_id]
        
        return len(cancelled_tasks)
