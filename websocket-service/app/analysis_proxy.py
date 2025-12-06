# C:\Users\user\Desktop\TechStats\websocket-service\app\analysis_proxy.py
import asyncio
import time
import json
from typing import Dict, Any, List, Optional, Callable
import structlog
import httpx

from config import settings
from app.session_store import SessionStore
from app.connection_manager import ConnectionManager

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
        
    async def start_analysis(self, websocket: WebSocket, request_data: Dict[str, Any]):
        """Запуск анализа с отправкой прогресса через WebSocket"""
        # Валидация запроса
        required_fields = ["vacancy_title", "technology"]
        for field in required_fields:
            if field not in request_data:
                await self._send_error(websocket, f"Missing required field: {field}")
                return
        
        vacancy_title = request_data["vacancy_title"]
        technology = request_data["technology"]
        exact_search = request_data.get("exact_search", True)
        area = request_data.get("area", 113)
        max_pages = request_data.get("max_pages", 10)
        per_page = request_data.get("per_page", 100)
        
        # Создание сессии
        session_data = {
            "vacancy_title": vacancy_title,
            "technology": technology,
            "exact_search": exact_search,
            "area": area,
            "max_pages": max_pages,
            "per_page": per_page,
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
                    per_page
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
                logger.error("Analysis task failed", session_id=session_id, error=str(e))
                await self._send_error(websocket, f"Analysis failed: {str(e)}")
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
        per_page: int
    ):
        """Выполнение анализа с отправкой прогресса"""
        try:
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
            search_query = f'"{vacancy_title}"' if exact_search else vacancy_title
            
            search_response = await self.vacancy_client.get(
                "/api/v1/search",
                params={
                    "query": search_query,
                    "area": area,
                    "page": 0,
                    "per_page": per_page,
                    "search_field": "name",
                    "exact_search": exact_search,
                    "use_cache": True
                }
            )
            
            if search_response.status_code != 200:
                raise Exception(f"Vacancy search failed: {search_response.text}")
            
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
            
            total_vacancies = len(vacancies)
            vacancy_ids = [v.get("id") for v in vacancies if v.get("id")]
            
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
            
            # Пакетное получение вакансий
            batch_response = await self.vacancy_client.post(
                "/api/v1/vacancies/batch",
                json={"vacancy_ids": vacancy_ids},
                params={"use_cache": True}
            )
            
            if batch_response.status_code != 200:
                raise Exception(f"Batch fetch failed: {batch_response.text}")
            
            batch_data = batch_response.json()
            detailed_vacancies = batch_data.get("vacancies", [])
            
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
                    "found_with_tech": 0
                }
            )
            
            # Пакетный анализ через analyzer service
            analysis_response = await self.analyzer_client.post(
                "/api/v1/analyze/batch",
                json={
                    "vacancy_ids": vacancy_ids,
                    "technologies": [technology],
                    "exact_search": exact_search
                }
            )
            
            if analysis_response.status_code != 200:
                raise Exception(f"Analysis failed: {analysis_response.text}")
            
            analysis_data = analysis_response.json()
            
            # Извлечение результатов
            tech_results = analysis_data.get("results_by_technology", {}).get(technology, {})
            tech_vacancies = tech_results.get("tech_vacancies", 0)
            tech_percentage = tech_results.get("tech_percentage", 0)
            
            # Получение списка вакансий с технологией
            vacancies_with_tech = []
            if tech_vacancies > 0:
                # Получение детальной информации о вакансиях с технологией
                for vacancy in detailed_vacancies:
                    vacancy_id = vacancy.get("id")
                    if vacancy_id:
                        # В реальном приложении здесь была бы проверка,
                        # содержит ли вакансия технологию
                        # Для упрощения берем первые N вакансий
                        if len(vacancies_with_tech) < 50:  # Ограничиваем список
                            vacancies_with_tech.append({
                                "id": vacancy_id,
                                "name": vacancy.get("name", ""),
                                "url": vacancy.get("alternate_url", ""),
                                "company": vacancy.get("employer", {}).get("name", "")
                            })
            
            # Отправка прогресса по мере обработки
            processed = 0
            batch_size = settings.batch_size_for_progress
            
            while processed < total_vacancies:
                await asyncio.sleep(settings.progress_update_interval)
                
                processed = min(processed + batch_size, total_vacancies)
                progress = 40 + (50 * processed / total_vacancies)
                
                await self.session_store.update_progress(
                    session_id,
                    progress=progress,
                    stage="analyzing",
                    message=f"Обработано вакансий: {processed}/{total_vacancies}",
                    metadata={
                        "processed": processed,
                        "total": total_vacancies,
                        "found_with_tech": tech_vacancies
                    }
                )
                
                await self._send_progress(
                    websocket,
                    stage="analyzing",
                    message=f"Обработано вакансий: {processed}/{total_vacancies}",
                    progress=progress,
                    session_id=session_id,
                    metadata={
                        "processed": processed,
                        "total": total_vacancies,
                        "found_with_tech": tech_vacancies,
                        "cache_stats": batch_data.get("cache_stats", {})
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
            
            # Формирование финального результата
            result = {
                "vacancy_title": vacancy_title,
                "technology": technology,
                "exact_search": exact_search,
                "total_vacancies": total_vacancies,
                "tech_vacancies": tech_vacancies,
                "tech_percentage": tech_percentage,
                "vacancies_with_tech": vacancies_with_tech,
                "analysis_timestamp": time.time(),
                "request_stats": {
                    "vacancy_requests": 2,  # Поиск + batch
                    "analysis_requests": 1,
                    "cache_hits": batch_data.get("cache_stats", {}).get("hits", 0),
                    "cache_misses": batch_data.get("cache_stats", {}).get("misses", 0)
                }
            }
            
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
                total_vacancies=total_vacancies,
                tech_vacancies=tech_vacancies,
                tech_percentage=tech_percentage
            )
            
        except Exception as e:
            logger.error("Analysis execution failed", session_id=session_id, error=str(e))
            
            await self.session_store.fail_session(
                session_id,
                str(e),
                {"error_type": type(e).__name__}
            )
            
            await self._send_error(
                websocket,
                f"Analysis failed: {str(e)}",
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
        except Exception as e:
            logger.error("Failed to send error", error=str(e))
    
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