# C:\Users\user\Desktop\TechStats\analyzer-service\app\routers\analyze.py
import asyncio
import time
from typing import List, Dict, Any, Optional
from uuid import uuid4
import httpx
from fastapi import APIRouter, HTTPException, Body, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse
import structlog

from config import settings
from app.cache import cache_manager
from app.analyzer import PatternMatcher
from app.tech_patterns import TechPatternsLoader

router = APIRouter()
logger = structlog.get_logger()

# Хранилище для фоновых задач
analysis_tasks: Dict[str, Dict[str, Any]] = {}


async def get_pattern_matcher(request: Request) -> PatternMatcher:
    """Dependency для получения PatternMatcher"""
    return request.app.state.pattern_matcher


async def get_patterns_loader(request: Request) -> TechPatternsLoader:
    """Dependency для получения TechPatternsLoader"""
    return request.app.state.patterns_loader


async def get_vacancy_client(request: Request) -> httpx.AsyncClient:
    """Dependency для получения HTTP клиента vacancy service"""
    return request.app.state.vacancy_client


@router.post("/analyze")
async def analyze_vacancies(
    request: Request,
    analysis_request: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher),
    vacancy_client: httpx.AsyncClient = Depends(get_vacancy_client),
    use_cache: bool = Query(True, description="Использовать кэш")
):
    """
    Анализ вакансий на наличие технологии
    """
    start_time = time.time()
    
    # Валидация запроса
    required_fields = ["vacancy_title", "technology"]
    for field in required_fields:
        if field not in analysis_request:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required field: {field}"
            )
    
    vacancy_title = analysis_request["vacancy_title"]
    technology = analysis_request["technology"]
    exact_search = analysis_request.get("exact_search", True)
    area = analysis_request.get("area", 113)
    max_pages = analysis_request.get("max_pages", 10)
    per_page = analysis_request.get("per_page", 100)
    
    # Формирование поискового запроса
    search_query = f'"{vacancy_title}"' if exact_search else vacancy_title
    
    try:
        # 1. Поиск вакансий через vacancy service
        logger.info(
            "Starting analysis",
            vacancy_title=vacancy_title,
            technology=technology,
            exact_search=exact_search
        )
        
        search_response = await vacancy_client.get(
            "/api/v1/search",
            params={
                "query": search_query,
                "area": area,
                "page": 0,
                "per_page": per_page,
                "search_field": "name",
                "exact_search": exact_search,
                "use_cache": use_cache
            }
        )
        
        if search_response.status_code != 200:
            raise HTTPException(
                status_code=search_response.status_code,
                detail=f"Vacancy service error: {search_response.text}"
            )
        
        search_data = search_response.json()
        vacancies = search_data.get("items", [])
        
        if not vacancies:
            return {
                "total_vacancies": 0,
                "tech_vacancies": 0,
                "tech_percentage": 0,
                "vacancies_with_tech": [],
                "request_stats": {
                    "real_requests": 1,
                    "cached_requests": 0,
                    "total_requests": 1
                }
            }
        
        total_vacancies = len(vacancies)
        vacancy_ids = [v.get("id") for v in vacancies if v.get("id")]
        
        # 2. Проверка кэша для всего набора
        cached_result = None
        if use_cache:
            cached_result = await cache_manager.get_analysis_result(
                vacancy_ids,
                technology,
                exact_search
            )
            
            if cached_result:
                logger.info(
                    "Analysis result from cache",
                    total_vacancies=total_vacancies,
                    tech_vacancies=cached_result.get("tech_vacancies", 0)
                )
                
                # Добавляем статистику запросов
                cached_result["request_stats"] = {
                    "real_requests": 1,  # Только поиск вакансий
                    "cached_requests": total_vacancies,  # Все анализы из кэша
                    "total_requests": total_vacancies + 1,
                    "cache_hit_rate": 100.0
                }
                
                return cached_result
        
        # 3. Пакетное получение детальной информации о вакансиях
        batch_response = await vacancy_client.post(
            "/api/v1/vacancies/batch",
            json={"vacancy_ids": vacancy_ids},
            params={"use_cache": use_cache}
        )
        
        if batch_response.status_code != 200:
            raise HTTPException(
                status_code=batch_response.status_code,
                detail=f"Batch fetch error: {batch_response.text}"
            )
        
        batch_data = batch_response.json()
        detailed_vacancies = batch_data.get("vacancies", [])
        
        # 4. Проверка кэша для отдельных вакансий
        cached_analyses = {}
        vacancies_to_analyze = []
        
        if use_cache:
            cached_analyses = await cache_manager.get_batch_analysis(
                vacancy_ids,
                technology,
                exact_search
            )
            
            for vacancy in detailed_vacancies:
                vacancy_id = vacancy.get("id")
                if vacancy_id in cached_analyses and cached_analyses[vacancy_id]:
                    # Анализ уже в кэше
                    cached_analyses[vacancy_id]["from_cache"] = True
                else:
                    # Нужно проанализировать
                    vacancies_to_analyze.append(vacancy)
        else:
            vacancies_to_analyze = detailed_vacancies
        
        # 5. Анализ вакансий, которых нет в кэше
        analysis_results = []
        if vacancies_to_analyze:
            logger.info(
                "Analyzing vacancies",
                total=len(vacancies_to_analyze),
                from_cache=len(cached_analyses)
            )
            
            analysis_results = await pattern_matcher.analyze_vacancies_batch(
                vacancies_to_analyze,
                technology,
                exact_search,
                batch_size=settings.batch_size
            )
            
            # Кэширование новых результатов
            if use_cache:
                await cache_manager.cache_batch_analysis(
                    analysis_results,
                    technology,
                    exact_search
                )
        
        # 6. Объединение результатов
        all_results = list(cached_analyses.values()) + analysis_results
        
        # Фильтрация вакансий с технологией
        vacancies_with_tech = []
        for result in all_results:
            if result.get("has_technology"):
                vacancies_with_tech.append({
                    "id": result.get("vacancy_id"),
                    "name": result.get("vacancy_name"),
                    "url": result.get("vacancy_url"),
                    "match_count": result.get("match_count", 0)
                })
        
        tech_vacancies = len(vacancies_with_tech)
        tech_percentage = (tech_vacancies / total_vacancies * 100) if total_vacancies > 0 else 0
        
        # 7. Формирование финального результата
        final_result = {
            "vacancy_title": vacancy_title,
            "technology": technology,
            "exact_search": exact_search,
            "total_vacancies": total_vacancies,
            "tech_vacancies": tech_vacancies,
            "tech_percentage": round(tech_percentage, 2),
            "vacancies_with_tech": vacancies_with_tech,
            "analysis_timestamp": time.time(),
            "cache_info": {
                "total_cached": len(cached_analyses),
                "newly_analyzed": len(analysis_results),
                "cache_usage_percentage": (len(cached_analyses) / total_vacancies * 100) if total_vacancies > 0 else 0
            }
        }
        
        # 8. Кэширование полного результата
        if use_cache:
            await cache_manager.cache_analysis_result(
                vacancy_ids,
                technology,
                exact_search,
                final_result
            )
        
        # 9. Расчет статистики запросов
        real_requests = 2  # Поиск + batch запрос
        cached_requests = total_vacancies - len(vacancies_to_analyze)
        total_requests = real_requests + cached_requests
        cache_hit_rate = (cached_requests / total_requests * 100) if total_requests > 0 else 0
        
        final_result["request_stats"] = {
            "real_requests": real_requests,
            "cached_requests": cached_requests,
            "total_requests": total_requests,
            "cache_hit_rate": round(cache_hit_rate, 2),
            "processing_time": time.time() - start_time
        }
        
        logger.info(
            "Analysis completed",
            total_vacancies=total_vacancies,
            tech_vacancies=tech_vacancies,
            tech_percentage=tech_percentage,
            cache_hit_rate=cache_hit_rate,
            processing_time=time.time() - start_time
        )
        
        return final_result
        
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Vacancy service timeout")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        logger.error("Analysis error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.post("/analyze/async")
async def analyze_vacancies_async(
    background_tasks: BackgroundTasks,
    request: Request,
    analysis_request: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher),
    vacancy_client: httpx.AsyncClient = Depends(get_vacancy_client)
):
    """
    Асинхронный анализ вакансий (возвращает ID задачи)
    """
    # Генерация уникального ID задачи
    task_id = str(uuid4())
    
    # Сохранение задачи
    analysis_tasks[task_id] = {
        "status": "pending",
        "created_at": time.time(),
        "request": analysis_request,
        "result": None,
        "error": None
    }
    
    # Запуск фоновой задачи
    background_tasks.add_task(
        execute_async_analysis,
        task_id,
        analysis_request,
        pattern_matcher,
        vacancy_client
    )
    
    return {
        "task_id": task_id,
        "status": "pending",
        "message": "Analysis started in background",
        "check_status_url": f"/api/v1/analyze/async/{task_id}/status",
        "get_result_url": f"/api/v1/analyze/async/{task_id}/result"
    }


async def execute_async_analysis(
    task_id: str,
    analysis_request: Dict[str, Any],
    pattern_matcher: PatternMatcher,
    vacancy_client: httpx.AsyncClient
):
    """Выполнение асинхронного анализа"""
    try:
        # Обновляем статус
        analysis_tasks[task_id]["status"] = "processing"
        
        # Выполняем анализ (упрощенная версия)
        # В реальности здесь должна быть полная логика анализа
        
        await asyncio.sleep(1)  # Имитация обработки
        
        # Сохраняем результат
        analysis_tasks[task_id]["status"] = "completed"
        analysis_tasks[task_id]["result"] = {
            "task_id": task_id,
            "status": "completed",
            "processed_at": time.time(),
            "sample_result": {
                "total_vacancies": 100,
                "tech_vacancies": 45,
                "tech_percentage": 45.0
            }
        }
        
    except Exception as e:
        analysis_tasks[task_id]["status"] = "failed"
        analysis_tasks[task_id]["error"] = str(e)
        logger.error("Async analysis failed", task_id=task_id, error=str(e))


@router.get("/analyze/async/{task_id}/status")
async def get_analysis_status(task_id: str):
    """Получение статуса асинхронного анализа"""
    if task_id not in analysis_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = analysis_tasks[task_id]
    
    return {
        "task_id": task_id,
        "status": task["status"],
        "created_at": task["created_at"],
        "progress": 50 if task["status"] == "processing" else 100,
        "has_result": task["result"] is not None,
        "has_error": task["error"] is not None
    }


@router.get("/analyze/async/{task_id}/result")
async def get_analysis_result(task_id: str):
    """Получение результата асинхронного анализа"""
    if task_id not in analysis_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = analysis_tasks[task_id]
    
    if task["status"] == "failed":
        raise HTTPException(status_code=500, detail=f"Analysis failed: {task['error']}")
    
    if task["status"] != "completed" or task["result"] is None:
        raise HTTPException(status_code=202, detail="Analysis still in progress")
    
    return task["result"]


@router.post("/analyze/batch")
async def analyze_batch_vacancies(
    request: Request,
    batch_request: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher),
    vacancy_client: httpx.AsyncClient = Depends(get_vacancy_client)
):
    """
    Пакетный анализ нескольких технологий для одного набора вакансий
    """
    # Валидация
    if "vacancy_ids" not in batch_request or "technologies" not in batch_request:
        raise HTTPException(
            status_code=400,
            detail="Missing vacancy_ids or technologies"
        )
    
    vacancy_ids = batch_request["vacancy_ids"]
    technologies = batch_request["technologies"]
    exact_search = batch_request.get("exact_search", True)
    
    if len(vacancy_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Too many vacancy_ids, maximum is 100"
        )
    
    if len(technologies) > 20:
        raise HTTPException(
            status_code=400,
            detail="Too many technologies, maximum is 20"
        )
    
    try:
        # Получение детальной информации о вакансиях
        batch_response = await vacancy_client.post(
            "/api/v1/vacancies/batch",
            json={"vacancy_ids": vacancy_ids}
        )
        
        if batch_response.status_code != 200:
            raise HTTPException(
                status_code=batch_response.status_code,
                detail=f"Batch fetch error: {batch_response.text}"
            )
        
        batch_data = batch_response.json()
        vacancies = batch_data.get("vacancies", [])
        
        # Параллельный анализ для каждой технологии
        tasks = []
        for tech in technologies:
            task = pattern_matcher.analyze_vacancies_batch(
                vacancies,
                tech,
                exact_search,
                batch_size=settings.batch_size
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # Формирование сводного отчета
        summary = {
            "total_vacancies": len(vacancies),
            "technologies_analyzed": len(technologies),
            "results_by_technology": {},
            "comparison": []
        }
        
        for i, tech in enumerate(technologies):
            tech_results = results[i]
            
            # Подсчет вакансий с технологией
            tech_vacancies = sum(1 for r in tech_results if r["has_technology"])
            tech_percentage = (tech_vacancies / len(vacancies) * 100) if vacancies else 0
            
            summary["results_by_technology"][tech] = {
                "tech_vacancies": tech_vacancies,
                "tech_percentage": round(tech_percentage, 2),
                "total_matches": sum(r.get("match_count", 0) for r in tech_results),
                "sample_matches": [
                    {
                        "vacancy_id": r["vacancy_id"],
                        "vacancy_name": r["vacancy_name"],
                        "match_count": r.get("match_count", 0)
                    }
                    for r in tech_results[:5] if r["has_technology"]
                ]
            }
            
            summary["comparison"].append({
                "technology": tech,
                "percentage": round(tech_percentage, 2),
                "vacancy_count": tech_vacancies
            })
        
        # Сортировка по проценту вхождения
        summary["comparison"].sort(key=lambda x: x["percentage"], reverse=True)
        
        return summary
        
    except Exception as e:
        logger.error("Batch analysis error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")


@router.post("/analyze/text")
async def analyze_text(
    request: Request,
    text_analysis: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher)
):
    """
    Анализ произвольного текста на наличие технологий
    """
    if "text" not in text_analysis:
        raise HTTPException(status_code=400, detail="Missing text field")
    
    text = text_analysis["text"]
    technology = text_analysis.get("technology")
    technologies = text_analysis.get("technologies", [])
    
    try:
        if technology:
            # Поиск одной технологии
            result = await pattern_matcher.find_technology(text, technology)
            return {
                "text_preview": text[:200] + ("..." if len(text) > 200 else ""),
                "analysis": result
            }
        elif technologies:
            # Поиск нескольких технологий
            result = await pattern_matcher.find_multiple_technologies(text, technologies)
            return {
                "text_preview": text[:200] + ("..." if len(text) > 200 else ""),
                "analysis": result
            }
        else:
            raise HTTPException(
                status_code=400,
                detail="Either technology or technologies field is required"
            )
        
    except Exception as e:
        logger.error("Text analysis error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Text analysis failed: {str(e)}")