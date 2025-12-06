# C:\Users\user\Desktop\TechStats\vacancy-service\app\routers\vacancies.py
import asyncio
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Query, Body, Request, Depends
from fastapi.responses import JSONResponse
import structlog

from config import settings
from app.cache import cache_manager
from app.hh_client import HHClient
from app.rate_limiter import RateLimiter

router = APIRouter()
logger = structlog.get_logger()


async def get_hh_client(request: Request) -> HHClient:
    """Dependency для получения HH клиента"""
    return request.app.state.hh_client


async def get_rate_limiter(request: Request) -> RateLimiter:
    """Dependency для получения rate limiter"""
    return request.app.state.rate_limiter


@router.get("/search")
async def search_vacancies(
    request: Request,
    query: str = Query(..., description="Поисковый запрос"),
    area: int = Query(113, description="ID региона (113 - Россия)"),
    page: int = Query(0, description="Номер страницы"),
    per_page: int = Query(100, description="Количество вакансий на странице (макс 100)"),
    search_field: str = Query("name", description="Поле поиска (name, description, company_name)"),
    exact_search: bool = Query(True, description="Точный поиск"),
    use_cache: bool = Query(True, description="Использовать кэш"),
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Поиск вакансий с HH.ru
    """
    # Валидация параметров
    if per_page > 100:
        per_page = 100
    if per_page < 1:
        per_page = 20
    
    if page < 0:
        page = 0
    if page > 19:  # HH API ограничивает 20 страниц
        page = 19
    
    # Формирование поискового запроса
    if exact_search and search_field == "name":
        search_query = f'"{query}"'
    else:
        search_query = query
    
    # Проверка кэша
    if use_cache:
        cached_results = await cache_manager.search_vacancies_cache(
            query=search_query,
            area=area,
            page=page,
            per_page=per_page,
            search_field=search_field
        )
        
        if cached_results:
            logger.info("Cache hit for search", query=search_query, area=area, page=page)
            return {
                "source": "cache",
                "cached": True,
                "timestamp": asyncio.get_event_loop().time(),
                **cached_results
            }
    
    # Проверка rate limiting
    if not await rate_limiter.can_make_request():
        raise HTTPException(
            status_code=429,
            detail="HH API rate limit exceeded. Please try again later."
        )
    
    try:
        # Поиск вакансий через HH API
        logger.info(
            "Searching vacancies",
            query=search_query,
            area=area,
            page=page,
            per_page=per_page
        )
        
        search_results = await hh_client.search_vacancies(
            query=search_query,
            area=area,
            page=page,
            per_page=per_page,
            search_field=search_field
        )
        
        # Увеличение счетчика дневных запросов
        await rate_limiter.increment_daily_counter()
        
        # Кэширование результатов
        if use_cache:
            await cache_manager.cache_search_results(
                query=search_query,
                area=area,
                page=page,
                per_page=per_page,
                search_field=search_field,
                results=search_results
            )
        
        # Обогащение данных
        enriched_results = {
            "source": "hh_api",
            "cached": False,
            "timestamp": asyncio.get_event_loop().time(),
            "search_params": {
                "query": query,
                "search_query": search_query,
                "area": area,
                "page": page,
                "per_page": per_page,
                "search_field": search_field,
                "exact_search": exact_search
            },
            **search_results
        }
        
        return enriched_results
        
    except Exception as e:
        logger.error("Search error", query=query, error=str(e))
        
        # Возвращаем кэшированные данные, если есть
        if use_cache and cached_results:
            logger.info("Returning cached data due to error")
            return {
                "source": "cache_fallback",
                "cached": True,
                "error": str(e),
                "timestamp": asyncio.get_event_loop().time(),
                **cached_results
            }
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search vacancies: {str(e)}"
        )


@router.get("/vacancies/{vacancy_id}")
async def get_vacancy(
    request: Request,
    vacancy_id: str,
    use_cache: bool = Query(True, description="Использовать кэш"),
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение детальной информации о вакансии
    """
    # Проверка кэша
    if use_cache:
        cached_vacancy = await cache_manager.get_vacancy_cache(vacancy_id)
        if cached_vacancy:
            logger.info("Cache hit for vacancy", vacancy_id=vacancy_id)
            return {
                "source": "cache",
                "cached": True,
                "timestamp": asyncio.get_event_loop().time(),
                "vacancy": cached_vacancy
            }
    
    # Проверка rate limiting
    if not await rate_limiter.can_make_request():
        raise HTTPException(
            status_code=429,
            detail="HH API rate limit exceeded. Please try again later."
        )
    
    try:
        # Получение вакансии через HH API
        logger.info("Fetching vacancy", vacancy_id=vacancy_id)
        
        vacancy_data = await hh_client.get_vacancy(vacancy_id)
        
        # Увеличение счетчика дневных запросов
        await rate_limiter.increment_daily_counter()
        
        # Кэширование вакансии
        if use_cache:
            await cache_manager.cache_vacancy(vacancy_id, vacancy_data)
        
        return {
            "source": "hh_api",
            "cached": False,
            "timestamp": asyncio.get_event_loop().time(),
            "vacancy": vacancy_data
        }
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Vacancy not found")
        logger.error("HTTP error fetching vacancy", vacancy_id=vacancy_id, error=str(e))
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        logger.error("Error fetching vacancy", vacancy_id=vacancy_id, error=str(e))
        
        # Возвращаем кэшированные данные, если есть
        if use_cache and cached_vacancy:
            logger.info("Returning cached vacancy due to error")
            return {
                "source": "cache_fallback",
                "cached": True,
                "error": str(e),
                "timestamp": asyncio.get_event_loop().time(),
                "vacancy": cached_vacancy
            }
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch vacancy: {str(e)}"
        )


@router.post("/vacancies/batch")
async def get_vacancies_batch(
    request: Request,
    batch_request: Dict[str, Any] = Body(...),
    use_cache: bool = Query(True, description="Использовать кэш"),
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение информации о нескольких вакансиях
    """
    vacancy_ids = batch_request.get("vacancy_ids", [])
    
    if not vacancy_ids:
        raise HTTPException(status_code=400, detail="No vacancy_ids provided")
    
    # Ограничение количества вакансий
    if len(vacancy_ids) > 100:
        vacancy_ids = vacancy_ids[:100]
        logger.warning("Too many vacancy_ids, limiting to 100")
    
    results = {
        "vacancies": [],
        "errors": [],
        "cache_stats": {"hits": 0, "misses": 0}
    }
    
    # Проверка кэша
    cached_vacancies = {}
    ids_to_fetch = []
    
    if use_cache:
        cached_vacancies = await cache_manager.get_vacancies_batch_cache(vacancy_ids)
        
        for vacancy_id in vacancy_ids:
            if vacancy_id in cached_vacancies and cached_vacancies[vacancy_id]:
                results["vacancies"].append(cached_vacancies[vacancy_id])
                results["cache_stats"]["hits"] += 1
            else:
                ids_to_fetch.append(vacancy_id)
                results["cache_stats"]["misses"] += 1
    else:
        ids_to_fetch = vacancy_ids
        results["cache_stats"]["misses"] = len(vacancy_ids)
    
    # Если все данные в кэше
    if not ids_to_fetch:
        logger.info("All vacancies from cache", count=len(results["vacancies"]))
        results["source"] = "cache"
        return results
    
    # Получение недостающих данных с HH API
    try:
        # Проверка rate limiting для batch запроса
        requests_needed = len(ids_to_fetch)
        for _ in range(requests_needed):
            if not await rate_limiter.can_make_request():
                logger.warning("Rate limit reached during batch fetch")
                results["errors"].append({
                    "type": "rate_limit",
                    "message": "HH API rate limit reached",
                    "vacancy_ids": ids_to_fetch
                })
                break
        
        # Параллельное получение вакансий
        fetched_vacancies = await hh_client.get_vacancies_batch(ids_to_fetch)
        
        # Увеличение счетчика дневных запросов
        for _ in range(len(fetched_vacancies)):
            await rate_limiter.increment_daily_counter()
        
        # Кэширование полученных данных
        if use_cache:
            await cache_manager.cache_vacancies_batch(fetched_vacancies)
        
        # Объединение результатов
        results["vacancies"].extend(fetched_vacancies)
        results["source"] = "mixed" if cached_vacancies else "hh_api"
        
        logger.info(
            "Batch fetch completed",
            total=len(vacancy_ids),
            from_cache=results["cache_stats"]["hits"],
            from_api=len(fetched_vacancies),
            errors=len(results["errors"])
        )
        
        return results
        
    except Exception as e:
        logger.error("Batch fetch error", error=str(e))
        
        # Возвращаем то, что смогли получить
        if results["vacancies"]:
            results["source"] = "partial"
            results["errors"].append({
                "type": "fetch_error",
                "message": str(e),
                "vacancy_ids": ids_to_fetch
            })
            return results
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch batch vacancies: {str(e)}"
        )


@router.get("/areas")
async def get_areas(
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение списка регионов
    """
    try:
        areas = await hh_client.get_areas()
        return {"areas": areas}
    except Exception as e:
        logger.error("Error fetching areas", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to fetch areas: {str(e)}")


@router.get("/metro/{city_id}")
async def get_metro(
    city_id: int,
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение станций метро для города
    """
    try:
        metro = await hh_client.get_metro(city_id)
        return {"metro": metro}
    except Exception as e:
        logger.error("Error fetching metro", city_id=city_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to fetch metro: {str(e)}")


@router.get("/industries")
async def get_industries(
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение списка отраслей
    """
    try:
        industries = await hh_client.get_industries()
        return {"industries": industries}
    except Exception as e:
        logger.error("Error fetching industries", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to fetch industries: {str(e)}")


@router.get("/professional-roles")
async def get_professional_roles(
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение профессиональных ролей
    """
    try:
        roles = await hh_client.get_professional_roles()
        return {"professional_roles": roles}
    except Exception as e:
        logger.error("Error fetching professional roles", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to fetch professional roles: {str(e)}")


@router.get("/rate-limit/stats")
async def get_rate_limit_stats(
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Получение статистики по rate limiting
    """
    try:
        stats = await rate_limiter.get_rate_limit_stats()
        return stats
    except Exception as e:
        logger.error("Error getting rate limit stats", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get rate limit stats: {str(e)}")