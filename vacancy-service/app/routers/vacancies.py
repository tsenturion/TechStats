import asyncio
from typing import Dict, Any, Optional
import httpx
from fastapi import APIRouter, HTTPException, Query, Body, Request, Depends
import structlog

from app.cache import cache_manager
from app.hh_client import HHClient, HHVacancySearchForbiddenError
from app.rate_limiter import RateLimiter

router = APIRouter()
logger = structlog.get_logger()


def _normalize_for_contains(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _title_matches_query_tokens(title: Any, query_tokens: list[str]) -> bool:
    normalized_title = _normalize_for_contains(title)
    if not normalized_title:
        return False
    return all(token in normalized_title for token in query_tokens)


def _filter_items_by_title_contains(items: Any, query: str) -> list:
    if not isinstance(items, list):
        return []

    normalized_query = _normalize_for_contains(query)
    if not normalized_query:
        return items
    query_tokens = [token for token in normalized_query.split(" ") if token]
    if not query_tokens:
        return items
    if len(query_tokens) > 1:
        # For multi-word non-exact search, trust HH relevance from `search_field=name`
        # and avoid over-restricting title matches locally.
        return [item for item in items if isinstance(item, dict)]

    return [
        item
        for item in items
        if isinstance(item, dict) and _title_matches_query_tokens(item.get("name", ""), query_tokens)
    ]


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
    date_from: Optional[str] = Query(None, description="Нижняя граница даты публикации (ISO-8601)"),
    date_to: Optional[str] = Query(None, description="Верхняя граница даты публикации (ISO-8601)"),
    exact_search: bool = Query(True, description="Точный поиск"),
    use_cache: bool = Query(True, description="Использовать кэш"),
    hh_client: HHClient = Depends(get_hh_client),
    rate_limiter: RateLimiter = Depends(get_rate_limiter)
):
    """
    Поиск вакансий с HH.ru
    """
    cached_results = None

    # Валидация параметров
    if per_page > 100:
        per_page = 100
    if per_page < 1:
        per_page = 20
    
    if page < 0:
        page = 0
    # HH API supports deeper pagination than 20 pages for many queries.
    # Keep an upper safety bound to avoid obviously invalid requests.
    if page > 99:
        page = 99
    
    # Формирование поискового запроса
    non_exact_name_contains_mode = not exact_search and search_field == "name"
    if exact_search and search_field == "name":
        search_query = f'"{query}"'
    else:
        search_query = query
    # For non-exact title mode keep HH search in title field and additionally
    # apply local token-based "contains" filter (order-independent).
    effective_search_field = search_field
    cache_search_field = "default_title_contains" if non_exact_name_contains_mode else search_field
    if date_from or date_to:
        cache_search_field = f"{cache_search_field}|date_from:{date_from or ''}|date_to:{date_to or ''}"
    
    # Проверка кэша
    if use_cache:
        cached_results = await cache_manager.search_vacancies_cache(
            query=search_query,
            area=area,
            page=page,
            per_page=per_page,
            search_field=cache_search_field
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
            search_field=effective_search_field,
            date_from=date_from,
            date_to=date_to,
        )

        if non_exact_name_contains_mode:
            original_items = search_results.get("items", [])
            filtered_items = _filter_items_by_title_contains(original_items, query)
            search_results = {
                **search_results,
                "items": filtered_items,
                "filtered_by_title_contains": True,
                "title_contains_query": query,
                "unfiltered_items_count": len(original_items) if isinstance(original_items, list) else 0,
            }
        
        # Увеличение счетчика дневных запросов
        await rate_limiter.increment_daily_counter()
        
        # Кэширование результатов
        if use_cache:
            await cache_manager.cache_search_results(
                query=search_query,
                area=area,
                page=page,
                per_page=per_page,
                search_field=cache_search_field,
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
                "effective_search_field": effective_search_field,
                "date_from": date_from,
                "date_to": date_to,
                "title_contains_mode": non_exact_name_contains_mode,
                "exact_search": exact_search
            },
            **search_results
        }
        
        return enriched_results
        
    except HHVacancySearchForbiddenError as e:
        logger.warning("HH vacancy search is forbidden", query=query, error=str(e))

        if use_cache and cached_results:
            logger.info("Returning cached data due to HH forbidden response")
            return {
                "source": "cache_fallback",
                "cached": True,
                "error": str(e),
                "timestamp": asyncio.get_event_loop().time(),
                **cached_results
            }

        raise HTTPException(
            status_code=503,
            detail=(
                "HH API temporarily blocked vacancy search (HTTP 403, captcha/anti-bot). "
                "Попробуйте повторить запрос позже, включить use_cache или снизить интенсивность запросов. "
                f"Details: {str(e)}"
            )
        )
    except httpx.HTTPStatusError as e:
        logger.error(
            "Search upstream HTTP error",
            query=query,
            status_code=e.response.status_code if e.response else None,
            error=str(e),
        )

        if use_cache and cached_results:
            logger.info("Returning cached data due to upstream HTTP error")
            return {
                "source": "cache_fallback",
                "cached": True,
                "error": str(e),
                "timestamp": asyncio.get_event_loop().time(),
                **cached_results
            }

        upstream_status = int(e.response.status_code) if e.response is not None else 502
        detail_text = str(e)
        raise HTTPException(
            status_code=upstream_status if 400 <= upstream_status < 500 else 502,
            detail=f"Failed to search vacancies: {detail_text}",
        )
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
