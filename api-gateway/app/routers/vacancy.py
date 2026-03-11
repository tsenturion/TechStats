# C:\Users\user\Desktop\TechStats\api-gateway\app\routers\vacancy.py
import asyncio
from typing import List

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.cache import cache_manager, cache_response, get_cached_response
from app.runtime_config import get_runtime_settings_effective
from app.security import require_user_or_admin
from config import settings

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/vacancies/search")
@limiter.limit("30/minute")
@cache_response(ttl=300)  # Кэшировать на 5 минут
async def search_vacancies(
    request: Request,
    query: str = Query(..., description="Поисковый запрос"),
    area: int = Query(113, description="ID региона (113 - Россия)"),
    page: int = Query(0, description="Номер страницы"),
    per_page: int = Query(100, description="Количество вакансий на странице"),
    exact_search: bool = Query(True, description="Точный поиск"),
    _: dict = Depends(require_user_or_admin),
):
    """
    Поиск вакансий с HH.ru
    """
    runtime_settings = await get_runtime_settings_effective()
    per_page_limit = int(runtime_settings.get("search_per_page_hard_limit", 100))
    max_pages_limit = int(runtime_settings.get("search_max_pages_hard_limit", 20))
    request_timeout = int(runtime_settings.get("gateway_vacancy_request_timeout_sec", settings.service_timeout))
    request_delay_ms = int(runtime_settings.get("gateway_vacancy_request_delay_ms", 0))

    if per_page > per_page_limit:
        per_page = per_page_limit
    if per_page < 1:
        per_page = 1

    if page < 0:
        page = 0
    max_page_index = max(0, max_pages_limit - 1)
    if page > max_page_index:
        page = max_page_index

    cache_key = f"vacancies:search:{query}:{area}:{page}:{per_page}:{exact_search}"

    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        try:
            if request_delay_ms > 0:
                await asyncio.sleep(request_delay_ms / 1000.0)

            params = {
                "query": query,
                "area": area,
                "page": page,
                "per_page": per_page,
                "exact_search": exact_search,
            }
            response = await client.get(
                f"{settings.vacancy_service_url}/api/v1/search",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            await cache_manager.set(cache_key, data, ttl=300)
            return data

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Vacancy service timeout")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(exc)}")


@router.get("/vacancies/batch")
@limiter.limit("20/minute")
async def get_vacancies_batch(
    request: Request,
    vacancy_ids: List[str] = Query(..., description="Список ID вакансий"),
    _: dict = Depends(require_user_or_admin),
):
    """
    Получение информации о нескольких вакансиях
    """
    runtime_settings = await get_runtime_settings_effective()
    batch_limit = int(runtime_settings.get("vacancy_batch_max_ids", 100))
    request_timeout = int(runtime_settings.get("gateway_vacancy_request_timeout_sec", settings.service_timeout))
    request_delay_ms = int(runtime_settings.get("gateway_vacancy_request_delay_ms", 0))

    vacancies = []
    ids_to_fetch = []

    for vacancy_id in vacancy_ids[:batch_limit]:
        cache_key = f"vacancy:{vacancy_id}"
        cached = await get_cached_response(cache_key)
        if cached:
            vacancies.append(cached)
        else:
            ids_to_fetch.append(vacancy_id)

    if not ids_to_fetch:
        return {"vacancies": vacancies}

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        try:
            if request_delay_ms > 0:
                await asyncio.sleep(request_delay_ms / 1000.0)

            response = await client.post(
                f"{settings.vacancy_service_url}/api/v1/vacancies/batch",
                json={"vacancy_ids": ids_to_fetch},
            )
            response.raise_for_status()
            batch_data = response.json()

            for vacancy in batch_data.get("vacancies", []):
                vacancy_id = vacancy.get("id")
                if vacancy_id:
                    cache_key = f"vacancy:{vacancy_id}"
                    await cache_manager.set(cache_key, vacancy, ttl=3600)

            vacancies.extend(batch_data.get("vacancies", []))
            return {"vacancies": vacancies}

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Vacancy service timeout")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(exc)}")


@router.get("/vacancies/{vacancy_id}")
@limiter.limit("60/minute")
@cache_response(ttl=3600)  # Кэшировать на 1 час
async def get_vacancy(
    request: Request,
    vacancy_id: str,
    _: dict = Depends(require_user_or_admin),
):
    """
    Получение детальной информации о вакансии
    """
    cache_key = f"vacancy:{vacancy_id}"

    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    runtime_settings = await get_runtime_settings_effective()
    request_timeout = int(runtime_settings.get("gateway_vacancy_request_timeout_sec", settings.service_timeout))
    request_delay_ms = int(runtime_settings.get("gateway_vacancy_request_delay_ms", 0))

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        try:
            if request_delay_ms > 0:
                await asyncio.sleep(request_delay_ms / 1000.0)

            response = await client.get(
                f"{settings.vacancy_service_url}/api/v1/vacancies/{vacancy_id}"
            )
            response.raise_for_status()
            data = response.json()

            await cache_manager.set(cache_key, data, ttl=3600)
            return data

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Vacancy service timeout")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(status_code=404, detail="Vacancy not found")
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(exc)}")
