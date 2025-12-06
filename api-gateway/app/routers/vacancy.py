# C:\Users\user\Desktop\TechStats\api-gateway\app\routers\vacancy.py
from typing import List, Dict, Any, Optional
import httpx
from fastapi import APIRouter, HTTPException, Query, Request, Depends
from slowapi import Limiter
from slowapi.util import get_remote_address

from config import settings
from app.cache import cache_manager, cache_response, get_cached_response
from app.rate_limiting import rate_limit

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
    exact_search: bool = Query(True, description="Точный поиск")
):
    """
    Поиск вакансий с HH.ru
    """
    cache_key = f"vacancies:search:{query}:{area}:{page}:{per_page}:{exact_search}"
    
    # Проверка кэша
    cached = await get_cached_response(cache_key)
    if cached:
        return cached
    
    # Проксирование запроса в сервис вакансий
    async with httpx.AsyncClient(timeout=settings.service_timeout) as client:
        try:
            params = {
                "query": query,
                "area": area,
                "page": page,
                "per_page": per_page,
                "exact_search": exact_search
            }
            response = await client.get(
                f"{settings.vacancy_service_url}/api/v1/search",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            # Кэширование результата
            await cache_manager.set(cache_key, data, ttl=300)
            
            return data
            
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Vacancy service timeout")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/vacancies/{vacancy_id}")
@limiter.limit("60/minute")
@cache_response(ttl=3600)  # Кэшировать на 1 час
async def get_vacancy(
    request: Request,
    vacancy_id: str
):
    """
    Получение детальной информации о вакансии
    """
    cache_key = f"vacancy:{vacancy_id}"
    
    # Проверка кэша
    cached = await get_cached_response(cache_key)
    if cached:
        return cached
    
    # Проксирование запроса в сервис вакансий
    async with httpx.AsyncClient(timeout=settings.service_timeout) as client:
        try:
            response = await client.get(
                f"{settings.vacancy_service_url}/api/v1/vacancies/{vacancy_id}"
            )
            response.raise_for_status()
            data = response.json()
            
            # Кэширование результата
            await cache_manager.set(cache_key, data, ttl=3600)
            
            return data
            
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Vacancy service timeout")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail="Vacancy not found")
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/vacancies/batch")
@limiter.limit("20/minute")
async def get_vacancies_batch(
    request: Request,
    vacancy_ids: List[str] = Query(..., description="Список ID вакансий")
):
    """
    Получение информации о нескольких вакансиях
    """
    # Проверка кэша для каждой вакансии
    vacancies = []
    ids_to_fetch = []
    
    for vacancy_id in vacancy_ids[:100]:  # Ограничение на 100 вакансий
        cache_key = f"vacancy:{vacancy_id}"
        cached = await get_cached_response(cache_key)
        if cached:
            vacancies.append(cached)
        else:
            ids_to_fetch.append(vacancy_id)
    
    # Если все данные в кэше
    if not ids_to_fetch:
        return {"vacancies": vacancies}
    
    # Получение недостающих данных
    async with httpx.AsyncClient(timeout=settings.service_timeout) as client:
        try:
            response = await client.post(
                f"{settings.vacancy_service_url}/api/v1/vacancies/batch",
                json={"vacancy_ids": ids_to_fetch}
            )
            response.raise_for_status()
            batch_data = response.json()
            
            # Кэширование новых данных
            for vacancy in batch_data.get("vacancies", []):
                vacancy_id = vacancy.get("id")
                if vacancy_id:
                    cache_key = f"vacancy:{vacancy_id}"
                    await cache_manager.set(cache_key, vacancy, ttl=3600)
            
            # Объединение результатов
            vacancies.extend(batch_data.get("vacancies", []))
            
            return {"vacancies": vacancies}
            
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Vacancy service timeout")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")