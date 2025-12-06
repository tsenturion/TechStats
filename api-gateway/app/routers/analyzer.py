# C:\Users\user\Desktop\TechStats\api-gateway\app\routers\analyzer.py
import json
from typing import Dict, Any
import httpx
from fastapi import APIRouter, HTTPException, Body, Request
from fastapi.responses import StreamingResponse
from slowapi import Limiter
from slowapi.util import get_remote_address

from config import settings
from app.cache import cache_manager, cache_response
from app.rate_limiting import rate_limit
from app.websocket_manager import websocket_manager

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.post("/analyze")
@limiter.limit("10/minute")
async def analyze_vacancies(
    request: Request,
    analysis_request: Dict[str, Any] = Body(...)
):
    """
    Запуск анализа вакансий на наличие технологии
    """
    # Валидация запроса
    required_fields = ["vacancy_title", "technology"]
    for field in required_fields:
        if field not in analysis_request:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required field: {field}"
            )
    
    # Проксирование запроса в сервис анализа
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                f"{settings.analyzer_service_url}/api/v1/analyze",
                json=analysis_request,
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
            
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Analyzer service timeout")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/analyze/stream/{analysis_id}")
@limiter.limit("30/minute")
async def stream_analysis_progress(
    request: Request,
    analysis_id: str
):
    """
    Потоковая передача прогресса анализа через Server-Sent Events
    """
    async def event_generator():
        # Подключение к WebSocket сервису через API Gateway
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                async with client.stream(
                    "GET",
                    f"{settings.websocket_service_url}/api/v1/analyze/progress/{analysis_id}",
                    timeout=30.0
                ) as response:
                    async for chunk in response.aiter_bytes():
                        yield f"data: {chunk.decode()}\n\n"
                        
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/analysis/results/{analysis_id}")
@limiter.limit("60/minute")
@cache_response(ttl=600)  # Кэшировать на 10 минут
async def get_analysis_results(
    request: Request,
    analysis_id: str
):
    """
    Получение результатов анализа
    """
    cache_key = f"analysis:results:{analysis_id}"
    
    # Проверка кэша
    cached = await cache_manager.get(cache_key)
    if cached:
        return json.loads(cached)
    
    # Проксирование запроса в сервис анализа
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(
                f"{settings.analyzer_service_url}/api/v1/analysis/{analysis_id}/results"
            )
            response.raise_for_status()
            data = response.json()
            
            # Кэширование результата
            await cache_manager.set(cache_key, json.dumps(data), ttl=600)
            
            return data
            
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Analyzer service timeout")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail="Analysis results not found")
            raise HTTPException(status_code=e.response.status_code, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")