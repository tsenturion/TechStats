# C:\Users\user\Desktop\TechStats\api-gateway\app\routers\analyzer.py
import asyncio
import json
from typing import Any, Dict

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.cache import cache_manager, cache_response
from app.runtime_config import get_runtime_settings_effective
from app.security import require_user_or_admin
from config import settings

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


def _normalize_analysis_request(
    analysis_request: Dict[str, Any],
    runtime_settings: Dict[str, Any],
) -> Dict[str, Any]:
    payload = dict(analysis_request)

    max_pages_limit = int(runtime_settings.get("analysis_max_pages_hard_limit", 20))
    per_page_limit = int(runtime_settings.get("analysis_per_page_hard_limit", 100))

    max_pages = int(payload.get("max_pages", runtime_settings.get("search_default_max_pages", 3)))
    per_page = int(payload.get("per_page", runtime_settings.get("search_default_per_page", 50)))

    payload["max_pages"] = max(1, min(max_pages, max_pages_limit))
    payload["per_page"] = max(1, min(per_page, per_page_limit))
    payload["use_cache"] = bool(payload.get("use_cache", runtime_settings.get("analysis_default_use_cache", True)))

    if "exact_search" not in payload:
        payload["exact_search"] = bool(runtime_settings.get("search_default_exact", True))
    if "area" not in payload:
        payload["area"] = int(runtime_settings.get("search_default_area", 113))

    return payload


@router.post("/analyze")
@limiter.limit("10/minute")
async def analyze_vacancies(
    request: Request,
    analysis_request: Dict[str, Any] = Body(...),
    _: dict = Depends(require_user_or_admin),
):
    """
    Запуск анализа вакансий на наличие технологии
    """
    required_fields = ["vacancy_title", "technology"]
    for field in required_fields:
        if field not in analysis_request:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required field: {field}",
            )

    runtime_settings = await get_runtime_settings_effective()
    request_timeout = int(runtime_settings.get("gateway_analyzer_request_timeout_sec", 30))
    request_delay_ms = int(runtime_settings.get("gateway_analyzer_request_delay_ms", 0))
    payload = _normalize_analysis_request(analysis_request, runtime_settings)
    use_cache = bool(payload.get("use_cache", True))

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        try:
            if request_delay_ms > 0:
                await asyncio.sleep(request_delay_ms / 1000.0)

            response = await client.post(
                f"{settings.analyzer_service_url}/api/v1/analyze",
                json=payload,
                params={"use_cache": use_cache},
                timeout=request_timeout,
            )
            response.raise_for_status()
            return response.json()

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Analyzer service timeout")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(exc)}")


@router.get("/analyze/stream/{analysis_id}")
@limiter.limit("30/minute")
async def stream_analysis_progress(
    request: Request,
    analysis_id: str,
    _: dict = Depends(require_user_or_admin),
):
    """
    Потоковая передача прогресса анализа через Server-Sent Events
    """

    async def event_generator():
        try:
            runtime_settings = await get_runtime_settings_effective()
            request_timeout = int(runtime_settings.get("gateway_analyzer_request_timeout_sec", 30))
            async with httpx.AsyncClient(timeout=request_timeout) as client:
                while True:
                    status_response = await client.get(
                        f"{settings.analyzer_service_url}/api/v1/analyze/async/{analysis_id}/status",
                    )
                    if status_response.status_code == 404:
                        yield f"data: {json.dumps({'error': 'Task not found'})}\n\n"
                        break

                    status_response.raise_for_status()
                    status_data = status_response.json()
                    yield f"data: {json.dumps(status_data)}\n\n"

                    if status_data.get("status") in {"completed", "failed"}:
                        if status_data.get("status") == "completed":
                            result_response = await client.get(
                                f"{settings.analyzer_service_url}/api/v1/analyze/async/{analysis_id}/result",
                            )
                            if result_response.status_code == 200:
                                yield f"data: {json.dumps({'result': result_response.json()})}\n\n"
                        break
                    await asyncio.sleep(1)
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/analysis/results/{analysis_id}")
@limiter.limit("60/minute")
@cache_response(ttl=600)  # Кэшировать на 10 минут
async def get_analysis_results(
    request: Request,
    analysis_id: str,
):
    """
    Получение результатов анализа
    """
    cache_key = f"analysis:results:{analysis_id}"

    cached = await cache_manager.get(cache_key)
    if cached:
        return cached

    runtime_settings = await get_runtime_settings_effective()
    request_timeout = int(runtime_settings.get("gateway_analyzer_request_timeout_sec", 30))

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        try:
            response = await client.get(
                f"{settings.analyzer_service_url}/api/v1/analysis/{analysis_id}/results",
            )
            response.raise_for_status()
            data = response.json()

            await cache_manager.set(cache_key, data, ttl=600)
            return data

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Analyzer service timeout")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(status_code=404, detail="Analysis results not found")
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(exc)}")

