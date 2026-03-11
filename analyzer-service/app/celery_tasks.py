from __future__ import annotations

from typing import Any, Dict

import httpx

from app.celery_app import celery_app
from config import settings


@celery_app.task(
    name="analyzer.perform_analysis",
    autoretry_for=(httpx.HTTPError,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def perform_analysis_task(analysis_request: Dict[str, Any], use_cache: bool = True) -> Dict[str, Any]:
    base_url = settings.analyzer_internal_url.rstrip("/")
    endpoint = f"{base_url}/api/v1/analyze"
    with httpx.Client(timeout=300.0) as client:
        response = client.post(
            endpoint,
            json=analysis_request,
            params={"use_cache": str(bool(use_cache)).lower()},
        )
        response.raise_for_status()
        return response.json()

