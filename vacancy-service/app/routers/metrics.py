# C:\Users\user\Desktop\TechStats\vacancy-service\app\routers\metrics.py
from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

router = APIRouter()


@router.get("/metrics")
async def get_metrics():
    """
    Prometheus метрики
    """
    metrics = generate_latest()
    return Response(
        content=metrics,
        media_type=CONTENT_TYPE_LATEST,
        headers={"Cache-Control": "no-cache"}
    )


@router.get("/metrics/summary")
async def get_metrics_summary():
    """
    Сводка метрик в JSON формате
    """
    # Здесь можно добавить кастомные метрики
    return {
        "service": "vacancy-service",
        "metrics": {
            "hh_api_calls": "Прометей метрика hh_api_calls_total",
            "vacancy_requests": "Прометей метрика vacancy_requests_total",
            "request_latency": "Прометей метрика vacancy_request_duration_seconds"
        },
        "prometheus_endpoint": "/metrics"
    }