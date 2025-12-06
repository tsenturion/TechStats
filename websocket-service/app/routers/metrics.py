# C:\Users\user\Desktop\TechStats\websocket-service\app\routers\metrics.py
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