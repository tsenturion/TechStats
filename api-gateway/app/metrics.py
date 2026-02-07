from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

metrics_router = APIRouter()


def setup_metrics() -> None:
    # Metrics are collected via global Prometheus registries in the service.
    return


@metrics_router.get("/metrics")
async def get_metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
        headers={"Cache-Control": "no-cache"},
    )

