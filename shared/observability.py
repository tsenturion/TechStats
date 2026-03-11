from __future__ import annotations

from typing import Any

from asgi_correlation_id import CorrelationIdMiddleware
from prometheus_fastapi_instrumentator import Instrumentator


def setup_correlation_middleware(app: Any) -> None:
    app.add_middleware(
        CorrelationIdMiddleware,
        header_name="X-Request-ID",
        update_request_header=True,
    )


def setup_prometheus_instrumentation(app: Any, *, expose: bool = False, endpoint: str = "/metrics") -> Instrumentator:
    instrumentator = Instrumentator().instrument(app)
    if expose:
        instrumentator.expose(app, include_in_schema=False, endpoint=endpoint)
    return instrumentator

