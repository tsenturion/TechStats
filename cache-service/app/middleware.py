import time
from typing import Any, Dict

import structlog
from fastapi import Request
from starlette.responses import Response

from config import settings
from shared.middleware import BaseRequestLoggingMiddleware, BaseResponseTimeMiddleware

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseRequestLoggingMiddleware):
    service_name = "cache-service"

    def get_extra_context(self, request: Request) -> Dict[str, Any]:
        return {"node_id": settings.node_id}

    def apply_response_headers(self, request: Request, response, process_time: float) -> None:
        super().apply_response_headers(request, response, process_time)
        response.headers["X-Node-ID"] = settings.node_id


class ResponseTimeMiddleware(BaseResponseTimeMiddleware):
    service_name = "cache-service"
    slow_threshold_seconds = 1.0
    slow_message = "Slow cache request detected"


class RateLimitMiddleware(BaseRequestLoggingMiddleware):
    """Simple in-memory rate limiting for cache service API."""

    service_name = "cache-rate-limit"

    def __init__(self, app):
        super().__init__(app)
        self.rate_limits: Dict[str, Dict[str, Any]] = {}

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "unknown"
        current_minute = int(time.time() / 60)
        current_hour = int(time.time() / 3600)

        minute_key = f"minute:{client_ip}:{current_minute}"
        hour_key = f"hour:{client_ip}:{current_hour}"

        if minute_key not in self.rate_limits:
            self.rate_limits[minute_key] = {"count": 0}
        if hour_key not in self.rate_limits:
            self.rate_limits[hour_key] = {"count": 0}

        if (
            self.rate_limits[minute_key]["count"] >= settings.api_rate_limit_per_minute
            or self.rate_limits[hour_key]["count"] >= settings.api_rate_limit_per_hour
        ):
            logger.warning(
                "Rate limit exceeded",
                client_ip=client_ip,
                minute_count=self.rate_limits[minute_key]["count"],
                hour_count=self.rate_limits[hour_key]["count"],
                node_id=settings.node_id,
            )
            return Response(
                status_code=429,
                content="Rate limit exceeded. Please try again later.",
                headers={
                    "X-RateLimit-Limit-Minute": str(settings.api_rate_limit_per_minute),
                    "X-RateLimit-Limit-Hour": str(settings.api_rate_limit_per_hour),
                    "Retry-After": "60",
                },
            )

        self.rate_limits[minute_key]["count"] += 1
        self.rate_limits[hour_key]["count"] += 1

        self._cleanup_rate_limits(current_minute, current_hour)

        response = await call_next(request)
        response.headers["X-RateLimit-Minute-Remaining"] = str(
            max(0, settings.api_rate_limit_per_minute - self.rate_limits[minute_key]["count"])
        )
        response.headers["X-RateLimit-Hour-Remaining"] = str(
            max(0, settings.api_rate_limit_per_hour - self.rate_limits[hour_key]["count"])
        )
        return response

    def _cleanup_rate_limits(self, current_minute: int, current_hour: int) -> None:
        keys_to_delete = []
        for key in self.rate_limits:
            parts = key.split(":")
            if len(parts) != 3:
                continue
            kind, _, timestamp_raw = parts
            try:
                timestamp = int(timestamp_raw)
            except ValueError:
                continue
            if kind == "minute" and timestamp < current_minute - 120:
                keys_to_delete.append(key)
            if kind == "hour" and timestamp < current_hour - 24:
                keys_to_delete.append(key)
        for key in keys_to_delete:
            self.rate_limits.pop(key, None)

