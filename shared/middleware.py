import time
import uuid
from typing import Any, Dict

import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger()


class BaseRequestLoggingMiddleware(BaseHTTPMiddleware):
    """Reusable request logging middleware for HTTP endpoints."""

    service_name = "service"

    def get_extra_context(self, request: Request) -> Dict[str, Any]:
        return {}

    def apply_response_headers(self, request: Request, response, process_time: float) -> None:
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Process-Time"] = str(process_time)

    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        start_time = time.time()

        base_context = {
            "request_id": request_id,
            "method": request.method,
            "url": str(request.url),
            "client_ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
            **self.get_extra_context(request),
        }

        logger.info(f"{self.service_name} request started", **base_context)

        try:
            response = await call_next(request)
            process_time = time.time() - start_time

            logger.info(
                f"{self.service_name} request completed",
                status_code=response.status_code,
                process_time=process_time,
                **base_context,
            )
            self.apply_response_headers(request, response, process_time)
            return response
        except Exception as exc:
            process_time = time.time() - start_time
            logger.error(
                f"{self.service_name} request failed",
                error=str(exc),
                process_time=process_time,
                **base_context,
            )
            raise


class BaseResponseTimeMiddleware(BaseHTTPMiddleware):
    """Reusable response-time middleware with slow-request logging."""

    service_name = "service"
    slow_threshold_seconds = 2.0
    slow_message = "Slow request detected"

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Response-Time"] = str(process_time)

        if process_time > self.slow_threshold_seconds:
            logger.warning(
                self.slow_message,
                service=self.service_name,
                url=str(request.url),
                method=request.method,
                process_time=process_time,
            )

        return response

