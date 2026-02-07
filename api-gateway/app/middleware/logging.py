from typing import Dict

import structlog
from fastapi import Request
from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from config import settings
from shared.middleware import BaseRequestLoggingMiddleware, BaseResponseTimeMiddleware

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseRequestLoggingMiddleware):
    service_name = "api-gateway"


class ResponseTimeMiddleware(BaseResponseTimeMiddleware):
    service_name = "api-gateway"
    slow_threshold_seconds = 2.0
    slow_message = "Slow API Gateway request detected"


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """
    Lightweight auth middleware for protected gateway routes.
    Public routes and websockets are excluded.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        public_prefixes = (
            "/",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/api/v1/health",
            "/api/v1/metrics",
            "/ws/",
        )
        if path.startswith(public_prefixes):
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return await call_next(request)

        try:
            scheme, token = auth_header.split(" ", 1)
        except ValueError:
            return JSONResponse(status_code=401, content={"detail": "Invalid authorization header"})

        if scheme.lower() != "bearer":
            return JSONResponse(status_code=401, content={"detail": "Unsupported auth scheme"})

        try:
            payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
            request.state.user = payload
        except JWTError:
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

        return await call_next(request)


class ServiceHealthMiddleware(BaseHTTPMiddleware):
    """Attach downstream service endpoints to response headers for quick diagnostics."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Services"] = ",".join(
            [
                settings.vacancy_service_url,
                settings.analyzer_service_url,
                settings.cache_service_url,
                settings.websocket_service_url,
            ]
        )
        return response

