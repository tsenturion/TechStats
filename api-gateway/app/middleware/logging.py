import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from config import settings
from app.security import decode_access_token, enforce_rbac
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
        method = request.method.upper()

        public_exact_paths = {
            "/",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/api/v1/auth/login",
            "/api/v1/auth/register",
            "/api/v1/auth/public",
            "/api/v1/auth/refresh",
        }
        public_prefixes = (
            "/api/v1/health",
            "/api/v1/metrics",
            "/api/v1/runtime-settings/public",
            "/ws/",
        )
        if path in public_exact_paths or path.startswith(public_prefixes):
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            if not enforce_rbac("guest", path, method):
                return JSONResponse(status_code=401, content={"detail": "Authentication required"})
            return await call_next(request)

        try:
            scheme, token = auth_header.split(" ", 1)
        except ValueError:
            return JSONResponse(status_code=401, content={"detail": "Invalid authorization header"})

        if scheme.lower() != "bearer":
            return JSONResponse(status_code=401, content={"detail": "Unsupported auth scheme"})

        try:
            payload = decode_access_token(token)
            request.state.user = payload
        except Exception:
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

        role = str(payload.get("role", "guest"))
        if not enforce_rbac(role, path, method):
            return JSONResponse(status_code=403, content={"detail": "Not enough permissions"})

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
