import structlog

from shared.middleware import BaseRequestLoggingMiddleware

logger = structlog.get_logger()


class RequestLoggingMiddleware(BaseRequestLoggingMiddleware):
    service_name = "websocket-service"


class WebSocketMiddleware:
    """Middleware for incoming websocket connection attempts."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "websocket":
            client = scope.get("client")
            client_ip = client[0] if client else "unknown"
            logger.info(
                "WebSocket connection attempt",
                path=scope.get("path", ""),
                client_ip=client_ip,
            )
        await self.app(scope, receive, send)
