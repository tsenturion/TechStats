from shared.middleware import BaseRequestLoggingMiddleware, BaseResponseTimeMiddleware


class RequestLoggingMiddleware(BaseRequestLoggingMiddleware):
    service_name = "vacancy-service"


class ResponseTimeMiddleware(BaseResponseTimeMiddleware):
    service_name = "vacancy-service"
    slow_threshold_seconds = 2.0
    slow_message = "Slow vacancy request detected"

