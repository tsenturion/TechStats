from shared.middleware import BaseRequestLoggingMiddleware, BaseResponseTimeMiddleware


class RequestLoggingMiddleware(BaseRequestLoggingMiddleware):
    service_name = "analyzer-service"


class ResponseTimeMiddleware(BaseResponseTimeMiddleware):
    service_name = "analyzer-service"
    slow_threshold_seconds = 5.0
    slow_message = "Slow analysis request detected"

