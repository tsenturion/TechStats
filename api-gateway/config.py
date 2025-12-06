# C:\Users\user\Desktop\TechStats\api-gateway\config.py
from pydantic_settings import BaseSettings
from typing import List, Optional
import os


class Settings(BaseSettings):
    # Основные настройки
    app_name: str = "TechStats API Gateway"
    debug: bool = False
    environment: str = "production"
    
    # Настройки сервисов
    vacancy_service_url: str = "http://vacancy-service:8001"
    analyzer_service_url: str = "http://analyzer-service:8002"
    cache_service_url: str = "http://cache-service:8003"
    websocket_service_url: str = "http://websocket-service:8004"
    # stats_service_url: str = "http://stats-service:8005"
    
    # Redis для кэша и rate limiting
    redis_url: str = "redis://redis:6379"
    
    # Rate limiting
    rate_limit_per_minute: int = 60
    rate_limit_per_hour: int = 1000
    
    # CORS
    cors_origins: List[str] = ["*"]
    
    # WebSocket
    websocket_ping_interval: int = 20
    websocket_ping_timeout: int = 30
    
    # Timeouts
    request_timeout: int = 30
    service_timeout: int = 10
    
    # JWT
    jwt_secret_key: str = "your-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 30
    
    class Config:
        env_file = ".env"


settings = Settings()