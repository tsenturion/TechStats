# C:\Users\user\Desktop\TechStats\websocket-service\config.py
from pydantic_settings import BaseSettings
from typing import List, Optional, Dict, Any
import os


class Settings(BaseSettings):
    # Основные настройки
    app_name: str = "TechStats WebSocket Service"
    debug: bool = False
    environment: str = "production"
    version: str = "1.0.0"
    
    # Сервисы
    analyzer_service_url: str = "http://analyzer-service:8002"
    cache_service_url: str = "http://cache-service:8003"
    vacancy_service_url: str = "http://vacancy-service:8001"
    redis_url: str = "redis://redis:6379"
    
    # WebSocket настройки
    websocket_ping_interval: int = 20
    websocket_ping_timeout: int = 30
    websocket_max_message_size: int = 16 * 1024 * 1024  # 16 MB
    websocket_queue_size: int = 1000
    
    # Connection management
    max_connections_per_ip: int = 10
    max_total_connections: int = 1000
    connection_timeout: int = 300  # 5 minutes
    
    # Session management
    session_ttl_seconds: int = 3600  # 1 hour
    cleanup_interval_seconds: int = 60
    
    # Analysis progress
    progress_update_interval: float = 0.5  # seconds
    batch_size_for_progress: int = 10
    
    # Service settings
    port: int = 8004
    workers: int = 4
    log_level: str = "info"
    
    # JWT для аутентификации
    jwt_secret_key: str = "your-websocket-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 60
    
    # CORS
    cors_origins: List[str] = ["*"]
    
    class Config:
        env_file = ".env"


settings = Settings()