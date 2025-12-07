# C:\Users\user\Desktop\TechStats\vacancy-service\config.py
from pydantic_settings import BaseSettings
from typing import List, Optional
import os


class Settings(BaseSettings):
    # Основные настройки
    app_name: str = "TechStats Vacancy Service"
    debug: bool = True
    environment: str = "production"
    version: str = "1.0.0"
    
    # HH.ru API
    hh_api_base_url: str = "https://api.hh.ru"
    hh_api_user_agent: str = "TechStats/1.0 (admin@techstats.local)"
    hh_api_timeout: int = 30
    
    # Rate limiting для HH API
    hh_rate_limit_per_second: int = 7
    hh_rate_limit_per_day: int = 50000
    
    # Redis
    redis_url: str = "redis://redis:6379"
    cache_ttl_hours: int = 24
    search_cache_ttl_minutes: int = 5
    
    # Service settings
    port: int = 8001
    workers: int = 4
    log_level: str = "info"
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    class Config:
        env_file = ".env"


settings = Settings()