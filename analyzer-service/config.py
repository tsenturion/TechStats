# C:\Users\user\Desktop\TechStats\analyzer-service\config.py
from pydantic_settings import BaseSettings
from typing import List, Optional, Dict, Any
import os


class Settings(BaseSettings):
    # Основные настройки
    app_name: str = "TechStats Analyzer Service"
    debug: bool = True
    environment: str = "production"
    version: str = "1.0.0"
    
    # Сервисы
    vacancy_service_url: str = "http://vacancy-service:8001"
    redis_url: str = "redis://redis:6379"
    
    # Настройки анализа
    max_workers: int = 5
    batch_size: int = 10
    request_timeout: int = 30
    
    # Кэширование
    analysis_cache_ttl_hours: int = 24
    pattern_cache_ttl_hours: int = 168  # 7 дней
    tech_patterns_file: str = "data/tech_patterns.json"
    
    # NLP настройки
    enable_stemming: bool = True
    enable_lemmatization: bool = True
    remove_stopwords: bool = True
    language: str = "ru"
    
    # Service settings
    port: int = 8002
    workers: int = 4
    log_level: str = "info"
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Технологические паттерны
    tech_patterns: Optional[Dict[str, Any]] = None
    
    class Config:
        env_file = ".env"


settings = Settings()