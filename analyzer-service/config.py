# C:\Users\user\Desktop\TechStats\analyzer-service\config.py
from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any


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
    patterns_database_url: str = "sqlite:///data/tech_patterns.db"
    
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

    # Celery
    celery_enabled: bool = False
    celery_broker_url: str = "redis://redis:6379/1"
    celery_result_backend: str = "redis://redis:6379/2"
    celery_task_default_queue: str = "techstats-analyzer"
    analyzer_internal_url: str = "http://analyzer-service:8002"
    
    # Технологические паттерны
    tech_patterns: Optional[Dict[str, Any]] = None
    
    class Config:
        env_file = ".env"


settings = Settings()
