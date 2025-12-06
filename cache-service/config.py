# C:\Users\user\Desktop\TechStats\cache-service\config.py
from pydantic_settings import BaseSettings
from typing import List, Optional, Dict, Any, Union
from enum import Enum
import os


class CacheBackend(str, Enum):
    REDIS = "redis"
    REDIS_CLUSTER = "redis_cluster"
    MEMORY = "memory"
    MONGO = "mongo"


class CacheStrategy(str, Enum):
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    RANDOM = "random"


class Settings(BaseSettings):
    # Основные настройки
    app_name: str = "TechStats Cache Service"
    debug: bool = False
    environment: str = "production"
    version: str = "1.0.0"
    
    # Redis настройки
    redis_url: str = "redis://redis:6379"
    redis_cluster_nodes: List[str] = [
        "redis://redis-1:6379",
        "redis://redis-2:6379",
        "redis://redis-3:6379"
    ]
    redis_max_connections: int = 100
    redis_health_check_interval: int = 30
    
    # MongoDB настройки (для persistent cache)
    mongo_url: str = "mongodb://mongo:27017"
    mongo_database: str = "cache_db"
    mongo_collection: str = "cache_items"
    
    # Выбор бэкенда
    cache_backend: CacheBackend = CacheBackend.REDIS
    cache_strategy: CacheStrategy = CacheStrategy.LRU
    
    # Настройки кэша
    default_ttl_seconds: int = 3600  # 1 час по умолчанию
    max_cache_size_mb: int = 1024    # 1 GB max
    max_item_size_kb: int = 1024     # 1 MB max на элемент
    
    # Настройки очистки
    cleanup_interval_seconds: int = 300  # 5 минут
    cleanup_batch_size: int = 1000
    
    # Настройки кластера
    enable_clustering: bool = False
    cluster_nodes: List[str] = []
    node_id: str = "cache-node-1"
    
    # Rate limiting для API
    api_rate_limit_per_minute: int = 1000
    api_rate_limit_per_hour: int = 10000
    
    # Service settings
    port: int = 8003
    workers: int = 4
    log_level: str = "info"
    
    # Monitoring
    enable_prometheus: bool = True
    metrics_port: int = 9091
    
    class Config:
        env_file = ".env"


settings = Settings()