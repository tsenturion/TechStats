# C:\Users\user\Desktop\TechStats\analyzer-service\app\routers\stats.py
import asyncio
import time
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Query, Depends, Request
import structlog

from config import settings
from app.cache import cache_manager

router = APIRouter()
logger = structlog.get_logger()


@router.get("/stats/summary")
async def get_analysis_summary(
    hours: int = Query(24, description="За последние N часов"),
    technology: Optional[str] = Query(None, description="Фильтр по технологии")
):
    """Получение сводной статистики по анализам"""
    try:
        # В реальном приложении здесь был бы запрос к БД
        # Для примера возвращаем фиктивные данные
        
        now = time.time()
        start_time = now - (hours * 3600)
        
        # Эмулируем данные из "БД"
        summary = {
            "time_range": {
                "start": datetime.fromtimestamp(start_time).isoformat(),
                "end": datetime.fromtimestamp(now).isoformat(),
                "hours": hours
            },
            "total_analyses": 1250,
            "total_vacancies_processed": 125000,
            "total_technologies_found": 45600,
            "avg_processing_time_seconds": 2.5,
            "cache_hit_rate": 78.5,
            "by_technology": [
                {"technology": "Python", "count": 450, "percentage": 36.0},
                {"technology": "Java", "count": 320, "percentage": 25.6},
                {"technology": "JavaScript", "count": 280, "percentage": 22.4},
                {"technology": "SQL", "count": 150, "percentage": 12.0},
                {"technology": "Docker", "count": 50, "percentage": 4.0}
            ],
            "by_hour": [
                {"hour": f"{i}:00", "analyses": 50 + i*10}
                for i in range(24)
            ],
            "top_vacancies": [
                {"title": "Python Developer", "analysis_count": 120},
                {"title": "Java Backend Developer", "analysis_count": 95},
                {"title": "Full Stack Developer", "analysis_count": 80},
                {"title": "Data Engineer", "analysis_count": 75},
                {"title": "DevOps Engineer", "analysis_count": 60}
            ]
        }
        
        # Фильтрация по технологии если указана
        if technology:
            tech_summary = next(
                (item for item in summary["by_technology"] if item["technology"].lower() == technology.lower()),
                None
            )
            
            if tech_summary:
                summary["filtered"] = {
                    "technology": technology,
                    "count": tech_summary["count"],
                    "percentage": tech_summary["percentage"]
                }
            else:
                summary["filtered"] = {
                    "technology": technology,
                    "count": 0,
                    "percentage": 0.0
                }
        
        return summary
        
    except Exception as e:
        logger.error("Failed to get analysis summary", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get analysis summary: {str(e)}")


@router.get("/stats/technology/{technology}")
async def get_technology_stats(
    technology: str,
    days: int = Query(7, description="За последние N дней")
):
    """Получение статистики по конкретной технологии"""
    try:
        # Эмуляция данных
        now = datetime.now()
        
        stats = {
            "technology": technology,
            "time_period_days": days,
            "total_mentions": 1250,
            "total_vacancies_with_tech": 950,
            "avg_percentage_per_vacancy": 45.5,
            "trend": "increasing",  # increasing, decreasing, stable
            "trend_percentage": 12.5,
            "daily_stats": [
                {
                    "date": (now - timedelta(days=i)).strftime("%Y-%m-%d"),
                    "mentions": 100 + i*20,
                    "vacancies": 80 + i*15,
                    "percentage": 40.0 + i*1.5
                }
                for i in range(days-1, -1, -1)
            ],
            "related_technologies": [
                {"technology": "Django", "correlation": 0.85},
                {"technology": "Flask", "correlation": 0.78},
                {"technology": "PostgreSQL", "correlation": 0.72},
                {"technology": "Docker", "correlation": 0.65},
                {"technology": "AWS", "correlation": 0.58}
            ],
            "top_vacancies": [
                {"title": "Senior Python Developer", "mentions": 45},
                {"title": "Python Data Engineer", "mentions": 38},
                {"title": "Backend Python Developer", "mentions": 32},
                {"title": "Python DevOps", "mentions": 28},
                {"title": "ML Engineer Python", "mentions": 25}
            ],
            "category_breakdown": [
                {"category": "Programming Language", "percentage": 60.0},
                {"category": "Web Framework", "percentage": 25.0},
                {"category": "Data Science", "percentage": 10.0},
                {"category": "DevOps", "percentage": 5.0}
            ]
        }
        
        return stats
        
    except Exception as e:
        logger.error("Failed to get technology stats", technology=technology, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get technology stats: {str(e)}")


@router.get("/stats/comparison")
async def compare_technologies(
    technologies: List[str] = Query(..., description="Список технологий для сравнения"),
    days: int = Query(30, description="За последние N дней")
):
    """Сравнение нескольких технологий"""
    try:
        if len(technologies) > 10:
            raise HTTPException(
                status_code=400,
                detail="Maximum 10 technologies allowed for comparison"
            )
        
        # Эмуляция данных сравнения
        now = datetime.now()
        
        comparison = {
            "technologies": technologies,
            "time_period_days": days,
            "total_data_points": 15000,
            "comparison_data": [],
            "summary": {
                "most_popular": "",
                "fastest_growing": "",
                "most_stable": ""
            }
        }
        
        # Генерация данных для каждой технологии
        base_values = {
            "Python": {"base": 1000, "growth": 0.15},
            "Java": {"base": 800, "growth": 0.05},
            "JavaScript": {"base": 900, "growth": 0.12},
            "C++": {"base": 400, "growth": -0.02},
            "Go": {"base": 300, "growth": 0.25},
            "Rust": {"base": 200, "growth": 0.30},
            "TypeScript": {"base": 600, "growth": 0.20},
            "Kotlin": {"base": 350, "growth": 0.18},
            "Swift": {"base": 250, "growth": 0.10},
            "PHP": {"base": 500, "growth": -0.05}
        }
        
        for tech in technologies:
            tech_data = base_values.get(tech, {"base": 200, "growth": 0.10})
            
            daily_stats = []
            for i in range(days-1, -1, -1):
                day_value = tech_data["base"] * (1 + tech_data["growth"] * (i / days))
                daily_stats.append({
                    "date": (now - timedelta(days=i)).strftime("%Y-%m-%d"),
                    "mentions": int(day_value),
                    "percentage": (day_value / 5000) * 100  # Относительно общего объема
                })
            
            comparison["comparison_data"].append({
                "technology": tech,
                "total_mentions": sum(day["mentions"] for day in daily_stats),
                "avg_daily_mentions": sum(day["mentions"] for day in daily_stats) / days,
                "growth_rate": tech_data["growth"] * 100,
                "daily_stats": daily_stats
            })
        
        # Определение summary
        if comparison["comparison_data"]:
            sorted_by_total = sorted(
                comparison["comparison_data"],
                key=lambda x: x["total_mentions"],
                reverse=True
            )
            sorted_by_growth = sorted(
                comparison["comparison_data"],
                key=lambda x: x["growth_rate"],
                reverse=True
            )
            
            comparison["summary"] = {
                "most_popular": sorted_by_total[0]["technology"],
                "fastest_growing": sorted_by_growth[0]["technology"],
                "most_stable": sorted_by_growth[-1]["technology"]  # Минимальный рост
            }
        
        return comparison
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to compare technologies", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to compare technologies: {str(e)}")


@router.get("/stats/cache")
async def get_cache_stats():
    """Получение статистики кэша"""
    try:
        stats = await cache_manager.get_cache_stats()
        
        # Добавляем дополнительную информацию
        stats["cache_settings"] = {
            "analysis_cache_ttl_hours": settings.analysis_cache_ttl_hours,
            "pattern_cache_ttl_hours": settings.pattern_cache_ttl_hours,
            "redis_url": settings.redis_url
        }
        
        return stats
        
    except Exception as e:
        logger.error("Failed to get cache stats", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get cache stats: {str(e)}")


@router.get("/stats/performance")
async def get_performance_stats(
    hours: int = Query(24, description="За последние N часов")
):
    """Получение статистики производительности"""
    try:
        # Эмуляция метрик производительности
        now = time.time()
        start_time = now - (hours * 3600)
        
        performance = {
            "time_range": {
                "start": datetime.fromtimestamp(start_time).isoformat(),
                "end": datetime.fromtimestamp(now).isoformat(),
                "hours": hours
            },
            "requests": {
                "total": 12500,
                "successful": 12200,
                "failed": 300,
                "success_rate": 97.6
            },
            "response_times": {
                "p50_ms": 120,
                "p90_ms": 250,
                "p95_ms": 350,
                "p99_ms": 500,
                "avg_ms": 150,
                "max_ms": 1200
            },
            "throughput": {
                "requests_per_second": 0.14,
                "vacancies_per_second": 1.45,
                "analyses_per_second": 0.08
            },
            "cache_performance": {
                "hit_rate": 78.5,
                "miss_rate": 21.5,
                "avg_cache_time_ms": 5,
                "avg_db_time_ms": 45
            },
            "resource_usage": {
                "cpu_percent": 45.2,
                "memory_mb": 256.8,
                "active_connections": 24,
                "queue_length": 3
            },
            "hourly_metrics": [
                {
                    "hour": f"{i}:00",
                    "requests": 500 + i*20,
                    "avg_response_time_ms": 120 + i*5,
                    "cache_hit_rate": 75.0 + i*0.5,
                    "errors": 10 + i
                }
                for i in range(24)
            ]
        }
        
        return performance
        
    except Exception as e:
        logger.error("Failed to get performance stats", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get performance stats: {str(e)}")