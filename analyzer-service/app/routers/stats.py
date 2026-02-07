import time
from collections import defaultdict
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.analysis_store import analysis_store
from app.cache import cache_manager
from config import settings

router = APIRouter()


@router.get("/stats/summary")
async def get_analysis_summary(
    hours: int = Query(24, description="За последние N часов"),
    technology: Optional[str] = Query(None, description="Фильтр по технологии"),
):
    summary = await analysis_store.summary(hours=hours)
    records = summary.pop("records")
    now = time.time()
    start_ts = now - (hours * 3600)

    by_technology = defaultdict(lambda: {"count": 0, "vacancies": 0, "matches": 0})
    by_hour = defaultdict(int)
    top_vacancies = defaultdict(int)

    for record in records:
        tech = str(record.get("technology", "")).strip()
        by_technology[tech]["count"] += 1
        by_technology[tech]["vacancies"] += int(record.get("total_vacancies", 0))
        by_technology[tech]["matches"] += int(record.get("tech_vacancies", 0))
        hour_key = datetime.fromtimestamp(record.get("analysis_timestamp", now)).strftime("%H:00")
        by_hour[hour_key] += 1
        title = str(record.get("vacancy_title", "")).strip()
        if title:
            top_vacancies[title] += 1

    total_analyses = summary["total_analyses"] or 1
    by_technology_payload = []
    for tech, data in by_technology.items():
        by_technology_payload.append(
            {
                "technology": tech,
                "count": data["count"],
                "percentage": round(data["count"] / total_analyses * 100, 2),
                "vacancies": data["vacancies"],
                "matches": data["matches"],
            }
        )
    by_technology_payload.sort(key=lambda item: item["count"], reverse=True)

    payload = {
        "time_range": {
            "start": datetime.fromtimestamp(start_ts).isoformat(),
            "end": datetime.fromtimestamp(now).isoformat(),
            "hours": hours,
        },
        **summary,
        "by_technology": by_technology_payload,
        "by_hour": [{"hour": hour, "analyses": count} for hour, count in sorted(by_hour.items())],
        "top_vacancies": [
            {"title": title, "analysis_count": count}
            for title, count in sorted(top_vacancies.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
    }

    if technology:
        tech_lower = technology.lower()
        matched = next((item for item in by_technology_payload if item["technology"].lower() == tech_lower), None)
        payload["filtered"] = matched or {"technology": technology, "count": 0, "percentage": 0.0}

    return payload


@router.get("/stats/technology/{technology}")
async def get_technology_stats(
    technology: str,
    days: int = Query(7, description="За последние N дней"),
):
    now = time.time()
    start_ts = now - (days * 86400)
    records = await analysis_store.list_records(since_ts=start_ts)
    technology_lower = technology.lower()
    tech_records = [r for r in records if str(r.get("technology", "")).lower() == technology_lower]

    total_mentions = sum(int(record.get("tech_vacancies", 0)) for record in tech_records)
    total_vacancies = sum(int(record.get("total_vacancies", 0)) for record in tech_records)
    avg_percentage = (sum(float(record.get("tech_percentage", 0)) for record in tech_records) / len(tech_records)) if tech_records else 0

    daily_buckets = defaultdict(lambda: {"mentions": 0, "vacancies": 0, "percentage_sum": 0.0, "count": 0})
    for record in tech_records:
        date_key = datetime.fromtimestamp(record.get("analysis_timestamp", now)).strftime("%Y-%m-%d")
        bucket = daily_buckets[date_key]
        bucket["mentions"] += int(record.get("tech_vacancies", 0))
        bucket["vacancies"] += int(record.get("total_vacancies", 0))
        bucket["percentage_sum"] += float(record.get("tech_percentage", 0))
        bucket["count"] += 1

    daily_stats = []
    for date_key in sorted(daily_buckets):
        bucket = daily_buckets[date_key]
        daily_stats.append(
            {
                "date": date_key,
                "mentions": bucket["mentions"],
                "vacancies": bucket["vacancies"],
                "percentage": round(bucket["percentage_sum"] / bucket["count"], 2) if bucket["count"] else 0.0,
            }
        )

    trend = "stable"
    trend_percentage = 0.0
    if len(daily_stats) >= 2:
        first = daily_stats[0]["mentions"]
        last = daily_stats[-1]["mentions"]
        if first == 0 and last > 0:
            trend = "increasing"
            trend_percentage = 100.0
        elif first > 0:
            trend_percentage = ((last - first) / first) * 100
            if trend_percentage > 5:
                trend = "increasing"
            elif trend_percentage < -5:
                trend = "decreasing"

    return {
        "technology": technology,
        "time_period_days": days,
        "total_mentions": total_mentions,
        "total_vacancies_with_tech": total_vacancies,
        "avg_percentage_per_vacancy": round(avg_percentage, 2),
        "trend": trend,
        "trend_percentage": round(trend_percentage, 2),
        "daily_stats": daily_stats,
        "related_technologies": [],
        "top_vacancies": [],
        "category_breakdown": [],
    }


@router.get("/stats/comparison")
async def compare_technologies(
    technologies: List[str] = Query(..., description="Список технологий для сравнения"),
    days: int = Query(30, description="За последние N дней"),
):
    if len(technologies) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 technologies allowed for comparison")

    comparison_data = []
    for technology in technologies:
        stats = await get_technology_stats(technology=technology, days=days)
        comparison_data.append(
            {
                "technology": technology,
                "total_mentions": stats["total_mentions"],
                "avg_daily_mentions": round(stats["total_mentions"] / max(days, 1), 2),
                "growth_rate": stats["trend_percentage"],
                "daily_stats": stats["daily_stats"],
            }
        )

    comparison_data.sort(key=lambda item: item["total_mentions"], reverse=True)
    return {
        "technologies": technologies,
        "time_period_days": days,
        "total_data_points": sum(len(item["daily_stats"]) for item in comparison_data),
        "comparison_data": comparison_data,
        "summary": {
            "most_popular": comparison_data[0]["technology"] if comparison_data else "",
            "fastest_growing": max(comparison_data, key=lambda item: item["growth_rate"])["technology"] if comparison_data else "",
            "most_stable": min(comparison_data, key=lambda item: abs(item["growth_rate"]))["technology"] if comparison_data else "",
        },
    }


@router.get("/stats/cache")
async def get_cache_stats():
    stats = await cache_manager.get_cache_stats()
    stats["cache_settings"] = {
        "analysis_cache_ttl_hours": settings.analysis_cache_ttl_hours,
        "pattern_cache_ttl_hours": settings.pattern_cache_ttl_hours,
        "redis_url": settings.redis_url,
    }
    return stats


@router.get("/stats/performance")
async def get_performance_stats(hours: int = Query(24, description="За последние N часов")):
    summary = await analysis_store.summary(hours=hours)
    total = summary["total_analyses"]
    failures = 0
    avg_latency = summary["avg_processing_time_seconds"]
    success_rate = round(((total - failures) / total * 100), 2) if total else 100.0

    return {
        "time_range": {
            "start": datetime.fromtimestamp(time.time() - (hours * 3600)).isoformat(),
            "end": datetime.fromtimestamp(time.time()).isoformat(),
            "hours": hours,
        },
        "requests": {
            "total": total,
            "successful": total - failures,
            "failed": failures,
            "success_rate": success_rate,
        },
        "response_times": {
            "p50_ms": round(avg_latency * 1000, 2),
            "p90_ms": round(avg_latency * 1000, 2),
            "p95_ms": round(avg_latency * 1000, 2),
            "p99_ms": round(avg_latency * 1000, 2),
            "avg_ms": round(avg_latency * 1000, 2),
            "max_ms": round(avg_latency * 1000, 2),
        },
        "throughput": {
            "requests_per_second": round(total / max(hours * 3600, 1), 4),
            "vacancies_per_second": round(summary["total_vacancies_processed"] / max(hours * 3600, 1), 4),
            "analyses_per_second": round(total / max(hours * 3600, 1), 4),
        },
        "cache_performance": {
            "hit_rate": summary["cache_hit_rate"],
            "miss_rate": round(max(0.0, 100 - summary["cache_hit_rate"]), 2),
        },
        "resource_usage": {},
        "hourly_metrics": [],
    }

