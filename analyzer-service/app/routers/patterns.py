# C:\Users\user\Desktop\TechStats\analyzer-service\app\routers\patterns.py
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Body, Query, Depends, Request
import structlog

from app.tech_patterns import TechPatternsLoader

router = APIRouter()
logger = structlog.get_logger()


async def get_patterns_loader(request: Request) -> TechPatternsLoader:
    """Dependency для получения TechPatternsLoader"""
    return request.app.state.patterns_loader


@router.get("/patterns")
async def get_all_patterns(
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader),
    category: Optional[str] = Query(None, description="Фильтр по категории")
):
    """Получение всех паттернов технологий"""
    try:
        if category:
            patterns = patterns_loader.get_technologies_by_category(category)
        else:
            patterns_data = patterns_loader.get_all_patterns()
            patterns = [
                {**tech_data, "id": tech_id}
                for tech_id, tech_data in patterns_data.items()
            ]
        
        return {
            "total_patterns": len(patterns),
            "categories": patterns_loader.get_categories(),
            "patterns": patterns
        }
    except Exception as e:
        logger.error("Failed to get patterns", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get patterns: {str(e)}")


@router.get("/patterns/{technology}")
async def get_pattern(
    technology: str,
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Получение паттерна по названию технологии"""
    try:
        pattern = patterns_loader.get_pattern(technology)
        if not pattern:
            raise HTTPException(status_code=404, detail="Pattern not found")
        
        return {
            "technology": technology,
            "pattern": pattern,
            "compiled": patterns_loader.get_compiled_pattern(technology) is not None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get pattern", technology=technology, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get pattern: {str(e)}")


@router.post("/patterns")
async def add_pattern(
    request: Request,
    pattern_data: Dict[str, Any] = Body(...),
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Добавление нового паттерна"""
    try:
        required_fields = ["id", "name", "patterns"]
        for field in required_fields:
            if field not in pattern_data:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required field: {field}"
                )
        
        tech_id = pattern_data["id"]
        name = pattern_data["name"]
        patterns = pattern_data["patterns"]
        category = pattern_data.get("category", "other")
        aliases = pattern_data.get("aliases", [])
        weight = pattern_data.get("weight", 1.0)
        description = pattern_data.get("description", "")
        
        if not isinstance(patterns, list) or len(patterns) == 0:
            raise HTTPException(
                status_code=400,
                detail="Patterns must be a non-empty list"
            )
        
        success = patterns_loader.add_pattern(
            tech_id=tech_id,
            name=name,
            patterns=patterns,
            category=category,
            aliases=aliases,
            weight=weight,
            description=description
        )
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Pattern with id '{tech_id}' already exists"
            )
        
        # Сохранение в файл и кэш
        await patterns_loader.save_and_cache()
        
        return {
            "success": True,
            "message": f"Pattern '{name}' added successfully",
            "pattern_id": tech_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to add pattern", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to add pattern: {str(e)}")


@router.put("/patterns/{tech_id}")
async def update_pattern(
    tech_id: str,
    request: Request,
    update_data: Dict[str, Any] = Body(...),
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Обновление существующего паттерна"""
    try:
        # Проверка существования
        existing_pattern = patterns_loader.get_pattern(tech_id)
        if not existing_pattern:
            raise HTTPException(status_code=404, detail="Pattern not found")
        
        # Удаление старого паттерна
        patterns_loader.remove_pattern(tech_id)
        
        # Добавление обновленного
        new_tech_id = update_data.get("id", tech_id)
        name = update_data.get("name", existing_pattern.get("name", tech_id))
        patterns = update_data.get("patterns", existing_pattern.get("patterns", []))
        category = update_data.get("category", existing_pattern.get("category", "other"))
        aliases = update_data.get("aliases", existing_pattern.get("aliases", []))
        weight = update_data.get("weight", existing_pattern.get("weight", 1.0))
        description = update_data.get("description", existing_pattern.get("description", ""))
        
        success = patterns_loader.add_pattern(
            tech_id=new_tech_id,
            name=name,
            patterns=patterns,
            category=category,
            aliases=aliases,
            weight=weight,
            description=description
        )
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to update pattern"
            )
        
        # Сохранение в файл и кэш
        await patterns_loader.save_and_cache()
        
        return {
            "success": True,
            "message": f"Pattern '{name}' updated successfully",
            "pattern_id": new_tech_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update pattern", tech_id=tech_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to update pattern: {str(e)}")


@router.delete("/patterns/{tech_id}")
async def delete_pattern(
    tech_id: str,
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Удаление паттерна"""
    try:
        success = patterns_loader.remove_pattern(tech_id)
        if not success:
            raise HTTPException(status_code=404, detail="Pattern not found")
        
        # Сохранение в файл и кэш
        await patterns_loader.save_and_cache()
        
        return {
            "success": True,
            "message": f"Pattern '{tech_id}' deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete pattern", tech_id=tech_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to delete pattern: {str(e)}")


@router.get("/patterns/categories")
async def get_categories(
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Получение всех категорий технологий"""
    try:
        categories = patterns_loader.get_categories()
        
        # Подсчет технологий по категориям
        category_stats = []
        patterns_data = patterns_loader.get_all_patterns()
        
        for category in categories:
            tech_count = sum(
                1 for tech_data in patterns_data.values()
                if tech_data.get("category") == category
            )
            category_stats.append({
                "category": category,
                "technology_count": tech_count
            })
        
        return {
            "total_categories": len(categories),
            "categories": category_stats
        }
    except Exception as e:
        logger.error("Failed to get categories", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get categories: {str(e)}")


@router.post("/patterns/search")
async def search_patterns(
    request: Request,
    search_query: Dict[str, Any] = Body(...),
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Поиск паттернов по запросу"""
    try:
        query = search_query.get("query", "").lower()
        category = search_query.get("category")
        limit = search_query.get("limit", 20)
        
        if not query and not category:
            raise HTTPException(
                status_code=400,
                detail="Either query or category must be provided"
            )
        
        patterns_data = patterns_loader.get_all_patterns()
        results = []
        
        for tech_id, tech_data in patterns_data.items():
            # Фильтрация по категории
            if category and tech_data.get("category") != category:
                continue
            
            # Поиск по запросу
            if query:
                # Поиск в названии
                name_match = query in tech_data.get("name", "").lower()
                # Поиск в ID
                id_match = query in tech_id.lower()
                # Поиск в алиасах
                alias_match = any(query in alias.lower() for alias in tech_data.get("aliases", []))
                # Поиск в описании
                desc_match = query in tech_data.get("description", "").lower()
                
                if not (name_match or id_match or alias_match or desc_match):
                    continue
            
            results.append({
                "id": tech_id,
                **tech_data
            })
        
        # Ограничение результатов
        results = results[:limit]
        
        return {
            "query": query,
            "category": category,
            "total_found": len(results),
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to search patterns", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to search patterns: {str(e)}")


@router.get("/patterns/stats")
async def get_patterns_stats(
    patterns_loader: TechPatternsLoader = Depends(get_patterns_loader)
):
    """Получение статистики по паттернам"""
    try:
        patterns_data = patterns_loader.get_all_patterns()
        
        # Базовая статистика
        total_patterns = len(patterns_data)
        total_aliases = sum(len(tech_data.get("aliases", [])) for tech_data in patterns_data.values())
        
        # Статистика по категориям
        category_stats = {}
        for tech_data in patterns_data.values():
            category = tech_data.get("category", "unknown")
            category_stats[category] = category_stats.get(category, 0) + 1
        
        # Самые популярные паттерны (по количеству алиасов)
        patterns_with_aliases = [
            {
                "id": tech_id,
                "name": tech_data.get("name", tech_id),
                "aliases_count": len(tech_data.get("aliases", [])),
                "patterns_count": len(tech_data.get("patterns", []))
            }
            for tech_id, tech_data in patterns_data.items()
        ]
        
        patterns_with_aliases.sort(key=lambda x: x["aliases_count"], reverse=True)
        top_patterns = patterns_with_aliases[:10]
        
        return {
            "total_patterns": total_patterns,
            "total_aliases": total_aliases,
            "categories": {
                "count": len(category_stats),
                "distribution": [
                    {"category": cat, "count": count}
                    for cat, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True)
                ]
            },
            "top_patterns": top_patterns,
            "patterns_per_category_avg": total_patterns / len(category_stats) if category_stats else 0
        }
    except Exception as e:
        logger.error("Failed to get patterns stats", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get patterns stats: {str(e)}")