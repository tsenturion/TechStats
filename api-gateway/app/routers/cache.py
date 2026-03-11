from fastapi import APIRouter, Depends, HTTPException, Query

from app.cache import cache_manager
from app.security import require_admin

router = APIRouter()


@router.get("/cache/stats")
async def cache_stats():
    return await cache_manager.get_stats()


@router.delete("/cache/clear")
async def clear_cache(
    pattern: str = Query("gateway:*"),
    _: dict = Depends(require_admin),
):
    try:
        cleared = await cache_manager.clear(pattern=pattern)
        return {"cleared": cleared, "pattern": pattern}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
