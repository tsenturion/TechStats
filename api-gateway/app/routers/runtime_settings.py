from typing import Any, Dict

from fastapi import APIRouter, Body, HTTPException, Request, status

from app.runtime_config import (
    get_runtime_settings_effective,
    get_runtime_settings_raw,
    get_runtime_settings_schema,
    reset_runtime_settings,
    update_runtime_settings,
)
from app.security import UserRole, require_any_role

router = APIRouter()


@router.get("/runtime-settings/public")
async def get_runtime_public():
    effective = await get_runtime_settings_effective()
    schema = get_runtime_settings_schema()
    return {
        "settings": effective,
        "schema": schema,
    }


@router.get("/runtime-settings")
async def get_runtime_settings(request: Request):
    require_any_role(request, [UserRole.user, UserRole.admin])
    effective = await get_runtime_settings_effective()
    return {"settings": effective}


@router.get("/admin/runtime-settings")
async def get_runtime_settings_admin(request: Request):
    user = require_any_role(request, [UserRole.admin])
    effective = await get_runtime_settings_effective()
    raw = await get_runtime_settings_raw()
    schema = get_runtime_settings_schema()
    return {
        "settings": effective,
        "overrides": raw,
        "schema": schema,
        "updated_by": user.get("sub"),
    }


@router.put("/admin/runtime-settings")
async def update_runtime_settings_admin(request: Request, payload: Dict[str, Any] = Body(...)):
    require_any_role(request, [UserRole.admin])

    updates = payload.get("updates")
    if not isinstance(updates, dict) or not updates:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="updates object is required")

    try:
        effective = await update_runtime_settings(updates)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"validation_errors": exc.args[0]}) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {
        "success": True,
        "settings": effective,
    }


@router.post("/admin/runtime-settings/reset")
async def reset_runtime_settings_admin(request: Request):
    require_any_role(request, [UserRole.admin])

    try:
        effective = await reset_runtime_settings()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

    return {
        "success": True,
        "settings": effective,
    }

