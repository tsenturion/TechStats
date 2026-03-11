from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request, status

from app.auth_backend import (
    USERS_STORAGE_KEY,
    authenticate_user,
    ensure_bootstrap_users,
    get_user,
    normalize_username,
    register_user,
)
from app.cache import cache_manager
from app.runtime_config import get_runtime_settings_effective
from app.security import (
    UserRole,
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    get_current_user_optional,
    require_authenticated_user,
)

router = APIRouter()

# Backward-compatible alias used by tests and older integrations.
REGISTERED_USERS_KEY = USERS_STORAGE_KEY
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{3,64}$")
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128


def _validate_registration_fields(username: str, password: str) -> None:
    if not username or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="username and password are required")

    if not USERNAME_PATTERN.fullmatch(username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="username must be 3-64 chars and contain only letters, numbers, ., _, -",
        )

    if len(password) < MIN_PASSWORD_LENGTH or len(password) > MAX_PASSWORD_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"password length must be between {MIN_PASSWORD_LENGTH} and {MAX_PASSWORD_LENGTH}",
        )


async def _build_auth_response(username: str, role: UserRole) -> dict[str, Any]:
    runtime_settings = await get_runtime_settings_effective()
    access_expires_minutes = int(runtime_settings.get("auth_access_token_expire_minutes", 30))
    refresh_expires_minutes = int(runtime_settings.get("auth_refresh_token_expire_minutes", 60 * 24 * 7))

    access_token = create_access_token(
        username=username,
        role=role,
        expires_minutes=access_expires_minutes,
    )
    refresh_token = create_refresh_token(
        username=username,
        role=role,
        expires_minutes=refresh_expires_minutes,
    )

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "role": role.value,
        "username": username,
        "expires_in_minutes": access_expires_minutes,
        "refresh_expires_in_minutes": refresh_expires_minutes,
    }


@router.post("/auth/register")
async def register(payload: dict[str, Any] = Body(...)):
    await ensure_bootstrap_users()

    username_raw = str(payload.get("username", ""))
    password = str(payload.get("password", ""))
    username = normalize_username(username_raw)
    _validate_registration_fields(username, password)

    user_record = await register_user(username=username, password=password)
    role = UserRole(str(user_record.get("role", UserRole.user.value)))
    response = await _build_auth_response(username=username, role=role)
    response["registered"] = True
    return response


@router.post("/auth/login")
async def login(payload: dict[str, Any] = Body(...)):
    await ensure_bootstrap_users()

    username = normalize_username(str(payload.get("username", "")))
    password = str(payload.get("password", ""))
    if not username or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="username and password are required")

    user_record = await authenticate_user(username, password)
    if not user_record:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    role = UserRole(str(user_record.get("role", UserRole.user.value)))
    return await _build_auth_response(username=username, role=role)


@router.post("/auth/refresh")
async def refresh(payload: dict[str, Any] = Body(...)):
    refresh_token = str(payload.get("refresh_token", ""))
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="refresh_token is required")

    claims = decode_refresh_token(refresh_token)
    username = str(claims.get("sub", "")).strip().lower()
    role = UserRole(str(claims.get("role", UserRole.user.value)))

    existing = await get_user(username)
    if not existing:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    return await _build_auth_response(username=username, role=role)


@router.get("/auth/me")
async def me(request: Request):
    user = require_authenticated_user(request)
    return {
        "username": user.get("sub"),
        "role": user.get("role"),
        "authenticated": True,
    }


@router.get("/auth/public")
async def public_auth_info(request: Request):
    user = get_current_user_optional(request)
    return {
        "authenticated": bool(user),
        "role": user.get("role") if user else "guest",
        "username": user.get("sub") if user else None,
    }
