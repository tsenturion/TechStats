from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, status
from fastapi_users.password import PasswordHelper

from app.cache import cache_manager
from config import settings

USERS_STORAGE_KEY = "techstats:auth:users:v2"
password_helper = PasswordHelper()
_in_memory_users: dict[str, dict[str, Any]] = {}


def normalize_username(username: str) -> str:
    return str(username or "").strip().lower()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_user_record(*, username: str, password: str, role: str) -> dict[str, Any]:
    return {
        "id": str(uuid4()),
        "username": username,
        "email": f"{username}@techstats.local",
        "password_hash": password_helper.hash(password),
        "role": role,
        "is_active": True,
        "is_superuser": role == "admin",
        "is_verified": True,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }


async def _read_users() -> dict[str, dict[str, Any]]:
    raw = await cache_manager.get(USERS_STORAGE_KEY)
    source = raw if isinstance(raw, dict) else _in_memory_users
    users: dict[str, dict[str, Any]] = {}
    for username, payload in source.items():
        if isinstance(payload, dict):
            users[normalize_username(username)] = payload
    return users


async def _write_users(users: dict[str, dict[str, Any]]) -> None:
    _in_memory_users.clear()
    _in_memory_users.update(users)

    ok = await cache_manager.set(USERS_STORAGE_KEY, users, ttl=None)
    if not ok and cache_manager.redis_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Auth storage is unavailable",
        )


async def ensure_bootstrap_users() -> None:
    users = await _read_users()
    changed = False

    admin_username = normalize_username(settings.admin_username)
    if admin_username and admin_username not in users:
        users[admin_username] = _build_user_record(
            username=admin_username,
            password=settings.admin_password,
            role="admin",
        )
        changed = True

    default_user = normalize_username(settings.user_username)
    if default_user and default_user not in users:
        users[default_user] = _build_user_record(
            username=default_user,
            password=settings.user_password,
            role="user",
        )
        changed = True

    if changed:
        await _write_users(users)


async def register_user(username: str, password: str) -> dict[str, Any]:
    username = normalize_username(username)
    if not username:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="username is required")

    await ensure_bootstrap_users()
    users = await _read_users()
    if username in users:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already exists")

    users[username] = _build_user_record(username=username, password=password, role="user")
    await _write_users(users)
    return users[username]


async def get_user(username: str) -> dict[str, Any] | None:
    await ensure_bootstrap_users()
    users = await _read_users()
    return users.get(normalize_username(username))


async def authenticate_user(username: str, password: str) -> dict[str, Any] | None:
    user = await get_user(username)
    if not user:
        return None

    valid, updated_hash = password_helper.verify_and_update(password, str(user.get("password_hash", "")))
    if not valid:
        return None

    if updated_hash:
        users = await _read_users()
        key = normalize_username(username)
        if key in users:
            users[key]["password_hash"] = updated_hash
            users[key]["updated_at"] = _now_iso()
            await _write_users(users)

    return user
