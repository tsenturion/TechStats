from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Set

from authlib.jose import JoseError, jwt
from fastapi import HTTPException, Request, status

from config import settings
from shared.rbac import build_enforcer
from shared.runtime_settings import RUNTIME_SETTINGS_KEY, build_effective_runtime_settings


class UserRole(str, Enum):
    user = "user"
    admin = "admin"


rbac_enforcer = build_enforcer()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_role(value: Any) -> Optional[UserRole]:
    if value is None:
        return None
    try:
        return UserRole(str(value).lower())
    except Exception:  # noqa: BLE001
        return None


def _encode_jwt(payload: Dict[str, Any]) -> str:
    token = jwt.encode({"alg": settings.jwt_algorithm}, payload, settings.jwt_secret_key)
    if isinstance(token, bytes):
        return token.decode("utf-8")
    return str(token)


def _decode_jwt(token: str) -> Dict[str, Any]:
    claims = jwt.decode(token, settings.jwt_secret_key)
    claims.validate()
    return dict(claims)


def decode_access_token(token: str) -> Dict[str, Any]:
    try:
        payload = _decode_jwt(token)
    except JoseError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc

    if payload.get("typ") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid access token type")

    role = _normalize_role(payload.get("role"))
    if role is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token role")

    subject = payload.get("sub")
    if not subject:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token subject")

    payload["role"] = role.value
    return payload


def decode_refresh_token(token: str) -> Dict[str, Any]:
    try:
        payload = _decode_jwt(token)
    except JoseError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token") from exc

    if payload.get("typ") != "refresh":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token type")

    role = _normalize_role(payload.get("role"))
    if role is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token role")

    subject = payload.get("sub")
    if not subject:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token subject")

    payload["role"] = role.value
    return payload


def create_access_token(
    *,
    username: str,
    role: UserRole,
    expires_minutes: int,
) -> str:
    now = _utcnow()
    payload = {
        "sub": username,
        "role": role.value,
        "typ": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expires_minutes)).timestamp()),
    }
    return _encode_jwt(payload)


def create_refresh_token(
    *,
    username: str,
    role: UserRole,
    expires_minutes: int,
) -> str:
    now = _utcnow()
    payload = {
        "sub": username,
        "role": role.value,
        "typ": "refresh",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expires_minutes)).timestamp()),
    }
    return _encode_jwt(payload)


def get_current_user_optional(request: Request) -> Optional[Dict[str, Any]]:
    user = getattr(request.state, "user", None)
    if isinstance(user, dict):
        role = _normalize_role(user.get("role"))
        if role:
            user["role"] = role.value
            return user
    return None


def require_authenticated_user(request: Request) -> Dict[str, Any]:
    user = get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    return user


def require_any_role(request: Request, allowed_roles: Iterable[UserRole]) -> Dict[str, Any]:
    user = require_authenticated_user(request)
    allowed: Set[str] = {role.value for role in allowed_roles}
    if user.get("role") not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not enough permissions")
    return user


def require_user_or_admin(request: Request) -> Dict[str, Any]:
    return require_any_role(request, [UserRole.user, UserRole.admin])


def require_admin(request: Request) -> Dict[str, Any]:
    return require_any_role(request, [UserRole.admin])


def enforce_rbac(role: str, path: str, method: str) -> bool:
    normalized_role = role.lower() if role else "guest"
    normalized_method = method.upper()
    return bool(rbac_enforcer.enforce(normalized_role, path, normalized_method))


async def read_runtime_settings_for_auth(cache_manager) -> Dict[str, Any]:
    raw = await cache_manager.get(RUNTIME_SETTINGS_KEY)
    if not isinstance(raw, dict):
        return build_effective_runtime_settings()
    return build_effective_runtime_settings(raw)

