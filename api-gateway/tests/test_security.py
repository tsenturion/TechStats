from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException
from jose import jwt
from starlette.requests import Request

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if "app" in sys.modules and not hasattr(sys.modules["app"], "__path__"):
    sys.modules.pop("app", None)

from app.security import (
    UserRole,
    create_access_token,
    decode_access_token,
    get_current_user_optional,
    read_runtime_settings_for_auth,
    require_admin,
    require_any_role,
    require_user_or_admin,
)
from config import settings


def _request_with_user(user_payload=None):
    request = Request({"type": "http", "headers": []})
    if user_payload is not None:
        request.state.user = user_payload
    return request


def test_create_and_decode_access_token_roundtrip():
    token = create_access_token(username="alice", role=UserRole.admin, expires_minutes=15)
    payload = decode_access_token(token)

    assert payload["sub"] == "alice"
    assert payload["role"] == "admin"
    assert "exp" in payload
    assert "iat" in payload


def test_decode_access_token_rejects_invalid_token():
    with pytest.raises(HTTPException) as exc:
        decode_access_token("not-a-jwt")
    assert exc.value.status_code == 401


def test_decode_access_token_rejects_invalid_role():
    now = datetime.now(timezone.utc)
    token = jwt.encode(
        {
            "sub": "alice",
            "role": "guest",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
        },
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )

    with pytest.raises(HTTPException) as exc:
        decode_access_token(token)
    assert exc.value.status_code == 401


def test_get_current_user_optional_normalizes_role():
    request = _request_with_user({"sub": "bob", "role": "ADMIN"})
    user = get_current_user_optional(request)
    assert user == {"sub": "bob", "role": "admin"}


def test_require_any_role_success_for_user():
    request = _request_with_user({"sub": "john", "role": "user"})
    user = require_any_role(request, [UserRole.user, UserRole.admin])
    assert user["sub"] == "john"


def test_require_user_or_admin_rejects_guest_and_missing_user():
    request_without_user = _request_with_user()
    with pytest.raises(HTTPException) as exc_missing:
        require_user_or_admin(request_without_user)
    assert exc_missing.value.status_code == 401

    request_guest = _request_with_user({"sub": "guest", "role": "guest"})
    with pytest.raises(HTTPException) as exc_guest:
        require_user_or_admin(request_guest)
    assert exc_guest.value.status_code == 401


def test_require_admin_rejects_user_role():
    request = _request_with_user({"sub": "jane", "role": "user"})
    with pytest.raises(HTTPException) as exc:
        require_admin(request)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_read_runtime_settings_for_auth_fallback_and_override():
    class DummyCache:
        def __init__(self, value):
            self._value = value

        async def get(self, key):
            return self._value

    fallback = await read_runtime_settings_for_auth(DummyCache("not-a-dict"))
    assert "auth_access_token_expire_minutes" in fallback

    overridden = await read_runtime_settings_for_auth(DummyCache({"auth_access_token_expire_minutes": 120}))
    assert overridden["auth_access_token_expire_minutes"] == 120
