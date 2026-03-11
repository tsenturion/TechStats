from pathlib import Path
import sys

import pytest
from fastapi import HTTPException
from jose import jwt

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if "app" in sys.modules and not hasattr(sys.modules["app"], "__path__"):
    sys.modules.pop("app", None)

from app.routers.analyzer import _normalize_analysis_request
from app.routers.websocket import (
    _assert_ws_user_role,
    _extract_bearer_token_from_header,
    _normalize_ws_payload,
)
from app.security import UserRole, create_access_token
from config import settings


def test_normalize_analysis_request_applies_limits_and_defaults():
    runtime_settings = {
        "analysis_max_pages_hard_limit": 5,
        "analysis_per_page_hard_limit": 20,
        "analysis_default_use_cache": True,
        "search_default_max_pages": 3,
        "search_default_per_page": 10,
        "search_default_exact": True,
        "search_default_area": 113,
    }

    payload = _normalize_analysis_request(
        {
            "vacancy_title": "Python Developer",
            "technology": "react",
            "max_pages": 999,
            "per_page": 999,
            "use_cache": False,
        },
        runtime_settings,
    )

    assert payload["max_pages"] == 5
    assert payload["per_page"] == 20
    assert payload["use_cache"] is False
    assert payload["exact_search"] is True
    assert payload["area"] == 113


def test_extract_bearer_token_from_header():
    assert _extract_bearer_token_from_header("") == ""
    assert _extract_bearer_token_from_header("Basic abc") == ""
    assert _extract_bearer_token_from_header("Bearer my-token") == "my-token"


def test_normalize_ws_payload_applies_defaults_and_limits():
    runtime_settings = {
        "analysis_max_pages_hard_limit": 4,
        "analysis_per_page_hard_limit": 15,
        "search_default_max_pages": 3,
        "search_default_per_page": 10,
        "search_default_exact": False,
        "search_default_area": 1,
        "analysis_default_use_cache": True,
    }

    normalized = _normalize_ws_payload({"max_pages": 99, "per_page": 999}, runtime_settings)
    assert normalized["max_pages"] == 4
    assert normalized["per_page"] == 15
    assert normalized["exact_search"] is False
    assert normalized["area"] == 1
    assert normalized["use_cache"] is True


def test_assert_ws_user_role_rejects_missing_token():
    with pytest.raises(ValueError, match="Authentication required"):
        _assert_ws_user_role("")


def test_assert_ws_user_role_rejects_non_user_roles():
    invalid_token = jwt.encode(
        {"sub": "guest", "role": "guest", "iat": 1, "exp": 9_999_999_999},
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )
    with pytest.raises(HTTPException) as exc:
        _assert_ws_user_role(invalid_token)
    assert exc.value.status_code == 401


def test_assert_ws_user_role_accepts_admin_and_user():
    user_token = create_access_token(username="user", role=UserRole.user, expires_minutes=5)
    admin_token = create_access_token(username="admin", role=UserRole.admin, expires_minutes=5)

    assert _assert_ws_user_role(user_token)["role"] == "user"
    assert _assert_ws_user_role(admin_token)["role"] == "admin"
