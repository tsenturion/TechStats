from pathlib import Path
import sys

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app import auth_backend


@pytest.mark.asyncio
async def test_write_users_keeps_in_memory_fallback_when_storage_unavailable(mocker):
    mocker.patch.object(auth_backend.cache_manager, "redis_client", None)
    set_mock = mocker.patch.object(
        auth_backend.cache_manager,
        "set",
        new=mocker.AsyncMock(return_value=False),
    )

    users_payload = {"john": {"password_hash": "hashed", "role": "user"}}
    await auth_backend._write_users(users_payload)

    assert auth_backend._in_memory_users["john"]["password_hash"] == "hashed"
    set_mock.assert_awaited_once()
