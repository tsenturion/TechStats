import httpx
import pytest
import respx

from shared.http_client import build_async_client


@pytest.mark.asyncio
@respx.mock
async def test_retrying_client_retries_then_succeeds():
    attempts = {"count": 0}

    def _responder(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return httpx.Response(503, json={"detail": "temporary"})
        return httpx.Response(200, json={"ok": True})

    route = respx.get("http://example.test/ping").mock(side_effect=_responder)

    async with build_async_client(
        base_url="http://example.test",
        timeout=5.0,
        retries=2,
        statuses={503},
        methods={"GET"},
    ) as client:
        response = await client.get("/ping")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert route.call_count == 2
