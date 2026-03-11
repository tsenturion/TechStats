from __future__ import annotations

from collections.abc import Iterable

import httpx
from httpx_retries import Retry, RetryTransport


DEFAULT_RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_RETRY_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


def _build_retry(
    *,
    total: int = 3,
    backoff_factor: float = 0.5,
    statuses: Iterable[int] | None = None,
    methods: Iterable[str] | None = None,
) -> Retry:
    return Retry(
        total=max(0, int(total)),
        backoff_factor=max(0.0, float(backoff_factor)),
        status_forcelist={int(item) for item in (statuses or DEFAULT_RETRY_STATUSES)},
        allowed_methods={str(item).upper() for item in (methods or DEFAULT_RETRY_METHODS)},
    )


def build_async_client(
    *,
    base_url: str,
    timeout: float,
    headers: dict[str, str] | None = None,
    retries: int = 3,
    backoff_factor: float = 0.5,
    statuses: Iterable[int] | None = None,
    methods: Iterable[str] | None = None,
) -> httpx.AsyncClient:
    retry = _build_retry(
        total=retries,
        backoff_factor=backoff_factor,
        statuses=statuses,
        methods=methods,
    )
    transport = RetryTransport(retry=retry)
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        headers=headers or {},
        transport=transport,
    )
