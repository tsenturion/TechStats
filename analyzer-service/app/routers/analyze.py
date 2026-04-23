# C:\Users\user\Desktop\TechStats\analyzer-service\app\routers\analyze.py
import asyncio
import hashlib
import html
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional, Callable, Awaitable
from uuid import uuid4
import httpx
from fastapi import APIRouter, HTTPException, Body, Query, BackgroundTasks, Request, Depends
import structlog
from celery.result import AsyncResult

from config import settings
from app.cache import cache_manager
from app.analyzer import PatternMatcher
from app.tech_patterns import TechPatternsLoader
from app.analysis_store import analysis_store
from app.celery_app import celery_app
from app.celery_tasks import perform_analysis_task
from shared.runtime_settings import RUNTIME_SETTINGS_KEY, build_effective_runtime_settings

router = APIRouter()
logger = structlog.get_logger()

# Хранилище для фоновых задач
analysis_tasks: Dict[str, Dict[str, Any]] = {}
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
HH_DATE_SHARD_START_UTC = datetime(2000, 1, 1, tzinfo=timezone.utc)
HH_DATE_SHARD_MIN_WINDOW_SECONDS = 2
VACANCY_FETCH_CACHE_SCHEMA_VERSION = "v1"


async def _emit_progress(
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]],
    payload: Dict[str, Any],
) -> None:
    if progress_callback is None:
        return
    try:
        await progress_callback(payload)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Progress callback failed", error=str(exc))


def _truncate_text(value: Any, max_len: int = 500) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return f"{text[:max_len]}..."


def _http_exception_detail_text(exc: HTTPException) -> str:
    detail = getattr(exc, "detail", None)
    if isinstance(detail, str):
        return detail.strip()
    if detail is None:
        return ""
    try:
        return _truncate_text(detail)
    except Exception:  # noqa: BLE001
        return ""


def _format_exception_message(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        detail = _http_exception_detail_text(exc)
        if detail:
            return detail
        return f"HTTP {getattr(exc, 'status_code', 'unknown')} error"

    if isinstance(exc, httpx.TimeoutException):
        request = getattr(exc, "request", None)
        if request and getattr(request, "url", None):
            return f"Request timeout while calling {request.url}"
        return "Request timeout while waiting for upstream service response"

    if isinstance(exc, httpx.HTTPStatusError):
        response = getattr(exc, "response", None)
        request = getattr(exc, "request", None)
        status = response.status_code if response else "unknown"
        url = request.url if request and getattr(request, "url", None) else "unknown URL"
        body = _truncate_text(response.text) if response is not None else ""
        if body:
            return f"Upstream HTTP {status} from {url}: {body}"
        return f"Upstream HTTP {status} from {url}"

    if isinstance(exc, httpx.RequestError):
        request = getattr(exc, "request", None)
        if request and getattr(request, "url", None):
            return f"Request error while calling {request.url}: {type(exc).__name__}"
        return f"Request error: {type(exc).__name__}"

    text = str(exc).strip()
    if text:
        return text
    return f"{type(exc).__name__} (empty error message)"


def _retry_delay_seconds(attempt: int) -> float:
    base = max(0.1, float(settings.retry_delay))
    # Небольшой cap, чтобы не задерживать live-пайплайн слишком долго.
    return min(5.0, base * (2 ** max(0, attempt - 1)))


async def _request_vacancy_service_with_retry(
    vacancy_client: httpx.AsyncClient,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    request_timeout: Optional[float] = None,
    max_attempts: Optional[int] = None,
) -> httpx.Response:
    attempts = max(1, int(max_attempts if max_attempts is not None else settings.max_retries))
    normalized_timeout: Optional[float] = None
    if request_timeout is not None:
        try:
            parsed_timeout = float(request_timeout)
            if parsed_timeout > 0:
                normalized_timeout = parsed_timeout
        except Exception:
            normalized_timeout = None
    last_exception: Optional[Exception] = None
    last_response: Optional[httpx.Response] = None

    for attempt in range(1, attempts + 1):
        try:
            if method.upper() == "GET":
                request_kwargs: Dict[str, Any] = {"params": params}
                if normalized_timeout is not None:
                    request_kwargs["timeout"] = normalized_timeout
                response = await vacancy_client.get(path, **request_kwargs)
            elif method.upper() == "POST":
                request_kwargs = {"json": json_body, "params": params}
                if normalized_timeout is not None:
                    request_kwargs["timeout"] = normalized_timeout
                response = await vacancy_client.post(path, **request_kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method for retry helper: {method}")
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            last_exception = exc
            if attempt >= attempts:
                raise
            await asyncio.sleep(_retry_delay_seconds(attempt))
            continue

        status_code = int(response.status_code)
        if status_code == 200:
            return response

        last_response = response
        if status_code not in RETRYABLE_STATUS_CODES or attempt >= attempts:
            return response
        await asyncio.sleep(_retry_delay_seconds(attempt))

    if last_response is not None:
        return last_response
    if last_exception is not None:
        raise last_exception
    raise RuntimeError("Vacancy-service request failed without response")


def _normalize_duplicate_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _extract_employer_name(vacancy: Dict[str, Any]) -> str:
    employer = vacancy.get("employer")
    if isinstance(employer, dict):
        return _normalize_duplicate_text(employer.get("name", ""))
    if employer:
        return _normalize_duplicate_text(employer)
    return _normalize_duplicate_text(vacancy.get("employer_name", ""))


def _build_duplicate_signature(vacancy: Dict[str, Any]) -> Tuple[str, str, str]:
    employer_name = _extract_employer_name(vacancy)
    vacancy_name = _normalize_duplicate_text(vacancy.get("name", ""))
    vacancy_description = _normalize_duplicate_text(vacancy.get("description", ""))
    return employer_name, vacancy_name, vacancy_description


def _build_duplicate_group_key(signature: Tuple[str, str, str]) -> str:
    payload = "||".join(signature)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _calculate_duplicate_metrics(vacancies: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped_ids: Dict[Tuple[str, str, str], List[str]] = {}
    ordered_ids: List[str] = []

    for vacancy in vacancies:
        vacancy_id = str(vacancy.get("id", "")).strip()
        if not vacancy_id:
            continue
        ordered_ids.append(vacancy_id)

        signature = _build_duplicate_signature(vacancy)
        if not all(signature):
            continue
        grouped_ids.setdefault(signature, []).append(vacancy_id)

    duplicate_groups = [ids for ids in grouped_ids.values() if len(ids) > 1]
    duplicate_id_set = {vacancy_id for ids in duplicate_groups for vacancy_id in ids}

    duplicate_vacancy_ids: List[str] = []
    seen_ids = set()
    for vacancy_id in ordered_ids:
        if vacancy_id in duplicate_id_set and vacancy_id not in seen_ids:
            duplicate_vacancy_ids.append(vacancy_id)
            seen_ids.add(vacancy_id)

    duplicate_group_size_by_id = {}
    duplicate_group_key_by_id = {}
    for signature, ids in grouped_ids.items():
        if len(ids) <= 1:
            continue
        group_key = _build_duplicate_group_key(signature)
        group_size = len(ids)
        for vacancy_id in ids:
            duplicate_group_size_by_id[vacancy_id] = group_size
            duplicate_group_key_by_id[vacancy_id] = group_key

    duplicate_vacancies_count = len(duplicate_vacancy_ids)
    duplicate_extra_count = sum(max(0, len(ids) - 1) for ids in duplicate_groups)

    return {
        "duplicate_vacancies_count": duplicate_vacancies_count,
        "duplicate_groups_count": len(duplicate_groups),
        "duplicate_extra_count": duplicate_extra_count,
        "duplicate_vacancy_ids": duplicate_vacancy_ids,
        "duplicate_id_set": set(duplicate_id_set),
        "duplicate_group_size_by_id": duplicate_group_size_by_id,
        "duplicate_group_key_by_id": duplicate_group_key_by_id,
    }


async def _load_runtime_settings() -> Dict[str, Any]:
    raw = await cache_manager.get(RUNTIME_SETTINGS_KEY)
    if not isinstance(raw, dict):
        return build_effective_runtime_settings()
    return build_effective_runtime_settings(raw)


async def get_pattern_matcher(request: Request) -> PatternMatcher:
    """Dependency для получения PatternMatcher"""
    return request.app.state.pattern_matcher


async def get_patterns_loader(request: Request) -> TechPatternsLoader:
    """Dependency для получения TechPatternsLoader"""
    return request.app.state.patterns_loader


async def get_vacancy_client(request: Request) -> httpx.AsyncClient:
    """Dependency для получения HTTP клиента vacancy service"""
    return request.app.state.vacancy_client


def _utc_now_for_sharding() -> datetime:
    now = datetime.now(timezone.utc)
    # Stable day bucket improves cache reuse between repeated runs.
    return datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc)


def _format_hh_datetime(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _safe_positive_int(value: Any, default: int = 1) -> int:
    try:
        parsed = int(value)
        if parsed > 0:
            return parsed
    except Exception:
        pass
    return max(1, int(default))


def _safe_non_negative_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
        if parsed >= 0:
            return parsed
    except Exception:
        pass
    return max(0, int(default))


def _minify_search_item_for_cache(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "alternate_url": item.get("alternate_url"),
        "url": item.get("url"),
        "snippet": item.get("snippet") if isinstance(item.get("snippet"), dict) else {},
        "description": item.get("description"),
        "key_skills": item.get("key_skills"),
        "employer": item.get("employer") if isinstance(item.get("employer"), dict) else {},
    }


def _build_vacancy_fetch_cache_key(
    search_query: str,
    area: int,
    per_page: int,
    exact_search: bool,
    max_pages: int,
    shard_end_utc: datetime,
) -> str:
    payload = "|".join(
        [
            VACANCY_FETCH_CACHE_SCHEMA_VERSION,
            str(search_query),
            str(area),
            str(per_page),
            str(max_pages),
            str(bool(exact_search)),
            shard_end_utc.date().isoformat(),
        ]
    )
    payload_hash = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:24]
    return f"vacancy_fetch:{VACANCY_FETCH_CACHE_SCHEMA_VERSION}:{payload_hash}"


def _is_valid_cached_vacancy_fetch_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False

    ids = payload.get("ids")
    items_by_id = payload.get("items_by_id")
    if not isinstance(ids, list):
        return False
    if not isinstance(items_by_id, dict):
        return False
    for vacancy_id in ids:
        if not isinstance(vacancy_id, str):
            return False
    return True


def _split_hh_datetime_range(
    start_utc: datetime,
    end_utc: datetime,
) -> Optional[Tuple[Tuple[datetime, datetime], Tuple[datetime, datetime]]]:
    total_seconds = int((end_utc - start_utc).total_seconds())
    if total_seconds < HH_DATE_SHARD_MIN_WINDOW_SECONDS:
        return None

    mid = start_utc + timedelta(seconds=total_seconds // 2)
    right_start = mid + timedelta(seconds=1)
    if right_start > end_utc:
        return None
    return (start_utc, mid), (right_start, end_utc)


def _build_vacancy_search_params(
    search_query: str,
    area: int,
    page: int,
    per_page: int,
    exact_search: bool,
    use_cache: bool,
    date_from_utc: Optional[datetime] = None,
    date_to_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "query": search_query,
        "area": area,
        "page": page,
        "per_page": per_page,
        "search_field": "name",
        "exact_search": exact_search,
        "use_cache": use_cache,
    }
    if date_from_utc is not None:
        params["date_from"] = _format_hh_datetime(date_from_utc)
    if date_to_utc is not None:
        params["date_to"] = _format_hh_datetime(date_to_utc)
    return params


async def _fetch_vacancy_search_interval(
    vacancy_client: httpx.AsyncClient,
    search_query: str,
    area: int,
    per_page: int,
    exact_search: bool,
    use_cache: bool,
    pages_limit: int,
    date_from_utc: Optional[datetime] = None,
    date_to_utc: Optional[datetime] = None,
    split_if_overflow_below_slots: Optional[int] = None,
) -> Dict[str, Any]:
    safe_pages_limit = _safe_positive_int(pages_limit, default=1)
    request_params = _build_vacancy_search_params(
        search_query=search_query,
        area=area,
        page=0,
        per_page=per_page,
        exact_search=exact_search,
        use_cache=use_cache,
        date_from_utc=date_from_utc,
        date_to_utc=date_to_utc,
    )

    first_response = await _request_vacancy_service_with_retry(
        vacancy_client=vacancy_client,
        method="GET",
        path="/api/v1/search",
        params=request_params,
    )
    if first_response.status_code != 200:
        raise HTTPException(
            status_code=first_response.status_code,
            detail=f"Vacancy service error: {_truncate_text(first_response.text)}",
        )

    first_data = first_response.json()
    first_items_raw = first_data.get("items", [])
    first_items = first_items_raw if isinstance(first_items_raw, list) else []
    all_items: List[Dict[str, Any]] = [item for item in first_items if isinstance(item, dict)]
    total_pages_available = _safe_positive_int(first_data.get("pages", 1), default=1)
    found_total = max(
        len(all_items),
        _safe_non_negative_int(first_data.get("found", len(all_items)), default=len(all_items)),
    )
    retrievable_capacity = total_pages_available * max(1, int(per_page))
    hh_overflow_detected = found_total > retrievable_capacity
    split_probe_activated = False
    if split_if_overflow_below_slots is not None:
        threshold_slots = _safe_positive_int(split_if_overflow_below_slots, default=1)
        split_probe_activated = hh_overflow_detected and retrievable_capacity < threshold_slots

    pages_to_fetch = 1 if split_probe_activated else min(safe_pages_limit, total_pages_available)
    pages_fetched = 1

    if pages_to_fetch > 1:
        page_tasks: List[Tuple[int, Awaitable[httpx.Response]]] = []
        for page in range(1, pages_to_fetch):
            page_tasks.append(
                (
                    page,
                    _request_vacancy_service_with_retry(
                        vacancy_client=vacancy_client,
                        method="GET",
                        path="/api/v1/search",
                        params=_build_vacancy_search_params(
                            search_query=search_query,
                            area=area,
                            page=page,
                            per_page=per_page,
                            exact_search=exact_search,
                            use_cache=use_cache,
                            date_from_utc=date_from_utc,
                            date_to_utc=date_to_utc,
                        ),
                    ),
                )
            )

        pages = await asyncio.gather(
            *(task for _, task in page_tasks),
            return_exceptions=True,
        )
        for (page_number, _), page_response in zip(page_tasks, pages):
            if isinstance(page_response, Exception):
                logger.warning(
                    "Vacancy search page request failed",
                    page=page_number,
                    query=search_query,
                    date_from=_format_hh_datetime(date_from_utc) if date_from_utc else None,
                    date_to=_format_hh_datetime(date_to_utc) if date_to_utc else None,
                    error=_format_exception_message(page_response),
                )
                continue
            if page_response.status_code == 200:
                page_items_raw = page_response.json().get("items", [])
                page_items = page_items_raw if isinstance(page_items_raw, list) else []
                all_items.extend(item for item in page_items if isinstance(item, dict))
                pages_fetched += 1
            else:
                logger.warning(
                    "Vacancy search page returned non-200",
                    page=page_number,
                    query=search_query,
                    date_from=_format_hh_datetime(date_from_utc) if date_from_utc else None,
                    date_to=_format_hh_datetime(date_to_utc) if date_to_utc else None,
                    status_code=page_response.status_code,
                    body_preview=_truncate_text(page_response.text),
                )

    return {
        "items": all_items,
        "found_total": found_total,
        "total_pages_available": total_pages_available,
        "pages_fetched": pages_fetched,
        "retrievable_capacity": retrievable_capacity,
        "hh_overflow_detected": hh_overflow_detected,
        "split_probe_activated": split_probe_activated,
        "date_from": _format_hh_datetime(date_from_utc) if date_from_utc else None,
        "date_to": _format_hh_datetime(date_to_utc) if date_to_utc else None,
    }


async def _fetch_vacancy_ids(
    vacancy_client: httpx.AsyncClient,
    search_query: str,
    area: int,
    per_page: int,
    exact_search: bool,
    use_cache: bool,
    max_pages: int,
    shard_end_utc: Optional[datetime] = None,
    return_items: bool = False,
):
    safe_per_page = _safe_positive_int(per_page, default=50)
    safe_max_pages = _safe_positive_int(max_pages, default=1)
    max_items_limit = safe_per_page * safe_max_pages

    unique_ids: List[str] = []
    items_by_id: Dict[str, Dict[str, Any]] = {}
    seen = set()
    total_search_requests = 0
    date_sharding_used = False
    unresolved_overflow_ranges = 0
    split_guard_counter = 0

    pending_ranges: deque[Tuple[Optional[datetime], Optional[datetime]]] = deque([(None, None)])

    while pending_ranges and len(unique_ids) < max_items_limit:
        date_from_utc, date_to_utc = pending_ranges.popleft()
        remaining_slots = max_items_limit - len(unique_ids)
        pages_limit = max(1, (remaining_slots + safe_per_page - 1) // safe_per_page)

        interval_result = await _fetch_vacancy_search_interval(
            vacancy_client=vacancy_client,
            search_query=search_query,
            area=area,
            per_page=safe_per_page,
            exact_search=exact_search,
            use_cache=use_cache,
            pages_limit=pages_limit,
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
            split_if_overflow_below_slots=remaining_slots,
        )
        total_search_requests += int(interval_result.get("pages_fetched", 1) or 1)

        for item in interval_result.get("items", []):
            if not isinstance(item, dict):
                continue
            vacancy_id = item.get("id")
            if not vacancy_id:
                continue
            normalized_id = str(vacancy_id)
            if normalized_id not in seen:
                if len(unique_ids) >= max_items_limit:
                    break
                seen.add(normalized_id)
                unique_ids.append(normalized_id)
            if normalized_id not in items_by_id:
                items_by_id[normalized_id] = item

        remaining_slots = max_items_limit - len(unique_ids)
        if remaining_slots <= 0:
            break

        if not interval_result.get("hh_overflow_detected"):
            continue

        interval_capacity = int(interval_result.get("retrievable_capacity", 0) or 0)
        if interval_capacity >= remaining_slots:
            continue

        if date_from_utc is None or date_to_utc is None:
            base_start = HH_DATE_SHARD_START_UTC
            base_end = shard_end_utc if shard_end_utc is not None else _utc_now_for_sharding()
        else:
            base_start = date_from_utc
            base_end = date_to_utc

        split = _split_hh_datetime_range(base_start, base_end)
        if split is None:
            unresolved_overflow_ranges += 1
            logger.warning(
                "Vacancy search range overflow cannot be split further",
                query=search_query,
                date_from=_format_hh_datetime(base_start),
                date_to=_format_hh_datetime(base_end),
                retrievable_capacity=interval_capacity,
                remaining_slots=remaining_slots,
            )
            continue

        date_sharding_used = True
        split_guard_counter += 1
        (left_start, left_end), (right_start, right_end) = split
        # Newer interval first, then older interval.
        pending_ranges.appendleft((left_start, left_end))
        pending_ranges.appendleft((right_start, right_end))

        logger.info(
            "Applying HH date-sharding for overflowed vacancy search interval",
            query=search_query,
            area=area,
            per_page=safe_per_page,
            found_total=int(interval_result.get("found_total", 0) or 0),
            retrievable_capacity=interval_capacity,
            split_left_from=_format_hh_datetime(left_start),
            split_left_to=_format_hh_datetime(left_end),
            split_right_from=_format_hh_datetime(right_start),
            split_right_to=_format_hh_datetime(right_end),
        )

        if split_guard_counter > 2048:
            logger.warning(
                "Date-sharding split guard reached, stopping additional splits",
                query=search_query,
                split_guard_counter=split_guard_counter,
            )
            break

    if return_items:
        return {
            "ids": unique_ids,
            "items_by_id": items_by_id,
            "search_stats": {
                "vacancy_search_requests": total_search_requests,
                "date_sharding_used": date_sharding_used,
                "unresolved_overflow_ranges": unresolved_overflow_ranges,
                "vacancy_search_cache_hit": False,
            },
        }
    return unique_ids


def _build_fallback_vacancy_from_search_item(
    vacancy_id: str,
    search_item: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    item = search_item if isinstance(search_item, dict) else {}
    snippet = item.get("snippet") if isinstance(item.get("snippet"), dict) else {}
    requirement = str(snippet.get("requirement", "") or "")
    responsibility = str(snippet.get("responsibility", "") or "")
    fallback_description = " ".join(part for part in [requirement, responsibility] if part).strip()

    return {
        "id": vacancy_id,
        "name": str(item.get("name") or f"Vacancy {vacancy_id}"),
        "alternate_url": str(item.get("alternate_url") or item.get("url") or f"https://hh.ru/vacancy/{vacancy_id}"),
        "snippet": snippet,
        "description": str(item.get("description") or fallback_description),
        "key_skills": item.get("key_skills", []),
        "employer": item.get("employer") if isinstance(item.get("employer"), dict) else {},
        "_details_fallback": True,
    }


async def _fetch_detailed_vacancies(
    vacancy_client: httpx.AsyncClient,
    vacancy_ids,
    use_cache: bool,
    batch_chunk_size: int,
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    request_timeout_sec: Optional[float] = None,
    request_retry_attempts: Optional[int] = None,
    chunk_hard_timeout_sec: Optional[float] = None,
) -> Dict[str, Any]:
    detailed_vacancies = []
    combined_cache_stats = {"hits": 0, "misses": 0}
    failed_chunks: List[Dict[str, Any]] = []
    safe_chunk_size = max(1, int(batch_chunk_size or 1))
    total_ids = len(vacancy_ids)
    loaded_ids = set()
    resolved_ids = set()
    retry_attempts = max(1, int(request_retry_attempts if request_retry_attempts is not None else settings.max_retries))

    normalized_request_timeout: Optional[float] = None
    if request_timeout_sec is not None:
        try:
            parsed_timeout = float(request_timeout_sec)
            if parsed_timeout > 0:
                normalized_request_timeout = parsed_timeout
        except Exception:
            normalized_request_timeout = None

    normalized_chunk_timeout: Optional[float] = None
    if chunk_hard_timeout_sec is not None:
        try:
            parsed_chunk_timeout = float(chunk_hard_timeout_sec)
            if parsed_chunk_timeout > 0:
                normalized_chunk_timeout = parsed_chunk_timeout
        except Exception:
            normalized_chunk_timeout = None

    async def _emit_fetching_progress() -> None:
        if total_ids <= 0:
            return
        loaded_count = min(total_ids, len(loaded_ids))
        resolved_count = min(total_ids, len(resolved_ids))
        details_progress = 25 + (10 * (resolved_count / max(1, total_ids)))
        message = f"Загружаем детальную информацию о вакансиях... {loaded_count}/{total_ids}"
        if resolved_count > loaded_count:
            message += f" (обработано: {resolved_count}/{total_ids})"
        await _emit_progress(
            progress_callback,
            {
                "stage": "fetching_details",
                "message": message,
                "progress": details_progress,
                "processed": loaded_count,
                "total": total_ids,
            },
        )

    async def _request_chunk(chunk_ids: List[str]) -> httpx.Response:
        request_coro = _request_vacancy_service_with_retry(
            vacancy_client=vacancy_client,
            method="POST",
            path="/api/v1/vacancies/batch",
            json_body={"vacancy_ids": chunk_ids},
            params={"use_cache": use_cache},
            request_timeout=normalized_request_timeout,
            max_attempts=retry_attempts,
        )
        if normalized_chunk_timeout is None:
            return await request_coro
        return await asyncio.wait_for(request_coro, timeout=normalized_chunk_timeout)

    async def _request_single_vacancy(vacancy_id: str) -> Optional[Dict[str, Any]]:
        single_attempts = max(2, retry_attempts)
        single_response = await _request_vacancy_service_with_retry(
            vacancy_client=vacancy_client,
            method="GET",
            path=f"/api/v1/vacancies/{vacancy_id}",
            params={"use_cache": use_cache},
            request_timeout=normalized_request_timeout,
            max_attempts=single_attempts,
        )
        if int(single_response.status_code) != 200:
            return None
        payload = single_response.json()
        vacancy_payload = payload.get("vacancy")
        if not isinstance(vacancy_payload, dict):
            return None
        vacancy_id_from_payload = vacancy_payload.get("id")
        if vacancy_id_from_payload is None:
            return None
        return vacancy_payload

    pending_chunks = deque(
        [vacancy_ids[start:start + safe_chunk_size] for start in range(0, total_ids, safe_chunk_size)]
    )
    while pending_chunks:
        chunk_ids = [str(vacancy_id) for vacancy_id in pending_chunks.popleft() if str(vacancy_id).strip()]
        if not chunk_ids:
            continue

        try:
            batch_response = await _request_chunk(chunk_ids)
        except Exception as exc:  # noqa: BLE001
            chunk_error = _format_exception_message(exc)
            if len(chunk_ids) > 1:
                midpoint = max(1, len(chunk_ids) // 2)
                left_chunk = chunk_ids[:midpoint]
                right_chunk = chunk_ids[midpoint:]
                if right_chunk:
                    pending_chunks.appendleft(right_chunk)
                if left_chunk:
                    pending_chunks.appendleft(left_chunk)
                logger.warning(
                    "Batch fetch chunk failed, splitting into smaller chunks",
                    chunk_size=len(chunk_ids),
                    left_size=len(left_chunk),
                    right_size=len(right_chunk),
                    error=chunk_error,
                )
                continue

            vacancy_id = chunk_ids[0]
            resolved_ids.add(vacancy_id)
            failed_chunks.append(
                {
                    "vacancy_ids": [vacancy_id],
                    "status_code": 504 if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)) else 500,
                    "message": chunk_error,
                }
            )
            logger.warning(
                "Batch fetch single vacancy failed",
                vacancy_id=vacancy_id,
                error=chunk_error,
            )
            await _emit_fetching_progress()
            continue

        status_code = int(batch_response.status_code)
        if status_code != 200:
            is_retryable_status = status_code in RETRYABLE_STATUS_CODES
            if len(chunk_ids) > 1 and is_retryable_status:
                midpoint = max(1, len(chunk_ids) // 2)
                left_chunk = chunk_ids[:midpoint]
                right_chunk = chunk_ids[midpoint:]
                if right_chunk:
                    pending_chunks.appendleft(right_chunk)
                if left_chunk:
                    pending_chunks.appendleft(left_chunk)
                logger.warning(
                    "Batch fetch returned retryable non-200, splitting chunk",
                    status_code=status_code,
                    chunk_size=len(chunk_ids),
                    left_size=len(left_chunk),
                    right_size=len(right_chunk),
                    body_preview=_truncate_text(batch_response.text),
                )
                continue

            failed_chunks.append(
                {
                    "vacancy_ids": [str(vacancy_id) for vacancy_id in chunk_ids],
                    "status_code": status_code,
                    "message": _truncate_text(batch_response.text),
                }
            )
            for vacancy_id in chunk_ids:
                resolved_ids.add(str(vacancy_id))
            logger.warning(
                "Batch fetch returned non-200",
                status_code=status_code,
                chunk_size=len(chunk_ids),
                body_preview=_truncate_text(batch_response.text),
            )
            await _emit_fetching_progress()
            continue

        payload = batch_response.json()
        chunk_vacancies = payload.get("vacancies", [])
        detailed_vacancies.extend(chunk_vacancies)

        loaded_chunk_ids = set()
        for vacancy in chunk_vacancies:
            vacancy_id = vacancy.get("id")
            if vacancy_id:
                normalized_id = str(vacancy_id)
                loaded_ids.add(normalized_id)
                loaded_chunk_ids.add(normalized_id)
                resolved_ids.add(normalized_id)

        missing_chunk_ids = [vacancy_id for vacancy_id in chunk_ids if vacancy_id not in loaded_chunk_ids]
        if missing_chunk_ids:
            if len(loaded_chunk_ids) == 0 and len(chunk_ids) > 1:
                failed_chunks.append(
                    {
                        "vacancy_ids": [str(vacancy_id) for vacancy_id in chunk_ids],
                        "status_code": 404,
                        "message": "Batch response did not return vacancy details for requested IDs",
                    }
                )
                for vacancy_id in chunk_ids:
                    resolved_ids.add(str(vacancy_id))
                logger.warning(
                    "Batch fetch returned no vacancies for chunk, skipping recursive split",
                    chunk_size=len(chunk_ids),
                    missing_count=len(missing_chunk_ids),
                    missing_preview=missing_chunk_ids[:10],
                )
                await _emit_fetching_progress()
                continue

            logger.warning(
                "Batch fetch returned partial data, retrying missing IDs",
                chunk_size=len(chunk_ids),
                loaded_count=len(loaded_chunk_ids),
                missing_count=len(missing_chunk_ids),
                missing_preview=missing_chunk_ids[:10],
            )

            if len(missing_chunk_ids) > 1:
                midpoint = max(1, len(missing_chunk_ids) // 2)
                left_chunk = missing_chunk_ids[:midpoint]
                right_chunk = missing_chunk_ids[midpoint:]
                if right_chunk:
                    pending_chunks.appendleft(right_chunk)
                if left_chunk:
                    pending_chunks.appendleft(left_chunk)
            else:
                single_missing_id = missing_chunk_ids[0]
                try:
                    single_vacancy = await _request_single_vacancy(single_missing_id)
                except Exception as exc:  # noqa: BLE001
                    failed_chunks.append(
                        {
                            "vacancy_ids": [single_missing_id],
                            "status_code": 504 if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)) else 500,
                            "message": _format_exception_message(exc),
                        }
                    )
                    resolved_ids.add(single_missing_id)
                    logger.warning(
                        "Single vacancy fallback request failed",
                        vacancy_id=single_missing_id,
                        error=_format_exception_message(exc),
                    )
                else:
                    if single_vacancy:
                        detailed_vacancies.append(single_vacancy)
                        loaded_ids.add(single_missing_id)
                        resolved_ids.add(single_missing_id)
                        logger.info(
                            "Recovered missing vacancy via single fallback request",
                            vacancy_id=single_missing_id,
                        )
                    else:
                        failed_chunks.append(
                            {
                                "vacancy_ids": [single_missing_id],
                                "status_code": 404,
                                "message": "Vacancy was not returned by batch response and could not be fetched individually",
                            }
                        )
                        resolved_ids.add(single_missing_id)
                        logger.warning(
                            "Failed to recover missing vacancy via single fallback request",
                            vacancy_id=single_missing_id,
                        )

        cache_stats = payload.get("cache_stats") or {}
        try:
            combined_cache_stats["hits"] += int(cache_stats.get("hits", 0) or 0)
            combined_cache_stats["misses"] += int(cache_stats.get("misses", 0) or 0)
        except Exception:
            pass

        await _emit_fetching_progress()

    by_id = {}
    for vacancy in detailed_vacancies:
        vacancy_id = vacancy.get("id")
        if vacancy_id:
            by_id[str(vacancy_id)] = vacancy

    ordered_vacancies = [by_id[str(vacancy_id)] for vacancy_id in vacancy_ids if str(vacancy_id) in by_id]
    missing_ids = [str(vacancy_id) for vacancy_id in vacancy_ids if str(vacancy_id) not in by_id]

    return {
        "vacancies": ordered_vacancies,
        "cache_stats": combined_cache_stats,
        "missing_ids": missing_ids,
        "failed_chunks": failed_chunks,
    }


def _is_complete_cached_result(cached_result: Dict[str, Any]) -> bool:
    if not isinstance(cached_result, dict):
        return False

    total = cached_result.get("total_vacancies")
    requested = cached_result.get("requested_vacancies")
    with_tech = cached_result.get("vacancies_with_tech")
    without_tech = cached_result.get("vacancies_without_tech")
    duplicate_vacancies_count = cached_result.get("duplicate_vacancies_count")
    unprocessed = cached_result.get("unprocessed_vacancy_ids")

    if not isinstance(total, int) or total < 0:
        return False
    if not isinstance(with_tech, list) or not isinstance(without_tech, list):
        return False
    if not isinstance(duplicate_vacancies_count, int) or duplicate_vacancies_count < 0:
        return False
    if duplicate_vacancies_count > total:
        return False
    if requested is not None:
        if not isinstance(requested, int) or requested < total:
            return False
    if unprocessed is not None:
        if not isinstance(unprocessed, list):
            return False
        if len(unprocessed) > 0:
            return False

    for item in with_tech:
        if not isinstance(item, dict):
            return False
        if "text_match_count" not in item or "key_skills_match_count" not in item:
            return False
        if "duplicate_group_key" not in item:
            return False
        if not isinstance(item.get("duplicate_group_key"), str):
            return False

    for item in without_tech:
        if not isinstance(item, dict):
            return False
        if "duplicate_group_key" not in item:
            return False
        if not isinstance(item.get("duplicate_group_key"), str):
            return False

    return len(with_tech) + len(without_tech) == total


async def _perform_analysis(
    analysis_request: Dict[str, Any],
    pattern_matcher: PatternMatcher,
    vacancy_client: httpx.AsyncClient,
    use_cache: bool,
    progress_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    start_time = time.time()
    required_fields = ["vacancy_title", "technology"]
    for field in required_fields:
        if field not in analysis_request:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

    await _emit_progress(
        progress_callback,
        {
            "stage": "initializing",
            "message": "Инициализация анализа...",
            "progress": 2,
            "processed": 0,
            "total": 0,
        },
    )

    runtime_settings = await _load_runtime_settings()

    vacancy_title = analysis_request["vacancy_title"]
    technology = analysis_request["technology"]
    exact_search = analysis_request.get("exact_search", runtime_settings.get("search_default_exact", True))
    area = analysis_request.get("area", runtime_settings.get("search_default_area", 113))

    max_pages_limit = int(runtime_settings.get("analysis_max_pages_hard_limit", 20))
    per_page_limit = int(runtime_settings.get("analysis_per_page_hard_limit", 100))
    max_pages = int(analysis_request.get("max_pages", runtime_settings.get("search_default_max_pages", 3)))
    per_page = int(analysis_request.get("per_page", runtime_settings.get("search_default_per_page", 50)))
    max_pages = max(1, min(max_pages, max_pages_limit))
    per_page = max(1, min(per_page, per_page_limit))

    runtime_batch_size = int(runtime_settings.get("analyzer_batch_size", settings.batch_size))
    runtime_batch_size = max(1, runtime_batch_size)

    # vacancy-service сам применяет exact-search логику (добавляет кавычки при необходимости).
    search_query = vacancy_title

    await _emit_progress(
        progress_callback,
        {
            "stage": "fetching_vacancies",
            "message": "Получаем список вакансий...",
            "progress": 8,
            "processed": 0,
            "total": 0,
        },
    )

    shard_end_utc = _utc_now_for_sharding()
    vacancy_fetch_result: Dict[str, Any] = {}
    vacancy_fetch_cache_key: Optional[str] = None
    vacancy_search_index_cache_hit = False

    if use_cache:
        vacancy_fetch_cache_key = _build_vacancy_fetch_cache_key(
            search_query=search_query,
            area=area,
            per_page=per_page,
            exact_search=exact_search,
            max_pages=max_pages,
            shard_end_utc=shard_end_utc,
        )
        cached_vacancy_fetch = await cache_manager.get(vacancy_fetch_cache_key)
        if _is_valid_cached_vacancy_fetch_payload(cached_vacancy_fetch):
            vacancy_fetch_result = cached_vacancy_fetch
            vacancy_search_index_cache_hit = True
            cached_stats = (
                dict(vacancy_fetch_result.get("search_stats", {}))
                if isinstance(vacancy_fetch_result.get("search_stats"), dict)
                else {}
            )
            cached_stats["vacancy_search_cache_hit"] = True
            cached_stats["vacancy_search_requests"] = 0
            vacancy_fetch_result["search_stats"] = cached_stats

    if not vacancy_fetch_result:
        vacancy_fetch_result = await _fetch_vacancy_ids(
            vacancy_client=vacancy_client,
            search_query=search_query,
            area=area,
            per_page=per_page,
            exact_search=exact_search,
            use_cache=use_cache,
            max_pages=max_pages,
            shard_end_utc=shard_end_utc,
            return_items=True,
        )

        if use_cache and vacancy_fetch_cache_key:
            cached_items_by_id = {}
            raw_items_by_id = vacancy_fetch_result.get("items_by_id", {})
            if isinstance(raw_items_by_id, dict):
                for vacancy_id, item in raw_items_by_id.items():
                    normalized_id = str(vacancy_id).strip()
                    if not normalized_id or not isinstance(item, dict):
                        continue
                    cached_items_by_id[normalized_id] = _minify_search_item_for_cache(item)

            cached_payload = {
                "ids": [str(vacancy_id).strip() for vacancy_id in vacancy_fetch_result.get("ids", []) if str(vacancy_id).strip()],
                "items_by_id": cached_items_by_id,
                "search_stats": dict(vacancy_fetch_result.get("search_stats", {}))
                if isinstance(vacancy_fetch_result.get("search_stats"), dict)
                else {},
                "shard_end_date": shard_end_utc.date().isoformat(),
            }
            await cache_manager.set(
                vacancy_fetch_cache_key,
                cached_payload,
                ttl=settings.analysis_cache_ttl_hours * 3600,
            )

    vacancy_ids = list(vacancy_fetch_result.get("ids", []))
    search_items_by_id = vacancy_fetch_result.get("items_by_id", {})
    vacancy_search_stats = (
        dict(vacancy_fetch_result.get("search_stats", {}))
        if isinstance(vacancy_fetch_result.get("search_stats"), dict)
        else {}
    )
    vacancy_search_stats["vacancy_search_cache_hit"] = vacancy_search_index_cache_hit or bool(
        vacancy_search_stats.get("vacancy_search_cache_hit")
    )

    await _emit_progress(
        progress_callback,
        {
            "stage": "vacancies_found",
            "message": f"Найдено {len(vacancy_ids)} вакансий",
            "progress": 15,
            "processed": 0,
            "total": len(vacancy_ids),
        },
    )

    if not vacancy_ids:
        empty_result = {
            "vacancy_title": vacancy_title,
            "technology": technology,
            "exact_search": exact_search,
            "total_vacancies": 0,
            "tech_vacancies": 0,
            "tech_percentage": 0,
            "duplicate_vacancies_count": 0,
            "duplicate_groups_count": 0,
            "duplicate_extra_count": 0,
            "duplicate_vacancy_ids": [],
            "vacancies_with_tech": [],
            "vacancies_without_tech": [],
            "requested_vacancies": 0,
            "unprocessed_vacancy_ids": [],
            "request_stats": {"real_requests": 1, "cached_requests": 0, "total_requests": 1, "cache_hit_rate": 0.0, "processing_time": 0.0},
        }
        await analysis_store.add_record(empty_result)
        await _emit_progress(
            progress_callback,
            {
                "stage": "completed",
                "message": "Вакансии не найдены",
                "progress": 100,
                "processed": 0,
                "total": 0,
                "found_with_tech": 0,
            },
        )
        return empty_result

    requested_vacancies = len(vacancy_ids)

    if use_cache:
        cached_result = await cache_manager.get_analysis_result(vacancy_ids, technology, exact_search)
        if cached_result and _is_complete_cached_result(cached_result):
            total_vacancies = int(cached_result.get("total_vacancies", 0))
            cached_result["request_stats"] = {
                "real_requests": 1,
                "cached_requests": total_vacancies,
                "total_requests": total_vacancies + 1,
                "cache_hit_rate": 100.0,
                "processing_time": round(time.time() - start_time, 3),
            }
            await _emit_progress(
                progress_callback,
                {
                    "stage": "completed",
                    "message": f"Обработано вакансий: {total_vacancies}/{total_vacancies}",
                    "progress": 100,
                    "processed": total_vacancies,
                    "total": total_vacancies,
                    "found_with_tech": int(cached_result.get("tech_vacancies", 0) or 0),
                },
            )
            return cached_result

    batch_chunk_size = int(runtime_settings.get("vacancy_batch_max_ids", 100))
    detail_request_timeout_sec = float(
        runtime_settings.get("analyzer_detail_request_timeout_sec", settings.request_timeout)
    )
    detail_retry_attempts = int(
        runtime_settings.get("analyzer_detail_retry_attempts", 1)
    )
    detail_chunk_hard_timeout_sec = float(
        runtime_settings.get(
            "analyzer_detail_chunk_hard_timeout_sec",
            max(10.0, detail_request_timeout_sec * max(1, detail_retry_attempts) + 10.0),
        )
    )
    await _emit_progress(
        progress_callback,
        {
            "stage": "fetching_details",
            "message": f"Загружаем детальную информацию о вакансиях... 0/{len(vacancy_ids)}",
            "progress": 25,
            "processed": 0,
            "total": len(vacancy_ids),
        },
    )

    details_result = await _fetch_detailed_vacancies(
        vacancy_client=vacancy_client,
        vacancy_ids=vacancy_ids,
        use_cache=use_cache,
        batch_chunk_size=batch_chunk_size,
        progress_callback=progress_callback,
        request_timeout_sec=detail_request_timeout_sec,
        request_retry_attempts=detail_retry_attempts,
        chunk_hard_timeout_sec=detail_chunk_hard_timeout_sec,
    )
    detailed_vacancies = details_result.get("vacancies", [])
    missing_details_ids = [str(vacancy_id) for vacancy_id in details_result.get("missing_ids", [])]
    fallback_stub_ids: List[str] = []
    if missing_details_ids:
        fallback_vacancies = [
            _build_fallback_vacancy_from_search_item(vacancy_id, search_items_by_id.get(vacancy_id))
            for vacancy_id in missing_details_ids
        ]
        detailed_vacancies.extend(fallback_vacancies)
        fallback_stub_ids = [str(item.get("id")) for item in fallback_vacancies if item.get("id")]
        details_result["fallback_stub_ids"] = fallback_stub_ids
        logger.warning(
            "Detailed vacancy payload missing for part of IDs, using search-item fallback",
            missing_count=len(missing_details_ids),
            fallback_count=len(fallback_stub_ids),
        )
    detailed_total = len(detailed_vacancies)
    await _emit_progress(
        progress_callback,
        {
            "stage": "analyzing",
            "message": f"Обработано вакансий: 0/{detailed_total}",
            "progress": 35,
            "processed": 0,
            "total": detailed_total,
            "found_with_tech": 0,
        },
    )

    cached_analyses = {}
    vacancies_to_analyze = []

    if use_cache:
        cached_analyses = await cache_manager.get_batch_analysis(vacancy_ids, technology, exact_search)
        for vacancy in detailed_vacancies:
            vacancy_id = str(vacancy.get("id", ""))
            if vacancy_id in cached_analyses and cached_analyses[vacancy_id]:
                cached_analyses[vacancy_id]["from_cache"] = True
            else:
                vacancies_to_analyze.append(vacancy)
    else:
        vacancies_to_analyze = detailed_vacancies

    cached_processed = 0
    if use_cache and cached_analyses:
        cached_processed = sum(
            1
            for vacancy in detailed_vacancies
            if cached_analyses.get(str(vacancy.get("id", "")))
        )
        if detailed_total > 0:
            await _emit_progress(
                progress_callback,
                {
                    "stage": "analyzing",
                    "message": f"Обработано вакансий: {cached_processed}/{detailed_total}",
                    "progress": 35 + (55 * (cached_processed / max(1, detailed_total))),
                    "processed": cached_processed,
                    "total": detailed_total,
                    "found_with_tech": 0,
                },
            )

    analysis_results = []
    if vacancies_to_analyze:
        async def _batch_progress(event: Dict[str, Any]) -> None:
            processed_from_batches = int(event.get("processed", 0) or 0)
            processed_total = min(detailed_total, cached_processed + processed_from_batches)
            await _emit_progress(
                progress_callback,
                {
                    "stage": "analyzing",
                    "message": f"Обработано вакансий: {processed_total}/{detailed_total}",
                    "progress": 35 + (55 * (processed_total / max(1, detailed_total))),
                    "processed": processed_total,
                    "total": detailed_total,
                    "found_with_tech": 0,
                },
            )

        analysis_results = await pattern_matcher.analyze_vacancies_batch(
            vacancies_to_analyze,
            technology,
            exact_search,
            batch_size=runtime_batch_size,
            progress_callback=_batch_progress,
        )
        if use_cache:
            await cache_manager.cache_batch_analysis(analysis_results, technology, exact_search)
    elif detailed_total > 0:
        await _emit_progress(
            progress_callback,
            {
                "stage": "analyzing",
                "message": f"Обработано вакансий: {cached_processed}/{detailed_total}",
                "progress": 90,
                "processed": cached_processed,
                "total": detailed_total,
                "found_with_tech": 0,
            },
        )

    combined_results = [item for item in list(cached_analyses.values()) + analysis_results if item]
    results_by_id = {}
    for item in combined_results:
        vacancy_id = item.get("vacancy_id")
        if vacancy_id:
            results_by_id[str(vacancy_id)] = item

    ordered_results = [results_by_id[vid] for vid in vacancy_ids if vid in results_by_id]
    processed_vacancy_ids = [str(item.get("vacancy_id")) for item in ordered_results if item.get("vacancy_id")]
    processed_set = set(processed_vacancy_ids)
    unprocessed_vacancy_ids = [vid for vid in vacancy_ids if vid not in processed_set]
    detailed_by_id = {}
    for vacancy in detailed_vacancies:
        vacancy_id = vacancy.get("id")
        if vacancy_id:
            detailed_by_id[str(vacancy_id)] = vacancy
    processed_detailed_vacancies = [detailed_by_id[vid] for vid in processed_vacancy_ids if vid in detailed_by_id]
    duplicate_metrics = _calculate_duplicate_metrics(processed_detailed_vacancies)
    duplicate_id_set = duplicate_metrics["duplicate_id_set"]
    duplicate_group_size_by_id = duplicate_metrics["duplicate_group_size_by_id"]
    duplicate_group_key_by_id = duplicate_metrics["duplicate_group_key_by_id"]

    vacancies_with_tech = []
    vacancies_without_tech = []
    for result in ordered_results:
        vacancy_id = str(result.get("vacancy_id", "")).strip()
        is_duplicate = vacancy_id in duplicate_id_set
        duplicate_group_size = int(duplicate_group_size_by_id.get(vacancy_id, 1))
        duplicate_group_key = (
            str(duplicate_group_key_by_id.get(vacancy_id, "")).strip()
            if is_duplicate
            else ""
        )

        text_match_count = int(result.get("text_match_count", result.get("match_count", 0)) or 0)
        key_skills_match_count = int(result.get("key_skills_match_count", 0) or 0)
        total_match_count = text_match_count + key_skills_match_count

        if result.get("has_technology"):
            vacancies_with_tech.append(
                {
                    "id": result.get("vacancy_id"),
                    "name": result.get("vacancy_name"),
                    "url": result.get("vacancy_url"),
                    "match_count": total_match_count,
                    "text_match_count": text_match_count,
                    "key_skills_match_count": key_skills_match_count,
                    "is_duplicate": is_duplicate,
                    "duplicate_group_size": duplicate_group_size,
                    "duplicate_group_key": duplicate_group_key,
                }
            )
        else:
            vacancies_without_tech.append(
                {
                    "id": result.get("vacancy_id"),
                    "name": result.get("vacancy_name"),
                    "url": result.get("vacancy_url"),
                    "match_count": 0,
                    "text_match_count": 0,
                    "key_skills_match_count": 0,
                    "is_duplicate": is_duplicate,
                    "duplicate_group_size": duplicate_group_size,
                    "duplicate_group_key": duplicate_group_key,
                }
            )

    total_vacancies = len(ordered_results)
    tech_vacancies = len(vacancies_with_tech)
    tech_percentage = (tech_vacancies / total_vacancies * 100) if total_vacancies else 0
    vacancy_search_requests = _safe_non_negative_int(vacancy_search_stats.get("vacancy_search_requests", 0), default=0)
    real_requests = vacancy_search_requests + 1
    cached_requests = sum(1 for vacancy_id in processed_vacancy_ids if cached_analyses.get(vacancy_id))
    total_requests = real_requests + max(0, cached_requests)
    cache_hit_rate = (cached_requests / total_requests * 100) if total_requests else 0

    final_result = {
        "vacancy_title": vacancy_title,
        "technology": technology,
        "exact_search": exact_search,
        "total_vacancies": total_vacancies,
        "tech_vacancies": tech_vacancies,
        "tech_percentage": round(tech_percentage, 2),
        "duplicate_vacancies_count": duplicate_metrics["duplicate_vacancies_count"],
        "duplicate_groups_count": duplicate_metrics["duplicate_groups_count"],
        "duplicate_extra_count": duplicate_metrics["duplicate_extra_count"],
        "duplicate_vacancy_ids": duplicate_metrics["duplicate_vacancy_ids"],
        "vacancies_with_tech": vacancies_with_tech,
        "vacancies_without_tech": vacancies_without_tech,
        "requested_vacancies": requested_vacancies,
        "unprocessed_vacancy_ids": unprocessed_vacancy_ids,
        "analysis_timestamp": time.time(),
        "cache_info": {
            "total_cached": cached_requests,
            "newly_analyzed": len(analysis_results),
            "cache_usage_percentage": round((cached_requests / total_vacancies * 100), 2) if total_vacancies else 0.0,
            "detail_fetch_cache_stats": details_result.get("cache_stats", {}),
            "detail_fetch_missing_ids": details_result.get("missing_ids", []),
            "detail_fetch_failed_chunks": details_result.get("failed_chunks", []),
            "detail_fetch_fallback_stub_ids": details_result.get("fallback_stub_ids", []),
            "vacancy_search_stats": vacancy_search_stats,
            "vacancy_search_index_cache_hit": bool(vacancy_search_stats.get("vacancy_search_cache_hit")),
        },
        "request_stats": {
            "real_requests": real_requests,
            "cached_requests": max(0, cached_requests),
            "total_requests": total_requests,
            "cache_hit_rate": round(cache_hit_rate, 2),
            "processing_time": round(time.time() - start_time, 3),
        },
    }

    await _emit_progress(
        progress_callback,
        {
            "stage": "finalizing",
            "message": "Формирование результатов...",
            "progress": 95,
            "processed": total_vacancies,
            "total": total_vacancies,
            "found_with_tech": tech_vacancies,
        },
    )

    if use_cache:
        await cache_manager.cache_analysis_result(vacancy_ids, technology, exact_search, final_result)

    await analysis_store.add_record(final_result)

    await _emit_progress(
        progress_callback,
        {
            "stage": "completed",
            "message": "Анализ завершен!",
            "progress": 100,
            "processed": total_vacancies,
            "total": total_vacancies,
            "found_with_tech": tech_vacancies,
        },
    )
    return final_result


@router.post("/analyze")
async def analyze_vacancies(
    request: Request,
    analysis_request: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher),
    vacancy_client: httpx.AsyncClient = Depends(get_vacancy_client),
    use_cache: bool = Query(True, description="Использовать кэш"),
):
    try:
        return await _perform_analysis(analysis_request, pattern_matcher, vacancy_client, use_cache=use_cache)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Vacancy service timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:
        error_message = _format_exception_message(exc)
        logger.exception("Analysis error", error=error_message, error_type=type(exc).__name__)
        raise HTTPException(status_code=500, detail=f"Analysis failed: {error_message}")


@router.post("/analyze/async")
async def analyze_vacancies_async(
    background_tasks: BackgroundTasks,  # noqa: ARG001
    request: Request,
    analysis_request: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher),
    vacancy_client: httpx.AsyncClient = Depends(get_vacancy_client)
):
    """
    Асинхронный анализ вакансий (возвращает ID задачи)
    """
    if settings.celery_enabled:
        try:
            celery_task = perform_analysis_task.delay(
                analysis_request,
                use_cache=bool(analysis_request.get("use_cache", True)),
            )
            task_id = celery_task.id
            return {
                "task_id": task_id,
                "status": "pending",
                "execution_backend": "celery",
                "message": "Analysis queued in Celery",
                "check_status_url": f"/api/v1/analyze/async/{task_id}/status",
                "get_result_url": f"/api/v1/analyze/async/{task_id}/result",
            }
        except Exception as exc:
            logger.warning("Failed to queue Celery task, fallback to in-process mode", error=str(exc))

    # Fallback in-process background mode (works without Celery worker).
    task_id = str(uuid4())
    analysis_tasks[task_id] = {
        "status": "pending",
        "created_at": time.time(),
        "updated_at": time.time(),
        "progress": 0,
        "stage": "pending",
        "message": "Задача поставлена в очередь",
        "processed": 0,
        "total": 0,
        "found_with_tech": 0,
        "request": analysis_request,
        "result": None,
        "error": None,
    }

    runner = asyncio.create_task(
        execute_async_analysis(
            task_id,
            analysis_request,
            pattern_matcher,
            vacancy_client,
        )
    )
    analysis_tasks[task_id]["runner"] = runner

    return {
        "task_id": task_id,
        "status": "pending",
        "execution_backend": "in_process",
        "message": "Analysis started in background",
        "check_status_url": f"/api/v1/analyze/async/{task_id}/status",
        "get_result_url": f"/api/v1/analyze/async/{task_id}/result",
    }


async def execute_async_analysis(
    task_id: str,
    analysis_request: Dict[str, Any],
    pattern_matcher: PatternMatcher,
    vacancy_client: httpx.AsyncClient
):
    """Выполнение асинхронного анализа"""
    try:
        await asyncio.sleep(0)
        # Обновляем статус
        analysis_tasks[task_id]["status"] = "processing"
        analysis_tasks[task_id]["stage"] = "initializing"
        analysis_tasks[task_id]["message"] = "Инициализация анализа..."
        analysis_tasks[task_id]["progress"] = 1
        analysis_tasks[task_id]["updated_at"] = time.time()

        async def _progress_callback(payload: Dict[str, Any]) -> None:
            task = analysis_tasks.get(task_id)
            if not task:
                return
            task["status"] = "processing"
            task["updated_at"] = time.time()

            if "stage" in payload:
                task["stage"] = payload["stage"]
            if "message" in payload:
                task["message"] = payload["message"]
            if "progress" in payload:
                try:
                    task["progress"] = float(payload["progress"])
                except Exception:  # noqa: BLE001
                    pass
            if "processed" in payload:
                try:
                    task["processed"] = int(payload["processed"])
                except Exception:  # noqa: BLE001
                    pass
            if "total" in payload:
                try:
                    task["total"] = int(payload["total"])
                except Exception:  # noqa: BLE001
                    pass
            if "found_with_tech" in payload:
                try:
                    task["found_with_tech"] = int(payload["found_with_tech"])
                except Exception:  # noqa: BLE001
                    pass

        result = await _perform_analysis(
            analysis_request=analysis_request,
            pattern_matcher=pattern_matcher,
            vacancy_client=vacancy_client,
            use_cache=analysis_request.get("use_cache", True),
            progress_callback=_progress_callback,
        )

        analysis_tasks[task_id]["status"] = "completed"
        analysis_tasks[task_id]["stage"] = "completed"
        analysis_tasks[task_id]["message"] = "Анализ завершен!"
        analysis_tasks[task_id]["progress"] = 100
        analysis_tasks[task_id]["updated_at"] = time.time()
        analysis_tasks[task_id]["processed"] = int(result.get("total_vacancies", 0) or 0)
        analysis_tasks[task_id]["total"] = int(result.get("total_vacancies", 0) or 0)
        analysis_tasks[task_id]["found_with_tech"] = int(result.get("tech_vacancies", 0) or 0)
        analysis_tasks[task_id]["result"] = {
            "task_id": task_id,
            "status": "completed",
            "processed_at": time.time(),
            "result": result,
        }
        analysis_tasks[task_id].pop("runner", None)
    except Exception as e:
        error_message = _format_exception_message(e)
        analysis_tasks[task_id]["status"] = "failed"
        analysis_tasks[task_id]["stage"] = "failed"
        analysis_tasks[task_id]["message"] = error_message
        analysis_tasks[task_id]["progress"] = 100
        analysis_tasks[task_id]["updated_at"] = time.time()
        analysis_tasks[task_id]["error"] = error_message
        analysis_tasks[task_id].pop("runner", None)
        logger.exception(
            "Async analysis failed",
            task_id=task_id,
            error=error_message,
            error_type=type(e).__name__,
        )


@router.get("/analyze/async/{task_id}/status")
async def get_analysis_status(task_id: str):
    """Получение статуса асинхронного анализа"""
    if task_id in analysis_tasks:
        task = analysis_tasks[task_id]
        return {
            "task_id": task_id,
            "status": task["status"],
            "created_at": task["created_at"],
            "updated_at": task.get("updated_at", task["created_at"]),
            "progress": float(task.get("progress", 50 if task["status"] == "processing" else 100)),
            "stage": task.get("stage"),
            "message": task.get("message"),
            "processed": task.get("processed"),
            "total": task.get("total"),
            "found_with_tech": task.get("found_with_tech"),
            "has_result": task["result"] is not None,
            "has_error": task["error"] is not None,
            "execution_backend": "in_process",
        }

    if settings.celery_enabled:
        celery_result = AsyncResult(task_id, app=celery_app)
        raw_state = str(celery_result.state or "PENDING").upper()
        state_map = {
            "PENDING": ("pending", 0),
            "RECEIVED": ("processing", 15),
            "STARTED": ("processing", 50),
            "RETRY": ("processing", 50),
            "SUCCESS": ("completed", 100),
            "FAILURE": ("failed", 100),
            "REVOKED": ("failed", 100),
        }
        status, progress = state_map.get(raw_state, ("pending", 0))
        return {
            "task_id": task_id,
            "status": status,
            "created_at": None,
            "progress": progress,
            "has_result": raw_state == "SUCCESS",
            "has_error": raw_state in {"FAILURE", "REVOKED"},
            "execution_backend": "celery",
            "state": raw_state,
        }

    raise HTTPException(status_code=404, detail="Task not found")


@router.get("/analyze/async/{task_id}/result")
async def get_analysis_result(task_id: str):
    """Получение результата асинхронного анализа"""
    if task_id in analysis_tasks:
        task = analysis_tasks[task_id]
        if task["status"] == "failed":
            raise HTTPException(status_code=500, detail=f"Analysis failed: {task['error']}")
        if task["status"] != "completed" or task["result"] is None:
            raise HTTPException(status_code=202, detail="Analysis still in progress")
        return task["result"]

    if settings.celery_enabled:
        celery_result = AsyncResult(task_id, app=celery_app)
        raw_state = str(celery_result.state or "PENDING").upper()

        if raw_state == "SUCCESS":
            payload = celery_result.result
            if not isinstance(payload, dict):
                payload = {"raw_result": payload}
            return {
                "task_id": task_id,
                "status": "completed",
                "processed_at": time.time(),
                "result": payload,
                "execution_backend": "celery",
            }

        if raw_state in {"FAILURE", "REVOKED"}:
            raise HTTPException(status_code=500, detail=f"Analysis failed: {celery_result.result}")

        raise HTTPException(status_code=202, detail="Analysis still in progress")

    raise HTTPException(status_code=404, detail="Task not found")


@router.post("/analyze/batch")
async def analyze_batch_vacancies(
    request: Request,
    batch_request: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher),
    vacancy_client: httpx.AsyncClient = Depends(get_vacancy_client)
):
    """
    Пакетный анализ нескольких технологий для одного набора вакансий
    """
    # Валидация
    if "vacancy_ids" not in batch_request or "technologies" not in batch_request:
        raise HTTPException(
            status_code=400,
            detail="Missing vacancy_ids or technologies"
        )
    
    vacancy_ids = batch_request["vacancy_ids"]
    technologies = batch_request["technologies"]
    exact_search = batch_request.get("exact_search", True)
    
    if len(vacancy_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Too many vacancy_ids, maximum is 100"
        )
    
    if len(technologies) > 20:
        raise HTTPException(
            status_code=400,
            detail="Too many technologies, maximum is 20"
        )
    
    try:
        # Получение детальной информации о вакансиях
        batch_response = await vacancy_client.post(
            "/api/v1/vacancies/batch",
            json={"vacancy_ids": vacancy_ids}
        )
        
        if batch_response.status_code != 200:
            raise HTTPException(
                status_code=batch_response.status_code,
                detail=f"Batch fetch error: {batch_response.text}"
            )
        
        batch_data = batch_response.json()
        vacancies = batch_data.get("vacancies", [])
        
        runtime_settings = await _load_runtime_settings()
        runtime_batch_size = max(1, int(runtime_settings.get("analyzer_batch_size", settings.batch_size)))

        # Параллельный анализ для каждой технологии
        tasks = []
        for tech in technologies:
            task = pattern_matcher.analyze_vacancies_batch(
                vacancies,
                tech,
                exact_search,
                batch_size=runtime_batch_size
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # Формирование сводного отчета
        summary = {
            "total_vacancies": len(vacancies),
            "technologies_analyzed": len(technologies),
            "results_by_technology": {},
            "comparison": []
        }
        
        for i, tech in enumerate(technologies):
            tech_results = results[i]
            
            # Подсчет вакансий с технологией
            tech_vacancies = sum(1 for r in tech_results if r["has_technology"])
            tech_percentage = (tech_vacancies / len(vacancies) * 100) if vacancies else 0
            
            summary["results_by_technology"][tech] = {
                "tech_vacancies": tech_vacancies,
                "tech_percentage": round(tech_percentage, 2),
                "total_matches": sum(r.get("match_count", 0) for r in tech_results),
                "sample_matches": [
                    {
                        "vacancy_id": r["vacancy_id"],
                        "vacancy_name": r["vacancy_name"],
                        "match_count": r.get("match_count", 0)
                    }
                    for r in tech_results[:5] if r["has_technology"]
                ]
            }
            
            summary["comparison"].append({
                "technology": tech,
                "percentage": round(tech_percentage, 2),
                "vacancy_count": tech_vacancies
            })
        
        # Сортировка по проценту вхождения
        summary["comparison"].sort(key=lambda x: x["percentage"], reverse=True)
        
        return summary
        
    except Exception as e:
        logger.error("Batch analysis error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")


@router.post("/analyze/text")
async def analyze_text(
    request: Request,
    text_analysis: Dict[str, Any] = Body(...),
    pattern_matcher: PatternMatcher = Depends(get_pattern_matcher)
):
    """
    Анализ произвольного текста на наличие технологий
    """
    if "text" not in text_analysis:
        raise HTTPException(status_code=400, detail="Missing text field")
    
    text = text_analysis["text"]
    technology = text_analysis.get("technology")
    technologies = text_analysis.get("technologies", [])
    
    try:
        if technology:
            # Поиск одной технологии
            result = await pattern_matcher.find_technology(text, technology)
            return {
                "text_preview": text[:200] + ("..." if len(text) > 200 else ""),
                "analysis": result
            }
        elif technologies:
            # Поиск нескольких технологий
            result = await pattern_matcher.find_multiple_technologies(text, technologies)
            return {
                "text_preview": text[:200] + ("..." if len(text) > 200 else ""),
                "analysis": result
            }
        else:
            raise HTTPException(
                status_code=400,
                detail="Either technology or technologies field is required"
            )
        
    except Exception as e:
        logger.error("Text analysis error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Text analysis failed: {str(e)}")
