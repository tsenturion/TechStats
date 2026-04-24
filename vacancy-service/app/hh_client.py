# C:\Users\user\Desktop\TechStats\vacancy-service\app\hh_client.py
import asyncio
import time
from typing import Dict, Any, List, Optional
import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import settings
from app.rate_limiter import RateLimiter
from shared.http_client import build_async_client

logger = structlog.get_logger()


class HHVacancySearchForbiddenError(RuntimeError):
    """HH API vacancy-search request was blocked by captcha/anti-bot protection."""


class HHClient:
    """Клиент для работы с HH.ru API"""
    
    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.last_request_time: float = 0
        self.request_lock = asyncio.Lock()
        self.token_lock = asyncio.Lock()
        self.oauth_access_token: Optional[str] = None
        self.oauth_expires_at: float = 0.0
        
    async def initialize(self):
        """Инициализация клиента"""
        headers = {
            "User-Agent": settings.hh_api_user_agent,
            "HH-User-Agent": settings.hh_api_user_agent,
            "Accept": "application/json",
            "Accept-Charset": "utf-8"
        }

        self.client = build_async_client(
            base_url=settings.hh_api_base_url,
            timeout=settings.hh_api_timeout,
            headers=headers,
            retries=settings.max_retries,
            backoff_factor=0.4,
        )
        
    async def close(self):
        """Закрытие клиента"""
        if self.client:
            await self.client.aclose()
            
    async def _rate_limit(self):
        """Rate limiting для HH API"""
        async with self.request_lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            
            # Соблюдение лимита 7 запросов в секунду
            min_interval = 1.0 / settings.hh_rate_limit_per_second
            if time_since_last_request < min_interval:
                sleep_time = min_interval - time_since_last_request
                await asyncio.sleep(sleep_time)
                
            self.last_request_time = time.time()

    @staticmethod
    def _trimmed(value: Optional[str]) -> str:
        return str(value or "").strip()

    def _static_access_token(self) -> str:
        return self._trimmed(settings.hh_api_access_token)

    def _oauth_credentials_available(self) -> bool:
        return bool(self._trimmed(settings.hh_api_client_id) and self._trimmed(settings.hh_api_client_secret))

    async def _fetch_oauth_access_token(self) -> str:
        if self.client is None:
            raise RuntimeError("HH client is not initialized")

        form_data = {
            "grant_type": "client_credentials",
            "client_id": self._trimmed(settings.hh_api_client_id),
            "client_secret": self._trimmed(settings.hh_api_client_secret),
        }
        query_params: Dict[str, str] = {}
        if settings.hh_api_host:
            query_params["host"] = settings.hh_api_host
        query_params["locale"] = "RU"

        response = await self.client.post(
            "/token",
            data=form_data,
            params=query_params,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}

        token = self._trimmed(payload.get("access_token"))
        if not token:
            raise RuntimeError("HH token endpoint did not return access_token")

        expires_in_raw = payload.get("expires_in", 3600)
        try:
            expires_in = int(expires_in_raw)
        except (TypeError, ValueError):
            expires_in = 3600
        refresh_margin = 60
        ttl = max(60, expires_in - refresh_margin)

        self.oauth_access_token = token
        self.oauth_expires_at = time.time() + ttl
        logger.info("HH OAuth app token refreshed", expires_in=expires_in)
        return token

    async def _get_effective_access_token(self) -> str:
        static_token = self._static_access_token()
        if static_token:
            return static_token

        if not self._oauth_credentials_available():
            return ""

        now = time.time()
        if self.oauth_access_token and now < self.oauth_expires_at:
            return self.oauth_access_token

        async with self.token_lock:
            now = time.time()
            if self.oauth_access_token and now < self.oauth_expires_at:
                return self.oauth_access_token
            return await self._fetch_oauth_access_token()

    async def _invalidate_oauth_token(self) -> None:
        async with self.token_lock:
            self.oauth_access_token = None
            self.oauth_expires_at = 0.0
    
    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        reraise=True
    )
    async def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> httpx.Response:
        """Выполнение запроса к HH API"""
        await self._rate_limit()
        
        url = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        full_url = f"{settings.hh_api_base_url}{url}"
        request_params = dict(params or {})
        if settings.hh_api_host and "host" not in request_params:
            request_params["host"] = settings.hh_api_host

        try:
            access_token = await self._get_effective_access_token()
            request_headers: Optional[Dict[str, str]] = None
            if access_token:
                request_headers = {"Authorization": f"Bearer {access_token}"}

            response = await self.client.request(
                method=method,
                url=url,
                params=request_params,
                json=json_data,
                headers=request_headers,
            )

            # For OAuth app token auto-refresh once on auth failures.
            static_token = self._static_access_token()
            token_from_oauth = bool(access_token and not static_token and self._oauth_credentials_available())
            if response.status_code == 401 and token_from_oauth:
                await self._invalidate_oauth_token()
                refreshed_token = await self._get_effective_access_token()
                retry_headers = {"Authorization": f"Bearer {refreshed_token}"} if refreshed_token else None
                response = await self.client.request(
                    method=method,
                    url=url,
                    params=request_params,
                    json=json_data,
                    headers=retry_headers,
                )
            
            # Логирование
            logger.debug(
                "HH API request",
                method=method,
                url=full_url,
                status_code=response.status_code,
                params=request_params
            )
            
            response.raise_for_status()
            return response
            
        except httpx.TimeoutException:
            logger.error("HH API timeout", url=full_url, params=params)
            raise
        except httpx.HTTPStatusError as e:
            logger.error(
                "HH API error",
                url=full_url,
                status_code=e.response.status_code,
                error=str(e),
                params=request_params,
            )
            
            # Обработка специфичных ошибок HH
            if e.response.status_code == 429:
                logger.warning("HH API rate limit exceeded")
            elif e.response.status_code == 403:
                logger.warning(
                    "HH API access forbidden",
                    server=e.response.headers.get("server"),
                    request_id=e.response.headers.get("x-request-id"),
                )
                failed_path = ""
                try:
                    failed_path = str(e.request.url.path)
                except Exception:  # noqa: BLE001
                    failed_path = url

                if method.upper() == "GET" and failed_path == "/vacancies":
                    body_preview = (e.response.text or "").strip()
                    if len(body_preview) > 400:
                        body_preview = f"{body_preview[:400]}..."
                    server_name = str(e.response.headers.get("server", "") or "").strip()
                    request_id = str(e.response.headers.get("x-request-id", "") or "").strip()
                    raise HHVacancySearchForbiddenError(
                        "HH API blocked vacancy search with HTTP 403 "
                        "(captcha/anti-bot protection). "
                        f"server={server_name or 'unknown'}, request_id={request_id or 'unknown'}, "
                        f"response={body_preview or '<empty>'}"
                    ) from e
            elif e.response.status_code == 404:
                logger.info("HH API resource not found", url=full_url)
                
            raise
        except Exception as e:
            logger.error("HH API unexpected error", url=full_url, error=str(e))
            raise
    
    async def search_vacancies(
        self,
        query: str,
        area: int = 113,
        page: int = 0,
        per_page: int = 100,
        search_field: Optional[str] = "name",
        only_with_salary: bool = False,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Поиск вакансий"""
        params = {
            "text": query,
            "area": area,
            "page": page,
            "per_page": per_page,
            "only_with_salary": only_with_salary,
            "order_by": "relevance",
            "locale": "RU",
        }

        if search_field:
            params["search_field"] = search_field
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        
        response = await self.make_request("GET", "/vacancies", params=params)
        return response.json()
    
    async def get_vacancy(self, vacancy_id: str) -> Dict[str, Any]:
        """Получение информации о конкретной вакансии"""
        response = await self.make_request("GET", f"/vacancies/{vacancy_id}")
        return response.json()
    
    async def get_vacancies_batch(self, vacancy_ids: List[str]) -> List[Dict[str, Any]]:
        """Получение информации о нескольких вакансиях"""
        tasks = [self.get_vacancy(vacancy_id) for vacancy_id in vacancy_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        vacancies = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Failed to fetch vacancy", error=str(result))
            else:
                vacancies.append(result)
                
        return vacancies
    
    async def get_areas(self) -> List[Dict[str, Any]]:
        """Получение списка регионов"""
        response = await self.make_request("GET", "/areas")
        return response.json()
    
    async def get_metro(self, city_id: int) -> List[Dict[str, Any]]:
        """Получение станций метро для города"""
        response = await self.make_request("GET", f"/metro/{city_id}")
        return response.json()
    
    async def get_industries(self) -> List[Dict[str, Any]]:
        """Получение списка отраслей"""
        response = await self.make_request("GET", "/industries")
        return response.json()
    
    async def get_professional_roles(self) -> List[Dict[str, Any]]:
        """Получение профессиональных ролей"""
        response = await self.make_request("GET", "/professional_roles")
        return response.json()
