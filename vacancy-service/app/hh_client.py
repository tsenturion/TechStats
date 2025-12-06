# C:\Users\user\Desktop\TechStats\vacancy-service\app\hh_client.py
import asyncio
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import backoff

from config import settings
from app.rate_limiter import RateLimiter

logger = structlog.get_logger()


class HHClient:
    """Клиент для работы с HH.ru API"""
    
    def __init__(self):
        self.client: Optional[httpx.AsyncClient] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.last_request_time: float = 0
        self.request_lock = asyncio.Lock()
        
    async def initialize(self):
        """Инициализация клиента"""
        self.client = httpx.AsyncClient(
            timeout=settings.hh_api_timeout,
            headers={
                "User-Agent": settings.hh_api_user_agent,
                "Accept": "application/json",
                "Accept-Charset": "utf-8"
            },
            follow_redirects=True
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
    
    @retry(
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        reraise=True
    )
    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPStatusError, httpx.RequestError),
        max_tries=settings.max_retries,
        max_time=30
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
        
        url = f"{settings.hh_api_base_url}{endpoint}"
        
        try:
            response = await self.client.request(
                method=method,
                url=url,
                params=params,
                json=json_data
            )
            
            # Логирование
            logger.debug(
                "HH API request",
                method=method,
                url=url,
                status_code=response.status_code,
                params=params
            )
            
            response.raise_for_status()
            return response
            
        except httpx.TimeoutException:
            logger.error("HH API timeout", url=url, params=params)
            raise
        except httpx.HTTPStatusError as e:
            logger.error(
                "HH API error",
                url=url,
                status_code=e.response.status_code,
                error=str(e)
            )
            
            # Обработка специфичных ошибок HH
            if e.response.status_code == 429:
                logger.warning("HH API rate limit exceeded")
                await asyncio.sleep(10)  # Ждем 10 секунд при rate limit
            elif e.response.status_code == 403:
                logger.warning("HH API access forbidden")
            elif e.response.status_code == 404:
                logger.info("HH API resource not found", url=url)
                
            raise
        except Exception as e:
            logger.error("HH API unexpected error", url=url, error=str(e))
            raise
    
    async def search_vacancies(
        self,
        query: str,
        area: int = 113,
        page: int = 0,
        per_page: int = 100,
        search_field: str = "name",
        only_with_salary: bool = False
    ) -> Dict[str, Any]:
        """Поиск вакансий"""
        params = {
            "text": query,
            "search_field": search_field,
            "area": area,
            "page": page,
            "per_page": per_page,
            "only_with_salary": only_with_salary,
            "order_by": "relevance",
            "locale": "RU"
        }
        
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