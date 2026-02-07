import asyncio
import time
from typing import Any, Dict, List, Optional


class AnalysisStore:
    def __init__(self):
        self._records: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def add_record(self, record: Dict[str, Any]) -> None:
        async with self._lock:
            self._records.append(record)
            if len(self._records) > 5000:
                self._records = self._records[-5000:]

    async def list_records(self, since_ts: Optional[float] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            records = list(self._records)
        if since_ts is None:
            return records
        return [record for record in records if record.get("analysis_timestamp", 0) >= since_ts]

    async def summary(self, hours: int = 24) -> Dict[str, Any]:
        now = time.time()
        start_ts = now - (hours * 3600)
        records = await self.list_records(since_ts=start_ts)
        total = len(records)
        total_vacancies = sum(record.get("total_vacancies", 0) for record in records)
        total_matches = sum(record.get("tech_vacancies", 0) for record in records)
        avg_processing = (
            sum(record.get("request_stats", {}).get("processing_time", 0) for record in records) / total if total else 0
        )
        avg_cache_hit = (
            sum(record.get("request_stats", {}).get("cache_hit_rate", 0) for record in records) / total if total else 0
        )
        return {
            "total_analyses": total,
            "total_vacancies_processed": total_vacancies,
            "total_technologies_found": total_matches,
            "avg_processing_time_seconds": round(avg_processing, 3),
            "cache_hit_rate": round(avg_cache_hit, 2),
            "records": records,
        }


analysis_store = AnalysisStore()

