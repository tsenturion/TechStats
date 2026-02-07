import asyncio
import time
from datetime import datetime
from typing import Any, Dict

import psutil


def iso_now() -> str:
    return datetime.now().isoformat()


def build_process_stats() -> Dict[str, Any]:
    process = psutil.Process()
    memory_info = process.memory_info()
    try:
        uptime = asyncio.get_event_loop().time()
    except RuntimeError:
        uptime = time.time()
    return {
        "memory_usage_mb": memory_info.rss / 1024 / 1024,
        "cpu_percent": process.cpu_percent(),
        "threads": process.num_threads(),
        "uptime": uptime,
    }
