from pathlib import Path
import sys

_candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
for _candidate in _candidates:
    if (_candidate / "shared").exists() and str(_candidate) not in sys.path:
        sys.path.append(str(_candidate))

# Register Celery tasks.
from app import celery_tasks  # noqa: E402,F401
