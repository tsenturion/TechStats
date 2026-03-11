import os
import sys
import importlib
from pathlib import Path
import pytest


SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent

if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

def _module_path(module):
    module_file = getattr(module, "__file__", None)
    if module_file:
        return Path(module_file).resolve()
    module_paths = getattr(module, "__path__", None)
    if module_paths:
        try:
            return Path(next(iter(module_paths))).resolve()
        except Exception:
            return None
    return None


def _purge_foreign_app_modules():
    for name, module in list(sys.modules.items()):
        if name == "config":
            module_path = _module_path(module)
            if not module_path or not (module_path == SERVICE_ROOT or SERVICE_ROOT in module_path.parents):
                sys.modules.pop(name, None)
            continue

        if name != "app" and not name.startswith("app."):
            continue
        module_path = _module_path(module)
        if not module_path:
            sys.modules.pop(name, None)
            continue
        if module_path == SERVICE_ROOT or SERVICE_ROOT in module_path.parents:
            continue
        sys.modules.pop(name, None)


_purge_foreign_app_modules()

os.environ["DEBUG"] = "true"
os.environ["JWT_SECRET_KEY"] = "test-jwt-secret"
os.environ["JWT_ALGORITHM"] = "HS256"
os.environ["ADMIN_USERNAME"] = "admin"
os.environ["ADMIN_PASSWORD"] = "admin"
os.environ["USER_USERNAME"] = "user"
os.environ["USER_PASSWORD"] = "user"


@pytest.fixture(autouse=True)
def _ensure_local_modules():
    service_root_str = str(SERVICE_ROOT)
    if service_root_str in sys.path:
        sys.path.remove(service_root_str)
    sys.path.insert(0, service_root_str)

    _purge_foreign_app_modules()
    importlib.invalidate_caches()
    importlib.import_module("config")
    importlib.import_module("app")
