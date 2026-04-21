"""Environment-driven configuration shared by all sync jobs."""

import os
from decimal import Decimal
from pathlib import Path

from sync_jobs.env_file import load_env_file

_ACCESS_SYNC_ROOT = Path(__file__).resolve().parents[1]
# access_sync/.env first; then cwd (e.g. OptimaFlow root) overrides — same pattern as create_access_dupe_tables.
load_env_file(_ACCESS_SYNC_ROOT / ".env")
load_env_file(Path.cwd() / ".env", override=True)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(
            f"Missing required environment variable {name!r}. "
            f"Add it to {_ACCESS_SYNC_ROOT / '.env'} or {Path.cwd() / '.env'}, "
            "or set it in Windows environment variables. "
            f"See {_ACCESS_SYNC_ROOT / '.env.example'} for keys to copy."
        ) from None
    return value


SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = _require_env("SUPABASE_SERVICE_ROLE_KEY")

ACCESS_DB_PATH = os.environ.get("ACCESS_DB_PATH", r"G:\dbHyland\Hfsapp.accdb")
ACCESS_CONN_STR = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    rf"DBQ={ACCESS_DB_PATH};"
)

HTTP_TIMEOUT_SEC = 120
HTTP_MAX_RETRIES = 4
RETRYABLE_HTTP_STATUS = frozenset({408, 429, 500, 502, 503, 504})
UPSERT_BATCH_SIZE = 500
UPSERT_MIN_CHUNK = 1
SUPABASE_FETCH_PAGE_SIZE = 1000
DUPE_BEFORE_SUPABASE_FOR_TESTING = False

SYNC_TEST_MAX_CHANGED_ROWS = 0
SKIP_FULL_SUPABASE_DUPE_COMPARE_WHEN_ROW_CAP = True

def _int_env(key: str, default: str) -> int:
    raw = os.environ.get(key, default)
    try:
        return int(raw)
    except ValueError:
        return int(default)


DUPE_SUPABASE_COMMIT_EVERY_ROWS = _int_env("DUPE_SUPABASE_COMMIT_EVERY_ROWS", "2000")
# Larger = fewer commits (faster) but higher MaxLocksPerFile risk on older Jet; tune if dupe upsert is slow.
DUPE_ACCESS_COMMIT_EVERY_ROWS = _int_env("DUPE_ACCESS_COMMIT_EVERY_ROWS", "2000")

SUPABASE_DUPE_DIFF_LOG_SAMPLES = 10
ACCESS_TBL_VS_DUPE_DIAG_SAMPLES = 15
ACCESS_TBL_VS_DUPE_DIAG_TOP_N = 12

ACCESS_DATE_YEAR_MIN = 100
ACCESS_DATE_YEAR_MAX = 9999
DUPE_DATE_PLAUSIBLE_YEAR_MIN = 1970
DUPE_DATE_PLAUSIBLE_YEAR_MAX = 2100

MONEY_QUANT = Decimal("0.01")
