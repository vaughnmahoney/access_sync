"""Type coercion: Supabase JSON, Access ODBC, and dupe bindings."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from sync_jobs import config as cfg
from sync_jobs.spec_types import CompareSemantics


def clean_text(value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    return text if text != "" else None


def to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "-1", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return None


def to_access_yn_bool(value: Any) -> bool | None:
    b = to_bool(value)
    return b


def to_gl_offset_int(value: Any) -> int | None:
    raw = to_decimal_number(value)
    if raw is None:
        return None
    try:
        return int(Decimal(str(raw)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, OverflowError):
        return None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            print(f"Warning: could not convert to int: {value!r}")
            return None


def to_decimal_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        print(f"Warning: could not convert to decimal: {value!r}")
        return None


def to_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if text == "":
        return None
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except Exception:
        return text


def to_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    text = str(value).strip()
    if text == "":
        return None
    try:
        return datetime.fromisoformat(text).isoformat()
    except Exception:
        return text


def to_access_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    if text == "":
        return None
    try:
        text = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.replace(microsecond=0)
    except Exception:
        return None


def to_access_date(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return datetime.combine(dt.date(), datetime.min.time()).replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).replace(microsecond=0)
    text = str(value).strip()
    if text == "":
        return None
    try:
        text = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return datetime.combine(dt.date(), datetime.min.time()).replace(microsecond=0)
    except Exception:
        return None


def _access_strip_tz(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone().replace(tzinfo=None)


def _ole_serial_to_datetime(serial: float) -> datetime | None:
    if serial != serial:
        return None
    try:
        base = datetime(1899, 12, 30)
        dt = base + timedelta(days=float(serial))
    except (OverflowError, OSError, ValueError):
        return None
    if dt.year < cfg.ACCESS_DATE_YEAR_MIN or dt.year > cfg.ACCESS_DATE_YEAR_MAX:
        return None
    return dt


def _dupe_year_ok_for_odbc(dt: datetime) -> bool:
    return cfg.DUPE_DATE_PLAUSIBLE_YEAR_MIN <= dt.year <= cfg.DUPE_DATE_PLAUSIBLE_YEAR_MAX


def coerce_value_for_access_date_field(value: Any, *, date_only: bool) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        dt = _access_strip_tz(value)
        if date_only:
            dt = datetime.combine(dt.date(), datetime.min.time())
        if dt.year < cfg.ACCESS_DATE_YEAR_MIN or dt.year > cfg.ACCESS_DATE_YEAR_MAX:
            return None
        dt = dt.replace(microsecond=0)
        if not _dupe_year_ok_for_odbc(dt):
            return None
        return dt
    if isinstance(value, date) and not isinstance(value, datetime):
        if value.year < cfg.ACCESS_DATE_YEAR_MIN or value.year > cfg.ACCESS_DATE_YEAR_MAX:
            return None
        dt = datetime.combine(value, datetime.min.time()).replace(microsecond=0)
        if not _dupe_year_ok_for_odbc(dt):
            return None
        return dt
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        dt = _ole_serial_to_datetime(float(value))
        if dt is None:
            return None
        dt = _access_strip_tz(dt)
        if date_only:
            dt = datetime.combine(dt.date(), datetime.min.time())
        if dt.year < cfg.ACCESS_DATE_YEAR_MIN or dt.year > cfg.ACCESS_DATE_YEAR_MAX:
            return None
        dt = dt.replace(microsecond=0)
        if not _dupe_year_ok_for_odbc(dt):
            return None
        return dt
    if isinstance(value, str):
        t = value.strip()
        if not t:
            return None
        try:
            t = t.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(t)
            return coerce_value_for_access_date_field(parsed, date_only=date_only)
        except Exception:
            return None
    return None


def sanitize_dupe_row_for_access_insert(semantics: CompareSemantics, row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for col in semantics.dupe_datetime_cols:
        out[col] = coerce_value_for_access_date_field(out.get(col), date_only=False)
    for col in semantics.dupe_dateonly_cols:
        out[col] = coerce_value_for_access_date_field(out.get(col), date_only=True)
    return out


def snapshot_key_from_row(row: dict[str, Any], key_columns: tuple[str, ...]) -> str:
    """Stable dict key for dupe snapshot / compare (same order as access or supabase natural keys)."""
    parts: list[str] = []
    for col in key_columns:
        part = normalize_key(row.get(col))
        if not part:
            return ""
        parts.append(part)
    return "|".join(parts)


def normalize_key(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value).strip()
    text = str(value).strip()
    if text == "":
        return ""
    try:
        number = float(text)
        if number.is_integer():
            return str(int(number))
    except ValueError:
        pass
    return text


def to_money_number(value: Any) -> float | None:
    raw = to_decimal_number(value)
    if raw is None:
        return None
    dec = Decimal(str(raw)).quantize(cfg.MONEY_QUANT, rounding=ROUND_HALF_UP)
    return float(dec)
