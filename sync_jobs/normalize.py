"""Canonical string compare space for sync diff checks."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from sync_jobs import config as cfg
from sync_jobs.converters import coerce_value_for_access_date_field, to_access_yn_bool
from sync_jobs.spec_types import CompareSemantics


def normalize_compare_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        if value == -1:
            return "1"
        if value == 0:
            return "0"
        return str(value)
    if isinstance(value, float):
        if value == -1.0:
            return "1"
        if value == 0.0:
            return "0"
        if value.is_integer():
            return str(int(value))
        return format(Decimal(str(value)).normalize(), "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"true", "yes", "y"}:
        return "1"
    if lowered in {"false", "no", "n"}:
        return "0"
    try:
        dec = Decimal(text)
        if dec == Decimal("-1"):
            return "1"
        if dec == Decimal("0"):
            return "0"
        if dec == dec.to_integral_value():
            return str(int(dec))
        return format(dec.normalize(), "f")
    except (InvalidOperation, ValueError):
        pass
    return text


def normalize_compare_value_for_col(semantics: CompareSemantics, col: str, value: Any) -> str:
    if col in semantics.bool_yn_cols:
        return normalize_compare_value(to_access_yn_bool(value))

    if col in semantics.datetime_compare_cols:
        if value is None:
            return ""
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is not None:
                dt = dt.astimezone().replace(tzinfo=None)
            return dt.replace(microsecond=0).isoformat(sep="T")
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time()).replace(microsecond=0).isoformat(sep="T")
        text = str(value).strip()
        if text == "":
            return ""
        try:
            text = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone().replace(tzinfo=None)
            return parsed.replace(microsecond=0).isoformat(sep="T")
        except Exception:
            return normalize_compare_value(value)

    if col in semantics.dateonly_compare_cols:
        if value is None:
            return ""
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is not None:
                dt = dt.astimezone().replace(tzinfo=None)
            return dt.date().isoformat()
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        text = str(value).strip()
        if text == "":
            return ""
        try:
            text = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone().replace(tzinfo=None)
            return parsed.date().isoformat()
        except Exception:
            return normalize_compare_value(value)

    base = normalize_compare_value(value)
    if col in semantics.money_cols:
        if base == "":
            return ""
        try:
            dec = Decimal(base).quantize(cfg.MONEY_QUANT, rounding=ROUND_HALF_UP)
            return format(dec, "f")
        except (InvalidOperation, ValueError):
            return base

    if col in semantics.gloffset_cols:
        if base == "":
            return ""
        try:
            dec = Decimal(base).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            return str(int(dec))
        except (InvalidOperation, ValueError):
            return base

    return base


def compare_values_for_col(semantics: CompareSemantics, col: str, lhs: Any, rhs: Any) -> bool:
    left = normalize_compare_value_for_col(semantics, col, lhs)
    right = normalize_compare_value_for_col(semantics, col, rhs)
    return left == right
