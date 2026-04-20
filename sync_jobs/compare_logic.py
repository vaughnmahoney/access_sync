"""Row-vs-row comparisons using CompareSemantics + TableSyncSpec."""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from sync_jobs import config as cfg
from sync_jobs.converters import coerce_value_for_access_date_field
from sync_jobs.normalize import compare_values_for_col as norm_compare_values_for_col
from sync_jobs.normalize import normalize_compare_value_for_col
from sync_jobs.spec_types import CompareSemantics, TableSyncSpec


def decimal_for_diag(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def access_row_normalized_differs_from_dupe(
    semantics: CompareSemantics,
    real: dict[str, Any],
    dupe: dict[str, Any] | None,
) -> bool:
    if dupe is None:
        return True
    for col in semantics.compare_columns:
        if normalize_compare_value_for_col(semantics, col, real.get(col)) != normalize_compare_value_for_col(
            semantics, col, dupe.get(col)
        ):
            return True
    return False


def explain_jet_sql_only_mismatch(
    semantics: CompareSemantics, real: dict[str, Any], dupe: dict[str, Any]
) -> tuple[str, str | None, str, str]:
    for col in semantics.compare_columns:
        ln = normalize_compare_value_for_col(semantics, col, real.get(col))
        rn = normalize_compare_value_for_col(semantics, col, dupe.get(col))
        if ln != rn:
            return ("normalized_diff_inconsistent", col, ln, rn)

    for col in semantics.compare_columns:
        a, b = real.get(col), dupe.get(col)

        if col in semantics.money_cols:
            da = decimal_for_diag(a)
            db = decimal_for_diag(b)
            if da is None and db is None:
                continue
            if da is None or db is None:
                return ("money_null_pair", col, repr(a), repr(b))
            q2a = da.quantize(cfg.MONEY_QUANT, rounding=ROUND_HALF_UP)
            q2b = db.quantize(cfg.MONEY_QUANT, rounding=ROUND_HALF_UP)
            if q2a == q2b and da != db:
                return ("sub_cent_money", col, format(da, "f"), format(db, "f"))

        if col in semantics.dateonly_compare_cols:
            if isinstance(a, datetime) and isinstance(b, datetime):
                xa = a.replace(tzinfo=None) if a.tzinfo else a
                xb = b.replace(tzinfo=None) if b.tzinfo else b
                if xa.date() == xb.date() and xa != xb:
                    return ("date_only_time_skew", col, str(a), str(b))

        if col in semantics.datetime_compare_cols:
            da_dt = coerce_value_for_access_date_field(a, date_only=False)
            db_dt = coerce_value_for_access_date_field(b, date_only=False)
            if isinstance(da_dt, datetime) and isinstance(db_dt, datetime):
                na = da_dt.replace(tzinfo=None) if da_dt.tzinfo else da_dt
                nb = db_dt.replace(tzinfo=None) if db_dt.tzinfo else db_dt
                if na.replace(microsecond=0) == nb.replace(microsecond=0) and na != nb:
                    return ("submicrosecond_datetime", col, str(a), str(b))

        if col in semantics.text_trim_cols:
            sa = str(a).strip() if a is not None else ""
            sb = str(b).strip() if b is not None else ""
            if sa == sb and a != b:
                return ("text_whitespace", col, repr(a), repr(b))

    return ("unknown", None, "", "")


def rows_differ_supabase_vs_dupe(spec: TableSyncSpec, supabase_row: dict[str, Any], dupe_row: dict[str, Any] | None) -> bool:
    if dupe_row is None:
        return True
    mapped = spec.map_supabase_row_to_dupe_for_compare(supabase_row)
    sem = spec.semantics
    for col in sem.compare_columns:
        if not norm_compare_values_for_col(sem, col, mapped.get(col), dupe_row.get(col)):
            return True
    return False


def first_diff_column_supabase_vs_dupe(
    spec: TableSyncSpec, supabase_row: dict[str, Any], dupe_row: dict[str, Any] | None
) -> tuple[str, str, str] | None:
    if dupe_row is None:
        return ("<missing_dupe_row>", "", "")
    mapped = spec.map_supabase_row_to_dupe_for_compare(supabase_row)
    sem = spec.semantics
    for col in sem.compare_columns:
        lhs = normalize_compare_value_for_col(sem, col, mapped.get(col))
        rhs = normalize_compare_value_for_col(sem, col, dupe_row.get(col))
        if lhs != rhs:
            return (col, lhs, rhs)
    return None
