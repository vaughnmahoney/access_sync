"""Per-table sync contract (column semantics + callbacks)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class CompareSemantics:
    """Which Access column names use special compare/sanitize behavior."""

    compare_columns: tuple[str, ...]
    bool_yn_cols: frozenset[str]
    money_cols: frozenset[str]
    datetime_compare_cols: frozenset[str]
    dateonly_compare_cols: frozenset[str]
    gloffset_cols: frozenset[str]
    text_trim_cols: frozenset[str]
    dupe_datetime_cols: frozenset[str]
    dupe_dateonly_cols: frozenset[str]


@dataclass(frozen=True)
class TableSyncSpec:
    """One row-type (e.g. invoices): Access real + dupe + Supabase + mappings."""

    job_id: str

    real_table: str
    dupe_table: str
    supabase_table: str

    #: Access columns joining real to dupe (single or composite natural key).
    access_join_keys: tuple[str, ...]
    #: Supabase JSON columns for the same natural key (same length as access_join_keys).
    supabase_natural_key_columns: tuple[str, ...]
    #: Access columns that must be non-null for sync (Jet fetch filter).
    required_access_columns: tuple[str, ...]

    semantics: CompareSemantics
    #: Column order for INSERT/UPDATE into dupe (includes all join key columns first).
    dupe_columns_ordered: tuple[str, ...]

    supabase_select_columns: str
    supabase_on_conflict: str

    map_access_to_supabase: Callable[[dict[str, Any], str], dict[str, Any]]
    map_supabase_to_dupe: Callable[[dict[str, Any]], dict[str, Any]]

    sanitize_dupe_row_for_access_insert: Callable[[dict[str, Any]], dict[str, Any]]
    map_supabase_row_to_dupe_for_compare: Callable[[dict[str, Any]], dict[str, Any]]

    #: PostgREST keyset pagination on a single column (when full_fetch_use_offset is False).
    supabase_keyset_column: str = "invoice_number"
    #: Supabase JSON keys that must be non-None to upsert a row.
    supabase_upsert_nonnull: tuple[str, ...] = ("invoice_number", "customer_id")
    #: When True, full-table Supabase fetch uses OFFSET paging (needed for composite keys).
    full_fetch_use_offset: bool = False
    #: PostgREST order= value for offset full fetch (e.g. "invoice_number.asc,service_id.asc").
    supabase_offset_order: str | None = None
    #: Optional startup guard (e.g. forbid wrong table names in production).
    validate_before_run: Callable[[TableSyncSpec], None] | None = None
