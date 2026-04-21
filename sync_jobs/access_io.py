"""Access ODBC: changed-row query, dupe snapshot, dupe upserts."""

from __future__ import annotations

from typing import Any

import pyodbc

from sync_jobs import config as cfg
from sync_jobs.compare_logic import access_row_normalized_differs_from_dupe
from sync_jobs.converters import snapshot_key_from_row, to_int
from sync_jobs.spec_types import TableSyncSpec


def get_access_connection() -> pyodbc.Connection:
    return pyodbc.connect(cfg.ACCESS_CONN_STR)


def fetch_dupe_snapshot(conn: pyodbc.Connection, spec: TableSyncSpec) -> dict[str, dict]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM [{spec.dupe_table}]")
    rows = cursor.fetchall()
    snapshot: dict[str, dict] = {}
    for row in rows:
        record = {}
        for i, col in enumerate(cursor.description):
            record[col[0]] = row[i]
        key = snapshot_key_from_row(record, spec.access_join_keys)
        if key:
            snapshot[key] = record
    return snapshot


def preflight_access_tables(conn: pyodbc.Connection, spec: TableSyncSpec) -> None:
    """Fail fast with a clear message if Jet cannot open real / dupe (common: dupe never created)."""
    cur = conn.cursor()
    checks: tuple[tuple[str, str], ...] = (
        ("real (source)", spec.real_table),
        ("dupe (mirror)", spec.dupe_table),
    )
    for role, tbl in checks:
        try:
            cur.execute(f"SELECT TOP 1 * FROM [{tbl}]")
            cur.fetchone()
        except pyodbc.Error as e:
            low = str(e).lower()
            if "cannot find" not in low and "-1305" not in low and "not found" not in low:
                raise
            detail = ""
            if tbl == spec.dupe_table:
                detail = (
                    " Run create_access_dupe_tables.py once to clone empty mirrors from the real tables "
                    f'(point --database / ACCESS_DB_PATH at this same file: {cfg.ACCESS_DB_PATH}).'
                )
            raise RuntimeError(
                f"Access table [{tbl}] ({role}) is missing from {cfg.ACCESS_DB_PATH!r}.{detail}"
            ) from e


def _build_fetch_changed_sql(spec: TableSyncSpec) -> str:
    dup = "dup"
    join_on = " AND ".join(f"r.[{k}] = {dup}.[{k}]" for k in spec.access_join_keys)
    first_key = spec.access_join_keys[0]
    or_parts = [f"r.[{col}] <> {dup}.[{col}]" for col in spec.semantics.compare_columns]
    or_sql = "\n            OR ".join(or_parts)
    return f"""
        SELECT r.*
        FROM [{spec.real_table}] AS r
        LEFT JOIN [{spec.dupe_table}] AS {dup}
            ON {join_on}
        WHERE
            {dup}.[{first_key}] Is Null
            OR {or_sql}
    """


def fetch_changed_rows(
    conn: pyodbc.Connection, spec: TableSyncSpec
) -> tuple[list[dict], dict[str, dict] | None]:
    preflight_access_tables(conn, spec)
    cursor = conn.cursor()
    sql = _build_fetch_changed_sql(spec)
    cursor.execute(sql)
    rows = cursor.fetchall()

    candidates: list[dict] = []
    skipped_missing_keys = 0
    req = spec.required_access_columns

    for row in rows:
        record = {}
        for i, col in enumerate(cursor.description):
            record[col[0]] = row[i]

        if any(to_int(record.get(rc)) is None for rc in req):
            skipped_missing_keys += 1
            continue
        candidates.append(record)

    if skipped_missing_keys:
        print(f"Skipped {skipped_missing_keys} Access rows missing required keys ({spec.job_id})")

    if not candidates:
        return [], None

    dupe_snapshot = fetch_dupe_snapshot(conn, spec)
    sem = spec.semantics
    results = []
    for record in candidates:
        key = snapshot_key_from_row(record, spec.access_join_keys)
        dupe_row = dupe_snapshot.get(key)
        if access_row_normalized_differs_from_dupe(sem, record, dupe_row):
            results.append(record)

    return results, dupe_snapshot


def _insert_dupe_row_values(
    cursor: pyodbc.Cursor, spec: TableSyncSpec, safe: dict[str, Any]
) -> None:
    cols = spec.dupe_columns_ordered
    insert_cols_sql = ", ".join(f"[{col}]" for col in cols)
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT INTO [{spec.dupe_table}] ({insert_cols_sql}) VALUES ({placeholders})"
    values = [safe[col] for col in cols]
    try:
        cursor.execute(sql, values)
    except Exception:
        key_repr = "|".join(repr(safe.get(k)) for k in spec.access_join_keys)
        date_debug = {}
        for col in sorted(spec.semantics.dupe_datetime_cols | spec.semantics.dupe_dateonly_cols):
            v = safe.get(col)
            date_debug[col] = (repr(v), type(v).__name__)
        print(
            f"dupe INSERT failed for key={key_repr} ({spec.job_id}). "
            f"Sanitized date/time columns: {date_debug}"
        )
        raise


def _update_dupe_row_values(cursor: pyodbc.Cursor, spec: TableSyncSpec, safe: dict[str, Any]) -> int:
    join_keys = spec.access_join_keys
    set_cols = [col for col in spec.dupe_columns_ordered if col not in join_keys]
    set_sql = ", ".join(f"[{col}] = ?" for col in set_cols)
    where_sql = " AND ".join(f"[{k}] = ?" for k in join_keys)
    sql = f"UPDATE [{spec.dupe_table}] SET {set_sql} WHERE {where_sql}"
    values = [safe[col] for col in set_cols] + [safe.get(k) for k in join_keys]
    cursor.execute(sql, values)
    return cursor.rowcount


def _dupe_row_exists(cursor: pyodbc.Cursor, spec: TableSyncSpec, safe: dict[str, Any]) -> bool:
    keys = spec.access_join_keys
    where_sql = " AND ".join(f"[{k}] = ?" for k in keys)
    values = [safe.get(k) for k in keys]
    cursor.execute(f"SELECT 1 FROM [{spec.dupe_table}] WHERE {where_sql}", values)
    return cursor.fetchone() is not None


def _upsert_dupe_row_no_delete(cursor: pyodbc.Cursor, spec: TableSyncSpec, row: dict[str, Any]) -> None:
    # One sanitize pass per row (was 2–3× when UPDATE + INSERT + EXISTS each called sanitize).
    safe = spec.sanitize_dupe_row_for_access_insert(row)
    rowcount = _update_dupe_row_values(cursor, spec, safe)
    if rowcount == 0:
        _insert_dupe_row_values(cursor, spec, safe)
        return
    if rowcount == -1 and not _dupe_row_exists(cursor, spec, safe):
        _insert_dupe_row_values(cursor, spec, safe)


def upsert_dupe_rows_from_access(conn: pyodbc.Connection, spec: TableSyncSpec, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    cursor = conn.cursor()
    rows_since_commit = 0
    # One Jet UPDATE or INSERT per row — unavoidable without Access bulk APIs; commits batched only.
    for row in rows:
        _upsert_dupe_row_no_delete(cursor, spec, row)
        rows_since_commit += 1
        if rows_since_commit >= cfg.DUPE_ACCESS_COMMIT_EVERY_ROWS:
            conn.commit()
            print(
                f"Committed {rows_since_commit} Access->dupe row updates ({spec.job_id}) "
            )
            rows_since_commit = 0
    if rows_since_commit:
        conn.commit()
        print(
            f"Committed final {rows_since_commit} Access->dupe row updates ({spec.job_id}) "
        )


def upsert_dupe_rows_from_supabase(conn: pyodbc.Connection, spec: TableSyncSpec, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    cursor = conn.cursor()
    mapped_rows = [spec.map_supabase_to_dupe(row) for row in rows]
    rows_since_commit = 0
    try:
        for mapped in mapped_rows:
            _upsert_dupe_row_no_delete(cursor, spec, mapped)
            rows_since_commit += 1
            if rows_since_commit >= cfg.DUPE_SUPABASE_COMMIT_EVERY_ROWS:
                conn.commit()
                print(
                    f"Committed {rows_since_commit} Supabase->dupe row updates ({spec.job_id}) "
                )
                rows_since_commit = 0
    except Exception:
        sk = snapshot_key_from_row(mapped_rows[0], spec.access_join_keys) if mapped_rows else ""
        print(f"Failed dupe batch starting key={sk!r} ({spec.job_id})")
        raise
    if rows_since_commit:
        conn.commit()
        print(
            f"Committed final {rows_since_commit} Supabase->dupe row updates ({spec.job_id}) "
        )
