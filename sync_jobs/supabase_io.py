"""Supabase PostgREST: session, upsert, paged fetch."""

from __future__ import annotations

import random
import time
from typing import Any

import requests

from sync_jobs import config as cfg
from sync_jobs.converters import snapshot_key_from_row
from sync_jobs.state import utc_now_iso
from sync_jobs.spec_types import TableSyncSpec

_SUPABASE_HTTP_SESSION: requests.Session | None = None


def get_supabase_http_session() -> requests.Session:
    global _SUPABASE_HTTP_SESSION
    if _SUPABASE_HTTP_SESSION is None:
        s = requests.Session()
        s.headers.update(
            {
                "apikey": cfg.SUPABASE_SERVICE_ROLE_KEY,
                "Authorization": f"Bearer {cfg.SUPABASE_SERVICE_ROLE_KEY}",
            }
        )
        _SUPABASE_HTTP_SESSION = s
    return _SUPABASE_HTTP_SESSION


def supabase_rest_request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, str] | None = None,
    json_body: Any = None,
    extra_headers: dict[str, str] | None = None,
    timeout: int = cfg.HTTP_TIMEOUT_SEC,
) -> requests.Response:
    merged_headers = dict(extra_headers or {})
    attempt = 0
    while True:
        attempt += 1
        resp = session.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=merged_headers or None,
            timeout=timeout,
        )
        if resp.status_code in cfg.RETRYABLE_HTTP_STATUS and attempt < cfg.HTTP_MAX_RETRIES:
            delay = min(2 ** (attempt - 1), 30) + random.uniform(0, 0.5)
            print(
                f"HTTP {resp.status_code} on {method} — retry {attempt}/{cfg.HTTP_MAX_RETRIES} in {delay:.1f}s"
            )
            time.sleep(delay)
            continue
        return resp


def _upsert_batch_with_fallback(
    session: requests.Session,
    url: str,
    on_conflict: str,
    batch: list[dict[str, Any]],
    *,
    chunk_size: int,
    min_chunk: int,
) -> None:
    if not batch:
        return
    params = {"on_conflict": on_conflict}
    extra_headers = {
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    for i in range(0, len(batch), chunk_size):
        chunk = batch[i : i + chunk_size]
        response = supabase_rest_request(
            session, "POST", url, params=params, json_body=chunk, extra_headers=extra_headers
        )
        if response.ok:
            continue
        if chunk_size > min_chunk:
            next_size = max(min_chunk, chunk_size // 2)
            print(
                f"Upsert chunk size {chunk_size} failed ({response.status_code}); "
                f"retrying {len(chunk)} row(s) with POST size {next_size}"
            )
            for j in range(0, len(chunk), next_size):
                sub = chunk[j : j + next_size]
                _upsert_batch_with_fallback(
                    session, url, on_conflict, sub, chunk_size=next_size, min_chunk=min_chunk
                )
        else:
            print("Supabase upsert failed.")
            print("Status code:", response.status_code)
            print("Response text:", response.text)
            raise RuntimeError(f"Supabase upsert failed: {response.status_code}")


def upsert_supabase_rows(
    spec: TableSyncSpec, rows: list[dict[str, Any]], batch_size: int = cfg.UPSERT_BATCH_SIZE
) -> None:
    if not rows:
        return
    session = get_supabase_http_session()
    url = f"{cfg.SUPABASE_URL}/rest/v1/{spec.supabase_table}"
    sync_time = utc_now_iso()
    payload_rows = [spec.map_access_to_supabase(row, sync_time) for row in rows]

    skipped_missing_keys = 0
    cleaned_rows = []
    for row in payload_rows:
        if any(row.get(k) is None for k in spec.supabase_upsert_nonnull):
            skipped_missing_keys += 1
            continue
        cleaned_rows.append(row)

    if skipped_missing_keys:
        print(f"Skipped {skipped_missing_keys} rows missing required Supabase keys ({spec.job_id})")

    if not cleaned_rows:
        return

    _upsert_batch_with_fallback(
        session,
        url,
        spec.supabase_on_conflict,
        cleaned_rows,
        chunk_size=batch_size,
        min_chunk=cfg.UPSERT_MIN_CHUNK,
    )


def fetch_all_supabase_invoices(
    spec: TableSyncSpec,
    batch_size: int = cfg.SUPABASE_FETCH_PAGE_SIZE,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Full table scan: keyset pagination (single column) or offset (composite keys)."""

    session = session or get_supabase_http_session()
    url = f"{cfg.SUPABASE_URL}/rest/v1/{spec.supabase_table}"
    all_rows: list[dict[str, Any]] = []

    if spec.full_fetch_use_offset:
        order = spec.supabase_offset_order
        if not order:
            raise RuntimeError(f"full_fetch_use_offset requires supabase_offset_order ({spec.job_id})")
        offset = 0
        while True:
            params: dict[str, str] = {
                "select": spec.supabase_select_columns,
                "order": order,
                "limit": str(batch_size),
                "offset": str(offset),
            }
            response = supabase_rest_request(session, "GET", url, params=params)
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            all_rows.extend(batch)
            print(f"Fetched Supabase batch: {len(batch)} rows, total so far: {len(all_rows)} ({spec.job_id})")
            offset += batch_size
            if len(batch) < batch_size:
                break
        return all_rows

    kcol = spec.supabase_keyset_column
    last_key: Any = None
    while True:
        params = {
            "select": spec.supabase_select_columns,
            "order": f"{kcol}.asc",
            "limit": str(batch_size),
        }
        if last_key is not None:
            params[kcol] = f"gt.{last_key}"

        response = supabase_rest_request(session, "GET", url, params=params)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        all_rows.extend(batch)
        print(f"Fetched Supabase batch: {len(batch)} rows, total so far: {len(all_rows)} ({spec.job_id})")
        last_key = batch[-1].get(kcol)
        if last_key is None:
            break
        if len(batch) < batch_size:
            break
    return all_rows


def fetch_changed_supabase_rows_against_dupe(
    conn,
    spec: TableSyncSpec,
) -> list[dict[str, Any]]:
    from sync_jobs.access_io import fetch_dupe_snapshot
    from sync_jobs.compare_logic import first_diff_column_supabase_vs_dupe, rows_differ_supabase_vs_dupe
    from sync_jobs import config as cfg

    print("Loading dupe snapshot from Access...")
    dupe_snapshot = fetch_dupe_snapshot(conn, spec)
    print(f"Loaded dupe snapshot rows: {len(dupe_snapshot)}")

    print("Loading full Supabase table for compare (every row)...")
    supabase_rows = fetch_all_supabase_invoices(spec)
    print(f"Loaded Supabase rows: {len(supabase_rows)}")

    changed_rows: list[dict[str, Any]] = []
    diff_samples_logged = 0
    key_cols = spec.supabase_natural_key_columns

    for row in supabase_rows:
        key = snapshot_key_from_row(row, key_cols)
        if not key:
            continue
        dupe_row = dupe_snapshot.get(key)

        if rows_differ_supabase_vs_dupe(spec, row, dupe_row):
            changed_rows.append(row)
            if diff_samples_logged < cfg.SUPABASE_DUPE_DIFF_LOG_SAMPLES:
                first_diff = first_diff_column_supabase_vs_dupe(spec, row, dupe_row)
                if first_diff is not None:
                    col, lhs, rhs = first_diff
                    print(
                        "Supabase vs dupe diff sample "
                        f"key={key}, column={col}, "
                        f"supabase_mapped={lhs!r}, dupe={rhs!r}"
                    )
                    diff_samples_logged += 1

    return changed_rows
