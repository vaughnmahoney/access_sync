"""One full sync cycle for a TableSyncSpec (Access → Supabase → dupe; Supabase → dupe)."""

from __future__ import annotations

import time
from datetime import datetime

from sync_jobs import config as cfg
from sync_jobs.access_io import (
    fetch_changed_rows,
    get_access_connection,
    upsert_dupe_rows_from_access,
    upsert_dupe_rows_from_supabase,
)
from sync_jobs.diagnostics import diagnose_access_tbl_vs_dupe_changes
from sync_jobs.spec_types import TableSyncSpec
from sync_jobs.state import set_supabase_compare_watermark
from sync_jobs.supabase_io import (
    fetch_changed_supabase_rows_against_dupe,
    upsert_supabase_rows,
)


def _maybe_cap_changed_rows(rows: list, cap: int) -> list:
    if cap <= 0 or not rows:
        return rows
    original = len(rows)
    out = rows[:cap]
    print(
        f"*** SYNC TEST MODE: processing only first {len(out)} of {original} Access-changed rows "
        f"(SYNC_TEST_MAX_CHANGED_ROWS={cap}). Set cap to 0 for full sync. ***"
    )
    return out


def run_sync_once(spec: TableSyncSpec) -> None:
    if spec.validate_before_run is not None:
        spec.validate_before_run(spec)

    cycle_started = time.perf_counter()
    print(f"Starting sync cycle ({spec.job_id}) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print("Opening Access connection...")
    conn = get_access_connection()
    print("Access connection opened.")

    try:
        t0 = time.perf_counter()
        print("Fetching changed rows from Access...")
        changed_rows, dupe_snapshot_for_diag = fetch_changed_rows(conn, spec)
        print(
            f"Changed/new Access rows found: {len(changed_rows)} ({time.perf_counter() - t0:.2f}s)"
        )
        diagnose_access_tbl_vs_dupe_changes(conn, spec, changed_rows, dupe_snapshot_for_diag)

        if len(changed_rows) > 1_000_000:
            raise RuntimeError(f"Aborting, suspiciously high changed row count: {len(changed_rows)}")

        changed_rows = _maybe_cap_changed_rows(changed_rows, cfg.SYNC_TEST_MAX_CHANGED_ROWS)

        if changed_rows:
            if cfg.DUPE_BEFORE_SUPABASE_FOR_TESTING:
                t_dupe = time.perf_counter()
                print("Updating dupe table from Access changes (before Supabase; testing order)...")
                upsert_dupe_rows_from_access(conn, spec, changed_rows)
                conn.commit()
                print(f"dupe updated from Access ({time.perf_counter() - t_dupe:.2f}s).")

                t1 = time.perf_counter()
                print("Upserting Access changes to Supabase...")
                upsert_supabase_rows(spec, changed_rows)
                print(f"Supabase upsert complete ({time.perf_counter() - t1:.2f}s).")
            else:
                t1 = time.perf_counter()
                print("Upserting Access changes to Supabase...")
                upsert_supabase_rows(spec, changed_rows)
                print(f"Supabase upsert complete ({time.perf_counter() - t1:.2f}s).")

                t2 = time.perf_counter()
                print("Updating dupe table from Access changes...")
                upsert_dupe_rows_from_access(conn, spec, changed_rows)
                conn.commit()
                print(f"dupe updated from Access ({time.perf_counter() - t2:.2f}s).")
        else:
            print("No Access changes found.")

        changed_supabase_rows: list = []
        supabase_watermark: str | None = None

        if cfg.SYNC_TEST_MAX_CHANGED_ROWS > 0 and cfg.SKIP_FULL_SUPABASE_DUPE_COMPARE_WHEN_ROW_CAP:
            print(
                "*** SYNC TEST MODE: skipping full Supabase vs dupe compare "
                "(SKIP_FULL_SUPABASE_DUPE_COMPARE_WHEN_ROW_CAP). ***"
            )
        else:
            t3 = time.perf_counter()
            print("Checking Supabase table against dupe table...")
            changed_supabase_rows, supabase_watermark = fetch_changed_supabase_rows_against_dupe(conn, spec)
            print(
                f"Changed/new Supabase rows found: {len(changed_supabase_rows)} "
                f"({time.perf_counter() - t3:.2f}s)"
            )
            if supabase_watermark:
                print(f"New Supabase compare watermark candidate: {supabase_watermark}")

        if changed_supabase_rows:
            t4 = time.perf_counter()
            print("Updating dupe table from Supabase changes...")
            upsert_dupe_rows_from_supabase(conn, spec, changed_supabase_rows)
            conn.commit()
            print(f"dupe updated from Supabase ({time.perf_counter() - t4:.2f}s).")
            if supabase_watermark:
                set_supabase_compare_watermark(spec.state_file, supabase_watermark)
                print(f"Saved Supabase compare watermark: {supabase_watermark}")
        else:
            print("No Supabase changes found.")
            if supabase_watermark:
                set_supabase_compare_watermark(spec.state_file, supabase_watermark)
                print(f"Saved Supabase compare watermark: {supabase_watermark}")

        print(f"Sync cycle complete ({spec.job_id}, total {time.perf_counter() - cycle_started:.2f}s).")

    except Exception:
        conn.rollback()
        raise
    finally:
        print("Closing Access connection...")
        conn.close()
        print("Access connection closed.")
