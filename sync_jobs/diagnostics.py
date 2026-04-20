"""Optional diff histograms after fetch (Access vs dupe after semantic filter)."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

from sync_jobs import config as cfg
from sync_jobs.compare_logic import explain_jet_sql_only_mismatch
from sync_jobs.converters import snapshot_key_from_row
from sync_jobs.normalize import normalize_compare_value_for_col
from sync_jobs.spec_types import TableSyncSpec

if TYPE_CHECKING:
    import pyodbc

__all__ = ["diagnose_access_tbl_vs_dupe_changes"]


def diagnose_access_tbl_vs_dupe_changes(
    conn: "pyodbc.Connection",
    spec: TableSyncSpec,
    changed_rows: list[dict[str, Any]],
    dupe_snapshot: dict[str, dict[str, Any]] | None = None,
) -> None:
    # Local import avoids import-order issues when deploying partial copies of sync_jobs.
    from sync_jobs.access_io import fetch_dupe_snapshot

    if not changed_rows:
        return

    print(f"{spec.real_table} vs {spec.dupe_table} ({spec.job_id}) — diff diagnosis...")
    snapshot = dupe_snapshot if dupe_snapshot is not None else fetch_dupe_snapshot(conn, spec)
    sem = spec.semantics
    counts: Counter[str] = Counter()
    jet_sql_only = 0
    samples: list[tuple[Any, str, str, str]] = []

    for real in changed_rows:
        sk = snapshot_key_from_row(real, spec.access_join_keys)
        dupe = snapshot.get(sk)

        if dupe is None:
            counts["<no_dupe_row>"] += 1
            if len(samples) < cfg.ACCESS_TBL_VS_DUPE_DIAG_SAMPLES:
                samples.append((sk or "<bad_key>", "<no_dupe_row>", "", ""))
            continue

        diff_col = None
        lhs_txt = ""
        rhs_txt = ""
        for col in sem.compare_columns:
            lhs_txt = normalize_compare_value_for_col(sem, col, real.get(col))
            rhs_txt = normalize_compare_value_for_col(sem, col, dupe.get(col))
            if lhs_txt != rhs_txt:
                diff_col = col
                break

        if diff_col is None:
            jet_sql_only += 1
            kind, detail_col, jet_lhs, jet_rhs = explain_jet_sql_only_mismatch(sem, real, dupe)
            counts[f"<jet:{kind}>"] += 1
            label = f"<jet:{kind}:{detail_col}>" if detail_col else f"<jet:{kind}>"
            if len(samples) < cfg.ACCESS_TBL_VS_DUPE_DIAG_SAMPLES:
                samples.append((sk or "<bad_key>", label, jet_lhs or lhs_txt, jet_rhs or rhs_txt))
            continue

        counts[diff_col] += 1
        if len(samples) < cfg.ACCESS_TBL_VS_DUPE_DIAG_SAMPLES:
            samples.append((sk or "<bad_key>", diff_col, lhs_txt, rhs_txt))

    print(
        f"  Rows: {len(changed_rows)} | no dupe row: {counts.get('<no_dupe_row>', 0)} | "
        f"Jet SQL-only mismatch (normalized equal): {jet_sql_only}"
    )

    top = counts.most_common(cfg.ACCESS_TBL_VS_DUPE_DIAG_TOP_N)
    print("  Top diff columns: " + ", ".join(f"{k}: {v}" for k, v in top))

    for sk, col, lhs, rhs in samples:
        print(f"  sample key={sk}, column={col}, tbl~={lhs!r}, dupe~={rhs!r}")
