"""
Create the three composite-key dupe tables in Microsoft Access with the same column
definitions as their source tables.

Uses Jet/ACE ``SELECT * INTO [dupe] FROM [real] WHERE 0=1`` (empty clone; ``TOP 0`` errors
with some ODBC Access drivers). This copies field types and
sizes. It does **not** copy primary keys, indexes, defaults, validation rules, or
relationships — add those separately if your process requires them.

Requires:
  - Microsoft Access ODBC driver
  - Database path: ``--database``, or env ``DATABASE``, or ``ACCESS_DB_PATH`` (see ``resolve_database_path``),
    then default ``G:\\dbHyland\\Hfsapp.accdb``. Use the same ``.accdb`` that contains your real tables.

Usage:
  python create_access_dupe_tables.py
  python create_access_dupe_tables.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyodbc

from sync_jobs.env_file import load_env_file

# Load .env without importing sync_jobs.config (needs Supabase keys). Layer cwd over script dir.
_SCRIPT_DIR = Path(__file__).resolve().parent
load_env_file(_SCRIPT_DIR / ".env")
load_env_file(Path.cwd() / ".env", override=True)

_DEFAULT_DB = r"G:\dbHyland\Hfsapp.accdb"


def _strip_env_path(value: str) -> str:
    """Normalize path; tolerate pasted ``DATABASE=G:\\...accdb;TABLE=...`` in one variable."""
    v = value.strip().strip('"').strip("'")
    if ".accdb" not in v.lower() and ".mdb" not in v.lower():
        return v
    if ";" in v.upper() or v.upper().startswith("DATABASE="):
        for chunk in v.split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            if "=" in chunk:
                chunk = chunk.split("=", 1)[1].strip()
            low = chunk.lower()
            if low.endswith(".accdb") or low.endswith(".mdb"):
                return chunk
    return v


def resolve_database_path(cli_database: str | None) -> tuple[str, str | None]:
    """Return (path, source_label). Precedence: CLI > DATABASE > ACCESS_DB_PATH > default."""
    if cli_database:
        return cli_database.strip(), "--database"
    for key in ("DATABASE", "ACCESS_DB_PATH"):
        raw = os.environ.get(key)
        if raw:
            return _strip_env_path(raw), key
    return _DEFAULT_DB, None


# (real_table, dupe_table) — keep aligned with sync_jobs.specs.* (invoice_services,
# customer_services ACCESS_REAL_TABLE / ACCESS_DUPE_TABLE, customer_services_inventory).
TABLE_PAIRS: tuple[tuple[str, str], ...] = (
    ("tblInvoiceSvc", "dupeInvoiceSvc"),
    ("tblCustSvc", "dupeCustSvc"),
    ("tblCustSvcInv", "dupeCustSvcInv"),
)


def _table_exists(conn: pyodbc.Connection, name: str) -> bool:
    """Detect local or linked tables; fall back to a probe query if ODBC catalogs omit names."""
    needle = name.lower()
    cur = conn.cursor()
    try:
        for row in cur.tables():
            tn = getattr(row, "table_name", None)
            if tn is None and len(row) > 2:
                tn = row[2]
            if tn is not None and str(tn).lower() == needle:
                return True
    except Exception:
        pass
    try:
        probe = conn.cursor()
        probe.execute(f"SELECT TOP 1 * FROM [{name}]")
        probe.fetchone()
        return True
    except Exception:
        return False


def create_dupe_from_real(
    conn: pyodbc.Connection,
    real: str,
    dupe: str,
    *,
    dry_run: bool,
    recreate: bool,
) -> None:
    print(f"{real} → {dupe}")

    if dry_run:
        if recreate:
            print(f"  [dry-run] DROP TABLE [{dupe}]")
        print(f"  [dry-run] SELECT * INTO [{dupe}] FROM [{real}] WHERE 0=1")
        return

    if not _table_exists(conn, real):
        raise RuntimeError(
            f"Source table [{real}] does not exist in this database (wrong .accdb path?). "
            f"Expected something like DATABASE=G:\\path\\file.accdb matching the file where [{real}] lives."
        )

    cursor = conn.cursor()
    dupe_exists = _table_exists(conn, dupe)
    if dupe_exists:
        if recreate:
            cursor.execute(f"DROP TABLE [{dupe}]")
            conn.commit()
            print(f"  Dropped [{dupe}]")
        else:
            raise RuntimeError(
                f"[{dupe}] already exists. Omit --no-recreate to drop and recreate, or drop the table manually."
            )

    cursor.execute(f"SELECT * INTO [{dupe}] FROM [{real}] WHERE 0=1")
    conn.commit()
    print(f"  Created empty [{dupe}] with structure from [{real}].")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Create Access dupe tables from real tables")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions only",
    )
    parser.add_argument(
        "--no-recreate",
        action="store_true",
        help="If dupe table exists, abort instead of dropping it first (default: drop and recreate)",
    )
    parser.add_argument(
        "--database",
        metavar="PATH",
        default=None,
        help="Path to .accdb (overrides DATABASE / ACCESS_DB_PATH env)",
    )
    args = parser.parse_args(argv)

    db_path, db_src = resolve_database_path(args.database)
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={db_path};"
    )

    src_note = f" ({db_src})" if db_src else " (default)"
    print(f"Database: {db_path}{src_note}")
    recreate = not args.no_recreate

    if not args.dry_run:
        if not Path(db_path).is_file():
            print(f"Error: database file not found: {db_path}", file=sys.stderr)
            sys.exit(2)

    for real, dupe in TABLE_PAIRS:
        try:
            if args.dry_run:
                create_dupe_from_real(None, real, dupe, dry_run=True, recreate=recreate)
                continue
            conn = pyodbc.connect(conn_str)
            try:
                create_dupe_from_real(conn, real, dupe, dry_run=False, recreate=recreate)
            finally:
                conn.close()
        except Exception as e:
            print(f"Error on {real} → {dupe}: {e}", file=sys.stderr)
            sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main(sys.argv[1:])
