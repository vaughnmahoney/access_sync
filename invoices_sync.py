"""
Invoice sync entrypoint: tblInvoice → Supabase `invoices` → dupeInvoice; then Supabase → dupe.

Does not write to tblInvoice (read-only source). Configure ACCESS_DB_PATH and Supabase keys via `.env`.

Run once (recommended for Task Scheduler):
  python invoices_sync.py

Loop every N seconds (dev):
  python invoices_sync.py --loop 60
"""

from __future__ import annotations

import argparse
import os
import sys
import time


def main(argv: list[str] | None = None) -> None:
    from sync_jobs.pipeline import run_sync_once
    from sync_jobs.specs.invoices import INVOICES_SPEC

    parser = argparse.ArgumentParser(description="Sync invoices: Access ↔ Supabase ↔ dupeInvoice")
    parser.add_argument(
        "--loop",
        type=int,
        metavar="SEC",
        default=0,
        help="Repeat sync every SEC seconds (0 = run once and exit)",
    )
    args = parser.parse_args(argv)

    if args.loop <= 0:
        run_sync_once(INVOICES_SPEC)
        # Skip Python teardown on Windows — Access ODBC/pyodbc can crash on interpreter exit (0xC0000005).
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)

    while True:
        try:
            run_sync_once(INVOICES_SPEC)
        except Exception as e:
            print(f"Sync failed: {e}")
        print(f"Sleeping {args.loop} seconds...\n")
        time.sleep(args.loop)


if __name__ == "__main__":
    main(sys.argv[1:])
