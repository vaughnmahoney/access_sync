"""
Customer services: tblCustSvc → Supabase `customer_services` → dupeCustSvc; then Supabase → dupe.

Configure ACCESS_DB_PATH and Supabase keys via `.env`.

Run once:
  python customer_services_sync.py

Loop every N seconds (dev):
  python customer_services_sync.py --loop 60
"""

from __future__ import annotations

import argparse
import sys
import time


def main(argv: list[str] | None = None) -> None:
    from sync_jobs.pipeline import run_sync_once
    from sync_jobs.specs.customer_services import CUSTOMER_SERVICES_SPEC

    parser = argparse.ArgumentParser(
        description="Sync customer_services: Access ↔ Supabase ↔ dupeCustSvc"
    )
    parser.add_argument(
        "--loop",
        type=int,
        metavar="SEC",
        default=0,
        help="Repeat sync every SEC seconds (0 = run once and exit)",
    )
    args = parser.parse_args(argv)

    if args.loop <= 0:
        run_sync_once(CUSTOMER_SERVICES_SPEC)
        return

    while True:
        try:
            run_sync_once(CUSTOMER_SERVICES_SPEC)
        except Exception as e:
            print(f"Sync failed: {e}")
        print(f"Sleeping {args.loop} seconds...\n")
        time.sleep(args.loop)


if __name__ == "__main__":
    main(sys.argv[1:])
