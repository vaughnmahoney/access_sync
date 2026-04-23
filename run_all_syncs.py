"""
Run every Access ↔ Supabase sync once, in dependency order.

Each step starts after the previous process exits successfully. Any non-zero
exit code stops the chain.

  python run_all_syncs.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SYNC_SCRIPTS = (
    "customers_sync.py",
    "customer_services_sync.py",
    "customer_services_inventory_sync.py",
    "invoices_sync.py",
    "invoice_services_sync.py",
)


def main() -> None:
    root = Path(__file__).resolve().parent
    python = sys.executable

    for name in SYNC_SCRIPTS:
        script = root / name
        if not script.is_file():
            print(f"Missing script: {script}", file=sys.stderr)
            sys.exit(1)
        print(f"\n=== Running {name} ===\n")
        subprocess.run([python, str(script)], cwd=root, check=True)

    print("\n=== All syncs finished successfully ===")


if __name__ == "__main__":
    main()
