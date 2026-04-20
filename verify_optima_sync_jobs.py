"""Verify OptimaFlow's ``sync_jobs`` matches composite-key ``TableSyncSpec``.

From OptimaFlow project root::

    cd C:\\Users\\ws14\\Desktop\\OptimaFlow
    python C:\\Users\\wsnote21\\Desktop\\DEV\\access_sync\\verify_optima_sync_jobs.py

Replace sync_jobs in one shot (USB / mapped drive / second clone path)::

    cd C:\\Users\\ws14\\Desktop\\OptimaFlow
    python ..\\DEV\\access_sync\\verify_optima_sync_jobs.py --install-from E:\\repos\\DEV\\access_sync

Exit 0 = ``access_join_keys`` present. Exit 2 = stale (or failed install).

"""

from __future__ import annotations

import argparse
import inspect
import shutil
import sys
from pathlib import Path


def _clear_sync_jobs_imports() -> None:
    for name in list(sys.modules):
        if name == "sync_jobs" or name.startswith("sync_jobs."):
            del sys.modules[name]


def _verify_sync_jobs_under(root: Path) -> bool:
    sys.path.insert(0, str(root))
    try:
        from sync_jobs.spec_types import TableSyncSpec

        sig = inspect.signature(TableSyncSpec)
        need = ("access_join_keys", "supabase_natural_key_columns")
        missing = [n for n in need if n not in sig.parameters]
        if missing:
            print("FAIL: TableSyncSpec is missing:", ", ".join(missing))
            print("Loaded parameters:", tuple(sig.parameters.keys()))
            print()
            print("Manual options (run from folder that CONTAINS sync_jobs and install_* scripts):")
            print('  install_sync_jobs.cmd "C:\\Users\\ws14\\Desktop\\OptimaFlow"')
            print()
            print("One-line fix from OptimaFlow root if you have access_sync elsewhere:")
            print(r'  python verify_optima_sync_jobs.py --install-from "D:\path\to\access_sync"')
            return False
        print("OK: sync_jobs/spec_types.TableSyncSpec has composite keys.")
        return True
    except ImportError as e:
        print("FAIL: cannot import sync_jobs.spec_types:", e)
        return False


def _install_sync_jobs(access_sync_root: Path, destination_project: Path) -> Path:
    """Copy ``access_sync_root/sync_jobs`` → ``destination_project/sync_jobs``."""
    src = (access_sync_root / "sync_jobs").resolve()
    dst = (destination_project / "sync_jobs").resolve()
    if not src.is_dir():
        raise FileNotFoundError(f"Missing folder: {src}")

    print(f"Replacing {dst}")
    print(f"   from {src}")

    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    return dst


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--install-from",
        dest="install_from",
        metavar="ACCESS_SYNC_ROOT",
        type=Path,
        default=None,
        help="Path to access_sync repo (must contain sync_jobs/). Replaces cwd/sync_jobs.",
    )
    parser.add_argument(
        "--dst",
        type=Path,
        default=None,
        help="Destination project root (default: cwd). Ignored unless --install-from is set.",
    )
    args = parser.parse_args()

    cwd = Path.cwd()
    dst_project = args.dst.expanduser().resolve() if args.dst else cwd

    if args.install_from:
        sync_root = args.install_from.expanduser().resolve()
        try:
            _install_sync_jobs(sync_root, dst_project)
        except FileNotFoundError as e:
            print(e)
            sys.exit(3)
        _clear_sync_jobs_imports()

    verify_root = dst_project if args.install_from else cwd
    ok = _verify_sync_jobs_under(verify_root)
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()
