"""
Shared Access ↔ Supabase ↔ dupe sync framework.

Import orchestration explicitly to avoid pulling `.env` at package import time:
    from sync_jobs.pipeline import run_sync_once

Per-table specs:
    from sync_jobs.specs.invoices import INVOICES_SPEC

This package never writes to real Access tables — only dupe mirrors and Supabase.
"""

from sync_jobs.spec_types import CompareSemantics, TableSyncSpec

__all__ = ["CompareSemantics", "TableSyncSpec"]
