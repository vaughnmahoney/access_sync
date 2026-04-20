"""Per-table TableSyncSpec modules (e.g. `sync_jobs.specs.invoices`)."""

from __future__ import annotations

from typing import Any

# Re-export so mistaken `from sync_jobs.specs import converters` (legacy / bad copy) works.
from sync_jobs import converters

__all__ = [
    "INVOICES_SPEC",
    "CUSTOMERS_SPEC",
    "INVOICE_SERVICES_SPEC",
    "CUSTOMER_SERVICES_SPEC",
    "CUSTOMER_SERVICES_INVENTORY_SPEC",
    "converters",
]


def __getattr__(name: str) -> Any:
    if name == "INVOICES_SPEC":
        from sync_jobs.specs.invoices import INVOICES_SPEC as spec

        return spec
    if name == "CUSTOMERS_SPEC":
        from sync_jobs.specs.customers import CUSTOMERS_SPEC as spec

        return spec
    if name == "INVOICE_SERVICES_SPEC":
        from sync_jobs.specs.invoice_services import INVOICE_SERVICES_SPEC as spec

        return spec
    if name == "CUSTOMER_SERVICES_SPEC":
        from sync_jobs.specs.customer_services import CUSTOMER_SERVICES_SPEC as spec

        return spec
    if name == "CUSTOMER_SERVICES_INVENTORY_SPEC":
        from sync_jobs.specs.customer_services_inventory import CUSTOMER_SERVICES_INVENTORY_SPEC as spec

        return spec
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
