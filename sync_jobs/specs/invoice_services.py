"""Invoice line-item services: tblInvoiceSvc ↔ invoice_services ↔ dupeInvoiceSvc.

Requires a UNIQUE constraint on (invoice_number, service_id) in Postgres for upserts.
The Supabase table has no updated_at in the baseline DDL; incremental compare uses created_at
(won't catch in-place edits that don't change created_at — add updated_at in DB if needed).
"""

from __future__ import annotations

from pathlib import Path

from sync_jobs import converters as cv
from sync_jobs.spec_types import CompareSemantics, TableSyncSpec

_ACCESS_SYNC_ROOT = Path(__file__).resolve().parent.parent.parent

INVOICE_SERVICES_STATE_FILE = _ACCESS_SYNC_ROOT / "sync_state" / "invoice_services_sync_state.json"

INVOICE_SERVICES_COMPARE_COLUMNS = (
    "txtSvcDesc",
    "curSvcPrice",
    "txtSvcDescLabor",
    "nbrSvcQty",
    "txtComments",
)

INVOICE_SERVICES_SEMANTICS = CompareSemantics(
    compare_columns=INVOICE_SERVICES_COMPARE_COLUMNS,
    bool_yn_cols=frozenset(),
    money_cols=frozenset({"curSvcPrice"}),
    datetime_compare_cols=frozenset(),
    dateonly_compare_cols=frozenset(),
    gloffset_cols=frozenset(),
    text_trim_cols=frozenset({"txtSvcDesc", "txtSvcDescLabor", "txtComments"}),
    dupe_datetime_cols=frozenset(),
    dupe_dateonly_cols=frozenset(),
)

INVOICE_SERVICES_DUPE_COLUMNS_ORDERED = (
    "nbrInvoice",
    "nbrSvcID",
    "txtSvcDesc",
    "curSvcPrice",
    "txtSvcDescLabor",
    "nbrSvcQty",
    "txtComments",
)

INVOICE_SERVICES_SELECT = (
    "invoice_number,service_id,service_desc,price_of_service,labor_desc,labor_price,"
    "service_qty,created_at,service_date,is_complete"
)


def map_access_row_to_supabase(row: dict, sync_time: str) -> dict:
    # Notes live in Postgres `service_date` (text); Access column is txtComments.
    _ = sync_time
    return {
        "invoice_number": cv.to_int(row.get("nbrInvoice")),
        "service_id": cv.to_int(row.get("nbrSvcID")),
        "service_desc": cv.clean_text(row.get("txtSvcDesc")),
        "price_of_service": cv.to_money_number(row.get("curSvcPrice")),
        "labor_desc": cv.clean_text(row.get("txtSvcDescLabor")),
        "labor_price": cv.to_money_number(row.get("curSvcPriceLabor")),
        "service_qty": cv.to_int(row.get("nbrSvcQty")),
        "service_date": cv.clean_text(row.get("txtComments")),
    }


def map_supabase_row_to_dupe(row: dict) -> dict:
    return {
        "nbrInvoice": row.get("invoice_number"),
        "nbrSvcID": row.get("service_id"),
        "txtSvcDesc": row.get("service_desc"),
        "curSvcPrice": cv.to_money_number(row.get("price_of_service")),
        "txtSvcDescLabor": row.get("labor_desc"),
        "nbrSvcQty": row.get("service_qty"),
        "txtComments": row.get("service_date"),
    }


def map_supabase_row_to_dupe_for_compare(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(INVOICE_SERVICES_SEMANTICS, map_supabase_row_to_dupe(row))


def sanitize_dupe_row(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(INVOICE_SERVICES_SEMANTICS, row)


def _validate_invoice_services_spec(spec: TableSyncSpec) -> None:
    if spec.real_table != "tblInvoiceSvc" or spec.dupe_table != "dupeInvoiceSvc":
        raise RuntimeError("REAL_TABLE / DUPE_TABLE safety check failed for invoice_services job")


INVOICE_SERVICES_SPEC = TableSyncSpec(
    job_id="invoice_services",
    state_file=INVOICE_SERVICES_STATE_FILE,
    real_table="tblInvoiceSvc",
    dupe_table="dupeInvoiceSvc",
    supabase_table="invoice_services",
    access_join_keys=("nbrInvoice", "nbrSvcID"),
    supabase_natural_key_columns=("invoice_number", "service_id"),
    required_access_columns=("nbrInvoice", "nbrSvcID"),
    semantics=INVOICE_SERVICES_SEMANTICS,
    dupe_columns_ordered=INVOICE_SERVICES_DUPE_COLUMNS_ORDERED,
    supabase_select_columns=INVOICE_SERVICES_SELECT,
    supabase_on_conflict="invoice_number,service_id",
    map_access_to_supabase=map_access_row_to_supabase,
    map_supabase_to_dupe=map_supabase_row_to_dupe,
    sanitize_dupe_row_for_access_insert=sanitize_dupe_row,
    map_supabase_row_to_dupe_for_compare=map_supabase_row_to_dupe_for_compare,
    supabase_keyset_column="invoice_number",
    supabase_upsert_nonnull=("invoice_number", "service_id"),
    supabase_watermark_column="created_at",
    supabase_incremental_order="created_at.asc,invoice_number.asc,service_id.asc",
    full_fetch_use_offset=True,
    supabase_offset_order="invoice_number.asc,service_id.asc",
    validate_before_run=_validate_invoice_services_spec,
)
