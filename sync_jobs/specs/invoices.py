"""Invoice line sync: tblInvoice ↔ invoices ↔ dupeInvoice."""

from __future__ import annotations

from pathlib import Path

from sync_jobs import converters as cv
from sync_jobs.spec_types import CompareSemantics, TableSyncSpec

_ACCESS_SYNC_ROOT = Path(__file__).resolve().parent.parent.parent

INVOICES_STATE_FILE = _ACCESS_SYNC_ROOT / "sync_state" / "invoices_sync_state.json"

INVOICES_COMPARE_COLUMNS = (
    "txtCustID",
    "nbrSvcID",
    "dteGen",
    "curSvcPrice",
    "curSvcPriceLabor",
    "nbrSvcQty",
    "ynPrinted",
    "ynComplete",
    "dteService",
    "nbrPayType",
    "nbrChkNbr",
    "curTax",
    "nbrChangeWeek",
    "ynTransmitAR",
    "dteTransmit",
    "AROffset",
    "txtTech",
    "nbrWeekScheduled",
    "dteScheduled",
    "ProofOfService",
    "txtTechNote",
)

INVOICES_SEMANTICS = CompareSemantics(
    compare_columns=INVOICES_COMPARE_COLUMNS,
    bool_yn_cols=frozenset({"ynPrinted", "ynComplete", "ynTransmitAR"}),
    money_cols=frozenset({"curSvcPrice", "curSvcPriceLabor", "curTax"}),
    datetime_compare_cols=frozenset({"dteGen", "dteService"}),
    dateonly_compare_cols=frozenset({"dteTransmit", "dteScheduled"}),
    gloffset_cols=frozenset({"AROffset"}),
    text_trim_cols=frozenset({"txtCustID", "txtTech", "txtTechNote", "ProofOfService"}),
    dupe_datetime_cols=frozenset({"dteGen", "dteService"}),
    dupe_dateonly_cols=frozenset({"dteTransmit", "dteScheduled"}),
)

INVOICES_DUPE_COLUMNS_ORDERED = (
    "nbrInvoice",
    "txtCustID",
    "nbrSvcID",
    "dteGen",
    "curSvcPrice",
    "curSvcPriceLabor",
    "nbrSvcQty",
    "ynPrinted",
    "ynComplete",
    "dteService",
    "nbrPayType",
    "nbrChkNbr",
    "curTax",
    "nbrChangeWeek",
    "ynTransmitAR",
    "dteTransmit",
    "AROffset",
    "txtTech",
    "nbrWeekScheduled",
    "dteScheduled",
    "ProofOfService",
    "txtTechNote",
)

INVOICES_SELECT = (
    "invoice_number,customer_id,svc_id,generated_date,price_of_service,labor_price,service_qty,"
    "is_printed,is_complete,service_date,pay_type,check_number,sales_tax,change_week,transmit_flag,"
    "transmit_date,gl_offset,tech,week_scheduled,scheduled_date,proof_of_service,tech_note,updated_at"
)


def map_access_row_to_supabase(row: dict, sync_time: str) -> dict:
    return {
        "invoice_number": cv.to_int(row.get("nbrInvoice")),
        "customer_id": cv.to_int(row.get("txtCustID")),
        "svc_id": cv.to_int(row.get("nbrSvcID")),
        "generated_date": cv.to_iso_datetime(row.get("dteGen")),
        "price_of_service": cv.to_money_number(row.get("curSvcPrice")),
        "labor_price": cv.to_money_number(row.get("curSvcPriceLabor")),
        "service_qty": cv.to_int(row.get("nbrSvcQty")),
        "is_printed": cv.to_access_yn_bool(row.get("ynPrinted")),
        "is_complete": cv.to_access_yn_bool(row.get("ynComplete")),
        "service_date": cv.to_iso_datetime(row.get("dteService")),
        "pay_type": cv.to_int(row.get("nbrPayType")),
        "check_number": cv.clean_text(row.get("nbrChkNbr")),
        "sales_tax": cv.to_money_number(row.get("curTax")),
        "change_week": cv.to_int(row.get("nbrChangeWeek")),
        "transmit_flag": cv.to_access_yn_bool(row.get("ynTransmitAR")),
        "transmit_date": cv.to_iso_date(row.get("dteTransmit")),
        "gl_offset": cv.to_gl_offset_int(row.get("AROffset")),
        "tech": cv.clean_text(row.get("txtTech")),
        "week_scheduled": cv.to_int(row.get("nbrWeekScheduled")),
        "scheduled_date": cv.to_iso_date(row.get("dteScheduled")),
        "proof_of_service": cv.clean_text(row.get("ProofOfService")),
        "tech_note": cv.clean_text(row.get("txtTechNote")),
        "updated_at": sync_time,
    }


def map_supabase_row_to_dupe(row: dict) -> dict:
    return {
        "nbrInvoice": row.get("invoice_number"),
        "txtCustID": row.get("customer_id"),
        "nbrSvcID": row.get("svc_id"),
        "dteGen": cv.to_access_datetime(row.get("generated_date")),
        "curSvcPrice": cv.to_money_number(row.get("price_of_service")),
        "curSvcPriceLabor": cv.to_money_number(row.get("labor_price")),
        "nbrSvcQty": row.get("service_qty"),
        "ynPrinted": cv.to_access_yn_bool(row.get("is_printed")),
        "ynComplete": cv.to_access_yn_bool(row.get("is_complete")),
        "dteService": cv.to_access_datetime(row.get("service_date")),
        "nbrPayType": row.get("pay_type"),
        "nbrChkNbr": row.get("check_number"),
        "curTax": cv.to_money_number(row.get("sales_tax")),
        "nbrChangeWeek": row.get("change_week"),
        "ynTransmitAR": cv.to_access_yn_bool(row.get("transmit_flag")),
        "dteTransmit": cv.to_access_date(row.get("transmit_date")),
        "AROffset": cv.to_gl_offset_int(row.get("gl_offset")),
        "txtTech": row.get("tech"),
        "nbrWeekScheduled": row.get("week_scheduled"),
        "dteScheduled": cv.to_access_date(row.get("scheduled_date")),
        "ProofOfService": row.get("proof_of_service"),
        "txtTechNote": row.get("tech_note"),
    }


def map_supabase_row_to_dupe_for_compare(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(INVOICES_SEMANTICS, map_supabase_row_to_dupe(row))


def sanitize_dupe_row(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(INVOICES_SEMANTICS, row)


def _validate_invoices_spec(spec: TableSyncSpec) -> None:
    if spec.real_table != "tblInvoice" or spec.dupe_table != "dupeInvoice":
        raise RuntimeError("REAL_TABLE / DUPE_TABLE safety check failed for invoices job")


INVOICES_SPEC = TableSyncSpec(
    job_id="invoices",
    state_file=INVOICES_STATE_FILE,
    real_table="tblInvoice",
    dupe_table="dupeInvoice",
    supabase_table="invoices",
    access_join_keys=("nbrInvoice",),
    supabase_natural_key_columns=("invoice_number",),
    required_access_columns=("nbrInvoice", "txtCustID"),
    semantics=INVOICES_SEMANTICS,
    dupe_columns_ordered=INVOICES_DUPE_COLUMNS_ORDERED,
    supabase_select_columns=INVOICES_SELECT,
    supabase_on_conflict="invoice_number",
    map_access_to_supabase=map_access_row_to_supabase,
    map_supabase_to_dupe=map_supabase_row_to_dupe,
    sanitize_dupe_row_for_access_insert=sanitize_dupe_row,
    map_supabase_row_to_dupe_for_compare=map_supabase_row_to_dupe_for_compare,
    supabase_keyset_column="invoice_number",
    supabase_upsert_nonnull=("invoice_number", "customer_id"),
    validate_before_run=_validate_invoices_spec,
)
