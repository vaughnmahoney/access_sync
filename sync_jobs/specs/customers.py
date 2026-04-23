"""Customer sync: tblCustMast ↔ Supabase `customers` ↔ dupeCustMast."""

from __future__ import annotations

from sync_jobs import converters as cv
from sync_jobs.spec_types import CompareSemantics, TableSyncSpec

# Matches legacy rows_differ_supabase_vs_dupe compare list (not nbrPayType/ynTax — not mapped from Supabase).
CUSTOMERS_COMPARE_COLUMNS = (
    "txtCmpyName",
    "txtAddressOne",
    "txtAddressTwo",
    "txtCity",
    "txtStateCode",
    "txtZipCode",
    "txtPhone",
    "txtFax",
    "txtContactName",
    "memNote",
    "txtMessage",
    "ynSepBill",
    "txtBillCode",
    "txtRteCode",
    "ynActive",
    "txtPO",
    "dteLastSvc",
    "txtTaxCode",
    "txtEmployeeID",
    "ynDigitalBilling",
    "txtGLCode",
)

CUSTOMERS_SEMANTICS = CompareSemantics(
    compare_columns=CUSTOMERS_COMPARE_COLUMNS,
    bool_yn_cols=frozenset({"ynSepBill", "ynActive", "ynDigitalBilling"}),
    money_cols=frozenset(),
    datetime_compare_cols=frozenset(),
    dateonly_compare_cols=frozenset({"dteLastSvc"}),
    gloffset_cols=frozenset(),
    text_trim_cols=frozenset(
        {
            "txtCmpyName",
            "txtAddressOne",
            "txtAddressTwo",
            "txtCity",
            "txtStateCode",
            "txtZipCode",
            "txtPhone",
            "txtFax",
            "txtContactName",
            "memNote",
            "txtMessage",
            "txtBillCode",
            "txtRteCode",
            "txtPO",
            "txtTaxCode",
            "txtEmployeeID",
            "txtGLCode",
        }
    ),
    dupe_datetime_cols=frozenset(),
    dupe_dateonly_cols=frozenset({"dteLastSvc"}),
)

CUSTOMERS_DUPE_COLUMNS_ORDERED = (
    "txtCustID",
    "txtCmpyName",
    "txtAddressOne",
    "txtAddressTwo",
    "txtCity",
    "txtStateCode",
    "txtZipCode",
    "txtPhone",
    "txtFax",
    "txtContactName",
    "memNote",
    "txtMessage",
    "ynSepBill",
    "txtBillCode",
    "txtRteCode",
    "ynActive",
    "txtPO",
    "nbrPayType",
    "ynTax",
    "dteLastSvc",
    "txtTaxCode",
    "txtEmployeeID",
    "txtCompanyCode",
    "txtCustSvcID",
    "ynShowPrices",
    "ynDigitalBilling",
    "txtEmailorURL",
    "txtGLCode",
)

CUSTOMERS_SELECT = (
    "customer_id,company_name,address,address_two,city,state,zip,notes,customer_message,"
    "sep_bill,billing_code,route_code,is_active,dte_last_svc,employee_id,digital_billing,"
    "updated_at,phone,fax,contact,customer_po,tax_code,gl_code"
)


def map_access_row_to_supabase(row: dict, sync_time: str) -> dict:
    return {
        "customer_id": cv.to_int(row.get("txtCustID")),
        "company_name": cv.clean_text(row.get("txtCmpyName")),
        "address": cv.clean_text(row.get("txtAddressOne")),
        "address_two": cv.clean_text(row.get("txtAddressTwo")),
        "city": cv.clean_text(row.get("txtCity")),
        "state": cv.clean_text(row.get("txtStateCode")),
        "zip": cv.clean_text(row.get("txtZipCode")),
        "notes": cv.clean_text(row.get("memNote")),
        "customer_message": cv.clean_text(row.get("txtMessage")),
        "sep_bill": cv.to_bool(row.get("ynSepBill")),
        "billing_code": cv.clean_text(row.get("txtBillCode")),
        "route_code": cv.clean_text(row.get("txtRteCode")),
        "is_active": cv.to_bool(row.get("ynActive")),
        "dte_last_svc": cv.to_iso_date(row.get("dteLastSvc")),
        "employee_id": cv.clean_text(row.get("txtEmployeeID")),
        "digital_billing": cv.to_bool(row.get("ynDigitalBilling")),
        "phone": cv.clean_text(row.get("txtPhone")),
        "fax": cv.clean_text(row.get("txtFax")),
        "contact": cv.clean_text(row.get("txtContactName")),
        "customer_po": cv.clean_text(row.get("txtPO")),
        "tax_code": cv.clean_text(row.get("txtTaxCode")),
        "gl_code": cv.clean_text(row.get("txtGLCode")),
        "updated_at": sync_time,
    }


def map_supabase_row_to_dupe(row: dict) -> dict:
    return {
        "txtCustID": row.get("customer_id"),
        "txtCmpyName": row.get("company_name"),
        "txtAddressOne": row.get("address"),
        "txtAddressTwo": row.get("address_two"),
        "txtCity": row.get("city"),
        "txtStateCode": row.get("state"),
        "txtZipCode": row.get("zip"),
        "txtPhone": row.get("phone"),
        "txtFax": row.get("fax"),
        "txtContactName": row.get("contact"),
        "memNote": row.get("notes"),
        "txtMessage": row.get("customer_message"),
        "ynSepBill": cv.to_access_yn_bool(row.get("sep_bill")),
        "txtBillCode": row.get("billing_code"),
        "txtRteCode": row.get("route_code"),
        "ynActive": cv.to_access_yn_bool(row.get("is_active")),
        "txtPO": row.get("customer_po"),
        "nbrPayType": None,
        "ynTax": None,
        "dteLastSvc": cv.to_access_date(row.get("dte_last_svc")),
        "txtTaxCode": row.get("tax_code"),
        "txtEmployeeID": row.get("employee_id"),
        "txtCompanyCode": None,
        "txtCustSvcID": None,
        "ynShowPrices": None,
        "ynDigitalBilling": cv.to_access_yn_bool(row.get("digital_billing")),
        "txtEmailorURL": None,
        "txtGLCode": row.get("gl_code"),
    }


def map_supabase_row_to_dupe_for_compare(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(CUSTOMERS_SEMANTICS, map_supabase_row_to_dupe(row))


def sanitize_dupe_row(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(CUSTOMERS_SEMANTICS, row)


def _validate_customers_spec(spec: TableSyncSpec) -> None:
    if spec.real_table != "tblCustMast" or spec.dupe_table != "dupeCustMast":
        raise RuntimeError("REAL_TABLE / DUPE_TABLE safety check failed for customers job")


CUSTOMERS_SPEC = TableSyncSpec(
    job_id="customers",
    real_table="tblCustMast",
    dupe_table="dupeCustMast",
    supabase_table="customers",
    access_join_keys=("txtCustID",),
    supabase_natural_key_columns=("customer_id",),
    required_access_columns=("txtCustID",),
    semantics=CUSTOMERS_SEMANTICS,
    dupe_columns_ordered=CUSTOMERS_DUPE_COLUMNS_ORDERED,
    supabase_select_columns=CUSTOMERS_SELECT,
    supabase_on_conflict="customer_id",
    map_access_to_supabase=map_access_row_to_supabase,
    map_supabase_to_dupe=map_supabase_row_to_dupe,
    sanitize_dupe_row_for_access_insert=sanitize_dupe_row,
    map_supabase_row_to_dupe_for_compare=map_supabase_row_to_dupe_for_compare,
    supabase_keyset_column="customer_id",
    supabase_upsert_nonnull=("customer_id",),
    validate_before_run=_validate_customers_spec,
)
