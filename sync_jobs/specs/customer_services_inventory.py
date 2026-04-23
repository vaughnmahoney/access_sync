"""Per-customer service inventory lines: tblCustSvcInv ↔ customer_services_inventory ↔ dupeCustSvcInv.

Requires UNIQUE (customer_id, service_id, nbr_item_no) on Postgres for upserts.
"""

from __future__ import annotations

from sync_jobs import converters as cv
from sync_jobs.spec_types import CompareSemantics, TableSyncSpec

CUSTOMER_SERVICES_INVENTORY_COMPARE_COLUMNS = (
    "txtInvSKU",
    "nbrQty",
    "txtComments",
)

CUSTOMER_SERVICES_INVENTORY_SEMANTICS = CompareSemantics(
    compare_columns=CUSTOMER_SERVICES_INVENTORY_COMPARE_COLUMNS,
    bool_yn_cols=frozenset(),
    money_cols=frozenset(),
    datetime_compare_cols=frozenset(),
    dateonly_compare_cols=frozenset(),
    gloffset_cols=frozenset(),
    text_trim_cols=frozenset({"txtInvSKU", "txtComments"}),
    dupe_datetime_cols=frozenset(),
    dupe_dateonly_cols=frozenset(),
)

CUSTOMER_SERVICES_INVENTORY_DUPE_COLUMNS_ORDERED = (
    "txtCustID",
    "nbrSvcID",
    "nbrItemNo",
    "txtInvSKU",
    "nbrQty",
    "txtComments",
)

CUSTOMER_SERVICES_INVENTORY_SELECT = (
    "customer_id,service_id,nbr_item_no,inventory_sku,item_qty,comment,updated_at"
)


def map_access_row_to_supabase(row: dict, sync_time: str) -> dict:
    return {
        "customer_id": cv.to_int(row.get("txtCustID")),
        "service_id": cv.to_int(row.get("nbrSvcID")),
        "nbr_item_no": cv.to_int(row.get("nbrItemNo")),
        "inventory_sku": cv.clean_text(row.get("txtInvSKU")),
        "item_qty": cv.to_int(row.get("nbrQty")),
        "comment": cv.clean_text(row.get("txtComments")),
        "updated_at": sync_time,
    }


def map_supabase_row_to_dupe(row: dict) -> dict:
    return {
        "txtCustID": row.get("customer_id"),
        "nbrSvcID": row.get("service_id"),
        "nbrItemNo": row.get("nbr_item_no"),
        "txtInvSKU": row.get("inventory_sku"),
        "nbrQty": row.get("item_qty"),
        "txtComments": row.get("comment"),
    }


def map_supabase_row_to_dupe_for_compare(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(
        CUSTOMER_SERVICES_INVENTORY_SEMANTICS, map_supabase_row_to_dupe(row)
    )


def sanitize_dupe_row(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(CUSTOMER_SERVICES_INVENTORY_SEMANTICS, row)


def _validate_customer_services_inventory_spec(spec: TableSyncSpec) -> None:
    if spec.real_table != "tblCustSvcInv" or spec.dupe_table != "dupeCustSvcInv":
        raise RuntimeError(
            "REAL_TABLE / DUPE_TABLE safety check failed for customer_services_inventory job"
        )


CUSTOMER_SERVICES_INVENTORY_SPEC = TableSyncSpec(
    job_id="customer_services_inventory",
    real_table="tblCustSvcInv",
    dupe_table="dupeCustSvcInv",
    supabase_table="customer_services_inventory",
    access_join_keys=("txtCustID", "nbrSvcID", "nbrItemNo"),
    supabase_natural_key_columns=("customer_id", "service_id", "nbr_item_no"),
    required_access_columns=("txtCustID", "nbrSvcID", "nbrItemNo"),
    semantics=CUSTOMER_SERVICES_INVENTORY_SEMANTICS,
    dupe_columns_ordered=CUSTOMER_SERVICES_INVENTORY_DUPE_COLUMNS_ORDERED,
    supabase_select_columns=CUSTOMER_SERVICES_INVENTORY_SELECT,
    supabase_on_conflict="customer_id,service_id,nbr_item_no",
    map_access_to_supabase=map_access_row_to_supabase,
    map_supabase_to_dupe=map_supabase_row_to_dupe,
    sanitize_dupe_row_for_access_insert=sanitize_dupe_row,
    map_supabase_row_to_dupe_for_compare=map_supabase_row_to_dupe_for_compare,
    supabase_keyset_column="customer_id",
    supabase_upsert_nonnull=("customer_id", "service_id", "nbr_item_no"),
    full_fetch_use_offset=True,
    supabase_offset_order="customer_id.asc,service_id.asc,nbr_item_no.asc",
    validate_before_run=_validate_customer_services_inventory_spec,
)
