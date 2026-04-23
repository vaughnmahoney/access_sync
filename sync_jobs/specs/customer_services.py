"""Customer–service rows: tblCustSvc ↔ customer_services ↔ dupeCustSvc.

Requires UNIQUE (customer_id, service_id) on Postgres for upserts.
"""

from __future__ import annotations

from sync_jobs import converters as cv
from sync_jobs.spec_types import CompareSemantics, TableSyncSpec

# Access/Jet source + mirror (must match your .accdb; used by all SQL built from this spec).
ACCESS_REAL_TABLE = "tblCustSvc"
ACCESS_DUPE_TABLE = "dupeCustSvc"

CUSTOMER_SERVICES_COMPARE_COLUMNS = (
    "txtSvcType",
    "curSvcPrice",
    "txtSvcTypeLabor",
    "curSvcPriceLabor",
    "nbrSvcQty",
    "txtFrqCode",
    "nbrWkStartSvc",
    "nbrWkNextSvc",
    "dteLastCfg",
    "dteLastSvc",
    "nbrWkNextCfg",
    "ynActive",
    "memComments",
    "txtCommission",
)

CUSTOMER_SERVICES_SEMANTICS = CompareSemantics(
    compare_columns=CUSTOMER_SERVICES_COMPARE_COLUMNS,
    bool_yn_cols=frozenset({"ynActive"}),
    money_cols=frozenset({"curSvcPrice", "curSvcPriceLabor"}),
    datetime_compare_cols=frozenset({"dteLastCfg", "dteLastSvc"}),
    dateonly_compare_cols=frozenset(),
    gloffset_cols=frozenset(),
    text_trim_cols=frozenset(
        {
            "txtSvcType",
            "txtSvcTypeLabor",
            "txtFrqCode",
            "memComments",
            "txtCommission",
        }
    ),
    dupe_datetime_cols=frozenset({"dteLastCfg", "dteLastSvc"}),
    dupe_dateonly_cols=frozenset(),
)

CUSTOMER_SERVICES_DUPE_COLUMNS_ORDERED = (
    "txtCustID",
    "nbrSvcID",
    "txtSvcType",
    "curSvcPrice",
    "txtSvcTypeLabor",
    "curSvcPriceLabor",
    "nbrSvcQty",
    "txtFrqCode",
    "nbrWkStartSvc",
    "nbrWkNextSvc",
    "dteLastCfg",
    "dteLastSvc",
    "nbrWkNextCfg",
    "ynActive",
    "memComments",
    "txtCommission",
)

CUSTOMER_SERVICES_SELECT = (
    "customer_id,service_id,service_type_code,price_of_service,labor_service_type_code,"
    "price_of_labor_service,service_qty,frequency_code,start_week,date_last_serviced,"
    "date_last_config,date_last_service,next_year_start_week,is_active,comments,commission,"
    "updated_at"
)


def map_access_row_to_supabase(row: dict, sync_time: str) -> dict:
    return {
        "customer_id": cv.to_int(row.get("txtCustID")),
        "service_id": cv.to_int(row.get("nbrSvcID")),
        "service_type_code": cv.clean_text(row.get("txtSvcType")),
        "price_of_service": cv.to_money_number(row.get("curSvcPrice")),
        "labor_service_type_code": cv.clean_text(row.get("txtSvcTypeLabor")),
        "price_of_labor_service": cv.to_money_number(row.get("curSvcPriceLabor")),
        "service_qty": cv.to_int(row.get("nbrSvcQty")),
        "frequency_code": cv.clean_text(row.get("txtFrqCode")),
        "start_week": cv.to_int(row.get("nbrWkStartSvc")),
        "date_last_serviced": cv.to_int(row.get("nbrWkNextSvc")),
        "date_last_config": cv.to_iso_date(row.get("dteLastCfg")),
        "date_last_service": cv.to_iso_date(row.get("dteLastSvc")),
        "next_year_start_week": cv.to_int(row.get("nbrWkNextCfg")),
        "is_active": cv.to_access_yn_bool(row.get("ynActive")),
        "comments": cv.clean_text(row.get("memComments")),
        "commission": cv.clean_text(row.get("txtCommission")),
        "updated_at": sync_time,
    }


def map_supabase_row_to_dupe(row: dict) -> dict:
    return {
        "txtCustID": row.get("customer_id"),
        "nbrSvcID": row.get("service_id"),
        "txtSvcType": row.get("service_type_code"),
        "curSvcPrice": cv.to_money_number(row.get("price_of_service")),
        "txtSvcTypeLabor": row.get("labor_service_type_code"),
        "curSvcPriceLabor": cv.to_money_number(row.get("price_of_labor_service")),
        "nbrSvcQty": row.get("service_qty"),
        "txtFrqCode": row.get("frequency_code"),
        "nbrWkStartSvc": row.get("start_week"),
        "nbrWkNextSvc": row.get("date_last_serviced"),
        "dteLastCfg": cv.to_access_datetime(row.get("date_last_config")),
        "dteLastSvc": cv.to_access_datetime(row.get("date_last_service")),
        "nbrWkNextCfg": row.get("next_year_start_week"),
        "ynActive": cv.to_access_yn_bool(row.get("is_active")),
        "memComments": row.get("comments"),
        "txtCommission": row.get("commission"),
    }


def map_supabase_row_to_dupe_for_compare(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(CUSTOMER_SERVICES_SEMANTICS, map_supabase_row_to_dupe(row))


def sanitize_dupe_row(row: dict) -> dict:
    return cv.sanitize_dupe_row_for_access_insert(CUSTOMER_SERVICES_SEMANTICS, row)


def _validate_customer_services_spec(spec: TableSyncSpec) -> None:
    if spec.real_table != ACCESS_REAL_TABLE or spec.dupe_table != ACCESS_DUPE_TABLE:
        raise RuntimeError("REAL_TABLE / DUPE_TABLE safety check failed for customer_services job")


def _customer_services_spec() -> TableSyncSpec:
    """Build spec; clearer error when an old ``sync_jobs/spec_types.py`` lacks composite-key fields."""
    try:
        return TableSyncSpec(
            job_id="customer_services",
            real_table=ACCESS_REAL_TABLE,
            dupe_table=ACCESS_DUPE_TABLE,
            supabase_table="customer_services",
            access_join_keys=("txtCustID", "nbrSvcID"),
            supabase_natural_key_columns=("customer_id", "service_id"),
            required_access_columns=("txtCustID", "nbrSvcID"),
            semantics=CUSTOMER_SERVICES_SEMANTICS,
            dupe_columns_ordered=CUSTOMER_SERVICES_DUPE_COLUMNS_ORDERED,
            supabase_select_columns=CUSTOMER_SERVICES_SELECT,
            supabase_on_conflict="customer_id,service_id",
            map_access_to_supabase=map_access_row_to_supabase,
            map_supabase_to_dupe=map_supabase_row_to_dupe,
            sanitize_dupe_row_for_access_insert=sanitize_dupe_row,
            map_supabase_row_to_dupe_for_compare=map_supabase_row_to_dupe_for_compare,
            supabase_keyset_column="customer_id",
            supabase_upsert_nonnull=("customer_id", "service_id"),
            full_fetch_use_offset=True,
            supabase_offset_order="customer_id.asc,service_id.asc",
            validate_before_run=_validate_customer_services_spec,
        )
    except TypeError as e:
        msg = str(e).lower()
        if "access_join_keys" in msg or "unexpected keyword" in msg:
            raise RuntimeError(
                "sync_jobs/spec_types.py in this project is still the OLD TableSyncSpec (no access_join_keys). "
                "Copy the entire folder access_sync/sync_jobs into OptimaFlow so it replaces OptimaFlow/sync_jobs. "
                "From the access_sync folder on the machine that has the repo: "
                'install_sync_jobs.cmd "C:\\path\\OptimaFlow" '
                'or PowerShell .\\copy_sync_jobs.ps1 -Destination "C:\\path\\OptimaFlow". '
                "Then cd OptimaFlow and run python ..\\\\access_sync\\\\verify_optima_sync_jobs.py"
            ) from e
        raise


CUSTOMER_SERVICES_SPEC = _customer_services_spec()
