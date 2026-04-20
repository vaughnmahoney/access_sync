# OptimaFlow v1 table columns

## invoice_services
```json
[
  "id",
  "invoice_number",
  "service_id",
  "service_desc",
  "price_of_service",
  "labor_desc",
  "labor_price",
  "service_qty",
  "created_at",
  "service_date",
  "is_complete",
  "qc_status",
  "incomplete_reason",
  "incomplete_by",
  "incomplete_at"
]
```





## customer_services

Access mirrors: **`tblCustSvc`** (source) / **`dupeCustSvc`** (sync mirror). Supabase table name below is **`customer_services`**.

```json
[
  "id",
  "customer_id",
  "service_id",
  "service_type_code",
  "price_of_service",
  "labor_service_type_code",
  "price_of_labor_service",
  "service_qty",
  "frequency_code",
  "start_week",
  "date_last_serviced",
  "date_last_config",
  "date_last_service",
  "next_year_start_week",
  "is_active",
  "comments",
  "commission",
  "created_at",
  "updated_at"
]
```





## customer_services_inventory
```json
[
  "id",
  "customer_id",
  "service_id",
  "nbr_item_no",
  "inventory_sku",
  "item_qty",
  "comment",
  "created_at",
  "updated_at"
]
```
