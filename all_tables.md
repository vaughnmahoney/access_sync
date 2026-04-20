invoice_services supabase table will match Access REAL (tblInvoiceSvc), dupe (dupeInvoiceSvc).


create table public.invoice_services (
  id uuid not null default gen_random_uuid (),
  invoice_number integer not null,
  service_id integer not null,
  service_desc text null,
  price_of_service numeric(10, 2) null default 0,
  labor_desc text null,
  labor_price numeric(10, 2) null default 0,
  service_qty integer null default 0,
  created_at timestamp with time zone null default now(),
  service_date text null,
  is_complete boolean null default false,
  qc_status text null,
  incomplete_reason text null,
  incomplete_by text null,
  incomplete_at timestamp with time zone null,
  constraint invoice_services_pkey primary key (id),
  constraint invoice_services_invoice_number_fkey foreign KEY (invoice_number) references invoices (invoice_number) on delete CASCADE
) TABLESPACE pg_default;


tblInvoiceSvc

KEY nbrInvoice - Number
KEY nbrSvcID - Number
txtSvcDesc - Text
curSvcPrice - Currency
txtSvcDescLabor - Text
nbrSvcQty - Number
txtComments - Text


dupeInvoiceSvc
Same schema as tblInvoiceSvc

--------------------------------------------


customer_services — Supabase table name **`customer_services`**; Access real / dupe mirrors **`tblCustSvc`** and **`dupeCustSvc`** (same schema).

Supabase schema
create table public.customer_services (
  id uuid not null default gen_random_uuid (),
  customer_id integer not null,
  service_id integer not null default 1,
  service_type_code text null,
  price_of_service numeric(10, 2) null default 0,
  labor_service_type_code text null,
  price_of_labor_service numeric(10, 2) null default 0,
  service_qty integer null default 0,
  frequency_code text null,
  start_week integer null default 1,
  date_last_serviced integer null default 0,
  date_last_config date null,
  date_last_service date null,
  next_year_start_week integer null default 0,
  is_active boolean null default true,
  comments text null,
  commission text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  constraint customer_services_pkey primary key (id),
  constraint customer_services_customer_id_fkey foreign KEY (customer_id) references customers (customer_id) on delete CASCADE
) TABLESPACE pg_default;


tblCustSvc

Dupe mirror: **dupeCustSvc** (same schema).


Real schema
txtCustID - Number
nbrSvcID - Number
txtSvcType - Short Text
curSvcPrice - Currency
txtSvcTypeLabor - Short Text
curSvcPriceLabor - Currency
nbrSvcQty - Number
txtFrqCode - Short Text
nbrWkStartSvc - Number
nbrWkNextSvc - Number
dteLastCfg - Date/Time
dteLastSvc - Date/Time
nbrWkNextCfg - Number
ynActive - Yes/No
memComments - Long Text
txtCommission - Short Text


Dupe (**dupeCustSvc**) schema same as real (**tblCustSvc**).


customer_services_inventory supabase table will match Access real (tblCustSvcInv), dupe (dupeCustSvcInv).

supabase schema
create table public.customer_services_inventory (
  id uuid not null default gen_random_uuid (),
  customer_id integer not null,
  service_id integer not null,
  nbr_item_no integer not null default 1,
  inventory_sku text null,
  item_qty integer null default 0,
  comment text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  constraint customer_services_inventory_pkey primary key (id),
  constraint customer_services_inventory_customer_id_fkey foreign KEY (customer_id) references customers (customer_id) on delete CASCADE
) TABLESPACE pg_default;




Real schema

txtCustID - Number
nbrSvcID - Number
nbrItemNo - Number
txtInvSKU - Short Text
nbrQty - Number
txtComments - Short Text



dupe same as real