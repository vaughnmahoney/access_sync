import os
import time
import pyodbc
import requests
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://eijdqiyvuhregbydndnb.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVpamRxaXl2dWhyZWdieWRuZG5iIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczODA5ODcxMSwiZXhwIjoyMDUzNjc0NzExfQ.pTPdq-7HQuto7T6dgW9dB60hFiMoZgajFCt516tZdl0")

ACCESS_DB_PATH = r"G:\dbHyland\Hfsapp.accdb"
ACCESS_CONN_STR = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    rf"DBQ={ACCESS_DB_PATH};"
)

ACCESS_TABLE = "OptimaFlow"
ACCESS_UNIQUE_KEY = "invoice_no"

FIELD_MAP = {
    "order_no": "invoice_no",
    "location_no": "customer_id",
    "location_name": "customer_name",
    "location_address": "address",
    "end_time_local": "service_date",
    "optimoroute_status": "status",
    "status": "qc_status",
}


def fetch_work_orders(page_size=1000):
    url = f"{SUPABASE_URL}/rest/v1/work_orders"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    all_rows = []
    offset = 0

    while True:
        params = {
            "select": "order_no,location_no,location_name,location_address,end_time_local,optimoroute_status,status",
            "end_time_local": "gte.2026-04-16T00:00:00",
            "order": "end_time_local.asc",
            "limit": str(page_size),
            "offset": str(offset),
        }

        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        all_rows.extend(batch)
        print(f"Fetched batch: {len(batch)} rows, total so far: {len(all_rows)}")

        if len(batch) < page_size:
            break

        offset += page_size

    return all_rows


def normalize_value(field, value):
    if value is None:
        return None

    if field == "end_time_local" and isinstance(value, str):
        try:
            if "T" in value:
                return datetime.fromisoformat(value.replace("Z", "")).date()
            if " " in value and ":" in value:
                return datetime.fromisoformat(value).date()
            return datetime.fromisoformat(value).date()
        except Exception:
            return value

    return value


def access_row_exists(cursor, invoice_no):
    sql = f"SELECT COUNT(*) FROM [{ACCESS_TABLE}] WHERE [{ACCESS_UNIQUE_KEY}] = ?"
    cursor.execute(sql, invoice_no)
    row = cursor.fetchone()
    return bool(row and row[0] > 0)


def insert_access_row(cursor, source_row):
    mapped = {
        FIELD_MAP[src]: normalize_value(src, source_row.get(src))
        for src in FIELD_MAP
    }

    columns = list(mapped.keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_sql = ", ".join(f"[{c}]" for c in columns)

    sql = f"INSERT INTO [{ACCESS_TABLE}] ({col_sql}) VALUES ({placeholders})"
    values = [mapped[c] for c in columns]
    cursor.execute(sql, values)


def update_access_row(cursor, source_row):
    mapped = {
        FIELD_MAP[src]: normalize_value(src, source_row.get(src))
        for src in FIELD_MAP
    }

    set_columns = [c for c in mapped.keys() if c != ACCESS_UNIQUE_KEY]
    set_sql = ", ".join(f"[{c}] = ?" for c in set_columns)

    sql = f"UPDATE [{ACCESS_TABLE}] SET {set_sql} WHERE [{ACCESS_UNIQUE_KEY}] = ?"
    values = [mapped[c] for c in set_columns]
    values.append(mapped[ACCESS_UNIQUE_KEY])

    cursor.execute(sql, values)


def upsert_access_row(cursor, source_row):
    invoice_no = source_row.get("order_no")
    if invoice_no is None:
        return "skipped"

    if access_row_exists(cursor, invoice_no):
        update_access_row(cursor, source_row)
        return "updated"
    else:
        insert_access_row(cursor, source_row)
        return "inserted"


def run_sync_once():
    print("Pulling work orders from Supabase...")
    rows = fetch_work_orders(page_size=1000)
    print(f"Fetched total rows: {len(rows)}")

    conn = pyodbc.connect(ACCESS_CONN_STR)
    cursor = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    try:
        for row in rows:
            result = upsert_access_row(cursor, row)
            if result == "inserted":
                inserted += 1
            elif result == "updated":
                updated += 1
            else:
                skipped += 1

        conn.commit()
        print("Done.")
        print(f"Inserted: {inserted}")
        print(f"Updated: {updated}")
        print(f"Skipped: {skipped}")
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    while True:
        print("Starting sync cycle...")
        print(f"Run time: {datetime.now().isoformat(sep=' ', timespec='seconds')}")

        try:
            run_sync_once()
        except Exception as e:
            print(f"Sync failed: {e}")

        print("Sleeping 60 seconds before next check...")
        time.sleep(60)


if __name__ == "__main__":
    main()
