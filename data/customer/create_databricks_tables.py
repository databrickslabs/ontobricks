#!/usr/bin/env python3
"""
Create tables in Databricks Unity Catalog from CSV files using CTAS.
"""

import subprocess
import json

WAREHOUSE_ID = "66e8366e84d57752"
CATALOG = "benoit_cayla"
SCHEMA = "customer_large"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/customer_data"

TABLES = [
    "customer",
    "contract", 
    "subscription",
    "meter",
    "meter_reading",
    "invoice",
    "payment",
    "call",
    "claim",
    "interaction"
]


def execute_sql(statement: str) -> dict:
    """Execute SQL via Databricks API."""
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "50s"
    }
    
    cmd = ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(payload)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        return {"status": {"state": "ERROR", "error": {"message": result.stderr}}}
    
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"status": {"state": "ERROR", "error": {"message": result.stdout}}}


def get_state(result: dict) -> str:
    return result.get("status", {}).get("state", "UNKNOWN")


def get_error(result: dict) -> str:
    return result.get("status", {}).get("error", {}).get("message", "Unknown error")[:100]


def create_table(table_name: str) -> bool:
    """Create a table using CTAS from CSV."""
    # Drop existing table
    print(f"  DROP TABLE IF EXISTS...", end=" ", flush=True)
    result = execute_sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table_name}")
    print("✓" if get_state(result) == "SUCCEEDED" else f"({get_state(result)})")
    
    # Create table using CTAS with read_files
    print(f"  CREATE TABLE AS SELECT...", end=" ", flush=True)
    sql = f"""
        CREATE TABLE {CATALOG}.{SCHEMA}.{table_name} AS
        SELECT * FROM read_files(
            '{VOLUME_PATH}/{table_name}.csv',
            format => 'csv',
            header => true
        )
    """
    result = execute_sql(sql)
    state = get_state(result)
    
    if state == "SUCCEEDED":
        print("✓")
        return True
    else:
        print(f"✗ - {get_error(result)}")
        return False


def count_rows(table_name: str) -> int:
    """Count rows in a table."""
    result = execute_sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table_name}")
    if get_state(result) == "SUCCEEDED":
        data = result.get("result", {}).get("data_array", [[0]])
        return int(data[0][0]) if data else 0
    return -1


def main():
    print("=" * 60)
    print("Creating Energy Provider Customer Journey Tables")
    print(f"Catalog: {CATALOG}")
    print(f"Schema:  {SCHEMA}")
    print(f"Volume:  {VOLUME_PATH}")
    print("=" * 60)
    
    success_count = 0
    total_rows = 0
    
    for table_name in TABLES:
        print(f"\n📦 {table_name}")
        if create_table(table_name):
            success_count += 1
            rows = count_rows(table_name)
            if rows >= 0:
                print(f"  Rows: {rows}")
                total_rows += rows
    
    print()
    print("=" * 60)
    if success_count == len(TABLES):
        print(f"✅ All {success_count} tables created successfully!")
    else:
        print(f"⚠️  {success_count}/{len(TABLES)} tables created")
    print(f"📊 Total rows: {total_rows}")
    print("=" * 60)
    
    # List all tables
    if success_count > 0:
        print(f"\n📋 Tables in {CATALOG}.{SCHEMA}:")
        result = execute_sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
        if get_state(result) == "SUCCEEDED":
            data = result.get("result", {}).get("data_array", [])
            for row in data:
                print(f"   • {row[1] if len(row) > 1 else row[0]}")


if __name__ == "__main__":
    main()
