#!/usr/bin/env python3
"""
Create tables in Databricks Unity Catalog from CSV files using CTAS.
Project Management (PM) dataset.
"""

import subprocess
import json

WAREHOUSE_ID = "66e8366e84d57752"
CATALOG = "benoit_cayla"
SCHEMA = "pm"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/pm_data"

TABLES = [
    "person",
    "department",
    "domain",
    "person_department",
    "department_domain",
    "person_collaboration",
]


def execute_sql(statement: str) -> dict:
    """Execute SQL via Databricks API."""
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "50s",
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


def create_schema() -> bool:
    """Create the schema and volume if they don't exist."""
    print("Creating schema and volume...")

    result = execute_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    if get_state(result) != "SUCCEEDED":
        print(f"  Schema creation failed: {get_error(result)}")
        return False
    print(f"  Schema {CATALOG}.{SCHEMA} ready")

    result = execute_sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.pm_data"
    )
    if get_state(result) != "SUCCEEDED":
        print(f"  Volume creation failed: {get_error(result)}")
        return False
    print(f"  Volume {VOLUME_PATH} ready")
    return True


def upload_csv_files() -> bool:
    """Upload local CSV files to the volume via curl + Files API."""
    import glob
    import os

    csv_dir = os.path.dirname(os.path.abspath(__file__))
    csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))

    if not csv_files:
        print("  No CSV files found!")
        return False

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        print("  DATABRICKS_HOST and DATABRICKS_TOKEN env vars required for upload.")
        return False

    for csv_path in csv_files:
        name = os.path.basename(csv_path)
        url = f"{host}/api/2.0/fs/files{VOLUME_PATH}/{name}"
        print(f"  Uploading {name}...", end=" ", flush=True)
        cmd = [
            "curl", "-s", "-X", "PUT", url,
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: application/octet-stream",
            "--data-binary", f"@{csv_path}",
            "-o", "/dev/null", "-w", "%{http_code}",
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        http_code = res.stdout.strip()
        if http_code in ("200", "204"):
            print("done")
        else:
            print(f"FAILED (HTTP {http_code})")
            return False
    return True


def create_table(table_name: str) -> bool:
    """Create a table using CTAS from CSV."""
    print(f"  DROP TABLE IF EXISTS...", end=" ", flush=True)
    result = execute_sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table_name}")
    print("done" if get_state(result) == "SUCCEEDED" else f"({get_state(result)})")

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
        print("done")
        return True
    else:
        print(f"FAILED - {get_error(result)}")
        return False


def count_rows(table_name: str) -> int:
    """Count rows in a table."""
    result = execute_sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table_name}")
    if get_state(result) == "SUCCEEDED":
        data = result.get("result", {}).get("data_array", [[0]])
        return int(data[0][0]) if data else 0
    return -1


def create_views():
    """Create analytical views."""
    views = {
        "vw_person_with_department": f"""
            CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.vw_person_with_department AS
            SELECT p.*, pd.role_in_dept, pd.assignment_date,
                   d.department_name, d.location AS dept_location
            FROM {CATALOG}.{SCHEMA}.person p
            LEFT JOIN {CATALOG}.{SCHEMA}.person_department pd ON p.person_id = pd.person_id
            LEFT JOIN {CATALOG}.{SCHEMA}.department d ON pd.department_id = d.department_id
        """,
        "vw_domain_sponsorship": f"""
            CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.vw_domain_sponsorship AS
            SELECT dom.domain_id, dom.domain_name, dom.status, dom.budget AS total_budget,
                   d.department_name, dd.sponsorship_type, dd.funding_amount
            FROM {CATALOG}.{SCHEMA}.domain dom
            JOIN {CATALOG}.{SCHEMA}.department_domain dd ON dom.domain_id = dd.domain_id
            JOIN {CATALOG}.{SCHEMA}.department d ON dd.department_id = d.department_id
        """,
        "vw_collaboration_network": f"""
            CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.vw_collaboration_network AS
            SELECT p1.first_name || ' ' || p1.last_name AS person_1,
                   p2.first_name || ' ' || p2.last_name AS person_2,
                   pc.collaboration_type, dom.domain_name, pc.hours_per_week
            FROM {CATALOG}.{SCHEMA}.person_collaboration pc
            JOIN {CATALOG}.{SCHEMA}.person p1 ON pc.person_id_1 = p1.person_id
            JOIN {CATALOG}.{SCHEMA}.person p2 ON pc.person_id_2 = p2.person_id
            LEFT JOIN {CATALOG}.{SCHEMA}.domain dom ON pc.domain_id = dom.domain_id
        """,
    }

    for name, sql in views.items():
        print(f"  {name}...", end=" ", flush=True)
        result = execute_sql(sql)
        print("done" if get_state(result) == "SUCCEEDED" else f"FAILED - {get_error(result)}")


def main():
    print("=" * 60)
    print("Project Management (PM) Dataset")
    print(f"Catalog: {CATALOG}")
    print(f"Schema:  {SCHEMA}")
    print(f"Volume:  {VOLUME_PATH}")
    print("=" * 60)

    if not create_schema():
        print("Aborting: could not create schema/volume.")
        return

    print("\nUploading CSV files...")
    if not upload_csv_files():
        print("Aborting: CSV upload failed.")
        return

    print("\nCreating tables...")
    success_count = 0
    total_rows = 0

    for table_name in TABLES:
        print(f"\n  {table_name}")
        if create_table(table_name):
            success_count += 1
            rows = count_rows(table_name)
            if rows >= 0:
                print(f"  Rows: {rows}")
                total_rows += rows

    print("\nCreating views...")
    create_views()

    print()
    print("=" * 60)
    if success_count == len(TABLES):
        print(f"All {success_count} tables created successfully!")
    else:
        print(f"{success_count}/{len(TABLES)} tables created")
    print(f"Total rows: {total_rows}")
    print("=" * 60)

    if success_count > 0:
        print(f"\nTables in {CATALOG}.{SCHEMA}:")
        result = execute_sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
        if get_state(result) == "SUCCEEDED":
            data = result.get("result", {}).get("data_array", [])
            for row in data:
                print(f"   - {row[1] if len(row) > 1 else row[0]}")


if __name__ == "__main__":
    main()
