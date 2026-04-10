# Databricks notebook source
# MAGIC %md
# MAGIC # Load Île-de-France Public Transport Dataset into Unity Catalog
# MAGIC
# MAGIC This notebook creates and populates tables in Unity Catalog from CSV files stored in a Volume.
# MAGIC
# MAGIC ## Dataset Structure
# MAGIC - **Reference Tables**: Transport Operator, Transport Mode, Line, Station, Ticket Type
# MAGIC - **Network Tables**: Line Station, Schedule, Vehicle
# MAGIC - **Operational Tables**: Trip, Stop Event, Validation, Incident, Maintenance Task
# MAGIC - **Analytics Tables**: Passenger Survey, Accessibility Equipment, Traffic Daily
# MAGIC
# MAGIC ## Data Sources
# MAGIC Inspired by open data from [Île-de-France Mobilités](https://data.iledefrance-mobilites.fr/) and [RATP](https://data.ratp.fr/).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - CSV files uploaded to a Unity Catalog Volume
# MAGIC - Permissions to create schema and tables in the target catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

catalog = "benoit_cayla"
schema = "idf_transport"
volume_name = "transport_data"
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

print(f"Target Catalog: {catalog}")
print(f"Target Schema: {schema}")
print(f"Volume Path: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"Schema {catalog}.{schema} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Volume

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")
print(f"Volume {volume_path} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Tables from CSV
# MAGIC
# MAGIC Each table is created using `read_files` CTAS pattern for automatic schema inference.

# COMMAND ----------

tables = [
    "transport_operator",
    "transport_mode",
    "line",
    "station",
    "line_station",
    "schedule",
    "vehicle",
    "trip",
    "stop_event",
    "ticket_type",
    "validation",
    "incident",
    "maintenance_task",
    "passenger_survey",
    "accessibility_equipment",
    "traffic_daily",
]

results = []
for table_name in tables:
    full_name = f"{catalog}.{schema}.{table_name}"
    csv_path = f"{volume_path}/{table_name}.csv"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        spark.sql(f"""
            CREATE TABLE {full_name} AS
            SELECT * FROM read_files('{csv_path}', format => 'csv', header => true)
        """)
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").collect()[0]["cnt"]
        results.append((table_name, "OK", count))
        print(f"  ✅ {table_name}: {count} rows")
    except Exception as e:
        results.append((table_name, "FAIL", str(e)[:80]))
        print(f"  ❌ {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify

# COMMAND ----------

print(f"\n{'Table':<30} {'Status':<8} {'Rows'}")
print("-" * 55)
total = 0
for name, status, count in results:
    print(f"  {name:<28} {status:<8} {count}")
    if status == "OK":
        total += count
print(f"\nTotal rows: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Analytical Views

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.v_line_performance AS
SELECT
    l.line_code,
    l.line_name,
    m.mode_name,
    o.operator_name,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    AVG(t.trip_duration_min) AS avg_duration_min,
    SUM(CASE WHEN t.status = 'delayed' THEN 1 ELSE 0 END) AS delayed_trips,
    SUM(CASE WHEN t.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_trips
FROM {catalog}.{schema}.line l
JOIN {catalog}.{schema}.transport_mode m ON l.mode_id = m.mode_id
JOIN {catalog}.{schema}.transport_operator o ON l.operator_id = o.operator_id
LEFT JOIN {catalog}.{schema}.trip t ON l.line_id = t.line_id
GROUP BY l.line_code, l.line_name, m.mode_name, o.operator_name
""")
print("View v_line_performance created.")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.v_station_activity AS
SELECT
    s.station_name,
    s.commune,
    s.zone,
    s.annual_traffic,
    COUNT(DISTINCT ls.line_id) AS lines_served,
    COUNT(DISTINCT v.validation_id) AS total_validations,
    COUNT(DISTINCT i.incident_id) AS incidents,
    SUM(CASE WHEN ae.status = 'out_of_service' THEN 1 ELSE 0 END) AS equipment_outages
FROM {catalog}.{schema}.station s
LEFT JOIN {catalog}.{schema}.line_station ls ON s.station_id = ls.station_id
LEFT JOIN {catalog}.{schema}.validation v ON s.station_id = v.station_id
LEFT JOIN {catalog}.{schema}.incident i ON s.station_id = i.station_id
LEFT JOIN {catalog}.{schema}.accessibility_equipment ae ON s.station_id = ae.station_id
GROUP BY s.station_name, s.commune, s.zone, s.annual_traffic
""")
print("View v_station_activity created.")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.v_daily_operations AS
SELECT
    td.traffic_date,
    td.day_type,
    l.line_code,
    m.mode_name,
    td.total_ridership,
    td.peak_hour_ridership,
    COUNT(DISTINCT inc.incident_id) AS incidents,
    AVG(se.delay_seconds) AS avg_delay_seconds
FROM {catalog}.{schema}.traffic_daily td
JOIN {catalog}.{schema}.line l ON td.line_id = l.line_id
JOIN {catalog}.{schema}.transport_mode m ON l.mode_id = m.mode_id
LEFT JOIN {catalog}.{schema}.incident inc ON l.line_id = inc.line_id
LEFT JOIN {catalog}.{schema}.trip t ON l.line_id = t.line_id AND t.trip_date = td.traffic_date
LEFT JOIN {catalog}.{schema}.stop_event se ON t.trip_id = se.trip_id
GROUP BY td.traffic_date, td.day_type, l.line_code, m.mode_name, td.total_ridership, td.peak_hour_ridership
""")
print("View v_daily_operations created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **16 tables** and **3 analytical views** created in `benoit_cayla.idf_transport`.
# MAGIC
# MAGIC | View | Purpose |
# MAGIC |---|---|
# MAGIC | `v_line_performance` | Trip counts, delays, cancellations per line |
# MAGIC | `v_station_activity` | Validation counts, incidents, equipment status per station |
# MAGIC | `v_daily_operations` | Daily ridership with incident and delay metrics |
