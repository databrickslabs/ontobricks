# Databricks notebook source
# MAGIC %md
# MAGIC # Load Energy Provider Customer Journey Dataset into Unity Catalog
# MAGIC 
# MAGIC This notebook creates and populates tables in Unity Catalog from CSV files stored in a Volume.
# MAGIC 
# MAGIC ## Dataset Structure
# MAGIC - **4 Core Entity Tables**: Customer, Contract, Subscription, Meter
# MAGIC - **3 Transaction Tables**: Meter Reading, Invoice, Payment
# MAGIC - **3 Interaction Tables**: Call, Claim, Interaction
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - CSV files uploaded to a Unity Catalog Volume
# MAGIC - Permissions to create schema and tables in the target catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Initialize variables
catalog = "main"
schema = "customer_journey"
volume_name = "customer_data"
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# Display configuration
print(f"Target Catalog: {catalog}")
print(f"Target Schema: {schema}")
print(f"Volume Path: {volume_path}")
print(f"Full Table Path: {catalog}.{schema}.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Schema

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"✓ Schema {catalog}.{schema} is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create and Load Core Entity Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Customer Table

# COMMAND ----------

# Define schema for Customer table
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, BooleanType, IntegerType, TimestampType

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("street_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("registration_date", DateType(), True),
    StructField("segment", StringType(), True),
    StructField("loyalty_points", IntegerType(), True),
    StructField("is_active", StringType(), True)
])

# Read CSV from Volume
customer_df = spark.read.csv(
    f"{volume_path}/customer.csv",
    header=True,
    schema=customer_schema
)

# Create table
customer_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customer")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.customer")
print(f"  Row count: {customer_df.count()}")
display(customer_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Contract Table

# COMMAND ----------

# Define schema for Contract table
contract_schema = StructType([
    StructField("contract_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("energy_type", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("monthly_fee", DecimalType(10, 2), True),
    StructField("payment_method", StringType(), True),
    StructField("auto_renewal", StringType(), True),
    StructField("created_at", DateType(), True)
])

# Read CSV from Volume
contract_df = spark.read.csv(
    f"{volume_path}/contract.csv",
    header=True,
    schema=contract_schema
)

# Create table
contract_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.contract")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.contract")
print(f"  Row count: {contract_df.count()}")
display(contract_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Subscription Table

# COMMAND ----------

# Define schema for Subscription table
subscription_schema = StructType([
    StructField("subscription_id", StringType(), False),
    StructField("contract_id", StringType(), False),
    StructField("plan_type", StringType(), True),
    StructField("price_per_kwh", DecimalType(10, 4), True),
    StructField("price_per_m3", DecimalType(10, 4), True),
    StructField("standing_charge", DecimalType(10, 2), True),
    StructField("discount_percentage", DecimalType(5, 1), True),
    StructField("green_energy", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("status", StringType(), True)
])

# Read CSV from Volume
subscription_df = spark.read.csv(
    f"{volume_path}/subscription.csv",
    header=True,
    schema=subscription_schema
)

# Create table
subscription_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.subscription")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.subscription")
print(f"  Row count: {subscription_df.count()}")
display(subscription_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Meter Table

# COMMAND ----------

# Define schema for Meter table
meter_schema = StructType([
    StructField("meter_id", StringType(), False),
    StructField("contract_id", StringType(), False),
    StructField("meter_serial", StringType(), True),
    StructField("meter_type", StringType(), True),
    StructField("energy_type", StringType(), True),
    StructField("installation_date", DateType(), True),
    StructField("last_inspection_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True)
])

# Read CSV from Volume
meter_df = spark.read.csv(
    f"{volume_path}/meter.csv",
    header=True,
    schema=meter_schema
)

# Create table
meter_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.meter")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.meter")
print(f"  Row count: {meter_df.count()}")
display(meter_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create and Load Transaction Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Meter Reading Table

# COMMAND ----------

# Define schema for Meter Reading table
meter_reading_schema = StructType([
    StructField("reading_id", StringType(), False),
    StructField("meter_id", StringType(), False),
    StructField("reading_date", DateType(), True),
    StructField("reading_value", DecimalType(15, 2), True),
    StructField("unit", StringType(), True),
    StructField("reading_type", StringType(), True),
    StructField("reported_by", StringType(), True),
    StructField("validated", StringType(), True)
])

# Read CSV from Volume
meter_reading_df = spark.read.csv(
    f"{volume_path}/meter_reading.csv",
    header=True,
    schema=meter_reading_schema
)

# Create table
meter_reading_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.meter_reading")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.meter_reading")
print(f"  Row count: {meter_reading_df.count()}")
display(meter_reading_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Invoice Table

# COMMAND ----------

# Define schema for Invoice table
invoice_schema = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("contract_id", StringType(), False),
    StructField("issue_date", DateType(), True),
    StructField("due_date", DateType(), True),
    StructField("period_start", DateType(), True),
    StructField("period_end", DateType(), True),
    StructField("amount_ht", DecimalType(15, 2), True),
    StructField("vat_amount", DecimalType(15, 2), True),
    StructField("amount_ttc", DecimalType(15, 2), True),
    StructField("status", StringType(), True),
    StructField("payment_date", DateType(), True)
])

# Read CSV from Volume
invoice_df = spark.read.csv(
    f"{volume_path}/invoice.csv",
    header=True,
    schema=invoice_schema
)

# Create table
invoice_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.invoice")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.invoice")
print(f"  Row count: {invoice_df.count()}")
display(invoice_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Payment Table

# COMMAND ----------

# Define schema for Payment table
payment_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("invoice_id", StringType(), False),
    StructField("payment_date", DateType(), True),
    StructField("amount", DecimalType(15, 2), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_ref", StringType(), True),
    StructField("status", StringType(), True),
    StructField("processed_at", StringType(), True)
])

# Read CSV from Volume
payment_df = spark.read.csv(
    f"{volume_path}/payment.csv",
    header=True,
    schema=payment_schema
)

# Create table
payment_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.payment")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.payment")
print(f"  Row count: {payment_df.count()}")
display(payment_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create and Load Interaction Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Call Table (Customer Service Calls)

# COMMAND ----------

# Define schema for Call table
call_schema = StructType([
    StructField("call_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("call_datetime", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("reason", StringType(), True),
    StructField("agent_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("satisfaction_score", IntegerType(), True),
    StructField("notes", StringType(), True)
])

# Read CSV from Volume
call_df = spark.read.csv(
    f"{volume_path}/call.csv",
    header=True,
    schema=call_schema
)

# Create table
call_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.call")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.call")
print(f"  Row count: {call_df.count()}")
display(call_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Claim Table (Customer Complaints)

# COMMAND ----------

# Define schema for Claim table
claim_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("contract_id", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("open_date", DateType(), True),
    StructField("resolution_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("assigned_to", StringType(), True),
    StructField("compensation_amount", DecimalType(10, 2), True)
])

# Read CSV from Volume
claim_df = spark.read.csv(
    f"{volume_path}/claim.csv",
    header=True,
    schema=claim_schema
)

# Create table
claim_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.claim")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.claim")
print(f"  Row count: {claim_df.count()}")
display(claim_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Interaction Table (General Customer Interactions)

# COMMAND ----------

# Define schema for Interaction table
interaction_schema = StructType([
    StructField("interaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("channel", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("interaction_datetime", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("agent_id", StringType(), True),
    StructField("duration_minutes", IntegerType(), True),
    StructField("sentiment", StringType(), True)
])

# Read CSV from Volume
interaction_df = spark.read.csv(
    f"{volume_path}/interaction.csv",
    header=True,
    schema=interaction_schema
)

# Create table
interaction_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.interaction")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.interaction")
print(f"  Row count: {interaction_df.count()}")
display(interaction_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Tables and Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 List All Created Tables

# COMMAND ----------

# Show all tables in the schema
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(tables_df)

# Get row counts for all tables
print("\n📊 Table Summary:")
print("=" * 60)
for table in tables_df.collect():
    table_name = table['tableName']
    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.{table_name}").first()['count']
    print(f"  {table_name:30s} {count:5d} rows")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Test Queries - Verify Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 1: Customer journey - contracts with subscriptions

# COMMAND ----------

query1 = f"""
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.segment,
    ct.contract_id,
    ct.energy_type,
    ct.status as contract_status,
    s.plan_type,
    s.price_per_kwh
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.contract ct ON c.customer_id = ct.customer_id
JOIN {catalog}.{schema}.subscription s ON ct.contract_id = s.contract_id
WHERE ct.status = 'active'
ORDER BY c.last_name
LIMIT 20
"""

print("Query 1: Active Customers with Contracts and Subscriptions")
display(spark.sql(query1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2: Energy consumption by customer

# COMMAND ----------

query2 = f"""
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    m.energy_type,
    COUNT(mr.reading_id) as reading_count,
    SUM(mr.reading_value) as total_consumption,
    mr.unit
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.contract ct ON c.customer_id = ct.customer_id
JOIN {catalog}.{schema}.meter m ON ct.contract_id = m.contract_id
JOIN {catalog}.{schema}.meter_reading mr ON m.meter_id = mr.meter_id
GROUP BY c.customer_id, c.first_name, c.last_name, m.energy_type, mr.unit
ORDER BY total_consumption DESC
LIMIT 20
"""

print("Query 2: Energy Consumption by Customer")
display(spark.sql(query2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3: Invoice and payment status

# COMMAND ----------

query3 = f"""
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    i.invoice_id,
    i.amount_ttc,
    i.status as invoice_status,
    p.payment_id,
    p.amount as paid_amount,
    p.status as payment_status
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.contract ct ON c.customer_id = ct.customer_id
JOIN {catalog}.{schema}.invoice i ON ct.contract_id = i.contract_id
LEFT JOIN {catalog}.{schema}.payment p ON i.invoice_id = p.invoice_id
WHERE i.status IN ('pending', 'overdue')
ORDER BY i.due_date
LIMIT 20
"""

print("Query 3: Pending and Overdue Invoices")
display(spark.sql(query3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 4: Customer service interactions

# COMMAND ----------

query4 = f"""
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    COUNT(DISTINCT call.call_id) as call_count,
    COUNT(DISTINCT clm.claim_id) as claim_count,
    COUNT(DISTINCT int.interaction_id) as interaction_count,
    AVG(call.satisfaction_score) as avg_satisfaction
FROM {catalog}.{schema}.customer c
LEFT JOIN {catalog}.{schema}.call call ON c.customer_id = call.customer_id
LEFT JOIN {catalog}.{schema}.claim clm ON c.customer_id = clm.customer_id
LEFT JOIN {catalog}.{schema}.interaction int ON c.customer_id = int.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING (call_count > 0 OR claim_count > 0)
ORDER BY claim_count DESC, call_count DESC
LIMIT 20
"""

print("Query 4: Customer Service Interactions Summary")
display(spark.sql(query4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Views for Common Queries

# COMMAND ----------

# Create a view for customer 360 overview
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_customer_360 AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.segment,
    c.loyalty_points,
    c.is_active,
    COUNT(DISTINCT ct.contract_id) as contract_count,
    COUNT(DISTINCT m.meter_id) as meter_count,
    COUNT(DISTINCT i.invoice_id) as invoice_count,
    SUM(i.amount_ttc) as total_invoiced,
    COUNT(DISTINCT call.call_id) as call_count,
    COUNT(DISTINCT clm.claim_id) as claim_count
FROM {catalog}.{schema}.customer c
LEFT JOIN {catalog}.{schema}.contract ct ON c.customer_id = ct.customer_id
LEFT JOIN {catalog}.{schema}.meter m ON ct.contract_id = m.contract_id
LEFT JOIN {catalog}.{schema}.invoice i ON ct.contract_id = i.contract_id
LEFT JOIN {catalog}.{schema}.call call ON c.customer_id = call.customer_id
LEFT JOIN {catalog}.{schema}.claim clm ON c.customer_id = clm.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.phone, c.city, 
         c.segment, c.loyalty_points, c.is_active
""")

print(f"✓ Created view: {catalog}.{schema}.vw_customer_360")

# COMMAND ----------

# Create a view for billing summary
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_billing_summary AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    ct.contract_id,
    ct.energy_type,
    i.invoice_id,
    i.issue_date,
    i.amount_ht,
    i.vat_amount,
    i.amount_ttc,
    i.status as invoice_status,
    p.payment_id,
    p.payment_date,
    p.amount as paid_amount,
    p.status as payment_status
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.contract ct ON c.customer_id = ct.customer_id
JOIN {catalog}.{schema}.invoice i ON ct.contract_id = i.contract_id
LEFT JOIN {catalog}.{schema}.payment p ON i.invoice_id = p.invoice_id
""")

print(f"✓ Created view: {catalog}.{schema}.vw_billing_summary")

# COMMAND ----------

# Create a view for consumption analysis
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_consumption_analysis AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.city,
    ct.energy_type as contract_energy_type,
    m.meter_id,
    m.meter_type,
    m.energy_type as meter_energy_type,
    mr.reading_date,
    mr.reading_value,
    mr.unit,
    mr.reading_type,
    mr.validated
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.contract ct ON c.customer_id = ct.customer_id
JOIN {catalog}.{schema}.meter m ON ct.contract_id = m.contract_id
JOIN {catalog}.{schema}.meter_reading mr ON m.meter_id = mr.meter_id
""")

print(f"✓ Created view: {catalog}.{schema}.vw_consumption_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("✅ CUSTOMER JOURNEY DATA LOAD COMPLETE!")
print("=" * 70)
print(f"\n📁 Catalog: {catalog}")
print(f"📁 Schema: {schema}")
print(f"📦 Volume: {volume_path}")
print("\n📊 Tables Created (Entity Tables):")
print(f"   • {catalog}.{schema}.customer")
print(f"   • {catalog}.{schema}.contract")
print(f"   • {catalog}.{schema}.subscription")
print(f"   • {catalog}.{schema}.meter")
print("\n📊 Tables Created (Transaction Tables):")
print(f"   • {catalog}.{schema}.meter_reading")
print(f"   • {catalog}.{schema}.invoice")
print(f"   • {catalog}.{schema}.payment")
print("\n📊 Tables Created (Interaction Tables):")
print(f"   • {catalog}.{schema}.call")
print(f"   • {catalog}.{schema}.claim")
print(f"   • {catalog}.{schema}.interaction")
print("\n👁️ Views Created:")
print(f"   • {catalog}.{schema}.vw_customer_360")
print(f"   • {catalog}.{schema}.vw_billing_summary")
print(f"   • {catalog}.{schema}.vw_consumption_analysis")
print("\n🔗 Key Relationships:")
print("   • Customer → Contract (1:N)")
print("   • Contract → Subscription (1:N)")
print("   • Contract → Meter (1:N)")
print("   • Meter → Meter Reading (1:N)")
print("   • Contract → Invoice (1:N)")
print("   • Invoice → Payment (1:N)")
print("   • Customer → Call (1:N)")
print("   • Customer → Claim (1:N)")
print("   • Customer → Interaction (1:N)")
print("\n" + "=" * 70)
print("Ready for OntoBricks ontology mapping!")
print("=" * 70)

# COMMAND ----------


