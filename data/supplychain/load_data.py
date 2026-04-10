# Databricks notebook source
# MAGIC %md
# MAGIC # Load Supply Chain / Logistics Dataset into Unity Catalog
# MAGIC
# MAGIC This notebook creates and populates tables in Unity Catalog from CSV files stored in a Volume.
# MAGIC
# MAGIC ## Dataset Structure
# MAGIC - **Core Entity Tables**: Supplier, Product, Product Category, Warehouse, Customer
# MAGIC - **Transaction Tables**: Inventory, Purchase Order, Order Line, Shipment, Delivery Event, Sales Order, Sales Order Line
# MAGIC - **Quality & Returns**: Quality Inspection, Return Request
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
schema = "supply_chain"
volume_name = "supplychain_data"
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
# MAGIC ### 2.1 Supplier Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, BooleanType, IntegerType, DoubleType

supplier_schema = StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), False),
    StructField("contact_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("is_active", StringType(), True)
])

supplier_df = spark.read.csv(
    f"{volume_path}/supplier.csv",
    header=True,
    schema=supplier_schema
)
supplier_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.supplier")
print(f"✓ Created table: {catalog}.{schema}.supplier")
print(f"  Row count: {supplier_df.count()}")
display(supplier_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Product Category Table

# COMMAND ----------

product_category_schema = StructType([
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("parent_category", StringType(), True)
])

product_category_df = spark.read.csv(
    f"{volume_path}/product_category.csv",
    header=True,
    schema=product_category_schema
)
product_category_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.product_category")
print(f"✓ Created table: {catalog}.{schema}.product_category")
print(f"  Row count: {product_category_df.count()}")
display(product_category_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Product Table

# COMMAND ----------

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("category_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("unit_price", DecimalType(15, 2), True),
    StructField("weight_kg", DecimalType(10, 2), True),
    StructField("is_active", StringType(), True)
])

product_df = spark.read.csv(
    f"{volume_path}/product.csv",
    header=True,
    schema=product_schema
)
product_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.product")
print(f"✓ Created table: {catalog}.{schema}.product")
print(f"  Row count: {product_df.count()}")
display(product_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Warehouse Table

# COMMAND ----------

warehouse_schema = StructType([
    StructField("warehouse_id", StringType(), False),
    StructField("warehouse_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("capacity_sqm", IntegerType(), True),
    StructField("warehouse_type", StringType(), True)
])

warehouse_df = spark.read.csv(
    f"{volume_path}/warehouse.csv",
    header=True,
    schema=warehouse_schema
)
warehouse_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.warehouse")
print(f"✓ Created table: {catalog}.{schema}.warehouse")
print(f"  Row count: {warehouse_df.count()}")
display(warehouse_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Customer Table

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("credit_limit", DecimalType(15, 2), True)
])

customer_df = spark.read.csv(
    f"{volume_path}/customer.csv",
    header=True,
    schema=customer_schema
)
customer_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customer")
print(f"✓ Created table: {catalog}.{schema}.customer")
print(f"  Row count: {customer_df.count()}")
display(customer_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create and Load Transaction Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Inventory Table

# COMMAND ----------

inventory_schema = StructType([
    StructField("inventory_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("last_restock_date", StringType(), True)
])

inventory_df = spark.read.csv(
    f"{volume_path}/inventory.csv",
    header=True,
    schema=inventory_schema
)
inventory_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.inventory")
print(f"✓ Created table: {catalog}.{schema}.inventory")
print(f"  Row count: {inventory_df.count()}")
display(inventory_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Purchase Order Table

# COMMAND ----------

purchase_order_schema = StructType([
    StructField("po_id", StringType(), False),
    StructField("supplier_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("expected_delivery", DateType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DecimalType(15, 2), True)
])

purchase_order_df = spark.read.csv(
    f"{volume_path}/purchase_order.csv",
    header=True,
    schema=purchase_order_schema
)
purchase_order_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.purchase_order")
print(f"✓ Created table: {catalog}.{schema}.purchase_order")
print(f"  Row count: {purchase_order_df.count()}")
display(purchase_order_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Order Line Table (Purchase Order Lines)

# COMMAND ----------

order_line_schema = StructType([
    StructField("line_id", StringType(), False),
    StructField("po_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(15, 2), True),
    StructField("line_total", DecimalType(15, 2), True)
])

order_line_df = spark.read.csv(
    f"{volume_path}/order_line.csv",
    header=True,
    schema=order_line_schema
)
order_line_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.order_line")
print(f"✓ Created table: {catalog}.{schema}.order_line")
print(f"  Row count: {order_line_df.count()}")
display(order_line_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Shipment Table

# COMMAND ----------

shipment_schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("po_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("ship_date", DateType(), True),
    StructField("carrier", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("status", StringType(), True),
    StructField("estimated_arrival", DateType(), True)
])

shipment_df = spark.read.csv(
    f"{volume_path}/shipment.csv",
    header=True,
    schema=shipment_schema
)
shipment_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.shipment")
print(f"✓ Created table: {catalog}.{schema}.shipment")
print(f"  Row count: {shipment_df.count()}")
display(shipment_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Delivery Event Table

# COMMAND ----------

delivery_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("shipment_id", StringType(), True),
    StructField("event_datetime", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True)
])

delivery_event_df = spark.read.csv(
    f"{volume_path}/delivery_event.csv",
    header=True,
    schema=delivery_event_schema
)
delivery_event_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.delivery_event")
print(f"✓ Created table: {catalog}.{schema}.delivery_event")
print(f"  Row count: {delivery_event_df.count()}")
display(delivery_event_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Sales Order Table

# COMMAND ----------

sales_order_schema = StructType([
    StructField("so_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DecimalType(15, 2), True)
])

sales_order_df = spark.read.csv(
    f"{volume_path}/sales_order.csv",
    header=True,
    schema=sales_order_schema
)
sales_order_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales_order")
print(f"✓ Created table: {catalog}.{schema}.sales_order")
print(f"  Row count: {sales_order_df.count()}")
display(sales_order_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 Sales Order Line Table

# COMMAND ----------

sales_order_line_schema = StructType([
    StructField("line_id", StringType(), False),
    StructField("so_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(15, 2), True),
    StructField("line_total", DecimalType(15, 2), True)
])

sales_order_line_df = spark.read.csv(
    f"{volume_path}/sales_order_line.csv",
    header=True,
    schema=sales_order_line_schema
)
sales_order_line_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales_order_line")
print(f"✓ Created table: {catalog}.{schema}.sales_order_line")
print(f"  Row count: {sales_order_line_df.count()}")
display(sales_order_line_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 Quality Inspection Table

# COMMAND ----------

quality_inspection_schema = StructType([
    StructField("inspection_id", StringType(), False),
    StructField("shipment_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("inspection_date", DateType(), True),
    StructField("inspector", StringType(), True),
    StructField("result", StringType(), True),
    StructField("defect_count", IntegerType(), True),
    StructField("notes", StringType(), True)
])

quality_inspection_df = spark.read.csv(
    f"{volume_path}/quality_inspection.csv",
    header=True,
    schema=quality_inspection_schema
)
quality_inspection_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.quality_inspection")
print(f"✓ Created table: {catalog}.{schema}.quality_inspection")
print(f"  Row count: {quality_inspection_df.count()}")
display(quality_inspection_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.9 Return Request Table

# COMMAND ----------

return_request_schema = StructType([
    StructField("return_id", StringType(), False),
    StructField("so_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("request_date", DateType(), True),
    StructField("reason", StringType(), True),
    StructField("status", StringType(), True),
    StructField("refund_amount", DecimalType(15, 2), True)
])

return_request_df = spark.read.csv(
    f"{volume_path}/return_request.csv",
    header=True,
    schema=return_request_schema
)
return_request_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.return_request")
print(f"✓ Created table: {catalog}.{schema}.return_request")
print(f"  Row count: {return_request_df.count()}")
display(return_request_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Tables and Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 List All Created Tables

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(tables_df)

print("\n📊 Table Summary:")
print("=" * 60)
for table in tables_df.collect():
    table_name = table['tableName']
    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog}.{schema}.{table_name}").first()['count']
    print(f"  {table_name:30s} {count:5d} rows")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Verification Queries

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 1: Order fulfillment - PO to shipment to delivery

# COMMAND ----------

query1 = f"""
SELECT 
    po.po_id,
    s.supplier_name,
    po.order_date,
    po.status as po_status,
    sh.shipment_id,
    sh.carrier,
    sh.status as shipment_status,
    w.warehouse_name
FROM {catalog}.{schema}.purchase_order po
JOIN {catalog}.{schema}.supplier s ON po.supplier_id = s.supplier_id
JOIN {catalog}.{schema}.shipment sh ON po.po_id = sh.po_id
JOIN {catalog}.{schema}.warehouse w ON sh.warehouse_id = w.warehouse_id
WHERE po.status IN ('in_transit', 'received')
ORDER BY po.order_date DESC
LIMIT 20
"""
print("Query 1: Purchase Order Fulfillment Pipeline")
display(spark.sql(query1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2: Inventory status by warehouse

# COMMAND ----------

query2 = f"""
SELECT 
    w.warehouse_name,
    w.city,
    w.country,
    COUNT(i.inventory_id) as sku_count,
    SUM(i.quantity_on_hand) as total_units,
    SUM(CASE WHEN i.quantity_on_hand <= i.reorder_level THEN 1 ELSE 0 END) as below_reorder
FROM {catalog}.{schema}.warehouse w
JOIN {catalog}.{schema}.inventory i ON w.warehouse_id = i.warehouse_id
GROUP BY w.warehouse_id, w.warehouse_name, w.city, w.country
ORDER BY total_units DESC
"""
print("Query 2: Inventory Status by Warehouse")
display(spark.sql(query2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3: Supplier performance

# COMMAND ----------

query3 = f"""
SELECT 
    s.supplier_id,
    s.supplier_name,
    s.rating,
    s.country,
    COUNT(po.po_id) as order_count,
    SUM(po.total_amount) as total_spend,
    AVG(po.total_amount) as avg_order_value
FROM {catalog}.{schema}.supplier s
JOIN {catalog}.{schema}.purchase_order po ON s.supplier_id = po.supplier_id
WHERE s.is_active = 'true'
GROUP BY s.supplier_id, s.supplier_name, s.rating, s.country
ORDER BY total_spend DESC
LIMIT 20
"""
print("Query 3: Supplier Performance")
display(spark.sql(query3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Views for Common Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 vw_order_fulfillment

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_order_fulfillment AS
SELECT 
    po.po_id,
    s.supplier_name,
    po.order_date,
    po.expected_delivery,
    po.status as po_status,
    po.total_amount,
    sh.shipment_id,
    sh.carrier,
    sh.tracking_number,
    sh.status as shipment_status,
    sh.ship_date,
    sh.estimated_arrival,
    w.warehouse_name,
    w.city as warehouse_city
FROM {catalog}.{schema}.purchase_order po
JOIN {catalog}.{schema}.supplier s ON po.supplier_id = s.supplier_id
LEFT JOIN {catalog}.{schema}.shipment sh ON po.po_id = sh.po_id
LEFT JOIN {catalog}.{schema}.warehouse w ON sh.warehouse_id = w.warehouse_id
""")
print(f"✓ Created view: {catalog}.{schema}.vw_order_fulfillment")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 vw_inventory_status

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_inventory_status AS
SELECT 
    i.inventory_id,
    p.product_name,
    p.sku,
    pc.category_name,
    w.warehouse_name,
    w.city,
    w.country,
    i.quantity_on_hand,
    i.reorder_level,
    i.last_restock_date,
    CASE WHEN i.quantity_on_hand <= i.reorder_level THEN 'LOW' ELSE 'OK' END as stock_status
FROM {catalog}.{schema}.inventory i
JOIN {catalog}.{schema}.product p ON i.product_id = p.product_id
JOIN {catalog}.{schema}.product_category pc ON p.category_id = pc.category_id
JOIN {catalog}.{schema}.warehouse w ON i.warehouse_id = w.warehouse_id
""")
print(f"✓ Created view: {catalog}.{schema}.vw_inventory_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 vw_supplier_performance

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_supplier_performance AS
SELECT 
    s.supplier_id,
    s.supplier_name,
    s.contact_name,
    s.country,
    s.city,
    s.rating,
    s.is_active,
    COUNT(po.po_id) as total_orders,
    SUM(po.total_amount) as total_spend,
    AVG(po.total_amount) as avg_order_value,
    COUNT(DISTINCT sh.shipment_id) as shipments_received
FROM {catalog}.{schema}.supplier s
LEFT JOIN {catalog}.{schema}.purchase_order po ON s.supplier_id = po.supplier_id
LEFT JOIN {catalog}.{schema}.shipment sh ON po.po_id = sh.po_id AND sh.status = 'delivered'
GROUP BY s.supplier_id, s.supplier_name, s.contact_name, s.country, s.city, s.rating, s.is_active
""")
print(f"✓ Created view: {catalog}.{schema}.vw_supplier_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("✅ SUPPLY CHAIN DATA LOAD COMPLETE!")
print("=" * 70)
print(f"\n📁 Catalog: {catalog}")
print(f"📁 Schema: {schema}")
print(f"📦 Volume: {volume_path}")
print("\n📊 Tables Created:")
print(f"   • {catalog}.{schema}.supplier")
print(f"   • {catalog}.{schema}.product_category")
print(f"   • {catalog}.{schema}.product")
print(f"   • {catalog}.{schema}.warehouse")
print(f"   • {catalog}.{schema}.customer")
print(f"   • {catalog}.{schema}.inventory")
print(f"   • {catalog}.{schema}.purchase_order")
print(f"   • {catalog}.{schema}.order_line")
print(f"   • {catalog}.{schema}.shipment")
print(f"   • {catalog}.{schema}.delivery_event")
print(f"   • {catalog}.{schema}.sales_order")
print(f"   • {catalog}.{schema}.sales_order_line")
print(f"   • {catalog}.{schema}.quality_inspection")
print(f"   • {catalog}.{schema}.return_request")
print("\n👁️ Views Created:")
print(f"   • {catalog}.{schema}.vw_order_fulfillment")
print(f"   • {catalog}.{schema}.vw_inventory_status")
print(f"   • {catalog}.{schema}.vw_supplier_performance")
print("\n" + "=" * 70)
print("Ready for OntoBricks ontology mapping!")
print("=" * 70)

# COMMAND ----------
