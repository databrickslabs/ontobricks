# Databricks notebook source
# MAGIC %md
# MAGIC # Load Sample Data into Unity Catalog
# MAGIC 
# MAGIC This notebook creates and populates tables in Unity Catalog from CSV files stored in a Volume.
# MAGIC 
# MAGIC ## Dataset Structure
# MAGIC - **3 Entity Tables**: Person, Department, Project
# MAGIC - **3 Relationship Tables**: Person-Department, Department-Project, Person-Collaboration
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - CSV files uploaded to a Unity Catalog Volume
# MAGIC - Permissions to create schema and tables in the target catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Initialize variables
catalog = "benoit_cayla"
schema = "pm"
volume_name = "pm_data"
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
# MAGIC ## Step 2: Create and Load Entity Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Person Table

# COMMAND ----------

# Define schema for Person table
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType

person_schema = StructType([
    StructField("person_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("job_title", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("salary", DecimalType(10, 2), True)
])

# Read CSV from Volume
person_df = spark.read.csv(
    f"{volume_path}/person.csv",
    header=True,
    schema=person_schema
)

# Create table
person_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.person")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.person")
print(f"  Row count: {person_df.count()}")
display(person_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Department Table

# COMMAND ----------

# Define schema for Department table
department_schema = StructType([
    StructField("department_id", StringType(), False),
    StructField("department_name", StringType(), False),
    StructField("location", StringType(), True),
    StructField("budget", DecimalType(15, 2), True),
    StructField("manager_id", StringType(), True)
])

# Read CSV from Volume
department_df = spark.read.csv(
    f"{volume_path}/department.csv",
    header=True,
    schema=department_schema
)

# Create table
department_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.department")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.department")
print(f"  Row count: {department_df.count()}")
display(department_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Project Table

# COMMAND ----------

# Define schema for Project table
project_schema = StructType([
    StructField("project_id", StringType(), False),
    StructField("project_name", StringType(), False),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("budget", DecimalType(15, 2), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True)
])

# Read CSV from Volume
project_df = spark.read.csv(
    f"{volume_path}/project.csv",
    header=True,
    schema=project_schema
)

# Create table
project_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.project")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.project")
print(f"  Row count: {project_df.count()}")
display(project_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create and Load Relationship Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Person-Department Relationship

# COMMAND ----------

# Define schema for Person-Department relationship
person_dept_schema = StructType([
    StructField("person_id", StringType(), False),
    StructField("department_id", StringType(), False),
    StructField("role_in_dept", StringType(), True),
    StructField("assignment_date", DateType(), True)
])

# Read CSV from Volume
person_dept_df = spark.read.csv(
    f"{volume_path}/person_department.csv",
    header=True,
    schema=person_dept_schema
)

# Create table
person_dept_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.person_department")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.person_department")
print(f"  Row count: {person_dept_df.count()}")
display(person_dept_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Department-Project Relationship

# COMMAND ----------

# Define schema for Department-Project relationship
dept_project_schema = StructType([
    StructField("department_id", StringType(), False),
    StructField("project_id", StringType(), False),
    StructField("sponsorship_type", StringType(), True),
    StructField("funding_amount", DecimalType(15, 2), True),
    StructField("start_date", DateType(), True)
])

# Read CSV from Volume
dept_project_df = spark.read.csv(
    f"{volume_path}/department_project.csv",
    header=True,
    schema=dept_project_schema
)

# Create table
dept_project_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.department_project")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.department_project")
print(f"  Row count: {dept_project_df.count()}")
display(dept_project_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Person-Collaboration Relationship (Bi-directional)

# COMMAND ----------

# Define schema for Person-Collaboration relationship
person_collab_schema = StructType([
    StructField("person_id_1", StringType(), False),
    StructField("person_id_2", StringType(), False),
    StructField("collaboration_type", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("hours_per_week", DecimalType(5, 2), True)
])

# Read CSV from Volume
person_collab_df = spark.read.csv(
    f"{volume_path}/person_collaboration.csv",
    header=True,
    schema=person_collab_schema
)

# Create table
person_collab_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.person_collaboration")

# Display sample
print(f"✓ Created table: {catalog}.{schema}.person_collaboration")
print(f"  Row count: {person_collab_df.count()}")
display(person_collab_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Tables and Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 List All Created Tables

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
# MAGIC ### 4.2 Test Queries - Verify Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 1: Find all people in Data Engineering department

# COMMAND ----------

query1 = f"""
SELECT 
    p.person_id,
    p.first_name,
    p.last_name,
    p.job_title,
    d.department_name,
    pd.role_in_dept
FROM {catalog}.{schema}.person p
JOIN {catalog}.{schema}.person_department pd ON p.person_id = pd.person_id
JOIN {catalog}.{schema}.department d ON pd.department_id = d.department_id
WHERE d.department_name = 'Data Engineering'
ORDER BY p.last_name
"""

print("Query 1: People in Data Engineering")
display(spark.sql(query1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2: Find all projects with their sponsoring departments

# COMMAND ----------

query2 = f"""
SELECT 
    p.project_name,
    p.status,
    d.department_name,
    dp.sponsorship_type,
    dp.funding_amount
FROM {catalog}.{schema}.project p
JOIN {catalog}.{schema}.department_project dp ON p.project_id = dp.project_id
JOIN {catalog}.{schema}.department d ON dp.department_id = d.department_id
ORDER BY p.project_name, dp.sponsorship_type
"""

print("Query 2: Projects and Sponsoring Departments")
display(spark.sql(query2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3: Find collaboration partners (bi-directional relationship)

# COMMAND ----------

query3 = f"""
SELECT 
    p1.first_name || ' ' || p1.last_name as person_1,
    p2.first_name || ' ' || p2.last_name as person_2,
    pc.collaboration_type,
    pr.project_name,
    pc.hours_per_week
FROM {catalog}.{schema}.person_collaboration pc
JOIN {catalog}.{schema}.person p1 ON pc.person_id_1 = p1.person_id
JOIN {catalog}.{schema}.person p2 ON pc.person_id_2 = p2.person_id
LEFT JOIN {catalog}.{schema}.project pr ON pc.project_id = pr.project_id
ORDER BY p1.last_name, p2.last_name
"""

print("Query 3: Person Collaborations (Bi-directional)")
display(spark.sql(query3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 4: Department managers with their teams

# COMMAND ----------

query4 = f"""
SELECT 
    d.department_name,
    d.location,
    m.first_name || ' ' || m.last_name as manager_name,
    COUNT(DISTINCT pd.person_id) as team_size,
    d.budget
FROM {catalog}.{schema}.department d
LEFT JOIN {catalog}.{schema}.person m ON d.manager_id = m.person_id
LEFT JOIN {catalog}.{schema}.person_department pd ON d.department_id = pd.department_id
GROUP BY d.department_name, d.location, m.first_name, m.last_name, d.budget
ORDER BY team_size DESC
"""

print("Query 4: Department Managers and Team Sizes")
display(spark.sql(query4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Views for Common Queries

# COMMAND ----------

# Create a view for person with department information
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_person_with_department AS
SELECT 
    p.*,
    pd.role_in_dept,
    pd.assignment_date,
    d.department_name,
    d.location as dept_location
FROM {catalog}.{schema}.person p
LEFT JOIN {catalog}.{schema}.person_department pd ON p.person_id = pd.person_id
LEFT JOIN {catalog}.{schema}.department d ON pd.department_id = d.department_id
""")

print(f"✓ Created view: {catalog}.{schema}.vw_person_with_department")

# COMMAND ----------

# Create a view for project with sponsorship details
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_project_sponsorship AS
SELECT 
    p.project_id,
    p.project_name,
    p.status,
    p.budget as total_budget,
    d.department_name,
    dp.sponsorship_type,
    dp.funding_amount,
    m.first_name || ' ' || m.last_name as dept_manager
FROM {catalog}.{schema}.project p
JOIN {catalog}.{schema}.department_project dp ON p.project_id = dp.project_id
JOIN {catalog}.{schema}.department d ON dp.department_id = d.department_id
LEFT JOIN {catalog}.{schema}.person m ON d.manager_id = m.person_id
""")

print(f"✓ Created view: {catalog}.{schema}.vw_project_sponsorship")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("✅ DATA LOAD COMPLETE!")
print("=" * 70)
print(f"\n📁 Catalog: {catalog}")
print(f"📁 Schema: {schema}")
print(f"📦 Volume: {volume_path}")
print("\n📊 Tables Created:")
print(f"   • {catalog}.{schema}.person")
print(f"   • {catalog}.{schema}.department")
print(f"   • {catalog}.{schema}.project")
print(f"   • {catalog}.{schema}.person_department")
print(f"   • {catalog}.{schema}.department_project")
print(f"   • {catalog}.{schema}.person_collaboration")
print("\n👁️ Views Created:")
print(f"   • {catalog}.{schema}.vw_person_with_department")
print(f"   • {catalog}.{schema}.vw_project_sponsorship")
print("\n🔗 Relationships:")
print("   • Person → Department (one-directional, many-to-one)")
print("   • Department → Project (one-directional, many-to-many)")
print("   • Person ↔ Person (bi-directional collaboration)")
print("\n" + "=" * 70)
print("Ready for OntoBricks ontology mapping!")
print("=" * 70)

# COMMAND ----------





