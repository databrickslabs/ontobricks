# Databricks notebook source
# MAGIC %md
# MAGIC # Load University Academic Research Dataset into Unity Catalog
# MAGIC 
# MAGIC This notebook creates and populates tables in Unity Catalog from CSV files stored in a Volume.
# MAGIC 
# MAGIC ## Dataset Structure
# MAGIC - **Core Entity Tables**: Student, Faculty, Department, Course
# MAGIC - **Academic Tables**: Enrollment, Course Assignment, Course Prerequisite
# MAGIC - **Research Tables**: Research Domain, Publication, Grant Award, Domain Collaboration
# MAGIC - **Affiliation Table**: Department Affiliation
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
schema = "academic_research"
volume_name = "university_data"
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
# MAGIC ### 2.1 Student Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType

student_schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("enrollment_year", IntegerType(), True),
    StructField("major", StringType(), True),
    StructField("gpa", DecimalType(3, 2), True),
    StructField("status", StringType(), True),
    StructField("date_of_birth", DateType(), True)
])

student_df = spark.read.csv(
    f"{volume_path}/student.csv",
    header=True,
    schema=student_schema
)

student_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.student")
print(f"✓ Created table: {catalog}.{schema}.student")
print(f"  Row count: {student_df.count()}")
display(student_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Faculty Table

# COMMAND ----------

faculty_schema = StructType([
    StructField("faculty_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("title", StringType(), True),
    StructField("tenure_status", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("research_area", StringType(), True)
])

faculty_df = spark.read.csv(
    f"{volume_path}/faculty.csv",
    header=True,
    schema=faculty_schema
)

faculty_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.faculty")
print(f"✓ Created table: {catalog}.{schema}.faculty")
print(f"  Row count: {faculty_df.count()}")
display(faculty_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Department Table

# COMMAND ----------

department_schema = StructType([
    StructField("department_id", StringType(), False),
    StructField("department_name", StringType(), False),
    StructField("building", StringType(), True),
    StructField("budget", DecimalType(15, 2), True),
    StructField("head_faculty_id", StringType(), True)
])

department_df = spark.read.csv(
    f"{volume_path}/department.csv",
    header=True,
    schema=department_schema
)

department_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.department")
print(f"✓ Created table: {catalog}.{schema}.department")
print(f"  Row count: {department_df.count()}")
display(department_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Course Table

# COMMAND ----------

course_schema = StructType([
    StructField("course_id", StringType(), False),
    StructField("department_id", StringType(), False),
    StructField("course_name", StringType(), True),
    StructField("course_code", StringType(), True),
    StructField("credits", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("max_enrollment", IntegerType(), True)
])

course_df = spark.read.csv(
    f"{volume_path}/course.csv",
    header=True,
    schema=course_schema
)

course_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.course")
print(f"✓ Created table: {catalog}.{schema}.course")
print(f"  Row count: {course_df.count()}")
display(course_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create and Load Academic Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Enrollment Table

# COMMAND ----------

enrollment_schema = StructType([
    StructField("enrollment_id", StringType(), False),
    StructField("student_id", StringType(), False),
    StructField("course_id", StringType(), False),
    StructField("semester", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("grade", StringType(), True),
    StructField("status", StringType(), True)
])

enrollment_df = spark.read.csv(
    f"{volume_path}/enrollment.csv",
    header=True,
    schema=enrollment_schema
)

enrollment_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.enrollment")
print(f"✓ Created table: {catalog}.{schema}.enrollment")
print(f"  Row count: {enrollment_df.count()}")
display(enrollment_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Course Assignment Table

# COMMAND ----------

course_assignment_schema = StructType([
    StructField("assignment_id", StringType(), False),
    StructField("course_id", StringType(), False),
    StructField("faculty_id", StringType(), False),
    StructField("semester", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("role", StringType(), True)
])

course_assignment_df = spark.read.csv(
    f"{volume_path}/course_assignment.csv",
    header=True,
    schema=course_assignment_schema
)

course_assignment_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.course_assignment")
print(f"✓ Created table: {catalog}.{schema}.course_assignment")
print(f"  Row count: {course_assignment_df.count()}")
display(course_assignment_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Course Prerequisite Table

# COMMAND ----------

course_prerequisite_schema = StructType([
    StructField("course_id", StringType(), False),
    StructField("prerequisite_id", StringType(), False),
    StructField("requirement_type", StringType(), True)
])

course_prerequisite_df = spark.read.csv(
    f"{volume_path}/course_prerequisite.csv",
    header=True,
    schema=course_prerequisite_schema
)

course_prerequisite_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.course_prerequisite")
print(f"✓ Created table: {catalog}.{schema}.course_prerequisite")
print(f"  Row count: {course_prerequisite_df.count()}")
display(course_prerequisite_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create and Load Research Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Research Domain Table

# COMMAND ----------

research_domain_schema = StructType([
    StructField("domain_id", StringType(), False),
    StructField("department_id", StringType(), False),
    StructField("domain_name", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("funding_amount", DecimalType(15, 2), True),
    StructField("funding_source", StringType(), True)
])

research_domain_df = spark.read.csv(
    f"{volume_path}/research_domain.csv",
    header=True,
    schema=research_domain_schema
)

research_domain_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.research_domain")
print(f"✓ Created table: {catalog}.{schema}.research_domain")
print(f"  Row count: {research_domain_df.count()}")
display(research_domain_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Publication Table

# COMMAND ----------

publication_schema = StructType([
    StructField("publication_id", StringType(), False),
    StructField("domain_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("journal", StringType(), True),
    StructField("publication_date", DateType(), True),
    StructField("doi", StringType(), True),
    StructField("citation_count", IntegerType(), True)
])

publication_df = spark.read.csv(
    f"{volume_path}/publication.csv",
    header=True,
    schema=publication_schema
)

publication_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.publication")
print(f"✓ Created table: {catalog}.{schema}.publication")
print(f"  Row count: {publication_df.count()}")
display(publication_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Grant Award Table

# COMMAND ----------

grant_award_schema = StructType([
    StructField("grant_id", StringType(), False),
    StructField("domain_id", StringType(), False),
    StructField("faculty_id", StringType(), False),
    StructField("grant_name", StringType(), True),
    StructField("agency", StringType(), True),
    StructField("amount", DecimalType(15, 2), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True)
])

grant_award_df = spark.read.csv(
    f"{volume_path}/grant_award.csv",
    header=True,
    schema=grant_award_schema
)

grant_award_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.grant_award")
print(f"✓ Created table: {catalog}.{schema}.grant_award")
print(f"  Row count: {grant_award_df.count()}")
display(grant_award_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Domain Collaboration Table

# COMMAND ----------

domain_collaboration_schema = StructType([
    StructField("collab_id", StringType(), False),
    StructField("domain_id", StringType(), False),
    StructField("faculty_id", StringType(), False),
    StructField("role", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("hours_per_week", DecimalType(5, 2), True)
])

domain_collaboration_df = spark.read.csv(
    f"{volume_path}/domain_collaboration.csv",
    header=True,
    schema=domain_collaboration_schema
)

domain_collaboration_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.domain_collaboration")
print(f"✓ Created table: {catalog}.{schema}.domain_collaboration")
print(f"  Row count: {domain_collaboration_df.count()}")
display(domain_collaboration_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create and Load Department Affiliation Table

# COMMAND ----------

department_affiliation_schema = StructType([
    StructField("faculty_id", StringType(), False),
    StructField("department_id", StringType(), False),
    StructField("role", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("is_primary", StringType(), True)
])

department_affiliation_df = spark.read.csv(
    f"{volume_path}/department_affiliation.csv",
    header=True,
    schema=department_affiliation_schema
)

department_affiliation_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.department_affiliation")
print(f"✓ Created table: {catalog}.{schema}.department_affiliation")
print(f"  Row count: {department_affiliation_df.count()}")
display(department_affiliation_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Views for Common Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 vw_student_transcript

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_student_transcript AS
SELECT 
    s.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.major,
    s.gpa,
    s.status as student_status,
    c.course_code,
    c.course_name,
    c.credits,
    e.semester,
    e.year,
    e.grade,
    e.status as enrollment_status
FROM {catalog}.{schema}.student s
JOIN {catalog}.{schema}.enrollment e ON s.student_id = e.student_id
JOIN {catalog}.{schema}.course c ON e.course_id = c.course_id
ORDER BY s.student_id, e.year, e.semester
""")

print(f"✓ Created view: {catalog}.{schema}.vw_student_transcript")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 vw_faculty_profile

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_faculty_profile AS
SELECT 
    f.faculty_id,
    f.first_name || ' ' || f.last_name as faculty_name,
    f.title,
    f.tenure_status,
    f.research_area,
    f.hire_date,
    d.department_name,
    d.building,
    COUNT(DISTINCT ca.assignment_id) as course_assignments,
    COUNT(DISTINCT ga.grant_id) as grant_count,
    COUNT(DISTINCT pc.collab_id) as domain_collaborations
FROM {catalog}.{schema}.faculty f
LEFT JOIN {catalog}.{schema}.department_affiliation da ON f.faculty_id = da.faculty_id AND da.is_primary = 'true'
LEFT JOIN {catalog}.{schema}.department d ON da.department_id = d.department_id
LEFT JOIN {catalog}.{schema}.course_assignment ca ON f.faculty_id = ca.faculty_id
LEFT JOIN {catalog}.{schema}.grant_award ga ON f.faculty_id = ga.faculty_id
LEFT JOIN {catalog}.{schema}.domain_collaboration pc ON f.faculty_id = pc.faculty_id
GROUP BY f.faculty_id, f.first_name, f.last_name, f.title, f.tenure_status,
         f.research_area, f.hire_date, d.department_name, d.building
""")

print(f"✓ Created view: {catalog}.{schema}.vw_faculty_profile")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 vw_research_overview

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_research_overview AS
SELECT 
    rd.domain_id,
    rd.domain_name,
    rd.status as domain_status,
    rd.start_date,
    rd.end_date,
    rd.funding_amount,
    rd.funding_source,
    d.department_name,
    COUNT(DISTINCT p.publication_id) as publication_count,
    SUM(p.citation_count) as total_citations,
    COUNT(DISTINCT ga.grant_id) as grant_count,
    SUM(ga.amount) as total_grant_funding
FROM {catalog}.{schema}.research_domain rd
JOIN {catalog}.{schema}.department d ON rd.department_id = d.department_id
LEFT JOIN {catalog}.{schema}.publication p ON rd.domain_id = p.domain_id
LEFT JOIN {catalog}.{schema}.grant_award ga ON rd.domain_id = ga.domain_id
GROUP BY rd.domain_id, rd.domain_name, rd.status, rd.start_date, rd.end_date,
         rd.funding_amount, rd.funding_source, d.department_name
""")

print(f"✓ Created view: {catalog}.{schema}.vw_research_overview")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Verify Tables and Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 List All Created Tables

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
# MAGIC ### 7.2 Verification Query - Student Transcript

# COMMAND ----------

query1 = f"""
SELECT * FROM {catalog}.{schema}.vw_student_transcript
LIMIT 20
"""

print("Verification: vw_student_transcript")
display(spark.sql(query1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Verification Query - Faculty Profile

# COMMAND ----------

query2 = f"""
SELECT * FROM {catalog}.{schema}.vw_faculty_profile
LIMIT 20
"""

print("Verification: vw_faculty_profile")
display(spark.sql(query2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Verification Query - Research Overview

# COMMAND ----------

query3 = f"""
SELECT * FROM {catalog}.{schema}.vw_research_overview
ORDER BY total_citations DESC
LIMIT 20
"""

print("Verification: vw_research_overview")
display(spark.sql(query3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("✅ UNIVERSITY ACADEMIC RESEARCH DATA LOAD COMPLETE!")
print("=" * 70)
print(f"\n📁 Catalog: {catalog}")
print(f"📁 Schema: {schema}")
print(f"📦 Volume: {volume_path}")
print("\n📊 Tables Created:")
print(f"   • {catalog}.{schema}.student")
print(f"   • {catalog}.{schema}.faculty")
print(f"   • {catalog}.{schema}.department")
print(f"   • {catalog}.{schema}.course")
print(f"   • {catalog}.{schema}.enrollment")
print(f"   • {catalog}.{schema}.course_assignment")
print(f"   • {catalog}.{schema}.course_prerequisite")
print(f"   • {catalog}.{schema}.research_domain")
print(f"   • {catalog}.{schema}.publication")
print(f"   • {catalog}.{schema}.grant_award")
print(f"   • {catalog}.{schema}.domain_collaboration")
print(f"   • {catalog}.{schema}.department_affiliation")
print("\n👁️ Views Created:")
print(f"   • {catalog}.{schema}.vw_student_transcript")
print(f"   • {catalog}.{schema}.vw_faculty_profile")
print(f"   • {catalog}.{schema}.vw_research_overview")
print("\n🔗 Key Relationships:")
print("   • Student → Enrollment → Course (1:N)")
print("   • Faculty → Course Assignment → Course (1:N)")
print("   • Department → Course (1:N)")
print("   • Department → Research Domain (1:N)")
print("   • Research Domain → Publication (1:N)")
print("   • Research Domain → Grant Award (1:N)")
print("   • Faculty → Department Affiliation → Department (N:M)")
print("\n" + "=" * 70)
print("Ready for OntoBricks ontology mapping!")
print("=" * 70)

# COMMAND ----------
