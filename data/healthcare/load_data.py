# Databricks notebook source
# MAGIC %md
# MAGIC # Load Healthcare / Clinical Trials Dataset into Unity Catalog
# MAGIC
# MAGIC This notebook creates and populates tables in Unity Catalog from CSV files stored in a Volume.
# MAGIC
# MAGIC ## Dataset Structure
# MAGIC - **Core Entity Tables**: Patient, Physician, Hospital, Clinical Trial, Trial Site, Drug, Insurance Plan
# MAGIC - **Transaction Tables**: Enrollment, Visit, Lab Result, Adverse Event, Prescription, Diagnosis, Medical Procedure, Consent
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
schema = "healthcare_trials"
volume_name = "healthcare_data"
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
# MAGIC ### 2.1 Patient Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, BooleanType, IntegerType

patient_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("blood_type", StringType(), True),
    StructField("insurance_id", StringType(), True)
])

patient_df = spark.read.csv(
    f"{volume_path}/patient.csv",
    header=True,
    schema=patient_schema
)

patient_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.patient")
print(f"✓ Created table: {catalog}.{schema}.patient")
print(f"  Row count: {patient_df.count()}")
display(patient_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Physician Table

# COMMAND ----------

physician_schema = StructType([
    StructField("physician_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("specialty", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("email", StringType(), True),
    StructField("hospital_id", StringType(), True)
])

physician_df = spark.read.csv(
    f"{volume_path}/physician.csv",
    header=True,
    schema=physician_schema
)

physician_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.physician")
print(f"✓ Created table: {catalog}.{schema}.physician")
print(f"  Row count: {physician_df.count()}")
display(physician_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Hospital Table

# COMMAND ----------

hospital_schema = StructType([
    StructField("hospital_id", StringType(), False),
    StructField("hospital_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("bed_count", IntegerType(), True),
    StructField("trauma_level", StringType(), True)
])

hospital_df = spark.read.csv(
    f"{volume_path}/hospital.csv",
    header=True,
    schema=hospital_schema
)

hospital_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.hospital")
print(f"✓ Created table: {catalog}.{schema}.hospital")
print(f"  Row count: {hospital_df.count()}")
display(hospital_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Clinical Trial Table

# COMMAND ----------

clinical_trial_schema = StructType([
    StructField("trial_id", StringType(), False),
    StructField("trial_name", StringType(), True),
    StructField("phase", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("sponsor", StringType(), True),
    StructField("therapeutic_area", StringType(), True),
    StructField("target_enrollment", IntegerType(), True)
])

clinical_trial_df = spark.read.csv(
    f"{volume_path}/clinical_trial.csv",
    header=True,
    schema=clinical_trial_schema
)

clinical_trial_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.clinical_trial")
print(f"✓ Created table: {catalog}.{schema}.clinical_trial")
print(f"  Row count: {clinical_trial_df.count()}")
display(clinical_trial_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Trial Site Table

# COMMAND ----------

trial_site_schema = StructType([
    StructField("site_id", StringType(), False),
    StructField("trial_id", StringType(), False),
    StructField("hospital_id", StringType(), False),
    StructField("principal_investigator", StringType(), True),
    StructField("status", StringType(), True),
    StructField("enrollment_target", IntegerType(), True)
])

trial_site_df = spark.read.csv(
    f"{volume_path}/trial_site.csv",
    header=True,
    schema=trial_site_schema
)

trial_site_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.trial_site")
print(f"✓ Created table: {catalog}.{schema}.trial_site")
print(f"  Row count: {trial_site_df.count()}")
display(trial_site_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Drug Table

# COMMAND ----------

drug_schema = StructType([
    StructField("drug_id", StringType(), False),
    StructField("drug_name", StringType(), True),
    StructField("generic_name", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("drug_class", StringType(), True),
    StructField("route_of_administration", StringType(), True)
])

drug_df = spark.read.csv(
    f"{volume_path}/drug.csv",
    header=True,
    schema=drug_schema
)

drug_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.drug")
print(f"✓ Created table: {catalog}.{schema}.drug")
print(f"  Row count: {drug_df.count()}")
display(drug_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 Insurance Plan Table

# COMMAND ----------

insurance_plan_schema = StructType([
    StructField("plan_id", StringType(), False),
    StructField("plan_name", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("coverage_level", StringType(), True),
    StructField("monthly_premium", DecimalType(10, 2), True)
])

insurance_plan_df = spark.read.csv(
    f"{volume_path}/insurance_plan.csv",
    header=True,
    schema=insurance_plan_schema
)

insurance_plan_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.insurance_plan")
print(f"✓ Created table: {catalog}.{schema}.insurance_plan")
print(f"  Row count: {insurance_plan_df.count()}")
display(insurance_plan_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create and Load Transaction Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Enrollment Table

# COMMAND ----------

enrollment_schema = StructType([
    StructField("enrollment_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("trial_id", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("enrollment_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("consent_date", DateType(), True),
    StructField("arm", StringType(), True)
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
# MAGIC ### 3.2 Visit Table

# COMMAND ----------

visit_schema = StructType([
    StructField("visit_id", StringType(), False),
    StructField("enrollment_id", StringType(), False),
    StructField("visit_date", DateType(), True),
    StructField("visit_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("notes", StringType(), True)
])

visit_df = spark.read.csv(
    f"{volume_path}/visit.csv",
    header=True,
    schema=visit_schema
)

visit_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.visit")
print(f"✓ Created table: {catalog}.{schema}.visit")
print(f"  Row count: {visit_df.count()}")
display(visit_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Lab Result Table

# COMMAND ----------

lab_result_schema = StructType([
    StructField("result_id", StringType(), False),
    StructField("visit_id", StringType(), False),
    StructField("test_name", StringType(), True),
    StructField("test_value", DecimalType(15, 2), True),
    StructField("unit", StringType(), True),
    StructField("normal_range_low", DecimalType(10, 2), True),
    StructField("normal_range_high", DecimalType(10, 2), True),
    StructField("abnormal_flag", StringType(), True)
])

lab_result_df = spark.read.csv(
    f"{volume_path}/lab_result.csv",
    header=True,
    schema=lab_result_schema
)

lab_result_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.lab_result")
print(f"✓ Created table: {catalog}.{schema}.lab_result")
print(f"  Row count: {lab_result_df.count()}")
display(lab_result_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Adverse Event Table

# COMMAND ----------

adverse_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("enrollment_id", StringType(), False),
    StructField("event_date", DateType(), True),
    StructField("description", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("related_to_treatment", StringType(), True),
    StructField("reported_by", StringType(), True)
])

adverse_event_df = spark.read.csv(
    f"{volume_path}/adverse_event.csv",
    header=True,
    schema=adverse_event_schema
)

adverse_event_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.adverse_event")
print(f"✓ Created table: {catalog}.{schema}.adverse_event")
print(f"  Row count: {adverse_event_df.count()}")
display(adverse_event_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Prescription Table

# COMMAND ----------

prescription_schema = StructType([
    StructField("prescription_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("physician_id", StringType(), False),
    StructField("drug_id", StringType(), False),
    StructField("prescribe_date", DateType(), True),
    StructField("dosage", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("duration_days", IntegerType(), True),
    StructField("status", StringType(), True)
])

prescription_df = spark.read.csv(
    f"{volume_path}/prescription.csv",
    header=True,
    schema=prescription_schema
)

prescription_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.prescription")
print(f"✓ Created table: {catalog}.{schema}.prescription")
print(f"  Row count: {prescription_df.count()}")
display(prescription_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Diagnosis Table

# COMMAND ----------

diagnosis_schema = StructType([
    StructField("diagnosis_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("physician_id", StringType(), False),
    StructField("diagnosis_date", DateType(), True),
    StructField("icd_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("is_primary", StringType(), True)
])

diagnosis_df = spark.read.csv(
    f"{volume_path}/diagnosis.csv",
    header=True,
    schema=diagnosis_schema
)

diagnosis_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.diagnosis")
print(f"✓ Created table: {catalog}.{schema}.diagnosis")
print(f"  Row count: {diagnosis_df.count()}")
display(diagnosis_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 Medical Procedure Table

# COMMAND ----------

medical_procedure_schema = StructType([
    StructField("procedure_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("physician_id", StringType(), False),
    StructField("hospital_id", StringType(), False),
    StructField("procedure_date", DateType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("cost", DecimalType(15, 2), True),
    StructField("outcome", StringType(), True)
])

medical_procedure_df = spark.read.csv(
    f"{volume_path}/medical_procedure.csv",
    header=True,
    schema=medical_procedure_schema
)

medical_procedure_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.medical_procedure")
print(f"✓ Created table: {catalog}.{schema}.medical_procedure")
print(f"  Row count: {medical_procedure_df.count()}")
display(medical_procedure_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 Consent Table

# COMMAND ----------

consent_schema = StructType([
    StructField("consent_id", StringType(), False),
    StructField("enrollment_id", StringType(), False),
    StructField("consent_date", DateType(), True),
    StructField("consent_type", StringType(), True),
    StructField("version", StringType(), True),
    StructField("signed_by", StringType(), True),
    StructField("witness", StringType(), True)
])

consent_df = spark.read.csv(
    f"{volume_path}/consent.csv",
    header=True,
    schema=consent_schema
)

consent_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.consent")
print(f"✓ Created table: {catalog}.{schema}.consent")
print(f"  Row count: {consent_df.count()}")
display(consent_df.limit(5))

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
# MAGIC #### Query 1: Patient trial enrollments with trial details

# COMMAND ----------

query1 = f"""
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name as patient_name,
    ct.trial_name,
    ct.phase,
    e.enrollment_date,
    e.status as enrollment_status,
    e.arm
FROM {catalog}.{schema}.patient p
JOIN {catalog}.{schema}.enrollment e ON p.patient_id = e.patient_id
JOIN {catalog}.{schema}.clinical_trial ct ON e.trial_id = ct.trial_id
WHERE e.status = 'active'
ORDER BY p.last_name
LIMIT 20
"""

print("Query 1: Active Enrollments by Patient")
display(spark.sql(query1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 2: Adverse events by trial

# COMMAND ----------

query2 = f"""
SELECT
    ct.trial_id,
    ct.trial_name,
    ae.description as adverse_event,
    ae.severity,
    ae.related_to_treatment,
    COUNT(*) as event_count
FROM {catalog}.{schema}.clinical_trial ct
JOIN {catalog}.{schema}.enrollment e ON ct.trial_id = e.trial_id
JOIN {catalog}.{schema}.adverse_event ae ON e.enrollment_id = ae.enrollment_id
GROUP BY ct.trial_id, ct.trial_name, ae.description, ae.severity, ae.related_to_treatment
ORDER BY event_count DESC
LIMIT 20
"""

print("Query 2: Adverse Events by Trial")
display(spark.sql(query2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 3: Lab results with abnormal flags

# COMMAND ----------

query3 = f"""
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name as patient_name,
    lr.test_name,
    lr.test_value,
    lr.unit,
    lr.normal_range_low,
    lr.normal_range_high,
    lr.abnormal_flag
FROM {catalog}.{schema}.patient p
JOIN {catalog}.{schema}.enrollment e ON p.patient_id = e.patient_id
JOIN {catalog}.{schema}.visit v ON e.enrollment_id = v.enrollment_id
JOIN {catalog}.{schema}.lab_result lr ON v.visit_id = lr.visit_id
WHERE lr.abnormal_flag = 'true'
ORDER BY lr.test_name
LIMIT 20
"""

print("Query 3: Abnormal Lab Results")
display(spark.sql(query3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query 4: Drug prescriptions by physician specialty

# COMMAND ----------

query4 = f"""
SELECT
    ph.specialty,
    d.drug_name,
    d.drug_class,
    COUNT(*) as prescription_count
FROM {catalog}.{schema}.physician ph
JOIN {catalog}.{schema}.prescription rx ON ph.physician_id = rx.physician_id
JOIN {catalog}.{schema}.drug d ON rx.drug_id = d.drug_id
GROUP BY ph.specialty, d.drug_name, d.drug_class
ORDER BY prescription_count DESC
LIMIT 20
"""

print("Query 4: Prescriptions by Physician Specialty")
display(spark.sql(query4))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Views for Common Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 vw_patient_360 - Complete patient overview

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_patient_360 AS
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.email,
    p.phone,
    p.blood_type,
    p.insurance_id,
    ip.plan_name,
    ip.provider as insurance_provider,
    COUNT(DISTINCT e.enrollment_id) as trial_enrollment_count,
    COUNT(DISTINCT rx.prescription_id) as prescription_count,
    COUNT(DISTINCT d.diagnosis_id) as diagnosis_count,
    COUNT(DISTINCT ae.event_id) as adverse_event_count
FROM {catalog}.{schema}.patient p
LEFT JOIN {catalog}.{schema}.insurance_plan ip ON p.insurance_id = ip.plan_id
LEFT JOIN {catalog}.{schema}.enrollment e ON p.patient_id = e.patient_id
LEFT JOIN {catalog}.{schema}.prescription rx ON p.patient_id = rx.patient_id
LEFT JOIN {catalog}.{schema}.diagnosis d ON p.patient_id = d.patient_id
LEFT JOIN {catalog}.{schema}.enrollment e2 ON p.patient_id = e2.patient_id
LEFT JOIN {catalog}.{schema}.adverse_event ae ON e2.enrollment_id = ae.enrollment_id
GROUP BY p.patient_id, p.first_name, p.last_name, p.date_of_birth, p.gender,
         p.email, p.phone, p.blood_type, p.insurance_id, ip.plan_name, ip.provider
""")

print(f"✓ Created view: {catalog}.{schema}.vw_patient_360")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 vw_trial_overview - Trial enrollment and site summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_trial_overview AS
SELECT
    ct.trial_id,
    ct.trial_name,
    ct.phase,
    ct.status as trial_status,
    ct.therapeutic_area,
    ct.sponsor,
    ct.target_enrollment,
    ct.start_date,
    ct.end_date,
    COUNT(DISTINCT ts.site_id) as site_count,
    COUNT(DISTINCT e.enrollment_id) as actual_enrollment,
    COUNT(DISTINCT v.visit_id) as total_visits,
    COUNT(DISTINCT ae.event_id) as adverse_event_count
FROM {catalog}.{schema}.clinical_trial ct
LEFT JOIN {catalog}.{schema}.trial_site ts ON ct.trial_id = ts.trial_id
LEFT JOIN {catalog}.{schema}.enrollment e ON ct.trial_id = e.trial_id
LEFT JOIN {catalog}.{schema}.visit v ON e.enrollment_id = v.enrollment_id
LEFT JOIN {catalog}.{schema}.adverse_event ae ON e.enrollment_id = ae.enrollment_id
GROUP BY ct.trial_id, ct.trial_name, ct.phase, ct.status, ct.therapeutic_area,
         ct.sponsor, ct.target_enrollment, ct.start_date, ct.end_date
""")

print(f"✓ Created view: {catalog}.{schema}.vw_trial_overview")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 vw_drug_safety - Drug and adverse event correlation

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_drug_safety AS
SELECT
    d.drug_id,
    d.drug_name,
    d.generic_name,
    d.drug_class,
    d.manufacturer,
    d.route_of_administration,
    COUNT(DISTINCT rx.prescription_id) as prescription_count,
    COUNT(DISTINCT ae.event_id) as adverse_event_count,
    COUNT(DISTINCT CASE WHEN ae.related_to_treatment = 'true' THEN ae.event_id END) as treatment_related_ae_count
FROM {catalog}.{schema}.drug d
LEFT JOIN {catalog}.{schema}.prescription rx ON d.drug_id = rx.drug_id
LEFT JOIN {catalog}.{schema}.enrollment e ON rx.patient_id = e.patient_id
LEFT JOIN {catalog}.{schema}.adverse_event ae ON e.enrollment_id = ae.enrollment_id
GROUP BY d.drug_id, d.drug_name, d.generic_name, d.drug_class, d.manufacturer, d.route_of_administration
""")

print(f"✓ Created view: {catalog}.{schema}.vw_drug_safety")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("✅ HEALTHCARE CLINICAL TRIALS DATA LOAD COMPLETE!")
print("=" * 70)
print(f"\n📁 Catalog: {catalog}")
print(f"📁 Schema: {schema}")
print(f"📦 Volume: {volume_path}")
print("\n📊 Tables Created (Core Entity):")
print(f"   • {catalog}.{schema}.patient")
print(f"   • {catalog}.{schema}.physician")
print(f"   • {catalog}.{schema}.hospital")
print(f"   • {catalog}.{schema}.clinical_trial")
print(f"   • {catalog}.{schema}.trial_site")
print(f"   • {catalog}.{schema}.drug")
print(f"   • {catalog}.{schema}.insurance_plan")
print("\n📊 Tables Created (Transaction):")
print(f"   • {catalog}.{schema}.enrollment")
print(f"   • {catalog}.{schema}.visit")
print(f"   • {catalog}.{schema}.lab_result")
print(f"   • {catalog}.{schema}.adverse_event")
print(f"   • {catalog}.{schema}.prescription")
print(f"   • {catalog}.{schema}.diagnosis")
print(f"   • {catalog}.{schema}.medical_procedure")
print(f"   • {catalog}.{schema}.consent")
print("\n👁️ Views Created:")
print(f"   • {catalog}.{schema}.vw_patient_360")
print(f"   • {catalog}.{schema}.vw_trial_overview")
print(f"   • {catalog}.{schema}.vw_drug_safety")
print("\n🔗 Key Relationships:")
print("   • Patient → Enrollment (1:N)")
print("   • Clinical Trial → Trial Site (1:N)")
print("   • Enrollment → Visit (1:N)")
print("   • Visit → Lab Result (1:N)")
print("   • Enrollment → Adverse Event (1:N)")
print("   • Patient → Prescription (1:N)")
print("   • Patient → Diagnosis (1:N)")
print("\n" + "=" * 70)
print("Ready for OntoBricks ontology mapping!")
print("=" * 70)

# COMMAND ----------
