# Healthcare / Clinical Trials Dataset

This dataset simulates clinical trial management for a pharmaceutical research company. It contains 15 tables with approximately 7,300 records covering patients, physicians, trials, enrollments, visits, lab results, adverse events, drugs, prescriptions, diagnoses, and consent.

## Overview

**Industry**: Pharmaceutical / Clinical Research  
**Use Case**: Clinical Trials Management / Patient Safety Monitoring  
**Total Tables**: 15  
**Total Records**: ~7,300

The dataset models the complete clinical trial lifecycle:
- **Trial Setup**: Clinical trials, trial sites, hospitals, physicians
- **Patient Participation**: Enrollments, visits, lab results, consent
- **Safety Monitoring**: Adverse events, drug prescriptions
- **Clinical Care**: Diagnoses, medical procedures, insurance

---

## Data Model Schema

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                  HEALTHCARE / CLINICAL TRIALS - DATA MODEL                           │
└─────────────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────┐     ┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
    │   PATIENT   │     │  PHYSICIAN   │     │ CLINICAL_TRIAL  │     │   HOSPITAL   │
    ├─────────────┤     ├─────────────┤     ├─────────────────┤     ├─────────────┤
    │ patient_id  │     │ physician_id │     │ trial_id (PK)   │     │ hospital_id  │
    │ first_name  │     │ first_name  │     │ trial_name     │     │ hospital_name│
    │ last_name   │     │ last_name   │     │ phase          │     │ city         │
    │ date_of_birth│     │ specialty   │     │ status         │     │ state        │
    │ gender      │     │ license_no  │     │ start_date     │     │ bed_count    │
    │ email       │     │ email       │     │ end_date       │     │ trauma_level │
    │ phone       │     │ hospital_id ─┼────►│ sponsor        │     └──────┬──────┘
    │ blood_type  │     └──────┬──────┘     │ therapeutic_area│            │
    │ insurance_id │            │            │ target_enrollment│            │
    └──────┬──────┘            │            └────────┬────────┘            │
           │                   │                     │                      │
           │                   │                     │    ┌─────────────────┘
           │                   │                     │    │
           │                   │                     ▼    ▼
           │                   │            ┌─────────────────┐
           │                   │            │   TRIAL_SITE    │
           │                   │            ├─────────────────┤
           │                   │            │ site_id (PK)    │
           │                   │            │ trial_id (FK)   │
           │                   │            │ hospital_id(FK) │
           │                   │            │ principal_inv   │
           │                   │            │ status          │
           │                   │            │ enrollment_target│
           │                   │            └────────┬────────┘
           │                   │                     │
           │                   │                     │ enrolls (1:N)
           │                   │                     ▼
           │                   │            ┌─────────────────┐
           │                   └───────────►│   ENROLLMENT    │◄──────────────┐
           │                                ├─────────────────┤               │
           │                                │ enrollment_id   │               │
           │                                │ patient_id (FK)──┼───────────────┤
           │                                │ trial_id (FK)   │               │
           │                                │ site_id (FK)    │               │
           │                                │ enrollment_date │               │
           │                                │ status          │               │
           │                                │ consent_date    │               │
           │                                │ arm             │               │
           │                                └────────┬────────┘               │
           │                                         │                        │
           │                    ┌─────────────────────┼────────────────────┐  │
           │                    │                     │                    │  │
           │                    ▼                     ▼                    ▼  │
           │           ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
           │           │    VISIT    │      │ ADVERSE_EVENT│     │   CONSENT   │
           │           ├─────────────┤      ├─────────────┤     ├─────────────┤
           │           │ visit_id    │      │ event_id     │     │ consent_id   │
           │           │ enrollment_id│     │ enrollment_id│     │ enrollment_id│
           │           │ visit_date  │      │ event_date   │     │ consent_date │
           │           │ visit_type  │      │ description  │     │ consent_type │
           │           │ status      │      │ severity     │     │ version      │
           │           │ notes       │      │ outcome      │     │ signed_by    │
           │           └──────┬──────┘      │ related_to_tx│     │ witness     │
           │                  │            └──────────────┘     └─────────────┘
           │                  │ has_result (1:N)
           │                  ▼
           │           ┌─────────────┐
           │           │ LAB_RESULT  │
           │           ├─────────────┤
           │           │ result_id   │
           │           │ visit_id(FK)│
           │           │ test_name   │
           │           │ test_value  │
           │           │ unit        │
           │           │ normal_range│
           │           │ abnormal_flag│
           │           └─────────────┘
           │
           ├──────────────────────────────────────────────────────────────────┐
           │                                                                  │
           ▼                   ▼                   ▼                         │
    ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐    ┌─────────────┐
    │ PRESCRIPTION│    │  DIAGNOSIS  │    │ MEDICAL_PROCEDURE│    │ INSURANCE   │
    ├─────────────┤    ├─────────────┤    ├─────────────────┤    │   _PLAN     │
    │ prescription_id│ │ diagnosis_id│    │ procedure_id    │    ├─────────────┤
    │ patient_id(FK)─┼─│ patient_id  │    │ patient_id (FK)─┼────│ plan_id     │
    │ physician_id   │ │ physician_id│    │ physician_id    │    │ plan_name   │
    │ drug_id (FK)───┼─│ diagnosis_date│  │ hospital_id     │    │ provider    │
    │ prescribe_date │ │ icd_code    │    │ procedure_date   │    │ plan_type   │
    │ dosage         │ │ description │    │ procedure_code   │    │ coverage    │
    │ frequency      │ │ severity   │    │ cost             │    │ premium     │
    │ duration_days   │ │ is_primary  │    │ outcome          │    └─────────────┘
    └───────┬───────┘ └─────────────┘    └─────────────────┘
            │
            │ drug_id (FK)
            ▼
    ┌─────────────┐
    │    DRUG     │
    ├─────────────┤
    │ drug_id     │
    │ drug_name   │
    │ generic_name│
    │ manufacturer│
    │ drug_class  │
    │ route       │
    └─────────────┘
```

---

## Entity Tables

### 1. Patient (`patient.csv`)

Core patient information including demographics and clinical attributes.

| Column       | Type   | Description                              |
|--------------|--------|------------------------------------------|
| patient_id   | STRING | Primary Key (e.g., PAT00001)             |
| first_name   | STRING | Patient first name                      |
| last_name    | STRING | Patient last name                       |
| date_of_birth| DATE   | Date of birth                           |
| gender       | STRING | M, F, Other                             |
| email        | STRING | Email address                           |
| phone        | STRING | Phone number                            |
| blood_type   | STRING | A+, A-, B+, B-, AB+, AB-, O+, O-        |
| insurance_id | STRING | Foreign Key → Insurance Plan             |

**Rows:** 300

---

### 2. Physician (`physician.csv`)

Physician information and hospital affiliation.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| physician_id  | STRING | Primary Key (e.g., PHY001)              |
| first_name    | STRING | Physician first name                     |
| last_name     | STRING | Physician last name                      |
| specialty     | STRING | Cardiology, Oncology, etc.               |
| license_number| STRING | Medical license number                   |
| email         | STRING | Email address                            |
| hospital_id   | STRING | Foreign Key → Hospital                   |

**Rows:** 50

---

### 3. Hospital (`hospital.csv`)

Hospital and medical center information.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| hospital_id   | STRING | Primary Key (e.g., HSP001)              |
| hospital_name | STRING | Hospital name                            |
| city          | STRING | City                                     |
| state         | STRING | State (2-letter code)                     |
| bed_count     | INT    | Number of beds                           |
| trauma_level  | STRING | I, II, III, IV, V                        |

**Rows:** 15

---

### 4. Clinical Trial (`clinical_trial.csv`)

Clinical trial protocol information.

| Column           | Type   | Description                              |
|------------------|--------|------------------------------------------|
| trial_id         | STRING | Primary Key (e.g., TRL0001)             |
| trial_name       | STRING | Trial name                               |
| phase            | STRING | Phase 1, 2, 3, 4                        |
| status           | STRING | recruiting, active, completed, etc.     |
| start_date       | DATE   | Trial start date                         |
| end_date         | DATE   | Trial end date                           |
| sponsor          | STRING | Sponsor organization                     |
| therapeutic_area | STRING | Oncology, Cardiovascular, etc.          |
| target_enrollment| INT    | Target number of participants            |

**Rows:** 20

---

### 5. Trial Site (`trial_site.csv`)

Trial sites linking trials to hospitals.

| Column                 | Type   | Description                              |
|------------------------|--------|------------------------------------------|
| site_id                | STRING | Primary Key (e.g., SIT001)              |
| trial_id               | STRING | Foreign Key → Clinical Trial             |
| hospital_id            | STRING | Foreign Key → Hospital                   |
| principal_investigator | STRING | PI physician ID                           |
| status                 | STRING | active, recruiting, closed, suspended     |
| enrollment_target      | INT    | Site enrollment target                   |

**Rows:** 40

---

### 6. Enrollment (`enrollment.csv`)

Patient enrollment in clinical trials.

| Column         | Type   | Description                              |
|----------------|--------|------------------------------------------|
| enrollment_id  | STRING | Primary Key (e.g., ENR00001)            |
| patient_id     | STRING | Foreign Key → Patient                    |
| trial_id       | STRING | Foreign Key → Clinical Trial             |
| site_id        | STRING | Foreign Key → Trial Site                 |
| enrollment_date| DATE   | Date of enrollment                       |
| status         | STRING | screened, enrolled, active, completed    |
| consent_date   | DATE   | Date of informed consent                 |
| arm            | STRING | placebo, low_dose, medium_dose, etc.    |

**Rows:** 500

---

### 7. Visit (`visit.csv`)

Clinical trial visits.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| visit_id      | STRING | Primary Key (e.g., VIS000001)           |
| enrollment_id | STRING | Foreign Key → Enrollment                 |
| visit_date    | DATE   | Date of visit                            |
| visit_type    | STRING | screening, baseline, week_4, etc.        |
| status        | STRING | completed, missed, partial, scheduled    |
| notes         | STRING | Visit notes                              |

**Rows:** 1,500

---

### 8. Lab Result (`lab_result.csv`)

Laboratory test results from visits.

| Column             | Type    | Description                              |
|--------------------|---------|------------------------------------------|
| result_id          | STRING  | Primary Key (e.g., LAB000001)            |
| visit_id           | STRING  | Foreign Key → Visit                      |
| test_name          | STRING  | Hemoglobin, Creatinine, etc.              |
| test_value         | DECIMAL | Test result value                        |
| unit               | STRING  | g/dL, mg/dL, etc.                        |
| normal_range_low   | DECIMAL | Lower bound of normal range              |
| normal_range_high  | DECIMAL | Upper bound of normal range              |
| abnormal_flag      | STRING  | true/false                              |

**Rows:** 2,000

---

### 9. Adverse Event (`adverse_event.csv`)

Adverse events reported during trials.

| Column               | Type   | Description                              |
|----------------------|--------|------------------------------------------|
| event_id             | STRING | Primary Key (e.g., AE00001)             |
| enrollment_id        | STRING | Foreign Key → Enrollment                 |
| event_date           | DATE   | Date of event                            |
| description          | STRING | Nausea, Headache, etc.                   |
| severity             | STRING | mild, moderate, severe, life-threatening |
| outcome              | STRING | recovered, recovering, ongoing, etc.     |
| related_to_treatment | STRING | true/false                               |
| reported_by          | STRING | Physician ID                             |

**Rows:** 400

---

### 10. Drug (`drug.csv`)

Drug/medication catalog.

| Column                 | Type   | Description                              |
|------------------------|--------|------------------------------------------|
| drug_id                | STRING | Primary Key (e.g., DRG001)               |
| drug_name              | STRING | Brand/trade name                         |
| generic_name           | STRING | Generic name                             |
| manufacturer           | STRING | Pfizer, Merck, etc.                      |
| drug_class             | STRING | ACE inhibitor, SSRI, etc.                |
| route_of_administration| STRING | oral, IV, subcutaneous, etc.            |

**Rows:** 30

---

### 11. Prescription (`prescription.csv`)

Medication prescriptions.

| Column         | Type   | Description                              |
|----------------|--------|------------------------------------------|
| prescription_id| STRING | Primary Key (e.g., RX000001)            |
| patient_id     | STRING | Foreign Key → Patient                    |
| physician_id   | STRING | Foreign Key → Physician                  |
| drug_id        | STRING | Foreign Key → Drug                       |
| prescribe_date | DATE   | Date prescribed                          |
| dosage         | STRING | e.g., 10 mg                             |
| frequency      | STRING | once daily, twice daily, etc.            |
| duration_days  | INT    | Duration in days                         |
| status         | STRING | active, completed, discontinued          |

**Rows:** 800

---

### 12. Diagnosis (`diagnosis.csv`)

Patient diagnoses with ICD codes.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| diagnosis_id  | STRING | Primary Key (e.g., DIA00001)             |
| patient_id    | STRING | Foreign Key → Patient                    |
| physician_id  | STRING | Foreign Key → Physician                  |
| diagnosis_date| DATE   | Date of diagnosis                        |
| icd_code      | STRING | ICD-10 code (e.g., E11.9)               |
| description   | STRING | Diagnosis description                    |
| severity      | STRING | mild, moderate, severe, critical         |
| is_primary    | STRING | true/false                               |

**Rows:** 600

---

### 13. Insurance Plan (`insurance_plan.csv`)

Insurance plan information.

| Column          | Type    | Description                              |
|-----------------|---------|------------------------------------------|
| plan_id         | STRING  | Primary Key (e.g., INS01)                |
| plan_name       | STRING  | Plan name                                |
| provider        | STRING  | Blue Cross, Aetna, etc.                  |
| plan_type       | STRING  | PPO, HMO, Medicare, etc.                 |
| coverage_level  | STRING  | basic, standard, premium                 |
| monthly_premium | DECIMAL | Monthly premium amount                   |

**Rows:** 10

---

### 14. Medical Procedure (`medical_procedure.csv`)

Medical procedures performed.

| Column         | Type    | Description                              |
|----------------|---------|------------------------------------------|
| procedure_id   | STRING  | Primary Key (e.g., PRC00001)             |
| patient_id     | STRING  | Foreign Key → Patient                    |
| physician_id   | STRING  | Foreign Key → Physician                  |
| hospital_id    | STRING  | Foreign Key → Hospital                   |
| procedure_date | DATE    | Date of procedure                        |
| procedure_code | STRING  | CPT code (e.g., 99213)                   |
| description    | STRING  | Procedure description                    |
| cost           | DECIMAL | Procedure cost                           |
| outcome        | STRING  | successful, complications, cancelled     |

**Rows:** 500

---

### 15. Consent (`consent.csv`)

Informed consent records.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| consent_id    | STRING | Primary Key (e.g., CNS00001)             |
| enrollment_id | STRING | Foreign Key → Enrollment                 |
| consent_date  | DATE   | Date of consent                          |
| consent_type  | STRING | informed_consent, assent, reconsent       |
| version       | STRING | Consent form version                     |
| signed_by     | STRING | Patient ID                               |
| witness       | STRING | Witness identifier                       |

**Rows:** 500

---

## Relationships Summary

| Relationship | Source | Target | Cardinality | Description |
|--------------|--------|--------|-------------|-------------|
| has_enrollment | Patient | Enrollment | 1:N | Patient enrolls in trials |
| has_visit | Enrollment | Visit | 1:N | Enrollment has visits |
| has_lab_result | Visit | Lab Result | 1:N | Visit produces lab results |
| has_adverse_event | Enrollment | Adverse Event | 1:N | Enrollment has adverse events |
| has_consent | Enrollment | Consent | 1:N | Enrollment has consent |
| has_trial_site | Clinical Trial | Trial Site | 1:N | Trial has sites |
| at_hospital | Trial Site | Hospital | N:1 | Site at hospital |
| employs | Hospital | Physician | 1:N | Hospital employs physicians |
| prescribes | Physician | Prescription | 1:N | Physician prescribes |
| takes | Patient | Prescription | 1:N | Patient has prescriptions |
| for_drug | Prescription | Drug | N:1 | Prescription for drug |
| has_diagnosis | Patient | Diagnosis | 1:N | Patient has diagnoses |
| has_procedure | Patient | Medical Procedure | 1:N | Patient has procedures |
| has_insurance | Patient | Insurance Plan | N:1 | Patient has insurance |

---

## Loading the Data

### Prerequisites

1. Upload CSV files to a Unity Catalog Volume
2. Permissions to create schema and tables in the target catalog

### Using the Databricks Notebook

Use the provided notebook `load_data.py` to:

1. Create a schema in Unity Catalog
2. Load all 15 CSV files as tables with proper data types
3. Create useful views for analysis
4. Verify data integrity with test queries

### Quick Start

```python
# 1. Upload CSV files to a Unity Catalog Volume
# 2. Update notebook configuration
catalog = "your_catalog"
schema = "healthcare_trials"
volume_name = "healthcare_data"

# 3. Run the notebook - it will create all tables automatically
```

### Views Created

The loader script creates three analytical views:

| View | Description |
|------|-------------|
| `vw_patient_360` | Complete patient overview with enrollments, prescriptions, diagnoses, adverse events |
| `vw_trial_overview` | Trial enrollment and site summary with visit and AE counts |
| `vw_drug_safety` | Drug prescription and adverse event correlation |

---

## Sample Queries

### Patient 360 View

```sql
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name as patient_name,
    p.blood_type,
    COUNT(DISTINCT e.enrollment_id) as trial_count,
    COUNT(DISTINCT rx.prescription_id) as prescription_count,
    COUNT(DISTINCT ae.event_id) as adverse_event_count
FROM patient p
LEFT JOIN enrollment e ON p.patient_id = e.patient_id
LEFT JOIN prescription rx ON p.patient_id = rx.patient_id
LEFT JOIN adverse_event ae ON e.enrollment_id = ae.enrollment_id
GROUP BY p.patient_id, p.first_name, p.last_name, p.blood_type;
```

### Trial Enrollment by Phase

```sql
SELECT
    ct.phase,
    ct.status,
    COUNT(DISTINCT e.enrollment_id) as enrollment_count,
    COUNT(DISTINCT ts.site_id) as site_count
FROM clinical_trial ct
LEFT JOIN enrollment e ON ct.trial_id = e.trial_id
LEFT JOIN trial_site ts ON ct.trial_id = ts.trial_id
GROUP BY ct.phase, ct.status
ORDER BY ct.phase, ct.status;
```

### Abnormal Lab Results

```sql
SELECT
    p.patient_id,
    lr.test_name,
    lr.test_value,
    lr.unit,
    lr.normal_range_low,
    lr.normal_range_high,
    lr.abnormal_flag
FROM patient p
JOIN enrollment e ON p.patient_id = e.patient_id
JOIN visit v ON e.enrollment_id = v.enrollment_id
JOIN lab_result lr ON v.visit_id = lr.visit_id
WHERE lr.abnormal_flag = 'true'
ORDER BY lr.test_name;
```

### Adverse Events by Severity

```sql
SELECT
    ae.description,
    ae.severity,
    ae.related_to_treatment,
    COUNT(*) as event_count
FROM adverse_event ae
GROUP BY ae.description, ae.severity, ae.related_to_treatment
ORDER BY event_count DESC;
```

---

## Use Cases for OntoBricks

This dataset is ideal for testing and demonstrating:

### Ontology Modeling
- ✅ **Class Mapping**: Map tables to OWL classes (Patient, Physician, ClinicalTrial, Enrollment, etc.)
- ✅ **Object Properties**: Model relationships (hasEnrollment, hasVisit, hasAdverseEvent, etc.)
- ✅ **Data Properties**: Scalar values (icd_code, test_value, severity, etc.)
- ✅ **Hierarchical Concepts**: Therapeutic areas, trial phases, drug classes

### Knowledge Graph Construction
- ✅ **Patient 360**: Build complete patient profiles from trials, prescriptions, diagnoses
- ✅ **Trial Safety**: Connect adverse events to enrollments, drugs, and trials
- ✅ **Cross-Entity Analysis**: Link physicians, hospitals, trials, and patients

### SPARQL Queries
- ✅ **Path Queries**: Find all adverse events for patients on a specific drug
- ✅ **Aggregation**: Calculate enrollment rates, AE counts by severity
- ✅ **Pattern Matching**: Identify trials with high AE rates or abnormal lab patterns

---

## Dataset Statistics

| Table           | Rows  | Type          | Primary Key     |
|-----------------|-------|---------------|-----------------|
| patient         | 300   | Core Entity   | patient_id      |
| physician       | 50    | Core Entity   | physician_id    |
| hospital        | 15    | Core Entity   | hospital_id     |
| clinical_trial  | 20    | Core Entity   | trial_id        |
| trial_site      | 40    | Core Entity   | site_id         |
| enrollment      | 500   | Transaction   | enrollment_id   |
| visit           | 1,500 | Transaction   | visit_id        |
| lab_result      | 2,000 | Transaction   | result_id       |
| adverse_event   | 400   | Transaction   | event_id        |
| drug            | 30    | Reference     | drug_id         |
| prescription    | 800   | Transaction   | prescription_id |
| diagnosis       | 600   | Transaction   | diagnosis_id    |
| insurance_plan  | 10    | Reference     | plan_id         |
| medical_procedure| 500  | Transaction   | procedure_id    |
| consent         | 500   | Transaction   | consent_id      |

**Total:** 15 tables, ~7,300 records

---

## Data Quality Notes

- All IDs use meaningful prefixes (PAT, PHY, HSP, TRL, ENR, VIS, LAB, AE, DRG, RX, DIA, INS, PRC, CNS) for readability
- Date format: YYYY-MM-DD
- No NULL values in primary keys
- Foreign key references are valid and consistent
- Realistic medical data: drug names, ICD-10 codes, lab tests, physician specialties
- Blood types follow standard ABO/Rh classification
- Trial phases and statuses follow clinical trial conventions

---

## Files in This Directory

| File | Description |
|------|-------------|
| `patient.csv` | Patient records |
| `physician.csv` | Physician records |
| `hospital.csv` | Hospital records |
| `clinical_trial.csv` | Clinical trial protocols |
| `trial_site.csv` | Trial site assignments |
| `enrollment.csv` | Patient enrollments |
| `visit.csv` | Trial visits |
| `lab_result.csv` | Laboratory results |
| `adverse_event.csv` | Adverse events |
| `drug.csv` | Drug catalog |
| `prescription.csv` | Prescriptions |
| `diagnosis.csv` | Diagnoses |
| `insurance_plan.csv` | Insurance plans |
| `medical_procedure.csv` | Medical procedures |
| `consent.csv` | Informed consent records |
| `load_data.py` | Databricks loader notebook |
| `generate_data.py` | Data generation script |
| `create_databricks_tables.py` | CLI table creation script |
| `unstructured/business_description.txt` | PharmaCorp business description |
| `README.md` | This documentation |

---

## Regenerating Data

To regenerate the dataset with different parameters:

```bash
cd data/healthcare
python generate_data.py
```

Modify `generate_data.py` to:
- Change record counts
- Adjust date ranges
- Add/remove data fields
- Modify random seed for different data
