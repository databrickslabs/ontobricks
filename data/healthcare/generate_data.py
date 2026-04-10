#!/usr/bin/env python3
"""
Generate Healthcare / Clinical Trials Dataset for Pharmaceutical Research
Creates 15 tables with realistic data for clinical trial management.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(42)

# Output directory
OUTPUT_DIR = Path(__file__).parent

# Helper functions
def random_date(start_year, end_year):
    """Generate a random date between start_year and end_year."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

def random_datetime(start_year, end_year):
    """Generate a random datetime."""
    date = random_date(start_year, end_year)
    hour = random.randint(6, 22)
    minute = random.randint(0, 59)
    return f"{date} {hour:02d}:{minute:02d}:00"

# Data pools - Medical/Clinical
FIRST_NAMES = [
    "James", "Emma", "Michael", "Olivia", "William", "Ava", "Alexander", "Sophia",
    "Benjamin", "Isabella", "Lucas", "Mia", "Henry", "Charlotte", "Daniel", "Amelia",
    "Matthew", "Harper", "Joseph", "Evelyn", "David", "Abigail", "Samuel", "Emily",
    "Christopher", "Elizabeth", "Andrew", "Sofia", "Joshua", "Avery", "Ethan", "Ella",
    "Nathan", "Scarlett", "Ryan", "Grace", "Jacob", "Chloe", "Nicholas", "Victoria",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
]

BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
GENDERS = ["M", "F", "Other"]

PHYSICIAN_SPECIALTIES = [
    "Cardiology", "Oncology", "Neurology", "Rheumatology", "Endocrinology",
    "Pulmonology", "Gastroenterology", "Nephrology", "Hematology", "Infectious Disease",
    "Dermatology", "Psychiatry", "Internal Medicine", "Family Medicine", "Pediatrics",
]

HOSPITAL_CITIES = [
    ("Boston", "MA"), ("New York", "NY"), ("Philadelphia", "PA"), ("Baltimore", "MD"),
    ("Chicago", "IL"), ("Houston", "TX"), ("Los Angeles", "CA"), ("San Francisco", "CA"),
    ("Seattle", "WA"), ("Denver", "CO"), ("Atlanta", "GA"), ("Miami", "FL"),
    ("Cleveland", "OH"), ("Rochester", "MN"), ("Durham", "NC"),
]

TRIAL_PHASES = ["Phase 1", "Phase 2", "Phase 3", "Phase 4"]
TRIAL_STATUS = ["recruiting", "active", "completed", "suspended", "terminated"]
THERAPEUTIC_AREAS = [
    "Oncology", "Cardiovascular", "Neurology", "Metabolic", "Immunology",
    "Infectious Disease", "Respiratory", "Rheumatology", "Psychiatry", "Hematology",
]

SPONSORS = [
    "PharmaCorp Research", "BioMed Solutions", "Nova Therapeutics", "Global Pharma Inc",
    "MedResearch Corp", "Clinical Partners LLC", "Life Sciences Institute",
]

DRUG_NAMES = [
    ("Lisinopril", "lisinopril", "ACE inhibitor"),
    ("Metformin", "metformin", "Biguanide"),
    ("Atorvastatin", "atorvastatin", "Statin"),
    ("Amlodipine", "amlodipine", "Calcium channel blocker"),
    ("Omeprazole", "omeprazole", "PPI"),
    ("Losartan", "losartan", "ARB"),
    ("Gabapentin", "gabapentin", "Anticonvulsant"),
    ("Sertraline", "sertraline", "SSRI"),
    ("Levothyroxine", "levothyroxine", "Thyroid hormone"),
    ("Prednisone", "prednisone", "Corticosteroid"),
    ("Ibuprofen", "ibuprofen", "NSAID"),
    ("Warfarin", "warfarin", "Anticoagulant"),
    ("Albuterol", "albuterol", "Bronchodilator"),
    ("Insulin Glargine", "insulin glargine", "Long-acting insulin"),
    ("Pembrolizumab", "pembrolizumab", "PD-1 inhibitor"),
    ("Methotrexate", "methotrexate", "DMARD"),
    ("Hydrochlorothiazide", "hydrochlorothiazide", "Diuretic"),
    ("Escitalopram", "escitalopram", "SSRI"),
    ("Montelukast", "montelukast", "Leukotriene modifier"),
    ("Tramadol", "tramadol", "Opioid analgesic"),
    ("Duloxetine", "duloxetine", "SNRI"),
    ("Clopidogrel", "clopidogrel", "Antiplatelet"),
    ("Pantoprazole", "pantoprazole", "PPI"),
    ("Metoprolol", "metoprolol", "Beta blocker"),
    ("Furosemide", "furosemide", "Loop diuretic"),
    ("Simvastatin", "simvastatin", "Statin"),
    ("Fluoxetine", "fluoxetine", "SSRI"),
    ("Amitriptyline", "amitriptyline", "TCA"),
    ("Ciprofloxacin", "ciprofloxacin", "Fluoroquinolone"),
    ("Azithromycin", "azithromycin", "Macrolide"),
]

ROUTES = ["oral", "IV", "subcutaneous", "topical", "inhaled", "intramuscular"]
MANUFACTURERS = ["Pfizer", "Merck", "Johnson & Johnson", "Novartis", "Roche", "AstraZeneca", "Sanofi", "Bristol-Myers Squibb", "Eli Lilly", "AbbVie"]

ICD_CODES = [
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("I10", "Essential (primary) hypertension"),
    ("J44.9", "Chronic obstructive pulmonary disease, unspecified"),
    ("G47.33", "Obstructive sleep apnea"),
    ("E66.9", "Obesity, unspecified"),
    ("F32.9", "Major depressive disorder, single episode, unspecified"),
    ("M17.11", "Unilateral primary osteoarthritis, right knee"),
    ("K21.9", "Gastro-esophageal reflux disease without esophagitis"),
    ("E78.00", "Pure hypercholesterolemia, unspecified"),
    ("J06.9", "Acute upper respiratory infection, unspecified"),
    ("M54.5", "Low back pain"),
    ("G43.909", "Migraine, unspecified, not intractable"),
    ("R50.9", "Fever, unspecified"),
    ("J18.9", "Pneumonia, unspecified organism"),
    ("N18.9", "Chronic kidney disease, unspecified"),
    ("C34.90", "Malignant neoplasm of unspecified part of unspecified bronchus or lung"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery without angina pectoris"),
    ("F41.1", "Generalized anxiety disorder"),
    ("M79.3", "Panniculitis, unspecified"),
    ("E03.9", "Hypothyroidism, unspecified"),
]

LAB_TESTS = [
    ("Hemoglobin", "g/dL", 12.0, 17.0),
    ("White Blood Cell Count", "10^9/L", 4.5, 11.0),
    ("Platelet Count", "10^9/L", 150, 400),
    ("Creatinine", "mg/dL", 0.7, 1.3),
    ("Glucose (Fasting)", "mg/dL", 70, 100),
    ("HbA1c", "%", 4.0, 5.6),
    ("ALT", "U/L", 7, 56),
    ("AST", "U/L", 10, 40),
    ("Total Cholesterol", "mg/dL", 0, 200),
    ("LDL Cholesterol", "mg/dL", 0, 100),
    ("HDL Cholesterol", "mg/dL", 40, 60),
    ("Triglycerides", "mg/dL", 0, 150),
    ("TSH", "mIU/L", 0.4, 4.0),
    ("Potassium", "mEq/L", 3.5, 5.0),
    ("Sodium", "mEq/L", 136, 145),
    ("eGFR", "mL/min/1.73m2", 90, 120),
]

ADVERSE_EVENT_DESCRIPTIONS = [
    "Nausea", "Headache", "Fatigue", "Dizziness", "Rash",
    "Diarrhea", "Constipation", "Insomnia", "Dry mouth", "Increased appetite",
    "Weight gain", "Edema", "Hypertension", "Hypotension", "Tachycardia",
    "Cough", "Dyspnea", "Upper respiratory infection", "Back pain", "Arthralgia",
    "Myalgia", "Peripheral neuropathy", "Vision blurred", "Tinnitus", "Hepatotoxicity",
]

AE_SEVERITY = ["mild", "moderate", "severe", "life-threatening"]
AE_OUTCOME = ["recovered", "recovering", "ongoing", "sequelae", "fatal"]
VISIT_TYPES = ["screening", "baseline", "week_4", "week_8", "week_12", "month_6", "end_of_study", "unscheduled", "follow_up"]
VISIT_STATUS = ["completed", "missed", "partial", "scheduled"]
ENROLLMENT_STATUS = ["screened", "enrolled", "active", "completed", "withdrawn", "lost_to_followup"]
TRIAL_ARMS = ["placebo", "low_dose", "medium_dose", "high_dose", "active_control"]
CONSENT_TYPES = ["informed_consent", "assent", "reconsent", "withdrawal"]
INSURANCE_TYPES = ["PPO", "HMO", "EPO", "Medicare", "Medicaid", "Commercial"]
COVERAGE_LEVELS = ["basic", "standard", "premium", "catastrophic"]
PROCEDURE_CODES = ["99213", "99214", "99215", "36415", "80053", "85025", "71046", "93000", "80048", "84443"]

# Generate patient data
def generate_patients(count=300):
    patients = []
    for i in range(1, count + 1):
        patient = {
            "patient_id": f"PAT{i:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "date_of_birth": random_date(1940, 2005),
            "gender": random.choice(GENDERS),
            "email": f"patient{i:05d}@email.com",
            "phone": f"+1{random.randint(2000000000, 9999999999)}",
            "blood_type": random.choice(BLOOD_TYPES),
            "insurance_id": f"INS{random.randint(1, 10):02d}" if random.random() > 0.1 else None
        }
        patients.append(patient)
    return patients

# Generate hospital data
def generate_hospitals(count=15):
    hospitals = []
    used = set()
    for i in range(1, count + 1):
        city, state = random.choice(HOSPITAL_CITIES)
        name = f"{city} Medical Center" if (city, state) not in used else f"{city} Research Hospital"
        used.add((city, state))
        hospitals.append({
            "hospital_id": f"HSP{i:03d}",
            "hospital_name": name,
            "city": city,
            "state": state,
            "bed_count": random.randint(200, 1200),
            "trauma_level": random.choice(["I", "II", "III", "IV", "V"])
        })
    return hospitals

# Generate physician data
def generate_physicians(hospitals, count=50):
    physicians = []
    for i in range(1, count + 1):
        physicians.append({
            "physician_id": f"PHY{i:03d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "specialty": random.choice(PHYSICIAN_SPECIALTIES),
            "license_number": f"MD{random.randint(100000, 999999)}",
            "email": f"physician{i:03d}@hospital.org",
            "hospital_id": random.choice(hospitals)["hospital_id"]
        })
    return physicians

# Generate clinical trial data
def generate_clinical_trials(count=20):
    trials = []
    for i in range(1, count + 1):
        start = random_date(2020, 2024)
        duration_months = random.choice([12, 18, 24, 36])
        end_dt = datetime.strptime(start, "%Y-%m-%d") + timedelta(days=duration_months * 30)
        trials.append({
            "trial_id": f"TRL{i:04d}",
            "trial_name": f"{random.choice(THERAPEUTIC_AREAS)} Study {i}",
            "phase": random.choice(TRIAL_PHASES),
            "status": random.choice(TRIAL_STATUS),
            "start_date": start,
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "sponsor": random.choice(SPONSORS),
            "therapeutic_area": random.choice(THERAPEUTIC_AREAS),
            "target_enrollment": random.choice([100, 200, 300, 500, 1000])
        })
    return trials

# Generate trial site data
def generate_trial_sites(trials, hospitals, count=40):
    sites = []
    physicians = [f"PHY{i:03d}" for i in range(1, 51)]
    for i in range(1, count + 1):
        trial = random.choice(trials)
        hospital = random.choice(hospitals)
        sites.append({
            "site_id": f"SIT{i:03d}",
            "trial_id": trial["trial_id"],
            "hospital_id": hospital["hospital_id"],
            "principal_investigator": random.choice(physicians),
            "status": random.choice(["active", "recruiting", "closed", "suspended"]),
            "enrollment_target": random.randint(10, 50)
        })
    return sites

# Generate enrollment data
def generate_enrollments(patients, trials, trial_sites, count=500):
    enrollments = []
    for i in range(1, count + 1):
        patient = random.choice(patients)
        trial = random.choice(trials)
        sites_for_trial = [s for s in trial_sites if s["trial_id"] == trial["trial_id"]]
        site = random.choice(sites_for_trial) if sites_for_trial else random.choice(trial_sites)
        consent_dt = random_date(2021, 2024)
        enroll_dt = (datetime.strptime(consent_dt, "%Y-%m-%d") + timedelta(days=random.randint(0, 14))).strftime("%Y-%m-%d")
        enrollments.append({
            "enrollment_id": f"ENR{i:05d}",
            "patient_id": patient["patient_id"],
            "trial_id": trial["trial_id"],
            "site_id": site["site_id"],
            "enrollment_date": enroll_dt,
            "status": random.choice(ENROLLMENT_STATUS),
            "consent_date": consent_dt,
            "arm": random.choice(TRIAL_ARMS)
        })
    return enrollments

# Generate visit data
def generate_visits(enrollments, count=1500):
    visits = []
    for i in range(1, count + 1):
        enrollment = random.choice(enrollments)
        visit_date = random_date(2021, 2025)
        visits.append({
            "visit_id": f"VIS{i:06d}",
            "enrollment_id": enrollment["enrollment_id"],
            "visit_date": visit_date,
            "visit_type": random.choice(VISIT_TYPES),
            "status": random.choice(VISIT_STATUS),
            "notes": f"Visit notes for {random.choice(VISIT_TYPES)}" if random.random() > 0.5 else None
        })
    return visits

# Generate lab result data
def generate_lab_results(visits, count=2000):
    results = []
    for i in range(1, count + 1):
        visit = random.choice(visits)
        test_name, unit, low, high = random.choice(LAB_TESTS)
        base_val = random.uniform(low, high)
        variation = random.uniform(-0.2, 0.2) * (high - low)
        test_value = round(base_val + variation, 2)
        abnormal = "true" if test_value < low or test_value > high else "false"
        results.append({
            "result_id": f"LAB{i:06d}",
            "visit_id": visit["visit_id"],
            "test_name": test_name,
            "test_value": test_value,
            "unit": unit,
            "normal_range_low": low,
            "normal_range_high": high,
            "abnormal_flag": abnormal
        })
    return results

# Generate adverse event data
def generate_adverse_events(enrollments, physicians, count=400):
    events = []
    for i in range(1, count + 1):
        enrollment = random.choice(enrollments)
        events.append({
            "event_id": f"AE{i:05d}",
            "enrollment_id": enrollment["enrollment_id"],
            "event_date": random_date(2021, 2025),
            "description": random.choice(ADVERSE_EVENT_DESCRIPTIONS),
            "severity": random.choice(AE_SEVERITY),
            "outcome": random.choice(AE_OUTCOME),
            "related_to_treatment": random.choice(["true", "true", "false"]),
            "reported_by": random.choice(physicians)["physician_id"]
        })
    return events

# Generate drug data
def generate_drugs(count=30):
    drugs = []
    used = set()
    for i in range(1, count + 1):
        drug_name, generic_name, drug_class = random.choice(DRUG_NAMES)
        key = (drug_name, generic_name)
        if key in used:
            drug_name = f"{drug_name} {random.choice(['XR', 'ER', 'SR'])}"
        used.add(key)
        drugs.append({
            "drug_id": f"DRG{i:03d}",
            "drug_name": drug_name,
            "generic_name": generic_name,
            "manufacturer": random.choice(MANUFACTURERS),
            "drug_class": drug_class,
            "route_of_administration": random.choice(ROUTES)
        })
    return drugs

# Generate prescription data
def generate_prescriptions(patients, physicians, drugs, count=800):
    prescriptions = []
    for i in range(1, count + 1):
        patient = random.choice(patients)
        physician = random.choice(physicians)
        drug = random.choice(drugs)
        prescribe_date = random_date(2020, 2025)
        prescriptions.append({
            "prescription_id": f"RX{i:06d}",
            "patient_id": patient["patient_id"],
            "physician_id": physician["physician_id"],
            "drug_id": drug["drug_id"],
            "prescribe_date": prescribe_date,
            "dosage": f"{random.choice([5, 10, 20, 50, 100, 500])} mg",
            "frequency": random.choice(["once daily", "twice daily", "three times daily", "as needed", "weekly"]),
            "duration_days": random.choice([7, 14, 30, 60, 90]),
            "status": random.choice(["active", "active", "completed", "discontinued", "cancelled"])
        })
    return prescriptions

# Generate diagnosis data
def generate_diagnoses(patients, physicians, count=600):
    diagnoses = []
    for i in range(1, count + 1):
        patient = random.choice(patients)
        physician = random.choice(physicians)
        icd_code, description = random.choice(ICD_CODES)
        diagnoses.append({
            "diagnosis_id": f"DIA{i:05d}",
            "patient_id": patient["patient_id"],
            "physician_id": physician["physician_id"],
            "diagnosis_date": random_date(2018, 2025),
            "icd_code": icd_code,
            "description": description,
            "severity": random.choice(["mild", "moderate", "severe", "critical"]),
            "is_primary": random.choice(["true", "false"])
        })
    return diagnoses

# Generate insurance plan data
def generate_insurance_plans(count=10):
    plans = []
    providers = ["Blue Cross", "Aetna", "UnitedHealthcare", "Cigna", "Humana", "Kaiser", "Anthem", "Molina", "Centene", "WellCare"]
    for i in range(1, count + 1):
        plans.append({
            "plan_id": f"INS{i:02d}",
            "plan_name": f"{providers[i-1]} Plan {i}",
            "provider": providers[i-1],
            "plan_type": random.choice(INSURANCE_TYPES),
            "coverage_level": random.choice(COVERAGE_LEVELS),
            "monthly_premium": round(random.uniform(200, 800), 2)
        })
    return plans

# Generate medical procedure data
def generate_medical_procedures(patients, physicians, hospitals, count=500):
    procedures = []
    for i in range(1, count + 1):
        patient = random.choice(patients)
        physician = random.choice(physicians)
        hospital = random.choice(hospitals)
        proc_date = random_date(2019, 2025)
        procedures.append({
            "procedure_id": f"PRC{i:05d}",
            "patient_id": patient["patient_id"],
            "physician_id": physician["physician_id"],
            "hospital_id": hospital["hospital_id"],
            "procedure_date": proc_date,
            "procedure_code": random.choice(PROCEDURE_CODES),
            "description": f"Medical procedure - {random.choice(['evaluation', 'lab panel', 'imaging', 'EKG', 'consultation'])}",
            "cost": round(random.uniform(50, 5000), 2),
            "outcome": random.choice(["successful", "successful", "successful", "complications", "cancelled"])
        })
    return procedures

# Generate consent data
def generate_consents(enrollments, count=500):
    consents = []
    for i in range(1, count + 1):
        enrollment = random.choice(enrollments)
        consent_date = enrollment["consent_date"]
        consents.append({
            "consent_id": f"CNS{i:05d}",
            "enrollment_id": enrollment["enrollment_id"],
            "consent_date": consent_date,
            "consent_type": random.choice(CONSENT_TYPES),
            "version": f"1.{random.randint(0, 3)}",
            "signed_by": enrollment["patient_id"],
            "witness": f"WIT{random.randint(1, 20):03d}"
        })
    return consents

# Write CSV file
def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
    print(f"✓ Created {filename} ({len(data)} records)")

# Main execution
def main():
    print("=" * 60)
    print("Generating Healthcare / Clinical Trials Dataset")
    print("=" * 60)
    print()

    print("Generating data...")
    patients = generate_patients(300)
    hospitals = generate_hospitals(15)
    physicians = generate_physicians(hospitals, 50)
    trials = generate_clinical_trials(20)
    trial_sites = generate_trial_sites(trials, hospitals, 40)
    enrollments = generate_enrollments(patients, trials, trial_sites, 500)
    visits = generate_visits(enrollments, 1500)
    lab_results = generate_lab_results(visits, 2000)
    adverse_events = generate_adverse_events(enrollments, physicians, 400)
    drugs = generate_drugs(30)
    prescriptions = generate_prescriptions(patients, physicians, drugs, 800)
    diagnoses = generate_diagnoses(patients, physicians, 600)
    insurance_plans = generate_insurance_plans(10)
    medical_procedures = generate_medical_procedures(patients, physicians, hospitals, 500)
    consents = generate_consents(enrollments, 500)

    print()
    print("Writing CSV files...")

    write_csv("patient.csv", patients, [
        "patient_id", "first_name", "last_name", "date_of_birth", "gender",
        "email", "phone", "blood_type", "insurance_id"
    ])
    write_csv("hospital.csv", hospitals, [
        "hospital_id", "hospital_name", "city", "state", "bed_count", "trauma_level"
    ])
    write_csv("physician.csv", physicians, [
        "physician_id", "first_name", "last_name", "specialty", "license_number",
        "email", "hospital_id"
    ])
    write_csv("clinical_trial.csv", trials, [
        "trial_id", "trial_name", "phase", "status", "start_date", "end_date",
        "sponsor", "therapeutic_area", "target_enrollment"
    ])
    write_csv("trial_site.csv", trial_sites, [
        "site_id", "trial_id", "hospital_id", "principal_investigator", "status", "enrollment_target"
    ])
    write_csv("enrollment.csv", enrollments, [
        "enrollment_id", "patient_id", "trial_id", "site_id", "enrollment_date",
        "status", "consent_date", "arm"
    ])
    write_csv("visit.csv", visits, [
        "visit_id", "enrollment_id", "visit_date", "visit_type", "status", "notes"
    ])
    write_csv("lab_result.csv", lab_results, [
        "result_id", "visit_id", "test_name", "test_value", "unit",
        "normal_range_low", "normal_range_high", "abnormal_flag"
    ])
    write_csv("adverse_event.csv", adverse_events, [
        "event_id", "enrollment_id", "event_date", "description", "severity",
        "outcome", "related_to_treatment", "reported_by"
    ])
    write_csv("drug.csv", drugs, [
        "drug_id", "drug_name", "generic_name", "manufacturer", "drug_class", "route_of_administration"
    ])
    write_csv("prescription.csv", prescriptions, [
        "prescription_id", "patient_id", "physician_id", "drug_id", "prescribe_date",
        "dosage", "frequency", "duration_days", "status"
    ])
    write_csv("diagnosis.csv", diagnoses, [
        "diagnosis_id", "patient_id", "physician_id", "diagnosis_date", "icd_code",
        "description", "severity", "is_primary"
    ])
    write_csv("insurance_plan.csv", insurance_plans, [
        "plan_id", "plan_name", "provider", "plan_type", "coverage_level", "monthly_premium"
    ])
    write_csv("medical_procedure.csv", medical_procedures, [
        "procedure_id", "patient_id", "physician_id", "hospital_id", "procedure_date",
        "procedure_code", "description", "cost", "outcome"
    ])
    write_csv("consent.csv", consents, [
        "consent_id", "enrollment_id", "consent_date", "consent_type", "version", "signed_by", "witness"
    ])

    print()
    print("=" * 60)
    print("✅ Dataset generation complete!")
    print("=" * 60)

    all_data = [patients, hospitals, physicians, trials, trial_sites, enrollments,
                visits, lab_results, adverse_events, drugs, prescriptions, diagnoses,
                insurance_plans, medical_procedures, consents]
    total_records = sum(len(d) for d in all_data)

    print(f"\n📊 Summary:")
    print(f"   • patient.csv:           {len(patients):5d} records")
    print(f"   • physician.csv:         {len(physicians):5d} records")
    print(f"   • hospital.csv:         {len(hospitals):5d} records")
    print(f"   • clinical_trial.csv:    {len(trials):5d} records")
    print(f"   • trial_site.csv:        {len(trial_sites):5d} records")
    print(f"   • enrollment.csv:        {len(enrollments):5d} records")
    print(f"   • visit.csv:             {len(visits):5d} records")
    print(f"   • lab_result.csv:        {len(lab_results):5d} records")
    print(f"   • adverse_event.csv:     {len(adverse_events):5d} records")
    print(f"   • drug.csv:              {len(drugs):5d} records")
    print(f"   • prescription.csv:      {len(prescriptions):5d} records")
    print(f"   • diagnosis.csv:         {len(diagnoses):5d} records")
    print(f"   • insurance_plan.csv:    {len(insurance_plans):5d} records")
    print(f"   • medical_procedure.csv: {len(medical_procedures):5d} records")
    print(f"   • consent.csv:           {len(consents):5d} records")
    print(f"   ─────────────────────────────────")
    print(f"   Total:                  {total_records:5d} records")

if __name__ == "__main__":
    main()
