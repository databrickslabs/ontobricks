# Energy Provider Customer Journey Dataset

This dataset simulates a complete customer journey for an energy provider company (electricity, gas, dual-fuel). It contains 10 tables with approximately 4,700 records covering all aspects of customer interactions.

## Overview

**Industry**: Energy Provider (Electricity, Gas, Dual-Fuel)  
**Use Case**: Customer 360 View / Customer Journey Analysis  
**Total Tables**: 10  
**Total Records**: ~4,700

The dataset models the complete customer lifecycle:
- **Customer Acquisition**: Customer registration and segmentation
- **Service Delivery**: Contracts, subscriptions, meters, and consumption
- **Billing Cycle**: Invoices and payments
- **Customer Service**: Calls, claims, and general interactions

---

## Data Model Schema

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     ENERGY PROVIDER - CUSTOMER JOURNEY MODEL                    │
└─────────────────────────────────────────────────────────────────────────────────┘


                              ┌─────────────────────┐
                              │      CUSTOMER       │
                              ├─────────────────────┤
                              │ customer_id (PK)    │◄────────────────────────┐
                              │ first_name          │                         │
                              │ last_name           │                         │
                              │ email               │                         │
                              │ phone               │                         │
                              │ street_address      │                         │
                              │ city                │                         │
                              │ postal_code         │                         │
                              │ country             │                         │
                              │ date_of_birth       │                         │
                              │ registration_date   │                         │
                              │ segment             │                         │
                              │ loyalty_points      │                         │
                              │ is_active           │                         │
                              └──────────┬──────────┘                         │
                                         │                                     │
            ┌────────────────────────────┼────────────────────────────┐       │
            │                            │                            │       │
            │ has_contract (1:N)         │ makes_call (1:N)          │       │
            │                            │                            │       │
            ▼                            ▼                            ▼       │
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│      CONTRACT       │      │        CALL         │      │       CLAIM         │
├─────────────────────┤      ├─────────────────────┤      ├─────────────────────┤
│ contract_id (PK)    │      │ call_id (PK)        │      │ claim_id (PK)       │
│ customer_id (FK)────┼──────│ customer_id (FK)────┼──────│ customer_id (FK)────┼──┘
│ energy_type         │      │ call_datetime       │      │ contract_id (FK)────┼──┐
│ start_date          │      │ duration_seconds    │      │ claim_type          │  │
│ end_date            │      │ reason              │      │ description         │  │
│ status              │      │ agent_id            │      │ open_date           │  │
│ monthly_fee         │      │ status              │      │ resolution_date     │  │
│ payment_method      │      │ satisfaction_score  │      │ status              │  │
│ auto_renewal        │      │ notes               │      │ priority            │  │
│ created_at          │      └─────────────────────┘      │ assigned_to         │  │
└──────────┬──────────┘                                   │ compensation_amount │  │
           │                                               └─────────────────────┘  │
           │                  ┌─────────────────────┐                               │
           │                  │    INTERACTION      │                               │
           │                  ├─────────────────────┤                               │
           │                  │ interaction_id (PK) │                               │
           │                  │ customer_id (FK)────┼───────────────────────────────┤
           │                  │ channel             │                               │
           │                  │ interaction_type    │                               │
           │                  │ interaction_datetime│                               │
           │                  │ subject             │                               │
           │                  │ outcome             │                               │
           │                  │ agent_id            │                               │
           │                  │ duration_minutes    │                               │
           │                  │ sentiment           │                               │
           │                  └─────────────────────┘                               │
           │                                                                         │
           │◄────────────────────────────────────────────────────────────────────────┘
           │
           ├────────────────────────────────────────────┐
           │                                            │
           │ has_subscription (1:N)                     │ has_meter (1:N)
           │                                            │
           ▼                                            ▼
┌─────────────────────┐                      ┌─────────────────────┐
│    SUBSCRIPTION     │                      │       METER         │
├─────────────────────┤                      ├─────────────────────┤
│ subscription_id (PK)│                      │ meter_id (PK)       │◄────────────┐
│ contract_id (FK)────┼──────────────────────│ contract_id (FK)    │             │
│ plan_type           │                      │ meter_serial        │             │
│ price_per_kwh       │                      │ meter_type          │             │
│ price_per_m3        │                      │ energy_type         │             │
│ standing_charge     │                      │ installation_date   │             │
│ discount_percentage │                      │ last_inspection_date│             │
│ green_energy        │                      │ location            │             │
│ start_date          │                      │ status              │             │
│ status              │                      └──────────┬──────────┘             │
└─────────────────────┘                                 │                         │
                                                        │ has_reading (1:N)       │
           │                                            │                         │
           │ generates_invoice (1:N)                    ▼                         │
           │                               ┌─────────────────────┐                │
           ▼                               │   METER_READING     │                │
┌─────────────────────┐                    ├─────────────────────┤                │
│      INVOICE        │                    │ reading_id (PK)     │                │
├─────────────────────┤                    │ meter_id (FK)───────┼────────────────┘
│ invoice_id (PK)     │◄───────────────┐   │ reading_date        │
│ contract_id (FK)    │                │   │ reading_value       │
│ issue_date          │                │   │ unit                │
│ due_date            │                │   │ reading_type        │
│ period_start        │                │   │ reported_by         │
│ period_end          │                │   │ validated           │
│ amount_ht           │                │   └─────────────────────┘
│ vat_amount          │                │
│ amount_ttc          │                │
│ status              │                │
│ payment_date        │                │
└──────────┬──────────┘                │
           │                            │
           │ receives_payment (1:N)     │
           │                            │
           ▼                            │
┌─────────────────────┐                │
│      PAYMENT        │                │
├─────────────────────┤                │
│ payment_id (PK)     │                │
│ invoice_id (FK)─────┼────────────────┘
│ payment_date        │
│ amount              │
│ payment_method      │
│ transaction_ref     │
│ status              │
│ processed_at        │
└─────────────────────┘
```

---

## Entity Tables

### 1. Customer (`customer.csv`)

Core customer information including personal details and segmentation.

| Column            | Type    | Description                              |
|-------------------|---------|------------------------------------------|
| customer_id       | STRING  | Primary Key (e.g., CUST00001)            |
| first_name        | STRING  | Customer first name                      |
| last_name         | STRING  | Customer last name                       |
| email             | STRING  | Email address                            |
| phone             | STRING  | Phone number (+33...)                    |
| street_address    | STRING  | Street address                           |
| city              | STRING  | City name                                |
| postal_code       | STRING  | Postal/ZIP code                          |
| country           | STRING  | Country (France)                         |
| date_of_birth     | DATE    | Date of birth                            |
| registration_date | DATE    | Customer registration date               |
| segment           | STRING  | residential, small_business, professional|
| loyalty_points    | INTEGER | Accumulated loyalty points               |
| is_active         | BOOLEAN | Whether customer is active               |

**Rows:** 200

---

### 2. Contract (`contract.csv`)

Energy supply contracts linking customers to energy services.

| Column          | Type    | Description                              |
|-----------------|---------|------------------------------------------|
| contract_id     | STRING  | Primary Key (e.g., CON00001)             |
| customer_id     | STRING  | Foreign Key → Customer                   |
| energy_type     | STRING  | electricity, gas, dual_fuel              |
| start_date      | DATE    | Contract start date                      |
| end_date        | DATE    | Contract end date                        |
| status          | STRING  | active, terminated, suspended, pending   |
| monthly_fee     | DECIMAL | Monthly service fee                      |
| payment_method  | STRING  | direct_debit, credit_card, etc.          |
| auto_renewal    | BOOLEAN | Auto-renewal enabled                     |
| created_at      | DATE    | Contract creation date                   |

**Rows:** 300

---

### 3. Subscription (`subscription.csv`)

Pricing plans and tariff details associated with contracts.

| Column              | Type    | Description                              |
|---------------------|---------|------------------------------------------|
| subscription_id     | STRING  | Primary Key (e.g., SUB00001)             |
| contract_id         | STRING  | Foreign Key → Contract                   |
| plan_type           | STRING  | basic, standard, premium, eco, flex      |
| price_per_kwh       | DECIMAL | Electricity price per kWh                |
| price_per_m3        | DECIMAL | Gas price per m³ (null for electricity)  |
| standing_charge     | DECIMAL | Monthly standing charge                  |
| discount_percentage | DECIMAL | Applied discount percentage              |
| green_energy        | BOOLEAN | Green/renewable energy option            |
| start_date          | DATE    | Subscription start date                  |
| status              | STRING  | active, expired, pending                 |

**Rows:** 350

---

### 4. Meter (`meter.csv`)

Physical meters installed at customer premises.

| Column               | Type    | Description                              |
|----------------------|---------|------------------------------------------|
| meter_id             | STRING  | Primary Key (e.g., MTR000001)            |
| contract_id          | STRING  | Foreign Key → Contract                   |
| meter_serial         | STRING  | Physical serial number                   |
| meter_type           | STRING  | smart_meter, traditional, prepaid, etc.  |
| energy_type          | STRING  | electricity, gas                         |
| installation_date    | DATE    | Meter installation date                  |
| last_inspection_date | DATE    | Last inspection/maintenance date         |
| location             | STRING  | indoor, outdoor, basement, garage        |
| status               | STRING  | active, inactive, faulty, replaced       |

**Rows:** 400

---

## Transaction Tables

### 5. Meter Reading (`meter_reading.csv`)

Energy consumption readings from meters.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| reading_id    | STRING  | Primary Key (e.g., RDG000001)            |
| meter_id      | STRING  | Foreign Key → Meter                      |
| reading_date  | DATE    | Date of reading                          |
| reading_value | DECIMAL | Meter reading value                      |
| unit          | STRING  | kWh (electricity) or m³ (gas)            |
| reading_type  | STRING  | actual, estimated, smart_auto            |
| reported_by   | STRING  | customer, technician, smart_meter        |
| validated     | BOOLEAN | Whether reading is validated             |

**Rows:** 1,000

---

### 6. Invoice (`invoice.csv`)

Billing records for energy consumption.

| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| invoice_id   | STRING  | Primary Key (e.g., INV000001)            |
| contract_id  | STRING  | Foreign Key → Contract                   |
| issue_date   | DATE    | Invoice issue date                       |
| due_date     | DATE    | Payment due date                         |
| period_start | DATE    | Billing period start                     |
| period_end   | DATE    | Billing period end                       |
| amount_ht    | DECIMAL | Amount before tax (HT = Hors Taxe)       |
| vat_amount   | DECIMAL | VAT amount (20%)                         |
| amount_ttc   | DECIMAL | Total amount with tax (TTC)              |
| status       | STRING  | paid, pending, overdue, cancelled        |
| payment_date | DATE    | Actual payment date (if paid)            |

**Rows:** 800

---

### 7. Payment (`payment.csv`)

Payment transactions linked to invoices.

| Column          | Type    | Description                              |
|-----------------|---------|------------------------------------------|
| payment_id      | STRING  | Primary Key (e.g., PAY000001)            |
| invoice_id      | STRING  | Foreign Key → Invoice                    |
| payment_date    | DATE    | Date of payment                          |
| amount          | DECIMAL | Payment amount                           |
| payment_method  | STRING  | direct_debit, credit_card, etc.          |
| transaction_ref | STRING  | Bank transaction reference               |
| status          | STRING  | completed, pending, failed, refunded     |
| processed_at    | DATETIME| Processing timestamp                     |

**Rows:** 700

---

## Interaction Tables

### 8. Call (`call.csv`)

Customer service call records.

| Column             | Type    | Description                              |
|--------------------|---------|------------------------------------------|
| call_id            | STRING  | Primary Key (e.g., CALL00001)            |
| customer_id        | STRING  | Foreign Key → Customer                   |
| call_datetime      | DATETIME| Call date and time                       |
| duration_seconds   | INTEGER | Call duration in seconds                 |
| reason             | STRING  | billing_inquiry, meter_reading, etc.     |
| agent_id           | STRING  | Customer service agent ID                |
| status             | STRING  | completed, callback_required, etc.       |
| satisfaction_score | INTEGER | Customer satisfaction (1-5)              |
| notes              | STRING  | Agent notes                              |

**Rows:** 300

---

### 9. Claim (`claim.csv`)

Customer complaints and dispute records.

| Column              | Type    | Description                              |
|---------------------|---------|------------------------------------------|
| claim_id            | STRING  | Primary Key (e.g., CLM00001)             |
| customer_id         | STRING  | Foreign Key → Customer                   |
| contract_id         | STRING  | Foreign Key → Contract                   |
| claim_type          | STRING  | billing_error, service_interruption, etc.|
| description         | STRING  | Claim description                        |
| open_date           | DATE    | Claim opening date                       |
| resolution_date     | DATE    | Claim resolution date                    |
| status              | STRING  | open, in_progress, resolved, etc.        |
| priority            | STRING  | low, medium, high, urgent                |
| assigned_to         | STRING  | Assigned agent ID                        |
| compensation_amount | DECIMAL | Compensation paid (if any)               |

**Rows:** 150

---

### 10. Interaction (`interaction.csv`)

General customer interaction log across all channels.

| Column               | Type    | Description                              |
|----------------------|---------|------------------------------------------|
| interaction_id       | STRING  | Primary Key (e.g., INT000001)            |
| customer_id          | STRING  | Foreign Key → Customer                   |
| channel              | STRING  | phone, email, web_portal, mobile_app, etc.|
| interaction_type     | STRING  | inquiry, request, update, etc.           |
| interaction_datetime | DATETIME| Interaction timestamp                    |
| subject              | STRING  | Interaction subject                      |
| outcome              | STRING  | resolved, pending_response, etc.         |
| agent_id             | STRING  | Agent ID (if applicable)                 |
| duration_minutes     | INTEGER | Interaction duration                     |
| sentiment            | STRING  | positive, neutral, negative              |

**Rows:** 500

---

## Relationships Summary

| Relationship | Source | Target | Cardinality | Description |
|--------------|--------|--------|-------------|-------------|
| has_contract | Customer | Contract | 1:N | Customer owns contracts |
| has_subscription | Contract | Subscription | 1:N | Contract has pricing plans |
| has_meter | Contract | Meter | 1:N | Contract linked to meters |
| has_reading | Meter | Meter Reading | 1:N | Meter generates readings |
| generates_invoice | Contract | Invoice | 1:N | Contract generates invoices |
| receives_payment | Invoice | Payment | 1:N | Invoice receives payments |
| makes_call | Customer | Call | 1:N | Customer makes service calls |
| files_claim | Customer | Claim | 1:N | Customer files claims |
| has_interaction | Customer | Interaction | 1:N | Customer has interactions |

---

## Generating and Loading Data

The `generate_data.py` script generates synthetic data on the fly and writes it
directly into Databricks Unity Catalog tables.  No intermediate CSV files are
created.

### Prerequisites

1. Python 3.10+
2. `databricks-sql-connector` (already in project dependencies)
3. `pyarrow` (already in project dependencies; only needed for large datasets)
4. A Databricks workspace with a SQL Warehouse and permissions to create schemas/tables

### Quick Start

```bash
# Set credentials via environment variables
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_SQL_WAREHOUSE_ID="abc123def456"

# Generate default dataset (~4,700 rows)
python generate_data.py --catalog main --schema customer_journey
```

### Scaling to Millions of Rows

For large datasets, provide a UC Volume path for Parquet staging (much faster
than SQL INSERT for >50K rows):

```bash
python generate_data.py \
  --catalog main --schema cj_large \
  --customers 1000000 \
  --contracts 1500000 \
  --subscriptions 1750000 \
  --meters 2000000 \
  --meter-readings 5000000 \
  --invoices 4000000 \
  --payments 3500000 \
  --calls 1500000 \
  --claims 750000 \
  --interactions 2500000 \
  --volume /Volumes/main/cj_large/staging \
  --drop-existing
```

### All CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--catalog` | (required) | Unity Catalog catalog name |
| `--schema` | (required) | Unity Catalog schema name |
| `--customers` | 200 | Number of customer rows |
| `--contracts` | 300 | Number of contract rows |
| `--subscriptions` | 350 | Number of subscription rows |
| `--meters` | 400 | Number of meter rows |
| `--meter-readings` | 1000 | Number of meter_reading rows |
| `--invoices` | 800 | Number of invoice rows |
| `--payments` | 700 | Number of payment rows |
| `--calls` | 300 | Number of call rows |
| `--claims` | 150 | Number of claim rows |
| `--interactions` | 500 | Number of interaction rows |
| `--host` | env var | Databricks workspace host |
| `--token` | env var | Databricks PAT token |
| `--warehouse` | env var | SQL Warehouse ID |
| `--seed` | 42 | Random seed for reproducibility |
| `--drop-existing` | off | Drop tables before creating |
| `--volume` | none | UC Volume path for Parquet staging |
| `--skip-views` | off | Skip creation of analytical views |

### Views Created

The script creates three analytical views (unless `--skip-views` is set):

| View | Description |
|------|-------------|
| `vw_customer_360` | Complete customer overview with all metrics |
| `vw_billing_summary` | Invoice and payment status by customer |
| `vw_consumption_analysis` | Energy consumption patterns |

---

## Sample Queries

### Customer 360 View

```sql
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.segment,
    COUNT(DISTINCT ct.contract_id) as contracts,
    COUNT(DISTINCT clm.claim_id) as claims,
    AVG(call.satisfaction_score) as avg_satisfaction
FROM customer c
LEFT JOIN contract ct ON c.customer_id = ct.customer_id
LEFT JOIN claim clm ON c.customer_id = clm.customer_id
LEFT JOIN call ON c.customer_id = call.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.segment;
```

### Monthly Consumption by Energy Type

```sql
SELECT 
    DATE_TRUNC('month', mr.reading_date) as month,
    m.energy_type,
    COUNT(*) as reading_count,
    SUM(mr.reading_value) as total_consumption,
    AVG(mr.reading_value) as avg_consumption
FROM meter_reading mr
JOIN meter m ON mr.meter_id = m.meter_id
WHERE mr.validated = 'true'
GROUP BY DATE_TRUNC('month', mr.reading_date), m.energy_type
ORDER BY month, energy_type;
```

### Outstanding Payments

```sql
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email,
    i.invoice_id,
    i.amount_ttc,
    i.due_date,
    DATEDIFF(CURRENT_DATE, i.due_date) as days_overdue
FROM customer c
JOIN contract ct ON c.customer_id = ct.customer_id
JOIN invoice i ON ct.contract_id = i.contract_id
WHERE i.status = 'overdue'
ORDER BY days_overdue DESC;
```

### Customer Service Analysis

```sql
SELECT 
    call.reason,
    COUNT(*) as call_count,
    AVG(call.duration_seconds) as avg_duration,
    AVG(call.satisfaction_score) as avg_satisfaction
FROM call
WHERE call.status = 'completed'
GROUP BY call.reason
ORDER BY call_count DESC;
```

---

## Use Cases for OntoBricks

This dataset is ideal for testing and demonstrating:

### Ontology Modeling
- ✅ **Class Mapping**: Map tables to OWL classes (Customer, Contract, Meter, etc.)
- ✅ **Object Properties**: Model relationships (hasContract, hasReading, etc.)
- ✅ **Data Properties**: Scalar values (email, amount, reading_value, etc.)
- ✅ **Hierarchical Concepts**: Energy types, plan types, customer segments

### Knowledge Graph Construction
- ✅ **Customer 360**: Build complete customer profiles from multiple sources
- ✅ **Temporal Relationships**: Track customer journey over time
- ✅ **Cross-Entity Analysis**: Connect consumption to billing to service

### SPARQL Queries
- ✅ **Path Queries**: Find all meters for a customer through contracts
- ✅ **Aggregation**: Calculate total consumption or revenue
- ✅ **Pattern Matching**: Identify customers with specific behaviors

---

## Dataset Statistics (defaults)

| Table           | Default Rows | Type          | Primary Key     |
|-----------------|-------------|---------------|-----------------|
| customer        | 200         | Core Entity   | customer_id     |
| contract        | 300         | Core Entity   | contract_id     |
| subscription    | 350         | Core Entity   | subscription_id |
| meter           | 400         | Core Entity   | meter_id        |
| meter_reading   | 1,000       | Transaction   | reading_id      |
| invoice         | 800         | Transaction   | invoice_id      |
| payment         | 700         | Transaction   | payment_id      |
| call            | 300         | Interaction   | call_id         |
| claim           | 150         | Interaction   | claim_id        |
| interaction     | 500         | Interaction   | interaction_id  |

**Default total:** 10 tables, ~4,700 records (all configurable via CLI options)

---

## Data Quality Notes

- All IDs use meaningful prefixes (CUST, CON, MTR, etc.) with 7-digit padding for readability
- Date format: YYYY-MM-DD
- DateTime format: YYYY-MM-DD HH:MM:SS
- No NULL values in primary keys
- Foreign key references are valid and consistent
- Realistic French addresses and phone numbers
- Energy prices based on typical French market rates
- Customer segments: residential (majority), small_business, professional
- Names use combinatorial generation (200+ first x 200+ last names) with numeric suffixes for uniqueness beyond 40K customers
- Same seed always produces the same dataset (reproducible via `--seed`)

---

## Files in This Directory

| File | Description |
|------|-------------|
| `generate_data.py` | CLI script: generates synthetic data and loads it into Databricks tables |
| `generate.sh` | Wrapper script with preset size profiles (`default`, `medium`, `large`, `xlarge`, `custom`) |
| `load_customer_data.py` | Databricks notebook (alternative: loads from CSV files in a Volume) |
| `README.md` | This documentation |

---

## Shell Wrapper (`generate.sh`)

The `generate.sh` script wraps `generate_data.py` with preset size profiles.
Credentials are read from environment variables or the project root `.env` file.

```bash
./generate.sh                        # default ~4,700 rows
./generate.sh medium                 # ~23,500 rows
./generate.sh large                  # ~235,000 rows (Parquet staging auto-enabled)
./generate.sh xlarge                 # ~2,350,000 rows (Parquet staging auto-enabled)
./generate.sh custom 5000 7500       # custom: 5000 customers, 7500 contracts, rest scaled
```

| Profile | Customers | Total Rows | Parquet Staging |
|---------|-----------|------------|-----------------|
| `default` | 200 | ~4,700 | No |
| `medium` | 1,000 | ~23,500 | No |
| `large` | 10,000 | ~235,000 | Yes (auto) |
| `xlarge` | 100,000 | ~2,350,000 | Yes (auto) |
| `custom` | User-defined | Scaled | When total >= 50K |

Override defaults via environment variables: `CATALOG`, `SCHEMA`, `VOLUME`, `SEED`.

---

## Regenerating Data

To regenerate the dataset with different parameters:

```bash
cd data/customer

# Default (~4,700 rows)
python generate_data.py --catalog main --schema customer_journey

# Custom counts and fresh tables
python generate_data.py --catalog main --schema cj_test \
    --customers 10000 --contracts 15000 --drop-existing

# Different random data with a new seed
python generate_data.py --catalog main --schema customer_journey --seed 123
```

The script generates data in memory and loads it directly -- no CSV files are
created.  For datasets exceeding 50,000 total rows, use `--volume` to enable
Parquet staging for faster loading.
