#!/usr/bin/env python3
"""
Generate Customer Journey Dataset for Energy Provider
Creates 10 tables with realistic data for an energy company (electricity, gas).
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

# Data pools
FIRST_NAMES = [
    "James", "Emma", "Michael", "Olivia", "William", "Ava", "Alexander", "Sophia",
    "Benjamin", "Isabella", "Lucas", "Mia", "Henry", "Charlotte", "Daniel", "Amelia",
    "Matthew", "Harper", "Joseph", "Evelyn", "David", "Abigail", "Samuel", "Emily",
    "Christopher", "Elizabeth", "Andrew", "Sofia", "Joshua", "Avery", "Ethan", "Ella",
    "Nathan", "Scarlett", "Ryan", "Grace", "Jacob", "Chloe", "Nicholas", "Victoria",
    "Thomas", "Riley", "Charles", "Aria", "George", "Lily", "Robert", "Aurora",
    "John", "Zoey", "Edward", "Penelope", "Oliver", "Layla", "Sebastian", "Nora",
    "Noah", "Camila", "Liam", "Hannah", "Logan", "Addison", "Mason", "Brooklyn",
    "Aiden", "Eleanor", "Owen", "Stella", "Luke", "Natalie", "Jack", "Leah",
    "Gabriel", "Savannah", "Elijah", "Audrey", "Isaac", "Claire", "Caleb", "Lucy"
]

LAST_NAMES = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand",
    "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David",
    "Bertrand", "Roux", "Vincent", "Fournier", "Morel", "Girard", "Andre", "Lefevre",
    "Mercier", "Dupont", "Lambert", "Bonnet", "Francois", "Martinez", "Legrand", "Garnier",
    "Faure", "Rousseau", "Blanc", "Guerin", "Muller", "Henry", "Roussel", "Nicolas",
    "Perrin", "Morin", "Mathieu", "Clement", "Gauthier", "Dumont", "Lopez", "Fontaine",
    "Chevalier", "Robin", "Masson", "Sanchez", "Gerard", "Nguyen", "Boyer", "Denis",
    "Lemaire", "Duval", "Joly", "Gautier", "Roger", "Roche", "Roy", "Noel"
]

CITIES = [
    ("Paris", "75001"), ("Lyon", "69001"), ("Marseille", "13001"), ("Toulouse", "31000"),
    ("Nice", "06000"), ("Nantes", "44000"), ("Strasbourg", "67000"), ("Montpellier", "34000"),
    ("Bordeaux", "33000"), ("Lille", "59000"), ("Rennes", "35000"), ("Reims", "51100"),
    ("Le Havre", "76600"), ("Saint-Etienne", "42000"), ("Toulon", "83000"), ("Grenoble", "38000"),
    ("Dijon", "21000"), ("Angers", "49000"), ("Nimes", "30000"), ("Aix-en-Provence", "13100"),
    ("Brest", "29200"), ("Clermont-Ferrand", "63000"), ("Villeurbanne", "69100"), ("Tours", "37000"),
    ("Amiens", "80000"), ("Limoges", "87000"), ("Metz", "57000"), ("Perpignan", "66000")
]

STREET_TYPES = ["Rue", "Avenue", "Boulevard", "Place", "Allée", "Chemin", "Impasse"]
STREET_NAMES = [
    "de la République", "Victor Hugo", "Jean Jaurès", "des Fleurs", "du Général Leclerc",
    "de la Liberté", "Pasteur", "du Commerce", "de la Gare", "des Lilas", "Voltaire",
    "de Paris", "Emile Zola", "du Moulin", "des Roses", "de Verdun", "Charles de Gaulle",
    "du Château", "des Alpes", "du Port", "Saint-Michel", "de Lyon", "du Marché"
]

ENERGY_TYPES = ["electricity", "gas", "dual_fuel"]
PLAN_TYPES = ["basic", "standard", "premium", "eco", "flex"]
CONTRACT_STATUS = ["active", "terminated", "suspended", "pending"]
PAYMENT_METHODS = ["direct_debit", "credit_card", "bank_transfer", "check"]
CUSTOMER_SEGMENTS = ["residential", "small_business", "professional"]
CALL_REASONS = [
    "billing_inquiry", "meter_reading", "contract_change", "technical_issue",
    "payment_question", "move_in", "move_out", "tariff_info", "complaint", "general_info"
]
CALL_STATUS = ["completed", "callback_required", "transferred", "resolved"]
CLAIM_TYPES = [
    "billing_error", "estimated_reading", "service_interruption", "meter_malfunction",
    "contract_dispute", "price_complaint", "delay_issue", "quality_issue"
]
CLAIM_STATUS = ["open", "in_progress", "resolved", "closed", "escalated"]
INTERACTION_CHANNELS = ["phone", "email", "web_portal", "mobile_app", "chat", "in_person"]
INTERACTION_TYPES = [
    "inquiry", "request", "update", "notification", "feedback", "service_activation"
]

# Generate customer data
def generate_customers(count=200):
    """Generate customer records."""
    customers = []
    for i in range(1, count + 1):
        city, postal_code = random.choice(CITIES)
        street_num = random.randint(1, 150)
        street = f"{random.choice(STREET_TYPES)} {random.choice(STREET_NAMES)}"
        
        customer = {
            "customer_id": f"CUST{i:05d}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": f"customer{i:05d}@email.fr",
            "phone": f"+33{random.randint(600000000, 799999999)}",
            "street_address": f"{street_num} {street}",
            "city": city,
            "postal_code": postal_code,
            "country": "France",
            "date_of_birth": random_date(1950, 2000),
            "registration_date": random_date(2015, 2024),
            "segment": random.choice(CUSTOMER_SEGMENTS),
            "loyalty_points": random.randint(0, 5000),
            "is_active": random.choice(["true", "true", "true", "false"])  # 75% active
        }
        customers.append(customer)
    return customers

# Generate contract data
def generate_contracts(customers, count=300):
    """Generate contract records."""
    contracts = []
    for i in range(1, count + 1):
        customer = random.choice(customers)
        start_date = random_date(2018, 2024)
        duration_months = random.choice([12, 24, 36])
        end_date = (datetime.strptime(start_date, "%Y-%m-%d") + 
                   timedelta(days=duration_months * 30)).strftime("%Y-%m-%d")
        
        contract = {
            "contract_id": f"CON{i:05d}",
            "customer_id": customer["customer_id"],
            "energy_type": random.choice(ENERGY_TYPES),
            "start_date": start_date,
            "end_date": end_date,
            "status": random.choice(CONTRACT_STATUS),
            "monthly_fee": round(random.uniform(5.00, 25.00), 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "auto_renewal": random.choice(["true", "false"]),
            "created_at": start_date
        }
        contracts.append(contract)
    return contracts

# Generate subscription data
def generate_subscriptions(contracts, count=350):
    """Generate subscription/plan records."""
    subscriptions = []
    for i in range(1, count + 1):
        contract = random.choice(contracts)
        
        subscription = {
            "subscription_id": f"SUB{i:05d}",
            "contract_id": contract["contract_id"],
            "plan_type": random.choice(PLAN_TYPES),
            "price_per_kwh": round(random.uniform(0.15, 0.35), 4),
            "price_per_m3": round(random.uniform(0.08, 0.18), 4) if contract["energy_type"] in ["gas", "dual_fuel"] else None,
            "standing_charge": round(random.uniform(8.00, 25.00), 2),
            "discount_percentage": round(random.uniform(0, 15), 1),
            "green_energy": random.choice(["true", "false"]),
            "start_date": contract["start_date"],
            "status": random.choice(["active", "expired", "pending"])
        }
        subscriptions.append(subscription)
    return subscriptions

# Generate meter data
def generate_meters(contracts, count=400):
    """Generate meter records."""
    meters = []
    meter_types = {
        "electricity": ["smart_meter", "traditional", "prepaid"],
        "gas": ["smart_meter", "diaphragm", "rotary"],
        "dual_fuel": ["smart_meter", "traditional", "prepaid"]
    }
    
    for i in range(1, count + 1):
        contract = random.choice(contracts)
        energy = contract["energy_type"]
        if energy == "dual_fuel":
            energy = random.choice(["electricity", "gas"])
        
        meter = {
            "meter_id": f"MTR{i:06d}",
            "contract_id": contract["contract_id"],
            "meter_serial": f"SN{random.randint(10000000, 99999999)}",
            "meter_type": random.choice(meter_types.get(energy, ["smart_meter"])),
            "energy_type": energy,
            "installation_date": random_date(2015, 2024),
            "last_inspection_date": random_date(2022, 2025),
            "location": random.choice(["indoor", "outdoor", "basement", "garage"]),
            "status": random.choice(["active", "inactive", "faulty", "replaced"])
        }
        meters.append(meter)
    return meters

# Generate meter readings
def generate_meter_readings(meters, count=1000):
    """Generate meter reading records."""
    readings = []
    for i in range(1, count + 1):
        meter = random.choice(meters)
        
        if meter["energy_type"] == "electricity":
            reading_value = round(random.uniform(100, 15000), 2)
            unit = "kWh"
        else:
            reading_value = round(random.uniform(50, 5000), 2)
            unit = "m3"
        
        reading = {
            "reading_id": f"RDG{i:06d}",
            "meter_id": meter["meter_id"],
            "reading_date": random_date(2022, 2025),
            "reading_value": reading_value,
            "unit": unit,
            "reading_type": random.choice(["actual", "estimated", "smart_auto"]),
            "reported_by": random.choice(["customer", "technician", "smart_meter"]),
            "validated": random.choice(["true", "true", "false"])
        }
        readings.append(reading)
    return readings

# Generate invoice data
def generate_invoices(contracts, count=800):
    """Generate invoice records."""
    invoices = []
    for i in range(1, count + 1):
        contract = random.choice(contracts)
        issue_date = random_date(2022, 2025)
        due_date = (datetime.strptime(issue_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
        
        amount_ht = round(random.uniform(30, 500), 2)
        vat = round(amount_ht * 0.20, 2)
        amount_ttc = round(amount_ht + vat, 2)
        
        invoice = {
            "invoice_id": f"INV{i:06d}",
            "contract_id": contract["contract_id"],
            "issue_date": issue_date,
            "due_date": due_date,
            "period_start": (datetime.strptime(issue_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d"),
            "period_end": issue_date,
            "amount_ht": amount_ht,
            "vat_amount": vat,
            "amount_ttc": amount_ttc,
            "status": random.choice(["paid", "paid", "paid", "pending", "overdue", "cancelled"]),
            "payment_date": due_date if random.random() > 0.2 else None
        }
        invoices.append(invoice)
    return invoices

# Generate payment data
def generate_payments(invoices, count=700):
    """Generate payment records."""
    payments = []
    paid_invoices = [inv for inv in invoices if inv["status"] == "paid"]
    
    for i in range(1, count + 1):
        invoice = random.choice(paid_invoices) if paid_invoices else random.choice(invoices)
        
        payment = {
            "payment_id": f"PAY{i:06d}",
            "invoice_id": invoice["invoice_id"],
            "payment_date": invoice["due_date"],
            "amount": invoice["amount_ttc"],
            "payment_method": random.choice(PAYMENT_METHODS),
            "transaction_ref": f"TRX{random.randint(100000000, 999999999)}",
            "status": random.choice(["completed", "completed", "completed", "pending", "failed", "refunded"]),
            "processed_at": random_datetime(2022, 2025)
        }
        payments.append(payment)
    return payments

# Generate call data
def generate_calls(customers, count=300):
    """Generate customer service call records."""
    calls = []
    for i in range(1, count + 1):
        customer = random.choice(customers)
        call_time = random_datetime(2022, 2025)
        duration = random.randint(60, 1800)  # 1 to 30 minutes
        
        call = {
            "call_id": f"CALL{i:05d}",
            "customer_id": customer["customer_id"],
            "call_datetime": call_time,
            "duration_seconds": duration,
            "reason": random.choice(CALL_REASONS),
            "agent_id": f"AGT{random.randint(100, 150)}",
            "status": random.choice(CALL_STATUS),
            "satisfaction_score": random.randint(1, 5) if random.random() > 0.3 else None,
            "notes": f"Customer called regarding {random.choice(CALL_REASONS).replace('_', ' ')}"
        }
        calls.append(call)
    return calls

# Generate claim data
def generate_claims(customers, contracts, count=150):
    """Generate customer claim records."""
    claims = []
    for i in range(1, count + 1):
        customer = random.choice(customers)
        contract = random.choice([c for c in contracts if c["customer_id"] == customer["customer_id"]] or contracts)
        
        open_date = random_date(2022, 2025)
        resolution_days = random.randint(1, 45)
        
        claim = {
            "claim_id": f"CLM{i:05d}",
            "customer_id": customer["customer_id"],
            "contract_id": contract["contract_id"],
            "claim_type": random.choice(CLAIM_TYPES),
            "description": f"Customer claim regarding {random.choice(CLAIM_TYPES).replace('_', ' ')}",
            "open_date": open_date,
            "resolution_date": (datetime.strptime(open_date, "%Y-%m-%d") + timedelta(days=resolution_days)).strftime("%Y-%m-%d") if random.random() > 0.3 else None,
            "status": random.choice(CLAIM_STATUS),
            "priority": random.choice(["low", "medium", "high", "urgent"]),
            "assigned_to": f"AGT{random.randint(100, 150)}",
            "compensation_amount": round(random.uniform(0, 200), 2) if random.random() > 0.7 else 0
        }
        claims.append(claim)
    return claims

# Generate interaction data
def generate_interactions(customers, count=500):
    """Generate customer interaction records."""
    interactions = []
    for i in range(1, count + 1):
        customer = random.choice(customers)
        
        interaction = {
            "interaction_id": f"INT{i:06d}",
            "customer_id": customer["customer_id"],
            "channel": random.choice(INTERACTION_CHANNELS),
            "interaction_type": random.choice(INTERACTION_TYPES),
            "interaction_datetime": random_datetime(2022, 2025),
            "subject": f"{random.choice(INTERACTION_TYPES).title()} via {random.choice(INTERACTION_CHANNELS)}",
            "outcome": random.choice(["resolved", "pending_response", "follow_up_needed", "escalated", "completed"]),
            "agent_id": f"AGT{random.randint(100, 150)}" if random.random() > 0.4 else None,
            "duration_minutes": random.randint(1, 30) if random.random() > 0.3 else None,
            "sentiment": random.choice(["positive", "neutral", "negative"]) if random.random() > 0.5 else None
        }
        interactions.append(interaction)
    return interactions

# Write CSV file
def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Created {filename} ({len(data)} records)")

# Main execution
def main():
    print("=" * 60)
    print("Generating Energy Provider Customer Journey Dataset")
    print("=" * 60)
    print()
    
    # Generate all data
    print("Generating data...")
    customers = generate_customers(200)
    contracts = generate_contracts(customers, 300)
    subscriptions = generate_subscriptions(contracts, 350)
    meters = generate_meters(contracts, 400)
    meter_readings = generate_meter_readings(meters, 1000)
    invoices = generate_invoices(contracts, 800)
    payments = generate_payments(invoices, 700)
    calls = generate_calls(customers, 300)
    claims = generate_claims(customers, contracts, 150)
    interactions = generate_interactions(customers, 500)
    
    print()
    print("Writing CSV files...")
    
    # Write all CSV files
    write_csv("customer.csv", customers, [
        "customer_id", "first_name", "last_name", "email", "phone",
        "street_address", "city", "postal_code", "country",
        "date_of_birth", "registration_date", "segment", "loyalty_points", "is_active"
    ])
    
    write_csv("contract.csv", contracts, [
        "contract_id", "customer_id", "energy_type", "start_date", "end_date",
        "status", "monthly_fee", "payment_method", "auto_renewal", "created_at"
    ])
    
    write_csv("subscription.csv", subscriptions, [
        "subscription_id", "contract_id", "plan_type", "price_per_kwh", "price_per_m3",
        "standing_charge", "discount_percentage", "green_energy", "start_date", "status"
    ])
    
    write_csv("meter.csv", meters, [
        "meter_id", "contract_id", "meter_serial", "meter_type", "energy_type",
        "installation_date", "last_inspection_date", "location", "status"
    ])
    
    write_csv("meter_reading.csv", meter_readings, [
        "reading_id", "meter_id", "reading_date", "reading_value", "unit",
        "reading_type", "reported_by", "validated"
    ])
    
    write_csv("invoice.csv", invoices, [
        "invoice_id", "contract_id", "issue_date", "due_date", "period_start",
        "period_end", "amount_ht", "vat_amount", "amount_ttc", "status", "payment_date"
    ])
    
    write_csv("payment.csv", payments, [
        "payment_id", "invoice_id", "payment_date", "amount", "payment_method",
        "transaction_ref", "status", "processed_at"
    ])
    
    write_csv("call.csv", calls, [
        "call_id", "customer_id", "call_datetime", "duration_seconds", "reason",
        "agent_id", "status", "satisfaction_score", "notes"
    ])
    
    write_csv("claim.csv", claims, [
        "claim_id", "customer_id", "contract_id", "claim_type", "description",
        "open_date", "resolution_date", "status", "priority", "assigned_to", "compensation_amount"
    ])
    
    write_csv("interaction.csv", interactions, [
        "interaction_id", "customer_id", "channel", "interaction_type",
        "interaction_datetime", "subject", "outcome", "agent_id", "duration_minutes", "sentiment"
    ])
    
    print()
    print("=" * 60)
    print("✅ Dataset generation complete!")
    print("=" * 60)
    
    # Summary
    total_records = (len(customers) + len(contracts) + len(subscriptions) + 
                   len(meters) + len(meter_readings) + len(invoices) + 
                   len(payments) + len(calls) + len(claims) + len(interactions))
    
    print(f"\n📊 Summary:")
    print(f"   • customer.csv:       {len(customers):5d} records")
    print(f"   • contract.csv:       {len(contracts):5d} records")
    print(f"   • subscription.csv:   {len(subscriptions):5d} records")
    print(f"   • meter.csv:          {len(meters):5d} records")
    print(f"   • meter_reading.csv:  {len(meter_readings):5d} records")
    print(f"   • invoice.csv:        {len(invoices):5d} records")
    print(f"   • payment.csv:        {len(payments):5d} records")
    print(f"   • call.csv:           {len(calls):5d} records")
    print(f"   • claim.csv:          {len(claims):5d} records")
    print(f"   • interaction.csv:    {len(interactions):5d} records")
    print(f"   ─────────────────────────────────")
    print(f"   Total:                {total_records:5d} records")

if __name__ == "__main__":
    main()
