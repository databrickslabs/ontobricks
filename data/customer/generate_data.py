#!/usr/bin/env python3
"""
Generate Energy Provider Customer Journey synthetic dataset and load it
directly into Databricks Unity Catalog tables via databricks-sql-connector.

Scales from hundreds to millions of rows.  No intermediate CSV files are
created -- data is generated in memory and streamed to Databricks.

Usage
-----
    python generate_data.py --catalog main --schema customer_journey

    # Million-row scale (uses Parquet staging for speed)
    python generate_data.py --catalog main --schema cj_large \
        --customers 1000000 --volume /Volumes/main/cj_large/staging

Run ``python generate_data.py --help`` for the full list of options.
"""

from __future__ import annotations

import argparse
import math
import os
import random
import sys
import tempfile
import time
from datetime import datetime, timedelta
from itertools import islice
from typing import Any, Dict, Generator, List, Optional, Sequence

# ---------------------------------------------------------------------------
# Optional heavy imports -- deferred so ``--help`` stays fast
# ---------------------------------------------------------------------------
_pyarrow = None
_pyarrow_parquet = None


def _ensure_pyarrow():
    global _pyarrow, _pyarrow_parquet
    if _pyarrow is None:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            _pyarrow = pa
            _pyarrow_parquet = pq
        except ImportError:
            print(
                "ERROR: pyarrow is required for Parquet staging (large datasets). "
                "Install it with: pip install pyarrow",
                file=sys.stderr,
            )
            sys.exit(1)


# ===================================================================
# Data pools -- expanded for million-row uniqueness
# ===================================================================

FIRST_NAMES: List[str] = [
    "James", "Emma", "Michael", "Olivia", "William", "Ava", "Alexander",
    "Sophia", "Benjamin", "Isabella", "Lucas", "Mia", "Henry", "Charlotte",
    "Daniel", "Amelia", "Matthew", "Harper", "Joseph", "Evelyn", "David",
    "Abigail", "Samuel", "Emily", "Christopher", "Elizabeth", "Andrew",
    "Sofia", "Joshua", "Avery", "Ethan", "Ella", "Nathan", "Scarlett",
    "Ryan", "Grace", "Jacob", "Chloe", "Nicholas", "Victoria", "Thomas",
    "Riley", "Charles", "Aria", "George", "Lily", "Robert", "Aurora",
    "John", "Zoey", "Edward", "Penelope", "Oliver", "Layla", "Sebastian",
    "Nora", "Noah", "Camila", "Liam", "Hannah", "Logan", "Addison",
    "Mason", "Brooklyn", "Aiden", "Eleanor", "Owen", "Stella", "Luke",
    "Natalie", "Jack", "Leah", "Gabriel", "Savannah", "Elijah", "Audrey",
    "Isaac", "Claire", "Caleb", "Lucy",
    # Extended pool for variety at scale
    "Antoine", "Marie", "Pierre", "Camille", "Louis", "Lea", "Hugo",
    "Manon", "Jules", "Chloe", "Arthur", "Jade", "Paul", "Louise",
    "Adam", "Alice", "Tom", "Lina", "Raphael", "Rose", "Leo", "Anna",
    "Nathan", "Juliette", "Mathis", "Sarah", "Theo", "Eva", "Maxime",
    "Zoe", "Axel", "Clara", "Noel", "Ines", "Romain", "Margot",
    "Valentin", "Laura", "Bastien", "Pauline", "Quentin", "Marine",
    "Damien", "Oceane", "Florian", "Mathilde", "Kevin", "Anais",
    "Julien", "Emilie", "Clement", "Helene", "Vincent", "Cecile",
    "Laurent", "Aurelie", "Stephane", "Sandrine", "Sylvain", "Nathalie",
    "Frederic", "Isabelle", "Thierry", "Catherine", "Bruno", "Monique",
    "Patrick", "Veronique", "Michel", "Martine", "Philippe", "Christine",
    "Alain", "Sylvie", "Bernard", "Dominique", "Yves", "Brigitte",
    "Denis", "Anne", "Francis", "Nicole", "Gerard", "Danielle",
    "Marcel", "Jacqueline", "Andre", "Francoise", "Henri", "Genevieve",
    "Jean", "Madeleine", "Claude", "Simone", "Maurice", "Renee",
    "Fernand", "Odette", "Raymond", "Yvonne", "Roger", "Marcelle",
    "Lucien", "Andree", "Gustave", "Lucienne", "Emile", "Germaine",
    "Armand", "Henriette", "Albert", "Marguerite", "Ernest", "Jeanne",
    "Prosper", "Augustine", "Felix", "Eugenie", "Victor", "Clemence",
    "Auguste", "Adele", "Leopold", "Blanche", "Edmond", "Berthe",
    "Gaston", "Suzanne",
]

LAST_NAMES: List[str] = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit",
    "Durand", "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel",
    "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier", "Morel",
    "Girard", "Andre", "Lefevre", "Mercier", "Dupont", "Lambert", "Bonnet",
    "Francois", "Martinez", "Legrand", "Garnier", "Faure", "Rousseau",
    "Blanc", "Guerin", "Muller", "Henry", "Roussel", "Nicolas", "Perrin",
    "Morin", "Mathieu", "Clement", "Gauthier", "Dumont", "Lopez",
    "Fontaine", "Chevalier", "Robin", "Masson", "Sanchez", "Gerard",
    "Nguyen", "Boyer", "Denis", "Lemaire", "Duval", "Joly", "Gautier",
    "Roger", "Roche", "Roy", "Noel",
    # Extended pool
    "Picard", "Marchand", "Riviere", "Colin", "Aubert", "Renard",
    "Giraud", "Leclerc", "Gaillard", "Brun", "Barbier", "Arnaud",
    "Caron", "Moulin", "Menard", "Dumas", "Blanchard", "Gilles",
    "Collet", "Legendre", "Cousin", "Maillard", "Lacroix", "Olivier",
    "Vidal", "Prevost", "Bouchet", "Rey", "Renault", "Brunet",
    "Lemoine", "Benoit", "Charles", "Guillot", "Pichon", "Lecomte",
    "Martel", "Berger", "Carpentier", "Payet", "Bourgeois", "Hubert",
    "Ferreira", "Aubry", "Descamps", "Marie", "Baron", "Schmitt",
    "Poirier", "Jacquet", "Collin", "Cordier", "Perrot", "Mallet",
    "Charpentier", "Vasseur", "Lejeune", "Marchal", "Boulanger",
    "Tessier", "Hamel", "Bouvier", "Chevallier", "Langlois", "Regnier",
    "Guyot", "Peltier", "Pasquier", "Georges", "Delorme", "Chartier",
    "Perrier", "Sauvage", "Schneider", "Pages", "Ferry", "Marin",
    "Bigot", "Royer", "Leblanc", "Coulon", "Monnier", "Navarro",
    "Briand", "Guillet", "Delaunay", "Rossi", "Coste", "Marty",
    "Pons", "Bailly", "Pelletier", "Godard", "Launay", "Bertin",
    "Meunier", "Grondin", "Hoarau", "Lebreton", "Leduc", "Poulain",
    "Fischer", "Weber", "Marechal", "Raymond", "Hebert", "Gomes",
    "Perez", "Fernandez", "Gonzalez", "Rodrigues", "Alves", "Da Silva",
    "Dos Santos", "De Oliveira", "Costa", "Ferreira", "Sousa",
    "Correia", "Lopes", "Ribeiro", "Carvalho", "Teixeira", "Nunes",
    "Pinto", "Santos", "Vieira", "Cardoso",
]

CITIES: List[tuple] = [
    ("Paris", "75001"), ("Lyon", "69001"), ("Marseille", "13001"),
    ("Toulouse", "31000"), ("Nice", "06000"), ("Nantes", "44000"),
    ("Strasbourg", "67000"), ("Montpellier", "34000"), ("Bordeaux", "33000"),
    ("Lille", "59000"), ("Rennes", "35000"), ("Reims", "51100"),
    ("Le Havre", "76600"), ("Saint-Etienne", "42000"), ("Toulon", "83000"),
    ("Grenoble", "38000"), ("Dijon", "21000"), ("Angers", "49000"),
    ("Nimes", "30000"), ("Aix-en-Provence", "13100"), ("Brest", "29200"),
    ("Clermont-Ferrand", "63000"), ("Villeurbanne", "69100"),
    ("Tours", "37000"), ("Amiens", "80000"), ("Limoges", "87000"),
    ("Metz", "57000"), ("Perpignan", "66000"),
    # Extended
    ("Rouen", "76000"), ("Caen", "14000"), ("Orleans", "45000"),
    ("Mulhouse", "68100"), ("Nancy", "54000"), ("Besancon", "25000"),
    ("Pau", "64000"), ("La Rochelle", "17000"), ("Bayonne", "64100"),
    ("Avignon", "84000"), ("Cannes", "06400"), ("Antibes", "06600"),
    ("Valence", "26000"), ("Chambery", "73000"), ("Troyes", "10000"),
    ("Poitiers", "86000"), ("Bourges", "18000"), ("Chartres", "28000"),
    ("Vannes", "56000"), ("Lorient", "56100"), ("Quimper", "29000"),
    ("Saint-Malo", "35400"),
]

STREET_TYPES = ["Rue", "Avenue", "Boulevard", "Place", "Allee", "Chemin", "Impasse"]
STREET_NAMES = [
    "de la Republique", "Victor Hugo", "Jean Jaures", "des Fleurs",
    "du General Leclerc", "de la Liberte", "Pasteur", "du Commerce",
    "de la Gare", "des Lilas", "Voltaire", "de Paris", "Emile Zola",
    "du Moulin", "des Roses", "de Verdun", "Charles de Gaulle",
    "du Chateau", "des Alpes", "du Port", "Saint-Michel", "de Lyon",
    "du Marche", "des Acacias", "de la Fontaine", "des Tilleuls",
    "du Stade", "des Ecoles", "de la Mairie", "des Pres",
    "de la Paix", "du Lavoir", "des Champs", "des Vignes",
    "des Cerisiers", "du Bois", "de la Colline", "du Lac",
    "du Pont", "de la Plage", "des Oliviers", "des Pins",
    "de la Montagne", "du Ruisseau", "des Peupliers", "du Vallon",
    "de la Croix", "du Clos", "des Jardins", "de la Ferme",
]

ENERGY_TYPES = ["electricity", "gas", "dual_fuel"]
PLAN_TYPES = ["basic", "standard", "premium", "eco", "flex"]
CONTRACT_STATUS = ["active", "terminated", "suspended", "pending"]
PAYMENT_METHODS = ["direct_debit", "credit_card", "bank_transfer", "check"]
CUSTOMER_SEGMENTS = ["residential", "small_business", "professional"]
CALL_REASONS = [
    "billing_inquiry", "meter_reading", "contract_change",
    "technical_issue", "payment_question", "move_in", "move_out",
    "tariff_info", "complaint", "general_info",
]
CALL_STATUS = ["completed", "callback_required", "transferred", "resolved"]
CLAIM_TYPES = [
    "billing_error", "estimated_reading", "service_interruption",
    "meter_malfunction", "contract_dispute", "price_complaint",
    "delay_issue", "quality_issue",
]
CLAIM_STATUS = ["open", "in_progress", "resolved", "closed", "escalated"]
INTERACTION_CHANNELS = [
    "phone", "email", "web_portal", "mobile_app", "chat", "in_person",
]
INTERACTION_TYPES = [
    "inquiry", "request", "update", "notification", "feedback",
    "service_activation",
]

# ===================================================================
# Helpers
# ===================================================================

_EPOCH = datetime(2015, 1, 1)
_DAY_SECS = 86_400


def _random_date(rng: random.Random, start_year: int, end_year: int) -> str:
    start = datetime(start_year, 1, 1)
    span = (datetime(end_year, 12, 31) - start).days
    return (start + timedelta(days=rng.randint(0, span))).strftime("%Y-%m-%d")


def _random_datetime(rng: random.Random, start_year: int, end_year: int) -> str:
    d = _random_date(rng, start_year, end_year)
    h = rng.randint(6, 22)
    m = rng.randint(0, 59)
    return f"{d} {h:02d}:{m:02d}:00"


def _unique_name(index: int, first_names: List[str], last_names: List[str]) -> tuple:
    """Return (first_name, last_name) that is unique for *index*.

    Uses combinatorial pairing; appends a numeric suffix when the pool
    is exhausted so that uniqueness is guaranteed at any scale.
    """
    pool_size = len(first_names) * len(last_names)
    cycle = index // pool_size
    pos = index % pool_size
    fn = first_names[pos % len(first_names)]
    ln = last_names[pos // len(first_names)]
    if cycle > 0:
        ln = f"{ln}-{cycle + 1}"
    return fn, ln


# ===================================================================
# Generator functions -- yield one row dict at a time
# ===================================================================

def gen_customers(
    count: int, rng: random.Random
) -> Generator[Dict[str, Any], None, None]:
    for i in range(count):
        idx = i + 1
        fn, ln = _unique_name(i, FIRST_NAMES, LAST_NAMES)
        city, postal = rng.choice(CITIES)
        street_num = rng.randint(1, 9999)
        street = f"{rng.choice(STREET_TYPES)} {rng.choice(STREET_NAMES)}"
        yield {
            "customer_id": f"CUST{idx:07d}",
            "first_name": fn,
            "last_name": ln,
            "email": f"customer{idx:07d}@email.fr",
            "phone": f"+33{600000000 + idx:d}",
            "street_address": f"{street_num} {street}",
            "city": city,
            "postal_code": postal,
            "country": "France",
            "date_of_birth": _random_date(rng, 1950, 2000),
            "registration_date": _random_date(rng, 2015, 2024),
            "segment": rng.choice(CUSTOMER_SEGMENTS),
            "loyalty_points": rng.randint(0, 5000),
            "is_active": rng.choice(["true", "true", "true", "false"]),
        }


def gen_contracts(
    count: int, customer_ids: Sequence[str], rng: random.Random
) -> Generator[Dict[str, Any], None, None]:
    n_cust = len(customer_ids)
    for i in range(count):
        idx = i + 1
        cid = customer_ids[rng.randint(0, n_cust - 1)]
        start = _random_date(rng, 2018, 2024)
        dur = rng.choice([12, 24, 36])
        end = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=dur * 30)).strftime("%Y-%m-%d")
        yield {
            "contract_id": f"CON{idx:07d}",
            "customer_id": cid,
            "energy_type": rng.choice(ENERGY_TYPES),
            "start_date": start,
            "end_date": end,
            "status": rng.choice(CONTRACT_STATUS),
            "monthly_fee": round(rng.uniform(5.0, 25.0), 2),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "auto_renewal": rng.choice(["true", "false"]),
            "created_at": start,
        }


def gen_subscriptions(
    count: int, contract_ids: Sequence[str], contract_energy: Dict[str, str],
    contract_start: Dict[str, str], rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    n = len(contract_ids)
    for i in range(count):
        idx = i + 1
        cid = contract_ids[rng.randint(0, n - 1)]
        et = contract_energy.get(cid, "electricity")
        has_gas = et in ("gas", "dual_fuel")
        yield {
            "subscription_id": f"SUB{idx:07d}",
            "contract_id": cid,
            "plan_type": rng.choice(PLAN_TYPES),
            "price_per_kwh": round(rng.uniform(0.15, 0.35), 4),
            "price_per_m3": round(rng.uniform(0.08, 0.18), 4) if has_gas else None,
            "standing_charge": round(rng.uniform(8.0, 25.0), 2),
            "discount_percentage": round(rng.uniform(0, 15), 1),
            "green_energy": rng.choice(["true", "false"]),
            "start_date": contract_start.get(cid, "2022-01-01"),
            "status": rng.choice(["active", "expired", "pending"]),
        }


def gen_meters(
    count: int, contract_ids: Sequence[str], contract_energy: Dict[str, str],
    rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    meter_types = {
        "electricity": ["smart_meter", "traditional", "prepaid"],
        "gas": ["smart_meter", "diaphragm", "rotary"],
        "dual_fuel": ["smart_meter", "traditional", "prepaid"],
    }
    n = len(contract_ids)
    for i in range(count):
        idx = i + 1
        cid = contract_ids[rng.randint(0, n - 1)]
        energy = contract_energy.get(cid, "electricity")
        if energy == "dual_fuel":
            energy = rng.choice(["electricity", "gas"])
        yield {
            "meter_id": f"MTR{idx:07d}",
            "contract_id": cid,
            "meter_serial": f"SN{10000000 + idx}",
            "meter_type": rng.choice(meter_types.get(energy, ["smart_meter"])),
            "energy_type": energy,
            "installation_date": _random_date(rng, 2015, 2024),
            "last_inspection_date": _random_date(rng, 2022, 2025),
            "location": rng.choice(["indoor", "outdoor", "basement", "garage"]),
            "status": rng.choice(["active", "inactive", "faulty", "replaced"]),
        }


def gen_meter_readings(
    count: int, meter_ids: Sequence[str], meter_energy: Dict[str, str],
    rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    n = len(meter_ids)
    for i in range(count):
        idx = i + 1
        mid = meter_ids[rng.randint(0, n - 1)]
        energy = meter_energy.get(mid, "electricity")
        if energy == "electricity":
            val = round(rng.uniform(100, 15000), 2)
            unit = "kWh"
        else:
            val = round(rng.uniform(50, 5000), 2)
            unit = "m3"
        yield {
            "reading_id": f"RDG{idx:07d}",
            "meter_id": mid,
            "reading_date": _random_date(rng, 2022, 2025),
            "reading_value": val,
            "unit": unit,
            "reading_type": rng.choice(["actual", "estimated", "smart_auto"]),
            "reported_by": rng.choice(["customer", "technician", "smart_meter"]),
            "validated": rng.choice(["true", "true", "false"]),
        }


def gen_invoices(
    count: int, contract_ids: Sequence[str], rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    n = len(contract_ids)
    for i in range(count):
        idx = i + 1
        cid = contract_ids[rng.randint(0, n - 1)]
        issue = _random_date(rng, 2022, 2025)
        due = (datetime.strptime(issue, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
        period_start = (datetime.strptime(issue, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        amt_ht = round(rng.uniform(30, 500), 2)
        vat = round(amt_ht * 0.20, 2)
        status = rng.choice(["paid", "paid", "paid", "pending", "overdue", "cancelled"])
        yield {
            "invoice_id": f"INV{idx:07d}",
            "contract_id": cid,
            "issue_date": issue,
            "due_date": due,
            "period_start": period_start,
            "period_end": issue,
            "amount_ht": amt_ht,
            "vat_amount": vat,
            "amount_ttc": round(amt_ht + vat, 2),
            "status": status,
            "payment_date": due if status == "paid" else None,
        }


def gen_payments(
    count: int, invoice_ids: Sequence[str], invoice_data: Dict[str, dict],
    rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    paid = [iid for iid, d in invoice_data.items() if d["status"] == "paid"]
    pool = paid if paid else list(invoice_ids)
    n = len(pool)
    for i in range(count):
        idx = i + 1
        iid = pool[rng.randint(0, n - 1)]
        inv = invoice_data.get(iid, {})
        yield {
            "payment_id": f"PAY{idx:07d}",
            "invoice_id": iid,
            "payment_date": inv.get("due_date", "2023-01-01"),
            "amount": inv.get("amount_ttc", 100.0),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "transaction_ref": f"TRX{100000000 + idx}",
            "status": rng.choice(["completed", "completed", "completed", "pending", "failed", "refunded"]),
            "processed_at": _random_datetime(rng, 2022, 2025),
        }


def gen_calls(
    count: int, customer_ids: Sequence[str], rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    n = len(customer_ids)
    for i in range(count):
        idx = i + 1
        cid = customer_ids[rng.randint(0, n - 1)]
        reason = rng.choice(CALL_REASONS)
        yield {
            "call_id": f"CALL{idx:07d}",
            "customer_id": cid,
            "call_datetime": _random_datetime(rng, 2022, 2025),
            "duration_seconds": rng.randint(60, 1800),
            "reason": reason,
            "agent_id": f"AGT{rng.randint(100, 150)}",
            "status": rng.choice(CALL_STATUS),
            "satisfaction_score": rng.randint(1, 5) if rng.random() > 0.3 else None,
            "notes": f"Customer called regarding {reason.replace('_', ' ')}",
        }


def gen_claims(
    count: int, customer_ids: Sequence[str], contract_ids: Sequence[str],
    rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    nc = len(customer_ids)
    nct = len(contract_ids)
    for i in range(count):
        idx = i + 1
        cid = customer_ids[rng.randint(0, nc - 1)]
        ctid = contract_ids[rng.randint(0, nct - 1)]
        claim_type = rng.choice(CLAIM_TYPES)
        open_date = _random_date(rng, 2022, 2025)
        res_days = rng.randint(1, 45)
        resolved = rng.random() > 0.3
        yield {
            "claim_id": f"CLM{idx:07d}",
            "customer_id": cid,
            "contract_id": ctid,
            "claim_type": claim_type,
            "description": f"Customer claim regarding {claim_type.replace('_', ' ')}",
            "open_date": open_date,
            "resolution_date": (
                (datetime.strptime(open_date, "%Y-%m-%d") + timedelta(days=res_days)).strftime("%Y-%m-%d")
                if resolved else None
            ),
            "status": rng.choice(CLAIM_STATUS),
            "priority": rng.choice(["low", "medium", "high", "urgent"]),
            "assigned_to": f"AGT{rng.randint(100, 150)}",
            "compensation_amount": round(rng.uniform(0, 200), 2) if rng.random() > 0.7 else 0,
        }


def gen_interactions(
    count: int, customer_ids: Sequence[str], rng: random.Random,
) -> Generator[Dict[str, Any], None, None]:
    n = len(customer_ids)
    for i in range(count):
        idx = i + 1
        cid = customer_ids[rng.randint(0, n - 1)]
        itype = rng.choice(INTERACTION_TYPES)
        channel = rng.choice(INTERACTION_CHANNELS)
        yield {
            "interaction_id": f"INT{idx:07d}",
            "customer_id": cid,
            "channel": channel,
            "interaction_type": itype,
            "interaction_datetime": _random_datetime(rng, 2022, 2025),
            "subject": f"{itype.title()} via {channel}",
            "outcome": rng.choice(["resolved", "pending_response", "follow_up_needed", "escalated", "completed"]),
            "agent_id": f"AGT{rng.randint(100, 150)}" if rng.random() > 0.4 else None,
            "duration_minutes": rng.randint(1, 30) if rng.random() > 0.3 else None,
            "sentiment": rng.choice(["positive", "neutral", "negative"]) if rng.random() > 0.5 else None,
        }


# ===================================================================
# Table DDL definitions
# ===================================================================

TABLE_SCHEMAS: Dict[str, List[tuple]] = {
    "customer": [
        ("customer_id", "STRING"), ("first_name", "STRING"),
        ("last_name", "STRING"), ("email", "STRING"), ("phone", "STRING"),
        ("street_address", "STRING"), ("city", "STRING"),
        ("postal_code", "STRING"), ("country", "STRING"),
        ("date_of_birth", "STRING"), ("registration_date", "STRING"),
        ("segment", "STRING"), ("loyalty_points", "INT"),
        ("is_active", "STRING"),
    ],
    "contract": [
        ("contract_id", "STRING"), ("customer_id", "STRING"),
        ("energy_type", "STRING"), ("start_date", "STRING"),
        ("end_date", "STRING"), ("status", "STRING"),
        ("monthly_fee", "DECIMAL(10,2)"), ("payment_method", "STRING"),
        ("auto_renewal", "STRING"), ("created_at", "STRING"),
    ],
    "subscription": [
        ("subscription_id", "STRING"), ("contract_id", "STRING"),
        ("plan_type", "STRING"), ("price_per_kwh", "DECIMAL(10,4)"),
        ("price_per_m3", "DECIMAL(10,4)"), ("standing_charge", "DECIMAL(10,2)"),
        ("discount_percentage", "DECIMAL(5,1)"), ("green_energy", "STRING"),
        ("start_date", "STRING"), ("status", "STRING"),
    ],
    "meter": [
        ("meter_id", "STRING"), ("contract_id", "STRING"),
        ("meter_serial", "STRING"), ("meter_type", "STRING"),
        ("energy_type", "STRING"), ("installation_date", "STRING"),
        ("last_inspection_date", "STRING"), ("location", "STRING"),
        ("status", "STRING"),
    ],
    "meter_reading": [
        ("reading_id", "STRING"), ("meter_id", "STRING"),
        ("reading_date", "STRING"), ("reading_value", "DECIMAL(15,2)"),
        ("unit", "STRING"), ("reading_type", "STRING"),
        ("reported_by", "STRING"), ("validated", "STRING"),
    ],
    "invoice": [
        ("invoice_id", "STRING"), ("contract_id", "STRING"),
        ("issue_date", "STRING"), ("due_date", "STRING"),
        ("period_start", "STRING"), ("period_end", "STRING"),
        ("amount_ht", "DECIMAL(15,2)"), ("vat_amount", "DECIMAL(15,2)"),
        ("amount_ttc", "DECIMAL(15,2)"), ("status", "STRING"),
        ("payment_date", "STRING"),
    ],
    "payment": [
        ("payment_id", "STRING"), ("invoice_id", "STRING"),
        ("payment_date", "STRING"), ("amount", "DECIMAL(15,2)"),
        ("payment_method", "STRING"), ("transaction_ref", "STRING"),
        ("status", "STRING"), ("processed_at", "STRING"),
    ],
    "call": [
        ("call_id", "STRING"), ("customer_id", "STRING"),
        ("call_datetime", "STRING"), ("duration_seconds", "INT"),
        ("reason", "STRING"), ("agent_id", "STRING"),
        ("status", "STRING"), ("satisfaction_score", "INT"),
        ("notes", "STRING"),
    ],
    "claim": [
        ("claim_id", "STRING"), ("customer_id", "STRING"),
        ("contract_id", "STRING"), ("claim_type", "STRING"),
        ("description", "STRING"), ("open_date", "STRING"),
        ("resolution_date", "STRING"), ("status", "STRING"),
        ("priority", "STRING"), ("assigned_to", "STRING"),
        ("compensation_amount", "DECIMAL(10,2)"),
    ],
    "interaction": [
        ("interaction_id", "STRING"), ("customer_id", "STRING"),
        ("channel", "STRING"), ("interaction_type", "STRING"),
        ("interaction_datetime", "STRING"), ("subject", "STRING"),
        ("outcome", "STRING"), ("agent_id", "STRING"),
        ("duration_minutes", "INT"), ("sentiment", "STRING"),
    ],
}

# Ordered to respect FK dependencies
TABLE_ORDER = [
    "customer", "contract", "subscription", "meter",
    "meter_reading", "invoice", "payment",
    "call", "claim", "interaction",
]


# ===================================================================
# Analytical views
# ===================================================================

VIEW_DDLS = {
    "vw_customer_360": """
CREATE OR REPLACE VIEW {fq}.vw_customer_360 AS
SELECT
    c.customer_id, c.first_name, c.last_name, c.email, c.phone,
    c.city, c.segment, c.loyalty_points, c.is_active,
    COUNT(DISTINCT ct.contract_id) AS contract_count,
    COUNT(DISTINCT m.meter_id) AS meter_count,
    COUNT(DISTINCT i.invoice_id) AS invoice_count,
    SUM(i.amount_ttc) AS total_invoiced,
    COUNT(DISTINCT cl.call_id) AS call_count,
    COUNT(DISTINCT clm.claim_id) AS claim_count
FROM {fq}.customer c
LEFT JOIN {fq}.contract ct ON c.customer_id = ct.customer_id
LEFT JOIN {fq}.meter m ON ct.contract_id = m.contract_id
LEFT JOIN {fq}.invoice i ON ct.contract_id = i.contract_id
LEFT JOIN {fq}.call cl ON c.customer_id = cl.customer_id
LEFT JOIN {fq}.claim clm ON c.customer_id = clm.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email,
         c.phone, c.city, c.segment, c.loyalty_points, c.is_active
""",
    "vw_billing_summary": """
CREATE OR REPLACE VIEW {fq}.vw_billing_summary AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    ct.contract_id, ct.energy_type,
    i.invoice_id, i.issue_date,
    i.amount_ht, i.vat_amount, i.amount_ttc,
    i.status AS invoice_status,
    p.payment_id, p.payment_date,
    p.amount AS paid_amount, p.status AS payment_status
FROM {fq}.customer c
JOIN {fq}.contract ct ON c.customer_id = ct.customer_id
JOIN {fq}.invoice i ON ct.contract_id = i.contract_id
LEFT JOIN {fq}.payment p ON i.invoice_id = p.invoice_id
""",
    "vw_consumption_analysis": """
CREATE OR REPLACE VIEW {fq}.vw_consumption_analysis AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.city,
    ct.energy_type AS contract_energy_type,
    m.meter_id, m.meter_type,
    m.energy_type AS meter_energy_type,
    mr.reading_date, mr.reading_value, mr.unit,
    mr.reading_type, mr.validated
FROM {fq}.customer c
JOIN {fq}.contract ct ON c.customer_id = ct.customer_id
JOIN {fq}.meter m ON ct.contract_id = m.contract_id
JOIN {fq}.meter_reading mr ON m.meter_id = mr.meter_id
""",
}


# ===================================================================
# Chunked iteration helper
# ===================================================================

def _chunked(gen: Generator, size: int) -> Generator[List, None, None]:
    """Yield successive chunks of *size* items from generator *gen*."""
    while True:
        chunk = list(islice(gen, size))
        if not chunk:
            break
        yield chunk


# ===================================================================
# Loading strategies
# ===================================================================

BATCH_INSERT_SIZE = 500
PARQUET_CHUNK_SIZE = 500_000
LARGE_THRESHOLD = 50_000


def _build_insert_sql(fq_table: str, columns: List[str], rows: List[dict]) -> tuple:
    """Build a multi-row INSERT VALUES statement and flat param list."""
    placeholders = ", ".join(["%s"] * len(columns))
    row_tpl = f"({placeholders})"
    values_clause = ", ".join([row_tpl] * len(rows))
    sql = f"INSERT INTO {fq_table} ({', '.join(columns)}) VALUES {values_clause}"
    params: list = []
    for row in rows:
        for col in columns:
            params.append(row.get(col))
    return sql, params


def _load_batch_insert(cursor, fq_table: str, columns: List[str], gen, total: int):
    """Insert rows via SQL INSERT in batches."""
    loaded = 0
    for chunk in _chunked(gen, BATCH_INSERT_SIZE):
        sql, params = _build_insert_sql(fq_table, columns, chunk)
        cursor.execute(sql, params)
        loaded += len(chunk)
        if loaded % 5000 == 0 or loaded == total:
            print(f"    {loaded:,}/{total:,} rows inserted", end="\r")
    print()


def _load_parquet_staging(cursor, fq_table: str, columns: List[str],
                          col_types: List[tuple], gen, total: int,
                          volume_path: str, ws_client):
    """Stage rows via temp Parquet -> upload to Volume -> COPY INTO table."""
    _ensure_pyarrow()
    pa = _pyarrow
    pq = _pyarrow_parquet

    pa_type_map = {
        "STRING": pa.string(),
        "INT": pa.int32(),
        "DECIMAL(10,2)": pa.float64(),
        "DECIMAL(10,4)": pa.float64(),
        "DECIMAL(15,2)": pa.float64(),
        "DECIMAL(5,1)": pa.float64(),
    }

    pa_fields = []
    for col_name, col_type in col_types:
        pa_fields.append(pa.field(col_name, pa_type_map.get(col_type, pa.string())))
    schema = pa.schema(pa_fields)

    loaded = 0
    chunk_idx = 0
    for chunk in _chunked(gen, PARQUET_CHUNK_SIZE):
        col_arrays = {}
        for col_name, col_type in col_types:
            values = [row.get(col_name) for row in chunk]
            pa_t = pa_type_map.get(col_type, pa.string())
            col_arrays[col_name] = pa.array(values, type=pa_t)

        table = pa.table(col_arrays, schema=schema)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        pq.write_table(table, tmp_path, compression="snappy")

        staged_name = f"staging-{fq_table.replace('.', '-')}-{chunk_idx}.parquet"
        remote_path = f"{volume_path}/{staged_name}"

        try:
            with open(tmp_path, "rb") as f:
                ws_client.files.upload(remote_path, f, overwrite=True)

            col_list = ", ".join(columns)
            cursor.execute(
                f"INSERT INTO {fq_table} ({col_list}) "
                f"SELECT {col_list} FROM read_files('{remote_path}', format => 'parquet')"
            )
        finally:
            os.unlink(tmp_path)
            try:
                ws_client.files.delete(remote_path)
            except Exception:
                pass

        loaded += len(chunk)
        chunk_idx += 1
        print(f"    {loaded:,}/{total:,} rows loaded (Parquet chunk {chunk_idx})", end="\r")
    print()


# ===================================================================
# Orchestration
# ===================================================================

def _collect_ids(gen, id_col: str, count: int) -> tuple:
    """Consume a generator, collect all rows and extract the ID column."""
    rows: list = []
    ids: list = []
    for row in gen:
        rows.append(row)
        ids.append(row[id_col])
    return rows, ids


def run(args: argparse.Namespace):
    from databricks import sql as dbsql

    rng = random.Random(args.seed)
    fq = f"{args.catalog}.{args.schema}"
    total_rows = sum([
        args.customers, args.contracts, args.subscriptions, args.meters,
        args.meter_readings, args.invoices, args.payments,
        args.calls, args.claims, args.interactions,
    ])
    use_parquet = total_rows >= LARGE_THRESHOLD and args.volume is not None

    print("=" * 70)
    print("  Energy Provider Customer Journey - Synthetic Data Generator")
    print("=" * 70)
    print(f"  Target:       {fq}")
    print(f"  Total rows:   {total_rows:,}")
    print(f"  Load method:  {'Parquet staging' if use_parquet else 'Batch INSERT'}")
    if use_parquet:
        print(f"  Volume:       {args.volume}")
    print(f"  Seed:         {args.seed}")
    print("=" * 70)

    host = args.host or os.environ.get("DATABRICKS_HOST", "")
    token = args.token or os.environ.get("DATABRICKS_TOKEN", "")
    warehouse = args.warehouse or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    http_path = f"/sql/1.0/warehouses/{warehouse}"

    if not host or not token or not warehouse:
        print(
            "ERROR: Databricks connection requires --host, --token, --warehouse "
            "(or DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID env vars).",
            file=sys.stderr,
        )
        sys.exit(1)

    if use_parquet and not args.volume:
        print(
            "ERROR: --volume is required for Parquet staging with large datasets.",
            file=sys.stderr,
        )
        sys.exit(1)

    host = host.rstrip("/")
    if not host.startswith("https://"):
        host = f"https://{host}"

    # Workspace client for file uploads (Parquet staging)
    ws_client = None
    if use_parquet:
        from databricks.sdk import WorkspaceClient
        ws_client = WorkspaceClient(
            host=host,
            token=token,
        )

    print("\nConnecting to Databricks SQL Warehouse...")
    conn = dbsql.connect(
        server_hostname=host.replace("https://", ""),
        http_path=http_path,
        access_token=token,
    )
    cursor = conn.cursor()
    t0 = time.time()

    try:
        # Create schema (and staging volume if using Parquet staging)
        print(f"\nCreating schema {fq} ...")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {fq}")

        if use_parquet:
            parts = args.volume.strip("/").split("/")
            if len(parts) >= 4 and parts[0].lower() == "volumes":
                vol_fq = f"{parts[1]}.{parts[2]}.{parts[3]}"
                print(f"Ensuring staging volume {vol_fq} exists ...")
                cursor.execute(f"CREATE VOLUME IF NOT EXISTS {vol_fq}")

        # -- Phase 1: Generate all data, collecting IDs for FK lookups --
        # We need parent IDs before generating children.  For tables that
        # need per-row metadata (energy_type, status, amounts) for child
        # generation, we also keep lightweight lookup dicts.

        print(f"\nGenerating {args.customers:,} customers ...")
        cust_rows, customer_ids = _collect_ids(
            gen_customers(args.customers, rng), "customer_id", args.customers
        )

        print(f"Generating {args.contracts:,} contracts ...")
        con_rows, contract_ids = _collect_ids(
            gen_contracts(args.contracts, customer_ids, rng), "contract_id", args.contracts
        )
        contract_energy = {r["contract_id"]: r["energy_type"] for r in con_rows}
        contract_start = {r["contract_id"]: r["start_date"] for r in con_rows}

        print(f"Generating {args.subscriptions:,} subscriptions ...")
        sub_rows, _ = _collect_ids(
            gen_subscriptions(args.subscriptions, contract_ids, contract_energy, contract_start, rng),
            "subscription_id", args.subscriptions,
        )

        print(f"Generating {args.meters:,} meters ...")
        mtr_rows, meter_ids = _collect_ids(
            gen_meters(args.meters, contract_ids, contract_energy, rng),
            "meter_id", args.meters,
        )
        meter_energy = {r["meter_id"]: r["energy_type"] for r in mtr_rows}

        print(f"Generating {args.meter_readings:,} meter readings ...")
        rdg_rows, _ = _collect_ids(
            gen_meter_readings(args.meter_readings, meter_ids, meter_energy, rng),
            "reading_id", args.meter_readings,
        )

        print(f"Generating {args.invoices:,} invoices ...")
        inv_rows, invoice_ids = _collect_ids(
            gen_invoices(args.invoices, contract_ids, rng),
            "invoice_id", args.invoices,
        )
        invoice_data = {
            r["invoice_id"]: {"due_date": r["due_date"], "amount_ttc": r["amount_ttc"], "status": r["status"]}
            for r in inv_rows
        }

        print(f"Generating {args.payments:,} payments ...")
        pay_rows, _ = _collect_ids(
            gen_payments(args.payments, invoice_ids, invoice_data, rng),
            "payment_id", args.payments,
        )

        print(f"Generating {args.calls:,} calls ...")
        call_rows, _ = _collect_ids(
            gen_calls(args.calls, customer_ids, rng), "call_id", args.calls,
        )

        print(f"Generating {args.claims:,} claims ...")
        clm_rows, _ = _collect_ids(
            gen_claims(args.claims, customer_ids, contract_ids, rng),
            "claim_id", args.claims,
        )

        print(f"Generating {args.interactions:,} interactions ...")
        int_rows, _ = _collect_ids(
            gen_interactions(args.interactions, customer_ids, rng),
            "interaction_id", args.interactions,
        )

        all_table_rows = {
            "customer": cust_rows,
            "contract": con_rows,
            "subscription": sub_rows,
            "meter": mtr_rows,
            "meter_reading": rdg_rows,
            "invoice": inv_rows,
            "payment": pay_rows,
            "call": call_rows,
            "claim": clm_rows,
            "interaction": int_rows,
        }

        gen_time = time.time() - t0
        print(f"\nData generation completed in {gen_time:.1f}s")

        # -- Phase 2: Create tables and load data --
        print("\nLoading data into Databricks tables ...")
        load_t0 = time.time()

        for table_name in TABLE_ORDER:
            fq_table = f"{fq}.{table_name}"
            cols = TABLE_SCHEMAS[table_name]
            col_names = [c[0] for c in cols]
            rows = all_table_rows[table_name]

            if args.drop_existing:
                cursor.execute(f"DROP TABLE IF EXISTS {fq_table}")

            col_defs = ", ".join(f"{c[0]} {c[1]}" for c in cols)
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {fq_table} ({col_defs})")

            print(f"  [{table_name}] loading {len(rows):,} rows ...")

            if use_parquet:
                _load_parquet_staging(
                    cursor, fq_table, col_names, cols,
                    iter(rows), len(rows), args.volume, ws_client,
                )
            else:
                _load_batch_insert(cursor, fq_table, col_names, iter(rows), len(rows))

        load_time = time.time() - load_t0
        print(f"\nData loading completed in {load_time:.1f}s")

        # -- Phase 3: Create views --
        if not args.skip_views:
            print("\nCreating analytical views ...")
            for view_name, ddl_tpl in VIEW_DDLS.items():
                ddl = ddl_tpl.format(fq=fq)
                cursor.execute(ddl)
                print(f"  {fq}.{view_name}")

        elapsed = time.time() - t0
        print()
        print("=" * 70)
        print("  COMPLETE")
        print("=" * 70)
        print(f"\n  Tables in {fq}:")
        for table_name in TABLE_ORDER:
            count = len(all_table_rows[table_name])
            print(f"    {table_name:20s} {count:>10,} rows")
        print(f"    {'─' * 32}")
        print(f"    {'Total':20s} {total_rows:>10,} rows")
        print(f"\n  Elapsed: {elapsed:.1f}s (generation: {gen_time:.1f}s, loading: {load_time:.1f}s)")
        print("=" * 70)

    finally:
        cursor.close()
        conn.close()


# ===================================================================
# CLI
# ===================================================================

def main():
    p = argparse.ArgumentParser(
        description="Generate synthetic Energy Provider Customer Journey data "
                    "and load it directly into Databricks Unity Catalog tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default dataset (~4,700 rows) using env vars for credentials
  python generate_data.py --catalog main --schema customer_journey

  # Large dataset with Parquet staging
  python generate_data.py --catalog main --schema cj_large \\
      --customers 1000000 --contracts 1500000 --meter-readings 5000000 \\
      --volume /Volumes/main/cj_large/staging --drop-existing

  # Custom seed for reproducibility
  python generate_data.py --catalog main --schema cj_test --seed 123
""",
    )

    # Target
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    p.add_argument("--schema", required=True, help="Unity Catalog schema name")

    # Row counts (defaults match original dataset)
    p.add_argument("--customers", type=int, default=200, help="Number of customer rows (default: 200)")
    p.add_argument("--contracts", type=int, default=300, help="Number of contract rows (default: 300)")
    p.add_argument("--subscriptions", type=int, default=350, help="Number of subscription rows (default: 350)")
    p.add_argument("--meters", type=int, default=400, help="Number of meter rows (default: 400)")
    p.add_argument("--meter-readings", type=int, default=1000, dest="meter_readings", help="Number of meter_reading rows (default: 1000)")
    p.add_argument("--invoices", type=int, default=800, help="Number of invoice rows (default: 800)")
    p.add_argument("--payments", type=int, default=700, help="Number of payment rows (default: 700)")
    p.add_argument("--calls", type=int, default=300, help="Number of call rows (default: 300)")
    p.add_argument("--claims", type=int, default=150, help="Number of claim rows (default: 150)")
    p.add_argument("--interactions", type=int, default=500, help="Number of interaction rows (default: 500)")

    # Connection
    p.add_argument("--host", default=None, help="Databricks workspace host (or DATABRICKS_HOST env var)")
    p.add_argument("--token", default=None, help="Databricks PAT token (or DATABRICKS_TOKEN env var)")
    p.add_argument("--warehouse", default=None, help="SQL Warehouse ID (or DATABRICKS_SQL_WAREHOUSE_ID env var)")

    # Options
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    p.add_argument("--drop-existing", action="store_true", dest="drop_existing", help="Drop tables before creating (clean run)")
    p.add_argument("--volume", default=None, help="UC Volume path for Parquet staging (e.g. /Volumes/cat/sch/vol). Required for large datasets (>50K rows).")
    p.add_argument("--skip-views", action="store_true", dest="skip_views", help="Skip creation of analytical views")

    args = p.parse_args()
    run(args)


if __name__ == "__main__":
    main()
