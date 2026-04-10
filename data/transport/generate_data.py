#!/usr/bin/env python3
"""
Generate Île-de-France Public Transport Dataset.
Inspired by RATP / Île-de-France Mobilités open data (data.ratp.fr, data.iledefrance-mobilites.fr).
Creates 16 tables with realistic data modelling a regional transit authority.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(2024)

OUTPUT_DIR = Path(__file__).parent


def random_date(start_year, end_year):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")


def random_datetime(start_year, end_year):
    d = random_date(start_year, end_year)
    h, m, s = random.randint(5, 23), random.randint(0, 59), random.randint(0, 59)
    return f"{d} {h:02d}:{m:02d}:{s:02d}"


def random_time(start_h=5, end_h=23):
    h = random.randint(start_h, end_h)
    m = random.randint(0, 59)
    return f"{h:02d}:{m:02d}:00"


def write_csv(filename, rows, fieldnames):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# 1. TRANSPORT_OPERATOR (20 rows)
# ──────────────────────────────────────────────────────────────────────────────
OPERATORS_DATA = [
    ("OP001", "RATP", "Régie Autonome des Transports Parisiens", "public", "Paris"),
    ("OP002", "SNCF Transilien", "SNCF Voyageurs — Transilien", "public", "Paris"),
    ("OP003", "Keolis", "Keolis SA", "private", "Paris"),
    ("OP004", "Transdev", "Transdev Group", "private", "Issy-les-Moulineaux"),
    ("OP005", "RATP Dev", "RATP Développement", "private", "Paris"),
    ("OP006", "Optile", "Optile Consortium", "consortium", "Paris"),
    ("OP007", "Île-de-France Mobilités", "Syndicat des transports d'Île-de-France", "authority", "Paris"),
    ("OP008", "Lacroix & Savac", "Groupe Lacroix-Savac", "private", "Bussy-Saint-Georges"),
    ("OP009", "Cars du Val de Marne", "RATP Dev Val de Marne", "private", "Créteil"),
    ("OP010", "Stivo", "Société de Transport Intercommunal du Val-d'Oise", "private", "Cergy"),
    ("OP011", "Albatrans", "Albatrans SARL", "private", "Mantes-la-Jolie"),
    ("OP012", "Sqybus", "Réseau Sqybus", "public", "Saint-Quentin-en-Yvelines"),
    ("OP013", "Phebus", "Réseau Phébus", "private", "Versailles"),
    ("OP014", "TRA", "Transports Rapides Automobiles", "private", "Boulogne"),
    ("OP015", "Noctilien", "Service RATP-SNCF Nuit", "public", "Paris"),
    ("OP016", "Orlyval", "RATP Orlyval", "public", "Orly"),
    ("OP017", "STILL", "Groupe Lacroix Seine-et-Marne", "private", "Melun"),
    ("OP018", "CSO", "Courriers de Seine-et-Oise", "private", "Poissy"),
    ("OP019", "Filéo", "Service Aéroport CDG", "private", "Roissy"),
    ("OP020", "CDG Val", "Aéroports de Paris VAL", "public", "Roissy"),
]


def gen_operators():
    rows = []
    for op_id, name, full_name, op_type, city in OPERATORS_DATA:
        rows.append({
            "operator_id": op_id,
            "operator_name": name,
            "full_name": full_name,
            "operator_type": op_type,
            "headquarters_city": city,
            "founded_year": random.randint(1900, 2010),
            "employee_count": random.randint(200, 45000),
            "website": f"https://www.{name.lower().replace(' ', '-').replace('é', 'e')}.fr",
        })
    return write_csv("transport_operator.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 2. TRANSPORT_MODE (8 rows)
# ──────────────────────────────────────────────────────────────────────────────
MODES_DATA = [
    ("MODE01", "Metro", "Underground metro rail", "rail", True),
    ("MODE02", "RER", "Réseau Express Régional", "rail", True),
    ("MODE03", "Tramway", "Light rail / tramway", "rail", True),
    ("MODE04", "Bus", "Urban and suburban bus", "road", True),
    ("MODE05", "Noctilien", "Night bus network", "road", True),
    ("MODE06", "Transilien", "Suburban heavy rail", "rail", True),
    ("MODE07", "Funicular", "Montmartre funicular", "rail", True),
    ("MODE08", "VAL", "Automated light rail (Orlyval, CDG Val)", "rail", True),
]


def gen_modes():
    rows = []
    for mode_id, name, desc, category, active in MODES_DATA:
        rows.append({
            "mode_id": mode_id,
            "mode_name": name,
            "description": desc,
            "category": category,
            "is_active": active,
            "avg_speed_kmh": random.randint(15, 80),
            "accessibility_compliant": random.choice([True, True, True, False]),
        })
    return write_csv("transport_mode.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 3. LINE (60 rows) — Metro 1-14, RER A-E, Tram T1-T13, plus bus lines
# ──────────────────────────────────────────────────────────────────────────────
def gen_lines():
    rows = []
    metro_colors = {
        "1": "#FFCD00", "2": "#003CA6", "3": "#837902", "3bis": "#6EC4E8",
        "4": "#CF009E", "5": "#FF7E2E", "6": "#6ECA97", "7": "#FA9ABA",
        "7bis": "#6ECA97", "8": "#E19BDF", "9": "#B6BD00", "10": "#C9910D",
        "11": "#704B1C", "12": "#007852", "13": "#6EC4E8", "14": "#62259D",
    }
    line_id = 0
    for num, color in metro_colors.items():
        line_id += 1
        rows.append({
            "line_id": f"L{line_id:03d}",
            "line_code": f"M{num}",
            "line_name": f"Métro ligne {num}",
            "mode_id": "MODE01",
            "operator_id": "OP001",
            "color_hex": color,
            "opening_year": random.randint(1900, 2024),
            "line_length_km": round(random.uniform(4.0, 25.0), 1),
            "station_count": random.randint(8, 30),
            "avg_daily_ridership": random.randint(100000, 1500000),
            "is_active": True,
        })

    rer_lines = [("A", "OP001"), ("B", "OP002"), ("C", "OP002"), ("D", "OP002"), ("E", "OP002")]
    for code, op in rer_lines:
        line_id += 1
        rows.append({
            "line_id": f"L{line_id:03d}",
            "line_code": f"RER{code}",
            "line_name": f"RER ligne {code}",
            "mode_id": "MODE02",
            "operator_id": op,
            "color_hex": random.choice(["#E2231A", "#4B92DB", "#FFCD00", "#00814F", "#C04191"]),
            "opening_year": random.randint(1969, 1999),
            "line_length_km": round(random.uniform(50.0, 120.0), 1),
            "station_count": random.randint(25, 60),
            "avg_daily_ridership": random.randint(300000, 1200000),
            "is_active": True,
        })

    trams = ["T1", "T2", "T3a", "T3b", "T4", "T5", "T6", "T7", "T8", "T9", "T10", "T11", "T13"]
    for t in trams:
        line_id += 1
        rows.append({
            "line_id": f"L{line_id:03d}",
            "line_code": t,
            "line_name": f"Tramway {t}",
            "mode_id": "MODE03",
            "operator_id": random.choice(["OP001", "OP003", "OP004"]),
            "color_hex": f"#{random.randint(0, 0xFFFFFF):06X}",
            "opening_year": random.randint(1992, 2023),
            "line_length_km": round(random.uniform(5.0, 25.0), 1),
            "station_count": random.randint(10, 30),
            "avg_daily_ridership": random.randint(20000, 200000),
            "is_active": True,
        })

    bus_lines = [
        "20", "21", "26", "27", "29", "38", "42", "56", "62", "63",
        "69", "72", "80", "87", "91", "95", "96", "183", "258", "350", "351",
    ]
    for b in bus_lines:
        line_id += 1
        rows.append({
            "line_id": f"L{line_id:03d}",
            "line_code": f"Bus{b}",
            "line_name": f"Bus ligne {b}",
            "mode_id": "MODE04",
            "operator_id": random.choice(["OP001", "OP003", "OP004", "OP006"]),
            "color_hex": "#36A2EB",
            "opening_year": random.randint(1950, 2020),
            "line_length_km": round(random.uniform(5.0, 30.0), 1),
            "station_count": random.randint(15, 50),
            "avg_daily_ridership": random.randint(5000, 80000),
            "is_active": True,
        })

    return write_csv("line.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 4. STATION (250 rows)
# ──────────────────────────────────────────────────────────────────────────────
STATION_NAMES = [
    "Châtelet", "Gare du Nord", "Gare de Lyon", "Saint-Lazare", "Montparnasse",
    "République", "Bastille", "Nation", "Opéra", "Concorde",
    "La Défense", "Charles de Gaulle-Étoile", "Trocadéro", "Belleville", "Ménilmontant",
    "Père Lachaise", "Gambetta", "Place d'Italie", "Bercy", "Bibliothèque",
    "Invalides", "Champs-Élysées-Clemenceau", "Franklin D. Roosevelt", "Madeleine", "Pyramides",
    "Palais Royal-Musée du Louvre", "Arts et Métiers", "Réaumur-Sébastopol", "Strasbourg-Saint-Denis", "Oberkampf",
    "Voltaire", "Charonne", "Porte de Vincennes", "Château de Vincennes", "Mairie de Montreuil",
    "Vincennes", "Fontenay-sous-Bois", "Nogent-sur-Marne", "Le Perreux-Nogent", "Bry-sur-Marne",
    "Noisy-le-Grand", "Torcy", "Marne-la-Vallée", "Val de Fontenay", "Neuilly-Plaisance",
    "Rosny-Bois-Perrier", "Villemomble", "Le Raincy", "Gagny", "Chelles",
    "Auber", "Haussmann-Saint-Lazare", "Magenta", "Gare de l'Est", "Châtelet-Les Halles",
    "Luxembourg", "Port-Royal", "Denfert-Rochereau", "Cité Universitaire", "Gentilly",
    "Laplace", "Arcueil-Cachan", "Bagneux", "Bourg-la-Reine", "Sceaux",
    "Robinson", "Antony", "Les Baconnets", "Massy-Verrières", "Massy-Palaiseau",
    "Orsay-Ville", "Le Guichet", "Saint-Rémy-lès-Chevreuse", "Aéroport CDG 1", "Aéroport CDG 2",
    "Villepinte", "Parc des Expositions", "Sevran", "Aulnay-sous-Bois", "Le Blanc-Mesnil",
    "Drancy", "Le Bourget", "La Courneuve", "Stade de France", "Saint-Denis",
    "Épinay", "Enghien-les-Bains", "Ermont-Eaubonne", "Saint-Gratien", "Argenteuil",
    "Sartrouville", "Maisons-Laffitte", "Poissy", "Cergy-Préfecture", "Cergy-le-Haut",
    "Pontoise", "Saint-Germain-en-Laye", "Le Vésinet", "Chatou", "Nanterre-Préfecture",
    "Nanterre-Université", "Rueil-Malmaison", "Suresnes", "Puteaux", "Courbevoie",
    "Boulogne-Jean Jaurès", "Boulogne-Pont de Saint-Cloud", "Issy-Val de Seine", "Clamart", "Meudon",
    "Chaville", "Versailles-Château", "Versailles-Chantiers", "Saint-Cloud", "Garches",
    "Vaucresson", "Marnes-la-Coquette", "Javel-André Citroën", "Boucicaut", "Lourmel",
    "Convention", "Vaugirard", "Pasteur", "Sèvres-Lecourbe", "Cambronne",
    "La Motte-Picquet", "Dupleix", "Bir-Hakeim", "Passy", "Ranelagh",
    "Jasmin", "Mirabeau", "Église d'Auteuil", "Michel-Ange-Auteuil", "Michel-Ange-Molitor",
    "Exelmans", "Porte de Saint-Cloud", "Marcel Sembat", "Billancourt", "Pont de Sèvres",
    "Bobigny", "Pantin", "Aubervilliers", "Romainville", "Les Lilas",
    "Pré-Saint-Gervais", "Mairie des Lilas", "Porte des Lilas", "Télégraphe", "Place des Fêtes",
    "Botzaris", "Buttes-Chaumont", "Laumière", "Jaurès", "Stalingrad",
    "Riquet", "Crimée", "Corentin Cariou", "Porte de la Villette", "Porte de Pantin",
    "Porte de la Chapelle", "Marx Dormoy", "Marcadet-Poissonniers", "Barbès-Rochechouart", "Pigalle",
    "Blanche", "Place de Clichy", "La Fourche", "Guy Môquet", "Porte de Saint-Ouen",
    "Garibaldi", "Mairie de Saint-Ouen", "Carrefour Pleyel", "Saint-Denis-Pleyel", "Villejuif",
    "Ivry-sur-Seine", "Vitry-sur-Seine", "Choisy-le-Roi", "Orly", "Aéroport d'Orly",
    "Créteil", "Maisons-Alfort", "Charenton", "Saint-Mandé", "Bérault",
    "Porte Dorée", "Porte de Charenton", "Liberté", "Mairie de Créteil",
    "Olympiades", "Tolbiac", "Nationale", "Chevaleret", "Quai de la Gare",
    "Cour Saint-Émilion", "Dugommier", "Reuilly-Diderot", "Faidherbe-Chaligny", "Ledru-Rollin",
    "Gare d'Austerlitz", "Saint-Marcel", "Les Gobelins", "Censier-Daubenton", "Monge",
    "Cardinal Lemoine", "Jussieu", "Cluny-La Sorbonne", "Odéon", "Saint-Michel",
    "Pont Neuf", "Louvre-Rivoli", "Étienne Marcel", "Sentier", "Bonne Nouvelle",
    "Poissonnière", "Cadet", "Le Peletier", "Richelieu-Drouot", "Grands Boulevards",
    "Bourse", "Quatre-Septembre", "Havre-Caumartin", "Liège", "Rome",
    "Villiers", "Monceau", "Wagram", "Pereire", "Porte Maillot",
    "Les Sablons", "Pont de Neuilly", "Esplanade de La Défense", "Mairie de Levallois", "Anatole France",
    "Louise Michel", "Porte de Champerret", "Ternes", "Courcelles",
    "George V", "Alma-Marceau", "Iéna",
    "Boissière", "Victor Hugo", "Argentine",
    "Porte Dauphine", "Avenue Foch", "Rue de la Pompe",
]


def gen_stations():
    rows = []
    communes = [
        "Paris 1er", "Paris 2e", "Paris 3e", "Paris 4e", "Paris 5e", "Paris 6e",
        "Paris 7e", "Paris 8e", "Paris 9e", "Paris 10e", "Paris 11e", "Paris 12e",
        "Paris 13e", "Paris 14e", "Paris 15e", "Paris 16e", "Paris 17e", "Paris 18e",
        "Paris 19e", "Paris 20e", "La Défense", "Boulogne-Billancourt", "Saint-Denis",
        "Montreuil", "Créteil", "Nanterre", "Versailles", "Argenteuil", "Cergy",
        "Massy", "Antony", "Vincennes", "Bobigny", "Ivry-sur-Seine", "Orly",
    ]
    zones = ["1", "1", "1", "1", "2", "2", "3", "3", "4", "5"]
    for i, name in enumerate(STATION_NAMES[:250]):
        lat = round(48.8 + random.uniform(-0.15, 0.15), 6)
        lon = round(2.35 + random.uniform(-0.25, 0.25), 6)
        rows.append({
            "station_id": f"ST{i+1:04d}",
            "station_name": name,
            "commune": random.choice(communes),
            "zone": random.choice(zones),
            "latitude": lat,
            "longitude": lon,
            "is_accessible": random.choice([True, True, True, False]),
            "has_elevator": random.choice([True, False]),
            "has_bike_parking": random.choice([True, True, False]),
            "opening_year": random.randint(1900, 2024),
            "annual_traffic": random.randint(50000, 50000000),
        })
    return write_csv("station.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 5. LINE_STATION (600 rows) — Links lines to stations (many-to-many)
# ──────────────────────────────────────────────────────────────────────────────
def gen_line_stations(lines, stations):
    rows = []
    seen = set()
    ls_id = 0
    for line in lines:
        n_stops = random.randint(8, min(30, len(stations)))
        stops = random.sample(stations, n_stops)
        for seq, st in enumerate(stops, 1):
            key = (line["line_id"], st["station_id"])
            if key in seen:
                continue
            seen.add(key)
            ls_id += 1
            rows.append({
                "line_station_id": f"LS{ls_id:04d}",
                "line_id": line["line_id"],
                "station_id": st["station_id"],
                "sequence_order": seq,
                "is_terminus": seq == 1 or seq == n_stops,
                "avg_dwell_time_sec": random.randint(20, 60),
            })
    return write_csv("line_station.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 6. SCHEDULE (500 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_schedules(lines):
    rows = []
    day_types = ["weekday", "saturday", "sunday_holiday"]
    periods = ["peak_morning", "off_peak_day", "peak_evening", "off_peak_night", "all_day"]
    for i in range(500):
        line = random.choice(lines)
        rows.append({
            "schedule_id": f"SCH{i+1:04d}",
            "line_id": line["line_id"],
            "day_type": random.choice(day_types),
            "period": random.choice(periods),
            "first_departure": random_time(5, 6),
            "last_departure": random_time(22, 24 if random.random() < 0.2 else 23),
            "frequency_minutes": random.choice([2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30]),
            "valid_from": random_date(2024, 2024),
            "valid_to": random_date(2025, 2025),
        })
    return write_csv("schedule.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 7. VEHICLE (120 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_vehicles():
    rows = []
    fleet_types = [
        ("MP14", "metro", 722, 2024), ("MP89", "metro", 583, 1997),
        ("MP73", "metro", 583, 1974), ("MF01", "metro", 600, 2007),
        ("MF77", "metro", 560, 1978), ("MF67", "metro", 560, 1967),
        ("MI09", "rer", 1060, 2011), ("MI2N", "rer", 1300, 2003),
        ("MI79", "rer", 900, 1980), ("Citadis 402", "tramway", 304, 2006),
        ("Citadis 302", "tramway", 210, 1998), ("Alstom Régiolis", "transilien", 500, 2014),
        ("MAN Lion's City", "bus", 90, 2015), ("Iveco Urbanway", "bus", 100, 2017),
        ("Mercedes Citaro", "bus", 105, 2016), ("Bluebus 12m", "bus_electric", 80, 2020),
        ("Heuliez GX 337", "bus", 95, 2018), ("Solaris Urbino 18", "bus_articulated", 150, 2019),
    ]
    for i in range(120):
        ft = random.choice(fleet_types)
        rows.append({
            "vehicle_id": f"VH{i+1:04d}",
            "fleet_type": ft[0],
            "vehicle_category": ft[1],
            "capacity": ft[2],
            "manufacture_year": ft[3] + random.randint(-2, 4),
            "manufacturer": random.choice(["Alstom", "Bombardier", "CAF", "MAN", "Iveco", "Mercedes", "Heuliez", "Solaris"]),
            "energy_type": random.choice(["electric", "electric", "electric", "diesel", "hybrid", "hydrogen"]),
            "is_air_conditioned": random.choice([True, True, False]),
            "status": random.choice(["in_service", "in_service", "in_service", "maintenance", "retired"]),
            "last_maintenance_date": random_date(2024, 2025),
        })
    return write_csv("vehicle.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 8. TRIP (1500 rows) — Individual revenue trips (runs)
# ──────────────────────────────────────────────────────────────────────────────
def gen_trips(lines, vehicles):
    rows = []
    directions = ["outbound", "inbound"]
    for i in range(1500):
        line = random.choice(lines)
        rows.append({
            "trip_id": f"TR{i+1:05d}",
            "line_id": line["line_id"],
            "vehicle_id": random.choice(vehicles)["vehicle_id"],
            "direction": random.choice(directions),
            "trip_date": random_date(2024, 2025),
            "departure_time": random_time(5, 23),
            "arrival_time": random_time(5, 23),
            "trip_duration_min": random.randint(15, 90),
            "status": random.choice(["completed", "completed", "completed", "cancelled", "delayed", "in_progress"]),
        })
    return write_csv("trip.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 9. STOP_EVENT (3000 rows) — Real-time arrival/departure at a station
# ──────────────────────────────────────────────────────────────────────────────
def gen_stop_events(trips, stations):
    rows = []
    for i in range(3000):
        trip = random.choice(trips)
        station = random.choice(stations)
        delay = random.choices([0, 0, 0, 1, 2, 3, 5, 10], k=1)[0]
        rows.append({
            "stop_event_id": f"SE{i+1:05d}",
            "trip_id": trip["trip_id"],
            "station_id": station["station_id"],
            "scheduled_arrival": random_time(),
            "actual_arrival": random_time(),
            "delay_seconds": delay * 60,
            "passenger_boarding": random.randint(0, 200),
            "passenger_alighting": random.randint(0, 200),
            "platform": random.choice(["A", "B", "1", "2", "Quai Nord", "Quai Sud", ""]),
        })
    return write_csv("stop_event.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 10. TICKET_TYPE (15 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_ticket_types():
    tickets = [
        ("TT01", "Ticket t+", "single_ride", 2.15, "1-2"),
        ("TT02", "Carnet 10 t+", "multi_ride", 16.90, "1-2"),
        ("TT03", "Navigo Mois", "monthly_pass", 86.40, "all"),
        ("TT04", "Navigo Annuel", "annual_pass", 950.40, "all"),
        ("TT05", "Navigo Semaine", "weekly_pass", 30.75, "all"),
        ("TT06", "Navigo Jour", "day_pass", 8.65, "all"),
        ("TT07", "Ticket Jeunes WE", "youth_weekend", 4.60, "all"),
        ("TT08", "Paris Visite 1j", "tourist_1day", 13.95, "1-3"),
        ("TT09", "Paris Visite 2j", "tourist_2day", 22.65, "1-3"),
        ("TT10", "Paris Visite 5j", "tourist_5day", 43.30, "1-5"),
        ("TT11", "Navigo Easy", "rechargeable", 2.00, "variable"),
        ("TT12", "Imagine R", "student_annual", 380.00, "all"),
        ("TT13", "Améthyste", "senior_reduced", 24.00, "all"),
        ("TT14", "Solidarité Transport", "social_reduced", 21.25, "all"),
        ("TT15", "Ticket Aéroport", "airport_single", 11.45, "1-5"),
    ]
    rows = []
    for tt_id, name, cat, price, zones in tickets:
        rows.append({
            "ticket_type_id": tt_id,
            "ticket_name": name,
            "category": cat,
            "unit_price_eur": price,
            "valid_zones": zones,
            "is_contactless": tt_id not in ["TT01"],
            "max_transfers": 0 if cat == "single_ride" else -1,
        })
    return write_csv("ticket_type.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 11. VALIDATION (2000 rows) — Ticket validations (tap-in events)
# ──────────────────────────────────────────────────────────────────────────────
def gen_validations(stations, ticket_types):
    rows = []
    for i in range(2000):
        station = random.choice(stations)
        tt = random.choice(ticket_types)
        rows.append({
            "validation_id": f"VAL{i+1:05d}",
            "station_id": station["station_id"],
            "ticket_type_id": tt["ticket_type_id"],
            "validation_datetime": random_datetime(2024, 2025),
            "gate_id": f"G{random.randint(1, 20):02d}",
            "card_id": f"CARD{random.randint(100000, 999999)}",
            "is_entry": random.choice([True, True, True, False]),
        })
    return write_csv("validation.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 12. INCIDENT (300 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_incidents(lines, stations):
    rows = []
    inc_types = [
        "signal_failure", "track_fault", "rolling_stock_issue", "passenger_incident",
        "power_outage", "door_malfunction", "medical_emergency", "suspicious_package",
        "strike_action", "weather_disruption", "trespasser_on_track", "overcrowding",
    ]
    severities = ["minor", "moderate", "major", "critical"]
    for i in range(300):
        rows.append({
            "incident_id": f"INC{i+1:04d}",
            "line_id": random.choice(lines)["line_id"],
            "station_id": random.choice(stations)["station_id"] if random.random() < 0.7 else "",
            "incident_type": random.choice(inc_types),
            "severity": random.choice(severities),
            "description": f"Incident reported on the network requiring attention.",
            "start_datetime": random_datetime(2024, 2025),
            "end_datetime": random_datetime(2024, 2025),
            "duration_minutes": random.randint(5, 240),
            "passengers_affected": random.randint(0, 50000),
            "status": random.choice(["resolved", "resolved", "resolved", "ongoing", "under_investigation"]),
        })
    return write_csv("incident.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 13. MAINTENANCE_TASK (200 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_maintenance(vehicles, stations):
    rows = []
    task_types = [
        "preventive_inspection", "corrective_repair", "overhaul", "cleaning",
        "brake_check", "door_repair", "hvac_service", "software_update",
        "wheel_profile", "pantograph_check",
    ]
    for i in range(200):
        is_vehicle = random.random() < 0.7
        rows.append({
            "task_id": f"MT{i+1:04d}",
            "vehicle_id": random.choice(vehicles)["vehicle_id"] if is_vehicle else "",
            "station_id": "" if is_vehicle else random.choice(stations)["station_id"],
            "task_type": random.choice(task_types),
            "priority": random.choice(["low", "medium", "high", "urgent"]),
            "scheduled_date": random_date(2024, 2025),
            "completed_date": random_date(2024, 2025) if random.random() < 0.8 else "",
            "cost_eur": round(random.uniform(50, 50000), 2),
            "technician_id": f"TECH{random.randint(1, 50):03d}",
            "status": random.choice(["completed", "completed", "in_progress", "scheduled", "cancelled"]),
        })
    return write_csv("maintenance_task.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 14. PASSENGER_SURVEY (400 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_surveys(lines, stations):
    rows = []
    aspects = ["punctuality", "cleanliness", "safety", "information", "comfort", "accessibility", "frequency", "crowding"]
    for i in range(400):
        rows.append({
            "survey_id": f"SV{i+1:04d}",
            "line_id": random.choice(lines)["line_id"],
            "station_id": random.choice(stations)["station_id"],
            "survey_date": random_date(2024, 2025),
            "aspect": random.choice(aspects),
            "rating": random.randint(1, 10),
            "comment": random.choice([
                "Service was on time and clean.",
                "Too crowded during rush hour.",
                "Elevator out of service again.",
                "Very comfortable new rolling stock.",
                "Information displays were not working.",
                "Staff was helpful and professional.",
                "Needs more frequent service on weekends.",
                "",
            ]),
            "respondent_age_group": random.choice(["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]),
            "respondent_commute_frequency": random.choice(["daily", "weekly", "occasional", "first_time"]),
        })
    return write_csv("passenger_survey.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 15. ACCESSIBILITY_EQUIPMENT (180 rows)
# ──────────────────────────────────────────────────────────────────────────────
def gen_accessibility(stations):
    rows = []
    equip_types = ["elevator", "escalator", "tactile_strip", "audio_announcement", "wheelchair_ramp", "induction_loop"]
    for i in range(180):
        rows.append({
            "equipment_id": f"EQ{i+1:04d}",
            "station_id": random.choice(stations)["station_id"],
            "equipment_type": random.choice(equip_types),
            "location_description": random.choice(["Main entrance", "Platform level", "Mezzanine", "Exit B", "Corridor to line transfer"]),
            "installation_date": random_date(2005, 2024),
            "last_inspection_date": random_date(2024, 2025),
            "status": random.choice(["operational", "operational", "operational", "out_of_service", "under_repair"]),
            "manufacturer": random.choice(["Otis", "Schindler", "Kone", "ThyssenKrupp", "Mitsubishi"]),
        })
    return write_csv("accessibility_equipment.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# 16. TRAFFIC_DAILY (500 rows) — Daily ridership per line
# ──────────────────────────────────────────────────────────────────────────────
def gen_traffic(lines):
    rows = []
    for i in range(500):
        line = random.choice(lines)
        base = line["avg_daily_ridership"]
        rows.append({
            "traffic_id": f"TD{i+1:04d}",
            "line_id": line["line_id"],
            "traffic_date": random_date(2024, 2025),
            "total_ridership": int(base * random.uniform(0.7, 1.3)),
            "peak_hour_ridership": int(base * random.uniform(0.15, 0.3)),
            "off_peak_ridership": int(base * random.uniform(0.4, 0.6)),
            "day_type": random.choice(["weekday", "weekday", "weekday", "saturday", "sunday"]),
        })
    return write_csv("traffic_daily.csv", rows, list(rows[0].keys()))


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Generating Île-de-France Public Transport Dataset")
    print("=" * 60)

    operators = gen_operators()
    modes = gen_modes()
    lines = gen_lines()
    stations = gen_stations()
    line_stations = gen_line_stations(lines, stations)
    schedules = gen_schedules(lines)
    vehicles = gen_vehicles()
    trips = gen_trips(lines, vehicles)
    stop_events = gen_stop_events(trips, stations)
    ticket_types = gen_ticket_types()
    validations = gen_validations(stations, ticket_types)
    incidents = gen_incidents(lines, stations)
    maintenance = gen_maintenance(vehicles, stations)
    surveys = gen_surveys(lines, stations)
    accessibility = gen_accessibility(stations)
    traffic = gen_traffic(lines)

    total = sum(len(x) for x in [
        operators, modes, lines, stations, line_stations, schedules,
        vehicles, trips, stop_events, ticket_types, validations,
        incidents, maintenance, surveys, accessibility, traffic,
    ])
    print(f"\n{'=' * 60}")
    print(f"Total: {total} rows across 16 tables")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
