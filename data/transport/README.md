# Île-de-France Public Transport Dataset

Realistic public transport dataset inspired by open data from
[Île-de-France Mobilités](https://data.iledefrance-mobilites.fr/) and
[RATP](https://data.ratp.fr/).

## Domain

Regional public transport operations covering metro, RER, tramway, bus, and suburban rail
services across the Île-de-France (greater Paris) region.

## Dataset Structure

### Reference Tables (5)

| Table | Description | Rows |
|---|---|---|
| `transport_operator` | Contracted transport companies (RATP, SNCF, Keolis, Transdev, etc.) | 20 |
| `transport_mode` | Transport modes (Metro, RER, Tramway, Bus, Noctilien, etc.) | 8 |
| `line` | Individual service lines (M1–M14, RER A–E, T1–T13, buses) | 55 |
| `station` | Network stations and stops with coordinates and zone | 247 |
| `ticket_type` | Fare products (Navigo, t+, Paris Visite, Imagine R, etc.) | 15 |

### Network Structure Tables (3)

| Table | Description | Rows |
|---|---|---|
| `line_station` | Line-to-station mapping (network graph edges) | 1,018 |
| `schedule` | Service timetable rules (frequency, day type, period) | 500 |
| `vehicle` | Rolling stock and bus fleet (MP14, MI09, Citadis, etc.) | 120 |

### Operations Tables (5)

| Table | Description | Rows |
|---|---|---|
| `trip` | Individual revenue service runs | 1,500 |
| `stop_event` | Real-time arrival/departure at stations | 3,000 |
| `validation` | Ticket tap-in events | 2,000 |
| `incident` | Network disruptions and events | 300 |
| `maintenance_task` | Vehicle and station maintenance tasks | 200 |

### Analytics Tables (3)

| Table | Description | Rows |
|---|---|---|
| `passenger_survey` | Satisfaction ratings and feedback | 400 |
| `accessibility_equipment` | Station accessibility infrastructure | 180 |
| `traffic_daily` | Line-level daily ridership figures | 500 |

**Total: ~10,063 rows across 16 tables**

## Key Relationships

```
Transport Operator ──(1:N)──► Line
Transport Mode ──(1:N)──► Line
Line ──(M:N)──► Station          [via Line Station]
Line ──(1:N)──► Schedule
Line ──(1:N)──► Trip
Trip ──(N:1)──► Vehicle
Trip ──(1:N)──► Stop Event
Stop Event ──(N:1)──► Station
Station ──(1:N)──► Validation
Ticket Type ──(1:N)──► Validation
Station ──(1:N)──► Accessibility Equipment
Line ──(1:N)──► Incident
Station ──(1:N)──► Incident
Vehicle ──(1:N)──► Maintenance Task
Station ──(1:N)──► Maintenance Task
Line ──(1:N)──► Traffic Daily
Line ──(1:N)──► Passenger Survey
```

## Unstructured Documents

The business description is split into 3 documents (both Markdown and PDF):

| Document | Content | Pages |
|---|---|---|
| `01_network_and_operations` | Operators, modes, lines, stations, fleet, scheduling | 8 |
| `02_ticketing_and_ridership` | Fare products, validations, traffic, surveys, accessibility | 9 |
| `03_maintenance_incidents_data` | Trips, incidents, maintenance, data model, KPIs, ER overview | 12 |

## Databricks Unity Catalog

- **Catalog:** `benoit_cayla`
- **Schema:** `idf_transport`
- **Volume:** `transport_data`

### Loading Options

1. **Databricks Notebook:** Use `load_data.py` in a Databricks workspace
2. **CLI Script:** Run `python3 create_databricks_tables.py` (requires Databricks CLI authentication)

### Analytical Views

| View | Purpose |
|---|---|
| `v_line_performance` | Trip counts, delays, and cancellations per line |
| `v_station_activity` | Validation counts, incidents, and equipment status per station |
| `v_daily_operations` | Daily ridership with incident and delay metrics |

## Ontology Candidates

This dataset supports several ontology modelling patterns:

- **Network topology** — Stations, lines, and their connections as a directed graph
- **Multimodal routing** — Transfers between modes at interchange stations
- **Passenger journey** — Validation → Trip → Stop Event chain
- **Asset management** — Vehicle lifecycle with trips and maintenance
- **Service quality** — Surveys, incidents, and accessibility tied to stations and lines
- **Revenue and demand** — Ticket types, validations, and daily traffic patterns

## Data Sources

Inspired by:
- [Île-de-France Mobilités Open Data](https://data.iledefrance-mobilites.fr/) — Network structure, GTFS data, ridership
- [RATP Open Data](https://data.ratp.fr/) — Station traffic, line correspondences
- [PRIM](https://prim.iledefrance-mobilites.fr/) — Real-time feeds and API documentation

## Generation

```bash
cd data/transport
python3 generate_data.py
```

Regenerates all 16 CSV files with deterministic seed (2024).
