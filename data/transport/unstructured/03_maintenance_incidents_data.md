# TransportIDF — Réseau de Transport d'Île-de-France

## Part 3: Maintenance, Incidents and Data Integration

**Internal Document — Engineering & Data Governance Department**

| | |
|---|---|
| **Document Owner** | Direction Technique & Data Governance — TransportIDF |
| **Classification** | Internal Use Only |
| **Version** | 1.0 |
| **Last Updated** | March 2025 |
| **Review Cycle** | Semi-Annual |

---

## Table of Contents

1. [Trip Operations and Real-Time Tracking](#1-trip-operations-and-real-time-tracking)
2. [Incident Management](#2-incident-management)
3. [Fleet Maintenance](#3-fleet-maintenance)
4. [Data Model Summary](#4-data-model-summary)
5. [Data Integration Challenges](#5-data-integration-challenges)
6. [Key Performance Indicators](#6-key-performance-indicators)
7. [Appendix: Entity Relationship Overview](#7-appendix-entity-relationship-overview)

---

## 1. Trip Operations and Real-Time Tracking

### Trip Records

Every revenue service run is recorded as a **trip** in the operational database:

| Field | Description |
|---|---|
| Trip ID | Unique trip identifier |
| Line ID | Which line the trip operates on |
| Vehicle ID | Assigned vehicle |
| Direction | Outbound or inbound |
| Trip Date | Date of service |
| Departure / Arrival Time | Scheduled start and end |
| Duration (minutes) | Total trip time |
| Status | Completed, cancelled, delayed, or in progress |

With **1,500 trip records**, the dataset models a representative sample of daily operations.

### Trip Status Distribution

| Status | Share | Description |
|---|---|---|
| Completed | ~75% | Trip ran as scheduled |
| Delayed | ~12% | Trip ran with significant delay |
| Cancelled | ~8% | Trip was suppressed |
| In Progress | ~5% | Currently running (real-time snapshot) |

### Stop Events: Real-Time Precision

Each trip generates multiple **stop events** — one per station visited:

| Field | Description |
|---|---|
| Stop Event ID | Unique identifier |
| Trip ID | Parent trip |
| Station ID | Station where the event occurred |
| Scheduled Arrival | Timetable arrival time |
| Actual Arrival | Real-time recorded arrival |
| Delay (seconds) | Difference between scheduled and actual |
| Passenger Boarding | Passengers entering at this stop |
| Passenger Alighting | Passengers exiting at this stop |
| Platform | Platform or quai identifier |

With **3,000 stop event records**, this table provides the foundation for:

- **Punctuality metrics** — Percentage of stops served within 2 minutes of schedule
- **Load profiling** — Passenger counts by stop to identify crowding hotspots
- **Dwell time analysis** — Time spent at each station for schedule optimisation
- **Delay propagation** — How delays cascade through the line

### Real-Time Data Sources

Stop event data in production systems is fed by:

- **SIEL** (Système d'Information En Ligne) — Real-time passenger information
- **SAE** (Système d'Aide à l'Exploitation) — Operations management system
- **GTFS-RT** — Real-time feed published on [PRIM](https://prim.iledefrance-mobilites.fr/) (Plateforme Régionale d'Information Multimodale)

---

## 2. Incident Management

### Incident Taxonomy

Network disruptions are classified into **12 incident types**:

| Incident Type | Description | Typical Severity |
|---|---|---|
| Signal Failure | Signalling system malfunction | Major |
| Track Fault | Rail or track infrastructure defect | Major |
| Rolling Stock Issue | Vehicle breakdown or malfunction | Moderate |
| Passenger Incident | Medical emergency, altercation | Minor–Moderate |
| Power Outage | Traction power loss | Critical |
| Door Malfunction | Train door failure | Minor |
| Medical Emergency | Passenger requiring medical attention | Moderate |
| Suspicious Package | Security alert | Major–Critical |
| Strike Action | Industrial action by staff | Critical |
| Weather Disruption | Storm, flooding, extreme heat | Major |
| Trespasser on Track | Unauthorised person on tracks | Major |
| Overcrowding | Platform or vehicle capacity exceeded | Minor |

### Severity Levels

| Severity | Impact | Response Time | Escalation |
|---|---|---|---|
| Minor | < 5 min delay, localised | < 15 min | Station controller |
| Moderate | 5–30 min delay, line section | < 30 min | Line operations centre |
| Major | 30–120 min, full line | Immediate | Network operations centre |
| Critical | > 2 hours, multiple lines | Immediate | Crisis management team |

### Incident Record Structure

| Field | Description |
|---|---|
| Incident ID | Unique identifier |
| Line ID | Affected line |
| Station ID | Affected station (if applicable) |
| Incident Type | Classification |
| Severity | Minor, moderate, major, critical |
| Description | Free-text description |
| Start / End Datetime | Duration of the incident |
| Duration (minutes) | Total disruption time |
| Passengers Affected | Estimated impacted passengers |
| Status | Resolved, ongoing, or under investigation |

### Incident Statistics (Annual)

| Metric | Value |
|---|---|
| Total incidents (dataset) | 300 |
| Average duration | ~65 minutes |
| Most common type | Rolling stock issues (~18%) |
| Most common severity | Moderate (~35%) |
| Incidents on metro lines | ~45% of total |
| Peak incident hours | 07:00–09:00 and 17:00–19:00 |

With **300 incident records**, this dataset enables disruption pattern analysis, root cause investigation, and resilience planning.

---

## 3. Fleet Maintenance

### Maintenance Strategy

TransportIDF operates a **mixed maintenance strategy** combining:

1. **Preventive maintenance** — Scheduled inspections at fixed intervals
2. **Corrective maintenance** — Reactive repairs following breakdowns
3. **Predictive maintenance** — Data-driven anticipation of failures (emerging)

### Maintenance Task Types

| Task Type | Description | Avg. Cost (EUR) |
|---|---|---|
| Preventive Inspection | Regular scheduled check | 500–2,000 |
| Corrective Repair | Fix after failure | 1,000–15,000 |
| Overhaul | Major component replacement | 10,000–50,000 |
| Cleaning | Interior/exterior cleaning | 50–300 |
| Brake Check | Safety-critical brake inspection | 200–800 |
| Door Repair | Door mechanism repair | 500–3,000 |
| HVAC Service | Air conditioning maintenance | 300–2,000 |
| Software Update | Onboard systems firmware update | 100–500 |
| Wheel Profile | Wheel wear measurement and turning | 2,000–8,000 |
| Pantograph Check | Current collector inspection (rail) | 1,000–5,000 |

### Maintenance Record Structure

| Field | Description |
|---|---|
| Task ID | Unique identifier |
| Vehicle ID | Which vehicle (if vehicle-related) |
| Station ID | Which station (if infrastructure-related) |
| Task Type | Category of maintenance |
| Priority | Low, medium, high, urgent |
| Scheduled Date | Planned maintenance date |
| Completed Date | Actual completion date |
| Cost (EUR) | Task cost |
| Technician ID | Assigned maintenance technician |
| Status | Completed, in progress, scheduled, cancelled |

### Maintenance Facilities

| Facility | Location | Modes Served |
|---|---|---|
| Ateliers de Fontenay | Fontenay-sous-Bois | Metro (Lines 1, 14) |
| Centre de maintenance Bobigny | Bobigny | Metro (Lines 5, 11) |
| TMF Saint-Ouen | Saint-Ouen | Metro (Line 14 — MP14) |
| Centre bus Lagny | Paris 20e | Bus fleet |
| Ateliers de Villeneuve | Villeneuve-Saint-Georges | RER D |
| CIMT Sucy | Sucy-en-Brie | RER A (MI09, MI2N) |

### Maintenance KPIs

| KPI | Target | Current |
|---|---|---|
| Fleet availability rate | > 95% | 93.8% |
| Mean Time Between Failures (MTBF) | > 50,000 km (metro) | 48,200 km |
| Preventive/corrective ratio | > 70/30 | 65/35 |
| Maintenance cost per vehicle-km | < €0.15 | €0.17 |
| On-time task completion | > 90% | 87% |

With **200 maintenance task records**, this dataset supports fleet lifecycle management, cost analysis, and reliability engineering.

---

## 4. Data Model Summary

The TransportIDF data model comprises **16 tables** organised in four layers:

### Reference Data

| Entity | Description | Records |
|---|---|---|
| Transport Operator | Contracted transport companies | 20 |
| Transport Mode | Metro, RER, Bus, Tramway, etc. | 8 |
| Line | Individual service lines | 55 |
| Station | Network stations and stops | 247 |
| Ticket Type | Fare products | 15 |

### Network Structure

| Entity | Description | Records |
|---|---|---|
| Line Station | Line-to-station mapping (graph edges) | 1,018 |
| Schedule | Service timetable rules | 500 |
| Vehicle | Rolling stock and bus fleet | 120 |

### Operations

| Entity | Description | Records |
|---|---|---|
| Trip | Individual revenue service runs | 1,500 |
| Stop Event | Real-time arrival/departure per station | 3,000 |
| Validation | Ticket tap-in events | 2,000 |
| Incident | Network disruptions and events | 300 |
| Maintenance Task | Vehicle and station maintenance | 200 |

### Analytics

| Entity | Description | Records |
|---|---|---|
| Passenger Survey | Satisfaction ratings and feedback | 400 |
| Accessibility Equipment | Station accessibility infrastructure | 180 |
| Traffic Daily | Line-level daily ridership figures | 500 |

**Total: ~10,063 records across 16 tables**

---

## 5. Data Integration Challenges

### Challenge 1: Multi-Operator Data Harmonisation

With 20 operators using different IT systems, data formats vary significantly:

| Issue | Impact | Mitigation |
|---|---|---|
| Inconsistent station naming | Duplicate/ambiguous station references | Master station registry with canonical IDs |
| Different vehicle ID schemes | Cannot track fleet across operators | Unified vehicle registry |
| Varying schedule formats | Incompatible timetable data | GTFS standard as common format |
| Non-standard incident coding | Inconsistent disruption reporting | Shared incident taxonomy |

### Challenge 2: Real-Time vs. Historical Integration

| Data Type | Latency | Volume | System |
|---|---|---|---|
| Validations | Near-real-time | 8M/day | AFC (Automatic Fare Collection) |
| Stop events | Real-time (10s) | 500K/day | SIEL / SAE |
| Traffic daily | Next-day batch | 55 records/day | BI platform |
| Incidents | Real-time | 50–100/day | Operations centre |
| Surveys | Quarterly batch | 400/quarter | Survey platform |

### Challenge 3: Network Graph Complexity

The line-station relationship creates a complex graph with:

- **Multi-line stations** — Hubs served by 5+ lines (e.g., Châtelet)
- **Branching lines** — RER lines with multiple branches (e.g., RER B: CDG ↔ Robinson + Saint-Rémy)
- **Temporal variation** — Service patterns change by day type and time of day
- **Transfers** — Physical connections not always captured in logical line-station data

### Challenge 4: Unified Passenger Journey

A complete passenger journey may span:

```
Validation (entry) → Trip 1 (Metro) → Transfer → Trip 2 (RER) → Validation (exit)
```

Linking these events into a single journey requires matching card IDs, timestamps, and station-level transfer logic — a key use case for knowledge graph construction.

---

## 6. Key Performance Indicators

### Operational KPIs

| KPI | Target | Measurement |
|---|---|---|
| Punctuality (metro) | > 96% | Trains within 2 min of schedule |
| Punctuality (RER) | > 90% | Trains within 5 min of schedule |
| Punctuality (bus) | > 85% | Buses within 5 min of schedule |
| Service delivery rate | > 98% | % of planned trips actually run |
| Incident resolution time | < 60 min | Average time to resolve disruptions |
| Fleet availability | > 95% | Vehicles available for service |

### Customer KPIs

| KPI | Target | Measurement |
|---|---|---|
| Overall satisfaction | > 7.0/10 | Passenger survey average |
| Information quality | > 6.5/10 | Real-time display and announcement rating |
| Accessibility availability | > 97% | Equipment operational rate |
| Complaint resolution time | < 10 days | Customer service response time |

### Financial KPIs

| KPI | Target | Measurement |
|---|---|---|
| Farebox recovery ratio | > 40% | Fare revenue / operating cost |
| Revenue per trip | > €1.50 | Total fare revenue / total trips |
| Maintenance cost efficiency | < €0.15/veh-km | Total maintenance spend / vehicle-km |
| Fraud rate | < 5% | Estimated non-validated journeys |

---

## 7. Appendix: Entity Relationship Overview

### Primary Relationships

```
Transport Operator ──(1:N)──► Line
Transport Mode ──(1:N)──► Line
Line ──(M:N)──► Station  [via Line Station]
Line ──(1:N)──► Schedule
Line ──(1:N)──► Trip
Line ──(1:N)──► Incident
Line ──(1:N)──► Traffic Daily
Trip ──(N:1)──► Vehicle
Trip ──(1:N)──► Stop Event
Stop Event ──(N:1)──► Station
Station ──(1:N)──► Validation
Station ──(1:N)──► Accessibility Equipment
Station ──(1:N)──► Incident
Station ──(1:N)──► Passenger Survey
Ticket Type ──(1:N)──► Validation
Vehicle ──(1:N)──► Maintenance Task
Station ──(1:N)──► Maintenance Task
```

### End-to-End Data Flow

```
Operator → Line → Schedule → Trip → Stop Event
                                        ↓
                                    Station
                                   ╱   |   ╲
                          Validation  Incident  Accessibility
                              ↓
                         Ticket Type

Line → Traffic Daily (aggregated ridership)
Line → Passenger Survey (quality feedback)
Vehicle → Trip (assignment)
Vehicle → Maintenance Task (lifecycle)
```

### Cross-Entity Analytics

- **Passenger Journey:** Validation → Station → Line Station → Line → Trip → Stop Event
- **Network Performance:** Line → Trip (completion rate) → Stop Event (delays) → Incident (disruptions)
- **Fleet Lifecycle:** Vehicle → Trip (utilisation) → Maintenance Task (costs) → Incident (breakdowns)
- **Station 360:** Station → Validation (traffic) + Incident (disruptions) + Equipment (accessibility) + Survey (satisfaction)
- **Revenue Analysis:** Ticket Type → Validation → Station → Line (modal revenue share)

---

*This document is Part 3 of the TransportIDF business description series. Together with Part 1 (Network Infrastructure & Operations) and Part 2 (Ticketing, Ridership & Quality of Service), it provides a complete business description for ontology mapping and knowledge graph construction using OntoBricks.*
