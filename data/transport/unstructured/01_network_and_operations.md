# TransportIDF — Réseau de Transport d'Île-de-France

## Part 1: Network Infrastructure and Operations

**Internal Document — Operations Department**

| | |
|---|---|
| **Document Owner** | Direction des Opérations — TransportIDF |
| **Classification** | Internal Use Only |
| **Version** | 1.0 |
| **Last Updated** | March 2025 |
| **Review Cycle** | Semi-Annual |

---

## Table of Contents

1. [Organisation Overview](#1-organisation-overview)
2. [Transport Modes and Network Coverage](#2-transport-modes-and-network-coverage)
3. [Line Infrastructure](#3-line-infrastructure)
4. [Station Network](#4-station-network)
5. [Network Topology: Line-Station Mapping](#5-network-topology-line-station-mapping)
6. [Fleet and Rolling Stock](#6-fleet-and-rolling-stock)
7. [Scheduling and Service Planning](#7-scheduling-and-service-planning)

---

## 1. Organisation Overview

TransportIDF is the regional public transport authority for the Île-de-France region, modelled after [Île-de-France Mobilités](https://data.iledefrance-mobilites.fr/) and drawing operational data structures from [RATP](https://data.ratp.fr/) open data publications. The authority coordinates public transport services across the greater Paris metropolitan area, serving approximately **12 million residents** through a multimodal network of metro, RER, tramway, and bus services.

> **Our Mission:** Provide safe, reliable, and accessible public transport to every resident and visitor of Île-de-France through coordinated multimodal services and data-driven operations.

### Organisational Scope

| Metric | Value |
|---|---|
| Region Served | Île-de-France (12,012 km²) |
| Population Served | ~12 million |
| Transport Operators | 20 (public, private, consortium) |
| Transport Modes | 8 (Metro, RER, Tramway, Bus, Noctilien, Transilien, Funicular, VAL) |
| Active Lines | 55+ |
| Stations and Stops | 250+ major stations |
| Daily Ridership | ~8 million journeys |
| Annual Budget | €10.5 billion |

### Operator Ecosystem

Transport services are delivered by a network of **20 operators** under contract with the regional authority:

| Operator | Type | Scope |
|---|---|---|
| RATP | Public | Metro, Bus, Tramway, RER A/B |
| SNCF Transilien | Public | RER C/D/E, Transilien suburban rail |
| Keolis | Private (contracted) | Bus and tramway in outer suburbs |
| Transdev | Private (contracted) | Bus networks in multiple départements |
| RATP Dev | Private (subsidiary) | Operations in Val-de-Marne, Orly |
| Optile | Consortium | Coordination of 80+ private bus operators |
| Île-de-France Mobilités | Authority | Planning, pricing, regulation |

Smaller operators such as Lacroix & Savac, Stivo, Albatrans, Sqybus, and Phébus serve specific suburban corridors. Specialty services include **Noctilien** (night buses operated jointly by RATP and SNCF), **Orlyval** (automated light rail to Orly Airport), and **CDG Val** (airport internal shuttle).

---

## 2. Transport Modes and Network Coverage

The Île-de-France transport system operates across **8 distinct modes**, each with specific characteristics:

| Mode | Category | Avg. Speed | Accessibility | Fleet Type |
|---|---|---|---|---|
| **Metro** | Rail (underground) | 25–35 km/h | Partial (varies by line) | MP14, MP89, MF01, MF77 |
| **RER** | Rail (regional express) | 45–80 km/h | Full | MI09, MI2N, MI79 |
| **Tramway** | Rail (light rail) | 18–25 km/h | Full | Citadis 402, Citadis 302 |
| **Bus** | Road | 12–20 km/h | Full (low-floor) | MAN, Iveco, Mercedes, Bluebus |
| **Noctilien** | Road (night) | 15–25 km/h | Full | Standard bus fleet |
| **Transilien** | Rail (suburban) | 40–70 km/h | Partial | Alstom Régiolis |
| **Funicular** | Rail | N/A | Full | Dedicated car |
| **VAL** | Rail (automated) | 30–40 km/h | Full | Automated light metro |

### Modal Share (Typical Weekday)

| Mode | Daily Ridership | Share |
|---|---|---|
| Metro | 4.2 million | 52% |
| RER | 1.8 million | 22% |
| Bus | 1.2 million | 15% |
| Tramway | 0.6 million | 8% |
| Other (Transilien, VAL, Funicular, Noctilien) | 0.2 million | 3% |

---

## 3. Line Infrastructure

The network comprises **55+ active lines** spanning all modes:

### Metro Lines

The Paris Metro is the backbone of inner-city transport, with **16 lines** (lines 1–14, plus 3bis and 7bis):

| Line | Key Terminals | Length (km) | Stations | Daily Ridership |
|---|---|---|---|---|
| M1 | La Défense ↔ Château de Vincennes | 16.5 | 25 | 750,000+ |
| M4 | Porte de Clignancourt ↔ Bagneux | 12.1 | 27 | 600,000+ |
| M13 | Asnières/Saint-Denis ↔ Châtillon | 24.3 | 32 | 550,000+ |
| M14 | Aéroport d'Orly ↔ Saint-Denis-Pleyel | 14.0 | 9 | 500,000+ |

Lines are identified by a unique **line_code** (e.g., "M1", "M14") and associated with a colour hex code for map rendering. Each line records its **operator**, **mode**, **length in km**, **station count**, and **average daily ridership**.

### RER Lines

Five regional express lines connect Paris with the suburbs:

| Line | Operator | Coverage | Length (km) |
|---|---|---|---|
| RER A | RATP / SNCF | West (Saint-Germain, Cergy) ↔ East (Marne-la-Vallée) | 108 |
| RER B | RATP / SNCF | CDG Airport ↔ Saint-Rémy-lès-Chevreuse | 80 |
| RER C | SNCF | Versailles, Pontoise ↔ Dourdan, Juvisy | 187 |
| RER D | SNCF | Creil, Orry ↔ Melun, Malesherbes | 197 |
| RER E | SNCF | Chelles, Tournan ↔ Haussmann (Nanterre extension) | 56 |

### Tramway and Bus Lines

- **13 tramway lines** (T1 through T13) serve the inner and outer suburbs
- **21+ bus lines** tracked in the dataset, representing a sample of the 350+ bus routes operated across the region

---

## 4. Station Network

The dataset models **250 major stations and stops** across the Île-de-France region. Each station record includes:

| Field | Description |
|---|---|
| Station ID | Unique identifier (e.g., ST0001) |
| Station Name | Official name (e.g., "Châtelet", "Gare du Nord") |
| Commune | Municipality (e.g., "Paris 10e", "La Défense") |
| Zone | Fare zone (1–5, where 1 = central Paris) |
| Coordinates | Latitude / Longitude (WGS84) |
| Accessibility | Wheelchair accessible (boolean) |
| Has Elevator | Elevator availability |
| Has Bike Parking | Vélib' or bike racks nearby |
| Opening Year | Year the station entered service |
| Annual Traffic | Yearly passenger entries |

### Major Interchange Stations

| Station | Annual Traffic | Lines Served | Role |
|---|---|---|---|
| Châtelet-Les Halles | 33+ million | Metro 1, 4, 7, 11, 14 / RER A, B, D | Largest underground station in Europe |
| Gare du Nord | 50+ million | Metro 4, 5 / RER B, D, E / Transilien | Busiest station in Europe |
| Gare de Lyon | 30+ million | Metro 1, 14 / RER A, D | Major south-east hub |
| Saint-Lazare | 27+ million | Metro 3, 9, 12, 13, 14 / Transilien | Major western suburbs gateway |
| La Défense | 25+ million | Metro 1 / RER A / Tramway T2 | Business district hub |

### Zone Distribution

| Zone | Stations | Description |
|---|---|---|
| Zone 1 | ~120 | Central Paris (intra-muros) |
| Zone 2 | ~60 | Inner suburbs (petite couronne) |
| Zone 3 | ~40 | Middle ring |
| Zone 4 | ~20 | Outer suburbs |
| Zone 5 | ~10 | Remote areas and airports |

---

## 5. Network Topology: Line-Station Mapping

The **line_station** table models the many-to-many relationship between lines and stations, representing the network graph. Each record captures:

- **Line ID** and **Station ID** — The connection
- **Sequence order** — Position of the station on the line
- **Is terminus** — Whether the station is a line endpoint
- **Average dwell time** — Typical stop duration in seconds

With **~1,000 line-station associations**, this table enables:

- Route planning and shortest-path calculations
- Transfer point identification (stations served by multiple lines)
- Service coverage analysis
- Network graph construction for ontology mapping

---

## 6. Fleet and Rolling Stock

TransportIDF coordinates a fleet of **120+ tracked vehicles** across all operators:

### Fleet by Category

| Category | Fleet Types | Capacity Range | Energy |
|---|---|---|---|
| Metro | MP14, MP89, MP73, MF01, MF77, MF67 | 560–722 pax | Electric |
| RER | MI09, MI2N, MI79 | 900–1,300 pax | Electric |
| Tramway | Citadis 402, Citadis 302 | 210–304 pax | Electric |
| Bus | MAN Lion's City, Iveco Urbanway, Mercedes Citaro | 80–105 pax | Diesel / Hybrid / Electric |
| Bus (articulated) | Solaris Urbino 18 | 150 pax | Diesel / Hybrid |
| Bus (electric) | Bluebus 12m | 80 pax | Electric |
| Suburban Rail | Alstom Régiolis | 500 pax | Electric |

### Vehicle Lifecycle

Each vehicle records its **manufacture year**, **manufacturer** (Alstom, Bombardier, CAF, etc.), **energy type**, **air conditioning status**, and **maintenance status**:

| Status | Description |
|---|---|
| In Service | Active revenue operations |
| Maintenance | Undergoing scheduled or corrective maintenance |
| Retired | Withdrawn from service |

The fleet is undergoing a significant **energy transition**, with electric buses (Bluebus) and hydrogen prototypes gradually replacing diesel vehicles. The new **MP14** metro trains (manufactured by Alstom) are being deployed on Line 14 and will progressively equip other lines.

---

## 7. Scheduling and Service Planning

Service schedules define **when and how frequently** trains and buses run on each line:

| Field | Description |
|---|---|
| Line ID | Which line the schedule applies to |
| Day Type | Weekday, Saturday, or Sunday/Holiday |
| Period | Peak morning, off-peak day, peak evening, off-peak night, all-day |
| First / Last Departure | Service hours |
| Frequency (minutes) | Headway between consecutive vehicles |
| Valid From / To | Timetable validity period |

### Typical Frequencies

| Mode | Peak (min) | Off-Peak (min) | Night |
|---|---|---|---|
| Metro | 2–3 | 5–8 | Not running (except Line 1, 14 on weekends) |
| RER | 3–5 | 10–15 | Not running |
| Tramway | 4–6 | 8–12 | Not running |
| Bus | 5–10 | 10–20 | Noctilien every 30–60 min |

With **500 schedule records**, this dataset supports timetable analysis, frequency optimization, and service gap identification across the entire network.

---

*This document is Part 1 of the TransportIDF business description series. See Part 2 (Ticketing, Ridership & Quality of Service) and Part 3 (Maintenance, Incidents & Data Integration) for additional coverage.*
