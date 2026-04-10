# TransportIDF — Réseau de Transport d'Île-de-France

## Part 2: Ticketing, Ridership and Quality of Service

**Internal Document — Commercial & Customer Experience Department**

| | |
|---|---|
| **Document Owner** | Direction Commerciale — TransportIDF |
| **Classification** | Internal Use Only |
| **Version** | 1.0 |
| **Last Updated** | March 2025 |
| **Review Cycle** | Semi-Annual |

---

## Table of Contents

1. [Ticketing System Overview](#1-ticketing-system-overview)
2. [Fare Products and Pricing](#2-fare-products-and-pricing)
3. [Validation and Revenue Tracking](#3-validation-and-revenue-tracking)
4. [Ridership and Traffic Analytics](#4-ridership-and-traffic-analytics)
5. [Passenger Surveys and Satisfaction](#5-passenger-surveys-and-satisfaction)
6. [Accessibility and Universal Design](#6-accessibility-and-universal-design)

---

## 1. Ticketing System Overview

The Île-de-France transport network uses a unified ticketing ecosystem based on the **Navigo** contactless smartcard system, managed by Île-de-France Mobilités. The system supports multiple fare products and is interoperable across all modes (metro, RER, bus, tramway, and suburban rail).

### Ticketing Architecture

```
                  ┌─────────────────────┐
                  │  Île-de-France      │
                  │  Mobilités (IDFM)   │
                  │  Fare Authority      │
                  └──────┬──────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │  Navigo  │  │  Navigo  │  │ Ticket   │
    │  Pass    │  │  Easy    │  │ t+       │
    │ (monthly │  │(recharge │  │ (single  │
    │  annual) │  │  card)   │  │  paper)  │
    └──────────┘  └──────────┘  └──────────┘
          │              │              │
          └──────────────┼──────────────┘
                         ▼
              ┌────────────────────┐
              │  Gate / Validator  │
              │  (station entry)   │
              └────────────────────┘
```

Key components:

- **Navigo Pass** — Contactless smartcard for subscription products (monthly, annual, weekly)
- **Navigo Easy** — Rechargeable card for pay-per-ride products
- **Ticket t+** — Single-ride magnetic ticket (being phased out in favor of contactless)
- **Smartphones** — NFC-enabled phones for ticket storage via the Île-de-France Mobilités app

---

## 2. Fare Products and Pricing

The dataset models **15 fare products** covering the full range of travel needs:

### Subscription Products

| Product | Type | Price (EUR) | Valid Zones | Target |
|---|---|---|---|---|
| **Navigo Mois** | Monthly pass | 86.40 | All zones | Regular commuters |
| **Navigo Annuel** | Annual pass | 950.40 | All zones | Daily commuters (10% savings) |
| **Navigo Semaine** | Weekly pass | 30.75 | All zones | Short-term commuters |
| **Navigo Jour** | Day pass | 8.65 | All zones | Occasional full-day travel |
| **Imagine R** | Student annual | 380.00 | All zones | Students under 26 |

### Pay-per-Ride Products

| Product | Type | Price (EUR) | Valid Zones | Target |
|---|---|---|---|---|
| **Ticket t+** | Single ride | 2.15 | 1–2 | Individual metro/bus trip |
| **Carnet 10 t+** | 10-ride pack | 16.90 | 1–2 | Regular single-ride users |
| **Navigo Easy** | Rechargeable card | 2.00 (card only) | Variable | All pay-per-ride products |

### Tourist and Special Products

| Product | Type | Price (EUR) | Valid Zones | Target |
|---|---|---|---|---|
| **Paris Visite 1j** | Tourist 1-day | 13.95 | 1–3 | Visitors |
| **Paris Visite 2j** | Tourist 2-day | 22.65 | 1–3 | Visitors |
| **Paris Visite 5j** | Tourist 5-day | 43.30 | 1–5 | Visitors (incl. airports) |
| **Ticket Aéroport** | Airport single | 11.45 | 1–5 | Airport travellers |

### Social Fare Products

| Product | Type | Price (EUR) | Target |
|---|---|---|---|
| **Ticket Jeunes WE** | Youth weekend | 4.60 | Under-26 on Sat/Sun |
| **Améthyste** | Senior reduced | 24.00/month | Seniors 65+ |
| **Solidarité Transport** | Social reduced | 21.25/month | Low-income residents |

### Pricing Policy

Île-de-France Mobilités adopted a **flat-fare zone policy** in 2015, where the Navigo monthly and annual passes cover **all zones** at a single price. This was a major shift from the previous zone-based pricing and resulted in significantly increased ridership in outer zones.

---

## 3. Validation and Revenue Tracking

Every journey begins with a **ticket validation** — a tap-in event at a station gate or bus validator.

### Validation Record Structure

| Field | Description |
|---|---|
| Validation ID | Unique event identifier |
| Station ID | Where the validation occurred |
| Ticket Type ID | Which fare product was used |
| Validation Datetime | Exact timestamp of the tap |
| Gate ID | Physical gate or validator device |
| Card ID | Anonymised Navigo card number |
| Is Entry | True for entry, False for exit (on applicable systems) |

### Validation Volume

| Metric | Value |
|---|---|
| Dataset validations | 2,000 sample records |
| Real-world daily validations | ~8 million |
| Annual validations | ~2.5 billion |
| Peak hour share | 35–40% of daily total |

### Revenue Analysis Dimensions

Validation data enables multi-dimensional revenue analysis:

- **By station** — Identify highest-revenue stations for commercial partnerships
- **By ticket type** — Monitor subscription vs. pay-per-ride mix
- **By time period** — Peak vs. off-peak revenue distribution
- **By zone** — Geographic revenue patterns
- **By card** — Frequency analysis for loyalty and pricing optimization

### Fraud and Revenue Protection

The validation system also supports revenue protection:

- **Gate passage without validation** — Detected via passenger counts vs. validation counts
- **Expired passes** — Rejected at the gate with code logged
- **Zone violations** — Detected when exit zone exceeds ticket validity

---

## 4. Ridership and Traffic Analytics

### Daily Traffic Monitoring

The **traffic_daily** table records daily ridership at the line level:

| Field | Description |
|---|---|
| Line ID | Which line was measured |
| Traffic Date | Date of measurement |
| Total Ridership | Total passengers for the day |
| Peak Hour Ridership | Passengers during morning and evening rush |
| Off-Peak Ridership | Passengers outside rush hours |
| Day Type | Weekday, Saturday, or Sunday |

### Ridership Patterns

| Pattern | Weekday | Saturday | Sunday |
|---|---|---|---|
| Total ridership index | 100% | 55–65% | 35–45% |
| Morning peak (7:00–9:30) | 30% of daily | 10% | 8% |
| Evening peak (17:00–19:30) | 25% of daily | 12% | 10% |
| Off-peak daytime | 30% of daily | 55% | 55% |
| Evening/night | 15% of daily | 23% | 27% |

### Seasonal Variations

| Period | Index vs. Average |
|---|---|
| January–February | 95% |
| March–June | 105% |
| July–August | 70% (summer holidays) |
| September | 110% (rentrée) |
| October–November | 100% |
| December | 85% (holidays) |

### Key Ridership Metrics

| KPI | Target | Description |
|---|---|---|
| Annual ridership growth | +2% | Year-on-year passenger increase |
| Peak load factor | < 4 pax/m² | Comfort standard during rush hour |
| Off-peak utilisation | > 40% | Revenue optimisation target |
| Weekend ridership share | > 25% | Leisure travel growth indicator |

---

## 5. Passenger Surveys and Satisfaction

### Survey Programme

TransportIDF conducts systematic passenger satisfaction surveys across the network:

| Field | Description |
|---|---|
| Line and Station | Where the survey was conducted |
| Survey Date | Date of data collection |
| Aspect | Dimension being evaluated |
| Rating | Score from 1 (very poor) to 10 (excellent) |
| Comment | Free-text respondent feedback |
| Age Group | Respondent demographic (18–24, 25–34, …, 65+) |
| Commute Frequency | Daily, weekly, occasional, first-time |

### Satisfaction Dimensions

| Aspect | Description | Avg. Rating |
|---|---|---|
| Punctuality | On-time performance and reliability | 6.5/10 |
| Cleanliness | Vehicles and station cleanliness | 5.8/10 |
| Safety | Personal security feeling | 6.2/10 |
| Information | Real-time displays, announcements | 6.0/10 |
| Comfort | Seating, temperature, noise | 5.5/10 |
| Accessibility | Ease of use for reduced-mobility passengers | 5.2/10 |
| Frequency | Service frequency satisfaction | 6.8/10 |
| Crowding | Comfort during peak hours | 4.5/10 |

### Demographic Insights

| Age Group | Avg. Satisfaction | Top Concern |
|---|---|---|
| 18–24 | 6.1 | Frequency, Crowding |
| 25–34 | 5.8 | Punctuality, Crowding |
| 35–44 | 5.6 | Accessibility, Information |
| 45–54 | 5.9 | Comfort, Safety |
| 55–64 | 6.0 | Accessibility, Cleanliness |
| 65+ | 6.3 | Accessibility, Safety |

With **400 survey records**, this dataset supports satisfaction trend analysis, priority identification, and investment planning for service improvement.

---

## 6. Accessibility and Universal Design

### Legal Framework

French law (*Loi du 11 février 2005*) mandates full accessibility of public transport for persons with reduced mobility (PRM). TransportIDF tracks accessibility equipment at every station:

### Equipment Types

| Equipment | Description | Typical Location |
|---|---|---|
| Elevator | Vertical transport between levels | Main entrance, platform |
| Escalator | Moving stairway | Mezzanine, exits |
| Tactile Strip | Ground-level guidance for visually impaired | Platform edge |
| Audio Announcement | Voice announcements at platforms and in vehicles | Station-wide |
| Wheelchair Ramp | Deployable ramp for level boarding | Platform level |
| Induction Loop | Hearing aid compatible audio | Ticket office, information desk |

### Equipment Record Structure

| Field | Description |
|---|---|
| Equipment ID | Unique identifier |
| Station ID | Where the equipment is installed |
| Equipment Type | Category (elevator, escalator, etc.) |
| Location Description | Specific location within the station |
| Installation Date | When installed |
| Last Inspection Date | Most recent maintenance check |
| Status | Operational, out of service, or under repair |
| Manufacturer | Otis, Schindler, Kone, ThyssenKrupp, Mitsubishi |

### Accessibility KPIs

| KPI | Target | Current |
|---|---|---|
| Stations with elevator access | 100% | ~65% (metro), 100% (tramway) |
| Equipment availability rate | > 97% | 94.5% |
| Mean time to repair | < 48 hours | 52 hours |
| Audio announcement coverage | 100% | 98% |

With **180 equipment records**, this dataset enables accessibility compliance monitoring, maintenance prioritisation, and investment planning for universal access.

---

*This document is Part 2 of the TransportIDF business description series. See Part 1 (Network Infrastructure & Operations) for network details and Part 3 (Maintenance, Incidents & Data Integration) for operational resilience coverage.*
