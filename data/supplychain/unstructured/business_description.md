# GlobalTrade Logistics Corp

## Supply Chain Operations & Data Management

**Internal Business Description Document**

| | |
|---|---|
| **Document Owner** | Supply Chain Data Governance |
| **Classification** | Confidential — Internal Use Only |
| **Version** | 2.0 |
| **Last Updated** | March 2025 |
| **Review Cycle** | Annual |
| **Next Review** | March 2026 |

---

## Table of Contents

1. [Company Overview](#1-company-overview)
2. [Supplier Relationships and Procurement](#2-supplier-relationships-and-procurement)
3. [Warehouse Management and Inventory](#3-warehouse-management-and-inventory)
4. [Inbound Logistics: Shipments and Delivery Events](#4-inbound-logistics-shipments-and-delivery-events)
5. [Quality Control](#5-quality-control)
6. [Sales and Outbound Distribution](#6-sales-and-outbound-distribution)
7. [Returns Management](#7-returns-management)
8. [Data Integration and Analytical Use Cases](#8-data-integration-and-analytical-use-cases)
9. [Data Model Summary](#9-data-model-summary)
10. [Key Performance Indicators](#10-key-performance-indicators)
11. [Appendix: Entity Relationship Overview](#11-appendix-entity-relationship-overview)

---

## 1. Company Overview

GlobalTrade Logistics Corp (GTL) is a multinational manufacturing and distribution company operating across **six continents**. Founded in 1995, the company has grown from a regional parts distributor into an integrated supply chain operator managing the full lifecycle of products — from raw material sourcing through to end-customer delivery.

> **Our Mission:** To deliver operational excellence across the global supply chain through data-driven decision-making, supplier collaboration, and customer-centric logistics.

### Business Segments

| Segment | Description |
|---|---|
| Industrial Components | Precision-engineered parts for manufacturing |
| Consumer Goods | Retail-ready products for direct distribution |
| Specialty Chemicals | Temperature-controlled and hazardous materials |

### Company at a Glance

| Metric | Value |
|---|---|
| Founded | 1995 |
| Headquarters | Rotterdam, Netherlands |
| Active Suppliers | 60+ worldwide |
| Active Customers | ~150 (retail, wholesale, OEM, e-commerce) |
| Distribution Centers | 10 global locations |
| Product SKUs | 100+ across 12 categories |
| Total Warehouse Capacity | 250,000+ m² |
| Quarterly Purchase Orders | ~400 |
| Quarterly Sales Orders | ~500 |

---

## 2. Supplier Relationships and Procurement

Supplier management is a cornerstone of GTL's operations. We maintain a curated base of **60 approved vendors**, each evaluated on quality, reliability, pricing, and geographic coverage.

### Supplier Evaluation Framework

| Criterion | Weight | Measurement |
|---|---|---|
| On-time Delivery | 35% | % of POs delivered within expected window |
| Quality Score | 30% | Inspection pass rate and defect counts |
| Pricing Competitiveness | 20% | Benchmarked against market rates |
| Responsiveness | 15% | Average response time to inquiries |

Each supplier receives a **performance rating** (3.0 to 5.0) that is refreshed quarterly and used to prioritize order allocation.

### Product Categories

Suppliers are categorized across 12 product families:

| Category | Subcategories |
|---|---|
| Electronics | Semiconductors |
| Raw Materials | — |
| Packaging | — |
| Mechanical Parts | Automotive |
| Chemicals | — |
| Textiles | — |
| Consumer Goods | — |
| Industrial Equipment | — |
| Food & Beverage | — |
| Pharmaceuticals | — |

### Purchase Order Workflow

Purchase orders (POs) are the primary mechanism for replenishment:

```
Draft → Submitted → Approved → In Transit → Received
                                    └──→ Cancelled
```

| PO Metric | Quarterly Volume |
|---|---|
| Purchase Orders | ~400 |
| Order Lines (avg. 3/PO) | ~1,200 |
| Order Value Range | $500 – $150,000 |

Each PO references a single supplier and includes order date, expected delivery date, total amount, and detailed line items (product, quantity, unit price, line total).

---

## 3. Warehouse Management and Inventory

### Warehouse Network

GTL operates 10 strategically located facilities:

| Location | Type | Function |
|---|---|---|
| Shanghai | Fulfillment Center | Asia-Pacific direct-to-consumer |
| Rotterdam | Distribution Center | European bulk replenishment |
| Singapore | Transit Hub | Cross-docking and transshipment |
| Dubai | Regional DC | Middle East and Africa distribution |
| Los Angeles | Distribution Center | Americas West Coast hub |
| Hamburg | Cold Storage | Temperature-sensitive products |
| Tokyo | Fulfillment Center | Japan direct distribution |
| Mumbai | Regional DC | Indian subcontinent operations |
| Antwerp | Transit Hub | European cross-docking |
| Busan | Distribution Center | Korean peninsula and East Asia |

### Inventory Management

Inventory is tracked at the **product × warehouse** level. Each position records:

- **Quantity on hand** — Current stock level
- **Reorder level** — Threshold for automatic replenishment
- **Last restock date** — Supports cycle counting and aging analysis

| Inventory Metric | Value |
|---|---|
| Active Inventory Positions | ~500 |
| Products Tracked | 100 SKUs |
| Warehouses | 10 |

When quantity on hand falls at or below the reorder level, the system triggers replenishment workflows automatically.

---

## 4. Inbound Logistics: Shipments and Delivery Events

When a purchase order is fulfilled by a supplier, goods are shipped to one of our warehouses.

### Carrier Partners

| Carrier | Primary Routes |
|---|---|
| Maersk | Ocean freight — Asia to Europe |
| CMA CGM | Ocean freight — Asia to Americas |
| MSC | Ocean freight — Global |
| DHL | Air and express — Global |
| FedEx | Air and express — Americas |
| UPS | Ground and express — Americas |
| Kuehne+Nagel | Multimodal — Global |
| DB Schenker | European road freight |
| XPO Logistics | North American logistics |
| CEVA | Contract logistics |

### Shipment Lifecycle

| Status | Description |
|---|---|
| Pending | Awaiting pickup from supplier |
| Picked Up | Collected by carrier |
| In Transit | En route to destination warehouse |
| Delivered | Received at destination |
| Delayed | Behind schedule |
| Exception | Issue requiring intervention |

We process approximately **600 shipments per quarter**.

### Delivery Event Tracking

Delivery events provide granular visibility into shipment progress:

| Event Type | Description |
|---|---|
| Pickup | Goods collected from supplier |
| Departure | Left origin facility |
| Arrival | Reached intermediate or final facility |
| Customs Clearance | Cleared border controls |
| Delivery | Final delivery confirmed |
| Exception | Damage, loss, or routing issue |
| Delay | Schedule deviation recorded |

We record approximately **1,500 delivery events per quarter**, enabling real-time tracking and exception management.

---

## 5. Quality Control

Quality inspections are performed on incoming shipments before goods enter inventory.

### Inspection Process

Each inspection records:

- **Shipment and product** — What was inspected
- **Inspector ID** — Who performed the inspection
- **Result** — Passed, Failed, Conditional, or Pending Review
- **Defect count** — Number of defective units
- **Notes** — Observations and recommended actions

| QC Metric | Quarterly Volume |
|---|---|
| Inspections Performed | ~300 |
| Average Pass Rate | Tracked per supplier |
| Defect Resolution Time | < 5 business days |

Failed or conditional results trigger:

1. Quarantine of affected goods
2. Supplier notification and corrective action request
3. Scorecard adjustment

---

## 6. Sales and Outbound Distribution

### Customer Base

GTL serves **150 active customers** across multiple segments:

| Customer Type | Description |
|---|---|
| Retail | Brick-and-mortar stores and chains |
| Wholesale | Bulk buyers and distributors |
| Distributor | Regional and specialty distributors |
| Manufacturer | OEM and contract manufacturers |
| E-commerce | Online retailers and marketplaces |

Customer **credit limits** are maintained to support order approval workflows.

### Sales Order Workflow

```
Draft → Confirmed → Picking → Shipped → Delivered
                                  └──→ Cancelled
```

| Sales Metric | Quarterly Volume |
|---|---|
| Sales Orders | ~500 |
| Order Lines (avg. 3/order) | ~1,500 |
| Order Value Range | $100 – $75,000 |

Each sales order references a customer and a fulfilling warehouse. Ship date is recorded when the order leaves the facility.

---

## 7. Returns Management

Return requests are initiated by customers and linked to the original sales order.

### Return Reasons

| Reason | Description |
|---|---|
| Defective | Product does not function as specified |
| Wrong Item | Incorrect product shipped |
| Damaged in Transit | Shipping damage |
| Customer Change | Buyer changed requirements |
| Quality Issue | Product does not meet expectations |
| Expired | Product past its use-by date |

### Return Workflow

```
Requested → Approved → Received → Refunded
                └──→ Rejected
```

| Returns Metric | Quarterly Volume |
|---|---|
| Return Requests | ~200 |
| Average Refund Amount | Varies by product |

Returns data supports reverse logistics planning, supplier quality feedback, and customer satisfaction analysis.

---

## 8. Data Integration and Analytical Use Cases

The supply chain data model supports end-to-end visibility from supplier to customer. Key analytical capabilities include:

### Order Fulfillment Tracking

```
Purchase Order → Shipment → Delivery Events → Warehouse Receipt → Inventory Update
```

### Core Analytics

| Use Case | Data Sources | Business Value |
|---|---|---|
| Inventory Optimization | Inventory, Purchase Orders, Sales Orders | Reduce stockouts and excess inventory |
| Supplier Performance | Suppliers, POs, Shipments, Inspections | Optimize vendor selection and negotiation |
| Customer Analytics | Customers, Sales Orders, Returns | Improve retention and order accuracy |
| Logistics Efficiency | Shipments, Delivery Events, Carriers | Reduce transit times and exception rates |
| Quality Trending | Inspections, Suppliers, Products | Early detection of quality deterioration |

---

## 9. Data Model Summary

### Master Data Entities

| Entity | Description | Approx. Records |
|---|---|---|
| Supplier | Approved vendors | 60 |
| Product Category | Product classification hierarchy | 12 |
| Product | SKU-level product catalog | 100 |
| Warehouse | Distribution and fulfillment centers | 10 |
| Customer | Buyers and trading partners | 150 |

### Transactional Entities

| Entity | Description | Approx. Records |
|---|---|---|
| Inventory | Stock positions (product × warehouse) | 500 |
| Purchase Order | Supplier replenishment orders | 400 |
| Order Line | PO line-item detail | 1,200 |
| Shipment | Inbound goods movement | 600 |
| Delivery Event | Shipment tracking milestones | 1,500 |
| Sales Order | Customer orders | 500 |
| Sales Order Line | SO line-item detail | 1,500 |
| Quality Inspection | Incoming goods inspection | 300 |
| Return Request | Customer returns | 200 |

**Total records: ~7,032**

---

## 10. Key Performance Indicators

| KPI | Target | Measurement |
|---|---|---|
| Order Fill Rate | ≥ 95% | % of orders shipped complete |
| On-time Delivery (Inbound) | ≥ 90% | % of POs received by expected date |
| On-time Delivery (Outbound) | ≥ 95% | % of SOs delivered by promised date |
| Inventory Turnover | ≥ 8x/year | COGS / Average Inventory |
| Quality Inspection Pass Rate | ≥ 97% | % of inspections passed |
| Return Rate | < 4% | Returns / Sales Orders |
| Supplier Scorecard Average | ≥ 4.0 / 5.0 | Weighted composite rating |

---

## 11. Appendix: Entity Relationship Overview

### Primary Relationships

```
Supplier ──(1:N)──► Purchase Order
Purchase Order ──(1:N)──► Order Line
Order Line ──(N:1)──► Product
Purchase Order ──(1:N)──► Shipment
Shipment ──(N:1)──► Warehouse
Shipment ──(1:N)──► Delivery Event
Shipment ──(1:N)──► Quality Inspection
Product ──(N:1)──► Product Category
Product + Warehouse ──(1:1)──► Inventory
Customer ──(1:N)──► Sales Order
Sales Order ──(1:N)──► Sales Order Line
Sales Order Line ──(N:1)──► Product
Sales Order ──(N:1)──► Warehouse
Sales Order ──(0:N)──► Return Request
```

### End-to-End Supply Chain Flow

```
Supplier → Purchase Order → Order Line → Shipment → Delivery Event
                                              ↓
                                     Quality Inspection
                                              ↓
                                     Warehouse (Inventory)
                                              ↓
                              Sales Order → Sales Order Line → Customer
                                              ↓
                                      Return Request
```

---

*This document supports ontology mapping and knowledge graph construction using OntoBricks. All entities and relationships are designed to be directly translatable into RDF/OWL ontology structures for semantic querying and AI-driven supply chain insights.*
