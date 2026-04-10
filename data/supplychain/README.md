# Supply Chain / Logistics Dataset

This dataset simulates a complete end-to-end supply chain for a manufacturing and distribution company. It contains 14 tables with approximately 7,000+ records covering suppliers, procurement, inventory, inbound/outbound logistics, quality control, and returns.

## Overview

**Industry**: Manufacturing & Distribution  
**Use Case**: End-to-end supply chain from suppliers to customers  
**Total Tables**: 14  
**Total Records**: ~7,000+

The dataset models the complete supply chain lifecycle:
- **Procurement**: Suppliers, purchase orders, order lines
- **Inventory**: Products, categories, warehouses, stock levels
- **Inbound Logistics**: Shipments, delivery events, quality inspections
- **Sales & Distribution**: Customers, sales orders, order fulfillment
- **Returns**: Return requests and refunds

---

## Data Model Schema

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                     SUPPLY CHAIN / LOGISTICS - END-TO-END MODEL                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘


    ┌─────────────────────┐                    ┌─────────────────────┐
    │      SUPPLIER       │                    │  PRODUCT_CATEGORY   │
    ├─────────────────────┤                    ├─────────────────────┤
    │ supplier_id (PK)    │                    │ category_id (PK)    │◄────────────┐
    │ supplier_name       │                    │ category_name       │             │
    │ contact_name        │                    │ description         │             │
    │ email               │                    │ parent_category     │             │
    │ phone               │                    └─────────────────────┘             │
    │ country             │                              │                        │
    │ city                │                              │ has_category (1:N)     │
    │ rating              │                              │                        │
    │ is_active           │                              ▼                        │
    └──────────┬──────────┘                    ┌─────────────────────┐             │
               │                              │      PRODUCT        │             │
               │ places_po (1:N)              ├─────────────────────┤             │
               │                              │ product_id (PK)     │             │
               ▼                              │ category_id (FK)────┼─────────────┘
    ┌─────────────────────┐                    │ product_name        │
    │  PURCHASE_ORDER     │                    │ sku                 │
    ├─────────────────────┤                    │ unit_price          │
    │ po_id (PK)          │◄───────────────────│ weight_kg           │
    │ supplier_id (FK)────┼──┐                  │ is_active           │
    │ order_date          │  │                  └──────────┬──────────┘
    │ expected_delivery   │  │                             │
    │ status              │  │                             │
    │ total_amount        │  │         ┌───────────────────┼───────────────────┐
    └──────────┬──────────┘  │         │                   │                   │
               │             │         │                   │                   │
               │             │         │ in_inventory      │ in_order_line     │ in_sales_line
               │             │         │ (1:N)             │ (1:N)             │ (1:N)
               │             │         ▼                   ▼                   ▼
               │             │  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────┐
               │             │  │  INVENTORY  │   │ ORDER_LINE  │   │  SALES_ORDER_LINE   │
               │             │  ├─────────────┤   ├─────────────┤   ├─────────────────────┤
               │             │  │ inventory_id│   │ line_id(PK) │   │ line_id (PK)        │
               │             │  │ product_id  │   │ po_id (FK)──┼───│ so_id (FK)──────────┼──┐
               │             │  │ warehouse_id│   │ product_id  │   │ product_id          │  │
               │             │  │ qty_on_hand│   │ quantity    │   │ quantity            │  │
               │             │  │ reorder_lvl│   │ unit_price  │   │ unit_price         │  │
               │             │  │ last_restock│  │ line_total  │   │ line_total          │  │
               │             │  └──────┬──────┘   └─────────────┘   └─────────────────────┘  │
               │             │         │                                                      │
               │             │         │                                                      │
               │             │  ┌───────┴───────┐                                              │
               │             │  │  WAREHOUSE   │                                              │
               │             │  ├─────────────┤                                              │
               │             │  │ warehouse_id │◄─────────────────────────────────────────────┤
               │             │  │ warehouse_nm│                                              │
               │             │  │ city        │         ┌─────────────────────┐               │
               │             │  │ country     │         │    SALES_ORDER     │               │
               │             │  │ capacity_sqm│         ├─────────────────────┤               │
               │             │  │ warehouse_ty│        │ so_id (PK)          │               │
               │             │  └──────┬──────┘         │ customer_id (FK)───┼──┐            │
               │             │         │                │ warehouse_id (FK)──┼──┼────────────┘
               │             │         │                │ order_date          │  │
               │             │         │                │ ship_date           │  │
               │             │         │                │ status              │  │
               │             │         │                │ total_amount        │  │
               │             │         │                └──────────┬──────────┘  │
               │             │         │                             │            │
               │             │         │ receives_shipment (1:N)      │            │
               │             │         │                             │            │
               │             │         ▼                             │            │
               │             │  ┌─────────────────────┐               │            │
               │             └─│     SHIPMENT        │               │            │
               │                ├─────────────────────┤               │            │
               │                │ shipment_id (PK)    │               │            │
               │                │ po_id (FK)──────────┼───────────────┘            │
               │                │ warehouse_id (FK)────┼───────────────┐            │
               │                │ ship_date            │               │            │
               │                │ carrier             │               │            │
               │                │ tracking_number     │               │            │
               │                │ status              │               │            │
               │                │ estimated_arrival   │               │            │
               │                └──────────┬──────────┘               │            │
               │                             │                         │            │
               │                             │ has_event (1:N)         │            │
               │                             │ has_inspection (1:N)    │            │
               │                             ▼                         │            │
               │                ┌─────────────────────┐  ┌─────────────────────┐   │
               │                │  DELIVERY_EVENT     │  │ QUALITY_INSPECTION │   │
               │                ├─────────────────────┤  ├─────────────────────┤   │
               │                │ event_id (PK)        │  │ inspection_id (PK)  │   │
               │                │ shipment_id (FK)    │  │ shipment_id (FK)    │   │
               │                │ event_datetime      │  │ product_id (FK)     │   │
               │                │ event_type          │  │ inspection_date     │   │
               │                │ location            │  │ inspector           │   │
               │                │ description         │  │ result              │   │
               │                └─────────────────────┘  │ defect_count        │   │
               │                                         │ notes               │   │
               │                                         └─────────────────────┘   │
               │                                                                   │
               │                ┌─────────────────────┐                            │
               │                │    RETURN_REQUEST   │                            │
               │                ├─────────────────────┤                            │
               └────────────────│ return_id (PK)      │                            │
                                │ so_id (FK)──────────┼────────────────────────────┘
                                │ customer_id (FK)────┼──┐
                                │ request_date        │  │
                                │ reason              │  │
                                │ status              │  │
                                │ refund_amount       │  │
                                └─────────────────────┘  │
                                                         │
                                ┌─────────────────────┐  │
                                │      CUSTOMER       │◄─┘
                                ├─────────────────────┤
                                │ customer_id (PK)    │
                                │ customer_name       │
                                │ contact_name        │
                                │ email               │
                                │ phone               │
                                │ city                │
                                │ country             │
                                │ customer_type       │
                                │ credit_limit        │
                                └─────────────────────┘
```

---

## Entity Tables

### 1. Supplier (`supplier.csv`)

Supplier/vendor information for procurement.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| supplier_id    | STRING  | Primary Key (e.g., SUP00001)             |
| supplier_name | STRING  | Supplier company name                    |
| contact_name  | STRING  | Primary contact person                   |
| email         | STRING  | Email address                            |
| phone         | STRING  | Phone number                             |
| country       | STRING  | Country                                  |
| city          | STRING  | City                                     |
| rating        | DECIMAL | Performance rating (3.0-5.0)             |
| is_active     | STRING  | true/false                               |

**Rows:** 60

---

### 2. Product Category (`product_category.csv`)

Product categorization with optional hierarchy.

| Column           | Type   | Description                              |
|------------------|--------|------------------------------------------|
| category_id      | STRING | Primary Key (e.g., CAT001)               |
| category_name    | STRING | Category name (Electronics, Raw Materials, etc.) |
| description      | STRING | Category description                     |
| parent_category  | STRING | Parent category (optional)                |

**Rows:** 12

---

### 3. Product (`product.csv`)

Product catalog with pricing and attributes.

| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| product_id   | STRING  | Primary Key (e.g., PRD00001)             |
| category_id  | STRING  | Foreign Key → Product Category           |
| product_name | STRING  | Product name                             |
| sku          | STRING  | Stock keeping unit                       |
| unit_price   | DECIMAL | Unit price                               |
| weight_kg    | DECIMAL | Weight in kilograms                      |
| is_active    | STRING  | true/false                               |

**Rows:** 100

---

### 4. Warehouse (`warehouse.csv`)

Distribution centers and fulfillment facilities.

| Column          | Type   | Description                              |
|-----------------|--------|------------------------------------------|
| warehouse_id    | STRING | Primary Key (e.g., WH001)                |
| warehouse_name  | STRING | Warehouse name                           |
| city            | STRING | City location                            |
| country         | STRING | Country                                  |
| capacity_sqm    | INT    | Capacity in square meters                |
| warehouse_type  | STRING | distribution_center, fulfillment_center, etc. |

**Rows:** 10

---

### 5. Inventory (`inventory.csv`)

Stock levels by product and warehouse.

| Column            | Type   | Description                              |
|-------------------|--------|------------------------------------------|
| inventory_id      | STRING | Primary Key (e.g., INV00001)             |
| product_id        | STRING | Foreign Key → Product                     |
| warehouse_id      | STRING | Foreign Key → Warehouse                  |
| quantity_on_hand  | INT    | Current stock quantity                   |
| reorder_level     | INT    | Reorder threshold                        |
| last_restock_date  | DATE   | Last restock date                        |

**Rows:** 500

---

### 6. Customer (`customer.csv`)

Customer/buyer information.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| customer_id   | STRING  | Primary Key (e.g., CUST00001)            |
| customer_name | STRING  | Customer company name                    |
| contact_name  | STRING  | Primary contact                          |
| email         | STRING  | Email address                            |
| phone         | STRING  | Phone number                             |
| city          | STRING  | City                                     |
| country       | STRING  | Country                                  |
| customer_type | STRING  | retail, wholesale, distributor, etc.     |
| credit_limit  | DECIMAL | Credit limit                             |

**Rows:** 150

---

## Transaction Tables

### 7. Purchase Order (`purchase_order.csv`)

Procurement orders to suppliers.

| Column            | Type    | Description                              |
|-------------------|---------|------------------------------------------|
| po_id             | STRING  | Primary Key (e.g., PO00001)              |
| supplier_id       | STRING  | Foreign Key → Supplier                   |
| order_date        | DATE    | Order date                               |
| expected_delivery | DATE    | Expected delivery date                    |
| status            | STRING  | draft, submitted, approved, in_transit, received, cancelled |
| total_amount      | DECIMAL | Total order amount                       |

**Rows:** 400

---

### 8. Order Line (`order_line.csv`)

Purchase order line items.

| Column     | Type    | Description                              |
|------------|---------|------------------------------------------|
| line_id    | STRING  | Primary Key (e.g., POL000001)           |
| po_id      | STRING  | Foreign Key → Purchase Order             |
| product_id  | STRING  | Foreign Key → Product                    |
| quantity   | INT     | Quantity ordered                         |
| unit_price | DECIMAL | Unit price                               |
| line_total | DECIMAL | Line total (qty × unit_price)            |

**Rows:** 1,200

---

### 9. Shipment (`shipment.csv`)

Inbound shipments from suppliers to warehouses.

| Column           | Type   | Description                              |
|------------------|--------|------------------------------------------|
| shipment_id      | STRING | Primary Key (e.g., SHP000001)           |
| po_id            | STRING | Foreign Key → Purchase Order             |
| warehouse_id     | STRING | Foreign Key → Warehouse                  |
| ship_date        | DATE   | Ship date                               |
| carrier          | STRING | FedEx, DHL, Maersk, etc.                 |
| tracking_number  | STRING | Carrier tracking number                  |
| status           | STRING | pending, picked_up, in_transit, delivered, etc. |
| estimated_arrival| DATE   | Estimated arrival date                   |

**Rows:** 600

---

### 10. Delivery Event (`delivery_event.csv`)

Shipment tracking events.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| event_id      | STRING | Primary Key (e.g., EVT000001)            |
| shipment_id   | STRING | Foreign Key → Shipment                   |
| event_datetime| STRING | Event timestamp                          |
| event_type    | STRING | pickup, departure, arrival, delivery, etc. |
| location      | STRING | Event location                           |
| description   | STRING | Event description                        |

**Rows:** 1,500

---

### 11. Sales Order (`sales_order.csv`)

Customer orders for outbound fulfillment.

| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| so_id        | STRING  | Primary Key (e.g., SO00001)              |
| customer_id  | STRING  | Foreign Key → Customer                   |
| warehouse_id | STRING  | Foreign Key → Warehouse                  |
| order_date   | DATE    | Order date                               |
| ship_date    | DATE    | Ship date (if shipped)                   |
| status       | STRING  | draft, confirmed, picking, shipped, delivered, cancelled |
| total_amount | DECIMAL | Total order amount                       |

**Rows:** 500

---

### 12. Sales Order Line (`sales_order_line.csv`)

Sales order line items.

| Column     | Type    | Description                              |
|------------|---------|------------------------------------------|
| line_id    | STRING  | Primary Key (e.g., SOL000001)           |
| so_id      | STRING  | Foreign Key → Sales Order                |
| product_id  | STRING  | Foreign Key → Product                    |
| quantity   | INT     | Quantity ordered                         |
| unit_price | DECIMAL | Unit price                               |
| line_total | DECIMAL | Line total                               |

**Rows:** 1,500

---

### 13. Quality Inspection (`quality_inspection.csv`)

Incoming quality inspections.

| Column          | Type   | Description                              |
|-----------------|--------|------------------------------------------|
| inspection_id   | STRING | Primary Key (e.g., QIN00001)             |
| shipment_id     | STRING | Foreign Key → Shipment                   |
| product_id      | STRING | Foreign Key → Product                    |
| inspection_date | DATE   | Inspection date                          |
| inspector       | STRING | Inspector ID                             |
| result          | STRING | passed, failed, conditional, pending_review |
| defect_count    | INT    | Number of defects found                  |
| notes           | STRING | Inspection notes                         |

**Rows:** 300

---

### 14. Return Request (`return_request.csv`)

Customer return requests.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| return_id     | STRING  | Primary Key (e.g., RET00001)             |
| so_id         | STRING  | Foreign Key → Sales Order                |
| customer_id   | STRING  | Foreign Key → Customer                   |
| request_date  | DATE    | Return request date                      |
| reason        | STRING  | defective, wrong_item, damaged_in_transit, etc. |
| status        | STRING  | requested, approved, received, refunded, rejected |
| refund_amount | DECIMAL | Refund amount                            |

**Rows:** 200

---

## Relationships Summary

| Relationship        | Source        | Target           | Cardinality | Description                    |
|---------------------|---------------|------------------|-------------|--------------------------------|
| places_po           | Supplier      | Purchase Order   | 1:N         | Supplier receives POs         |
| has_line            | Purchase Order| Order Line       | 1:N         | PO has line items             |
| contains_product    | Order Line    | Product          | N:1         | Line references product       |
| belongs_to_category | Product       | Product Category | N:1         | Product in category           |
| stored_at           | Inventory     | Warehouse        | N:1         | Inventory at warehouse        |
| has_stock           | Inventory     | Product          | N:1         | Inventory of product          |
| receives_shipment   | Shipment      | Warehouse        | N:1         | Shipment to warehouse         |
| fulfills_po         | Shipment      | Purchase Order   | N:1         | Shipment fulfills PO          |
| has_event           | Delivery Event| Shipment         | N:1         | Event for shipment            |
| has_inspection      | Quality Insp. | Shipment         | N:1         | Inspection of shipment        |
| inspects_product    | Quality Insp. | Product          | N:1         | Inspection of product         |
| places_order        | Customer      | Sales Order      | 1:N         | Customer places orders        |
| fulfills_from       | Sales Order   | Warehouse        | N:1         | Order fulfilled from warehouse|
| has_line            | Sales Order   | Sales Order Line | 1:N         | SO has line items             |
| returns             | Return Request| Sales Order      | N:1         | Return of SO                  |
| initiated_by        | Return Request| Customer         | N:1         | Return by customer            |

---

## Loading the Data

### Prerequisites

1. Upload CSV files to a Unity Catalog Volume
2. Permissions to create schema and tables in the target catalog

### Using the Databricks Notebook

Use the provided notebook `load_data.py` to:

1. Create a schema in Unity Catalog
2. Load all 14 CSV files as tables with proper data types
3. Create 3 analytical views (vw_order_fulfillment, vw_inventory_status, vw_supplier_performance)
4. Verify data integrity with test queries

### Quick Start

```python
# 1. Upload CSV files to a Unity Catalog Volume
# 2. Update notebook configuration
catalog = "main"
schema = "supply_chain"
volume_name = "supplychain_data"

# 3. Run the notebook - it will create all tables automatically
```

### Views Created

| View                   | Description                                      |
|------------------------|--------------------------------------------------|
| vw_order_fulfillment   | PO to shipment to warehouse pipeline             |
| vw_inventory_status    | Stock levels with reorder status by product/warehouse |
| vw_supplier_performance| Supplier spend, order count, delivery metrics   |

---

## Sample Queries

### Order Fulfillment Pipeline

```sql
SELECT 
    po.po_id,
    s.supplier_name,
    po.order_date,
    sh.carrier,
    sh.tracking_number,
    sh.status as shipment_status,
    w.warehouse_name
FROM purchase_order po
JOIN supplier s ON po.supplier_id = s.supplier_id
JOIN shipment sh ON po.po_id = sh.po_id
JOIN warehouse w ON sh.warehouse_id = w.warehouse_id
WHERE po.status IN ('in_transit', 'received')
ORDER BY po.order_date DESC;
```

### Inventory Below Reorder Level

```sql
SELECT 
    p.product_name,
    w.warehouse_name,
    i.quantity_on_hand,
    i.reorder_level,
    i.last_restock_date
FROM inventory i
JOIN product p ON i.product_id = p.product_id
JOIN warehouse w ON i.warehouse_id = w.warehouse_id
WHERE i.quantity_on_hand <= i.reorder_level
ORDER BY i.quantity_on_hand ASC;
```

### Supplier Performance

```sql
SELECT 
    s.supplier_name,
    s.rating,
    s.country,
    COUNT(po.po_id) as order_count,
    SUM(po.total_amount) as total_spend
FROM supplier s
JOIN purchase_order po ON s.supplier_id = po.supplier_id
WHERE s.is_active = 'true'
GROUP BY s.supplier_id, s.supplier_name, s.rating, s.country
ORDER BY total_spend DESC;
```

### Returns by Reason

```sql
SELECT 
    rr.reason,
    rr.status,
    COUNT(*) as return_count,
    SUM(rr.refund_amount) as total_refunds
FROM return_request rr
GROUP BY rr.reason, rr.status
ORDER BY return_count DESC;
```

---

## Use Cases for OntoBricks

This dataset is ideal for testing and demonstrating:

### Ontology Modeling
- ✅ **Class Mapping**: Map tables to OWL classes (Supplier, Product, Warehouse, Shipment, etc.)
- ✅ **Object Properties**: Model relationships (placesPO, hasLine, receivesShipment, etc.)
- ✅ **Data Properties**: Scalar values (unit_price, quantity_on_hand, rating, etc.)
- ✅ **Hierarchical Concepts**: Product categories with parent-child relationships

### Knowledge Graph Construction
- ✅ **End-to-End Traceability**: Supplier → PO → Shipment → Warehouse → Inventory
- ✅ **Order Fulfillment**: Sales Order → Customer, Warehouse, Order Lines
- ✅ **Quality & Returns**: Link inspections and returns to shipments and orders

### SPARQL Queries
- ✅ **Path Queries**: Trace product flow from supplier to customer
- ✅ **Aggregation**: Total spend by supplier, inventory value by warehouse
- ✅ **Pattern Matching**: Find products with failed inspections or high return rates

---

## Dataset Statistics

| Table              | Rows   | Type          | Primary Key     |
|--------------------|--------|---------------|-----------------|
| supplier           | 60     | Core Entity   | supplier_id     |
| product_category   | 12     | Core Entity   | category_id     |
| product            | 100    | Core Entity   | product_id      |
| warehouse          | 10     | Core Entity   | warehouse_id    |
| inventory          | 500    | Transaction   | inventory_id    |
| customer           | 150    | Core Entity   | customer_id     |
| purchase_order     | 400    | Transaction   | po_id           |
| order_line         | 1,200  | Transaction   | line_id         |
| shipment           | 600    | Transaction   | shipment_id     |
| delivery_event     | 1,500  | Transaction   | event_id        |
| sales_order        | 500    | Transaction   | so_id           |
| sales_order_line   | 1,500  | Transaction   | line_id         |
| quality_inspection | 300    | Transaction   | inspection_id   |
| return_request     | 200    | Transaction   | return_id       |

**Total:** 14 tables, ~7,000+ records

---

## Data Quality Notes

- All IDs use meaningful prefixes (SUP, PRD, WH, PO, SHP, etc.) for readability
- Date format: YYYY-MM-DD
- DateTime format: YYYY-MM-DD HH:MM:SS
- No NULL values in primary keys
- Foreign key references are valid and consistent
- Realistic supply chain data: carriers (FedEx, DHL, Maersk), product categories (Electronics, Raw Materials), cities worldwide
- Empty strings used for optional fields (e.g., parent_category, ship_date)

---

## Files in This Directory

| File                    | Description                          |
|-------------------------|--------------------------------------|
| supplier.csv            | Supplier records                     |
| product_category.csv    | Product categories                   |
| product.csv             | Product catalog                      |
| warehouse.csv           | Warehouse locations                  |
| inventory.csv           | Stock levels by product/warehouse    |
| customer.csv            | Customer records                     |
| purchase_order.csv      | Purchase orders                      |
| order_line.csv          | Purchase order lines                 |
| shipment.csv            | Inbound shipments                    |
| delivery_event.csv      | Shipment tracking events             |
| sales_order.csv         | Sales orders                         |
| sales_order_line.csv    | Sales order lines                    |
| quality_inspection.csv  | Quality inspections                 |
| return_request.csv      | Customer return requests             |
| load_data.py            | Databricks loader notebook           |
| create_databricks_tables.py | CLI table creation (CTAS)        |
| generate_data.py        | Data generation script               |
| README.md               | This documentation                   |
| unstructured/business_description.txt | Business context document   |

---

## Regenerating Data

To regenerate the dataset with different parameters:

```bash
cd data/supplychain
python generate_data.py
```

Modify `generate_data.py` to:
- Change record counts
- Adjust date ranges
- Add/remove data fields
- Modify random seed for different data
