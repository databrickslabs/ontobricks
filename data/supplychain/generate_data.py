#!/usr/bin/env python3
"""
Generate Supply Chain / Logistics Dataset for Manufacturing & Distribution
Creates 14 tables with realistic data for end-to-end supply chain from suppliers to customers.
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
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    return f"{date} {hour:02d}:{minute:02d}:00"

# Data pools - Supply Chain specific
SUPPLIER_PREFIXES = ["Global", "Pacific", "Atlantic", "Euro", "Asia", "Nordic", "Prime", "Elite"]
SUPPLIER_SUFFIXES = ["Supplies", "Materials", "Components", "Trading", "Logistics", "Sourcing", "Parts", "Goods"]
CONTACT_FIRST = ["James", "Maria", "Chen", "Olga", "Ahmed", "Sophie", "Raj", "Elena", "Marcus", "Yuki"]
CONTACT_LAST = ["Smith", "Garcia", "Wang", "Kozlov", "Hassan", "Müller", "Patel", "Rossi", "Johnson", "Tanaka"]
CITIES = [
    ("Shanghai", "China"), ("Rotterdam", "Netherlands"), ("Singapore", "Singapore"),
    ("Dubai", "UAE"), ("Los Angeles", "USA"), ("Hamburg", "Germany"), ("Tokyo", "Japan"),
    ("Mumbai", "India"), ("Antwerp", "Belgium"), ("Busan", "South Korea"),
    ("Houston", "USA"), ("Hong Kong", "China"), ("Felixstowe", "UK"), ("Valencia", "Spain"),
    ("Chicago", "USA"), ("Barcelona", "Spain"), ("Osaka", "Japan"), ("Mumbai", "India"),
    ("Sydney", "Australia"), ("Toronto", "Canada"), ("São Paulo", "Brazil"), ("Moscow", "Russia")
]
PRODUCT_CATEGORIES = [
    ("Electronics", "Electronic components and devices", None),
    ("Raw Materials", "Base materials for manufacturing", None),
    ("Packaging", "Packaging materials and supplies", None),
    ("Mechanical Parts", "Mechanical components and assemblies", None),
    ("Chemicals", "Industrial chemicals and compounds", None),
    ("Textiles", "Fabrics and textile materials", None),
    ("Automotive", "Automotive parts and accessories", "Mechanical Parts"),
    ("Consumer Goods", "Finished consumer products", "Electronics"),
    ("Industrial Equipment", "Heavy machinery and equipment", "Mechanical Parts"),
    ("Food & Beverage", "Food ingredients and beverages", "Raw Materials"),
    ("Pharmaceuticals", "Pharma ingredients and products", "Chemicals"),
    ("Semiconductors", "Chips and electronic components", "Electronics"),
]
CARRIERS = ["FedEx", "DHL", "Maersk", "UPS", "CMA CGM", "MSC", "Kuehne+Nagel", "DB Schenker", "XPO Logistics", "CEVA"]
WAREHOUSE_TYPES = ["distribution_center", "fulfillment_center", "cold_storage", "transit_hub", "regional_dc"]
CUSTOMER_TYPES = ["retail", "wholesale", "distributor", "manufacturer", "ecommerce"]
PO_STATUS = ["draft", "submitted", "approved", "in_transit", "received", "cancelled"]
SHIPMENT_STATUS = ["pending", "picked_up", "in_transit", "delivered", "delayed", "exception"]
EVENT_TYPES = ["pickup", "departure", "arrival", "customs_clearance", "delivery", "exception", "delay"]
INSPECTION_RESULTS = ["passed", "failed", "conditional", "pending_review"]
RETURN_REASONS = ["defective", "wrong_item", "damaged_in_transit", "customer_change", "quality_issue", "expired"]
RETURN_STATUS = ["requested", "approved", "received", "refunded", "rejected"]
SO_STATUS = ["draft", "confirmed", "picking", "shipped", "delivered", "cancelled"]

# Generate supplier data
def generate_suppliers(count=60):
    """Generate supplier records."""
    suppliers = []
    used_names = set()
    for i in range(1, count + 1):
        prefix = random.choice(SUPPLIER_PREFIXES)
        suffix = random.choice(SUPPLIER_SUFFIXES)
        name = f"{prefix} {suffix}"
        while name in used_names:
            name = f"{prefix} {suffix} {random.randint(1, 99)}"
        used_names.add(name)
        city, country = random.choice(CITIES)
        contact = f"{random.choice(CONTACT_FIRST)} {random.choice(CONTACT_LAST)}"
        suppliers.append({
            "supplier_id": f"SUP{i:05d}",
            "supplier_name": name,
            "contact_name": contact,
            "email": f"procurement@{name.lower().replace(' ', '')}.com",
            "phone": f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000000, 9999999)}",
            "country": country,
            "city": city,
            "rating": round(random.uniform(3.0, 5.0), 1),
            "is_active": random.choice(["true", "true", "true", "false"])
        })
    return suppliers

# Generate product category data
def generate_product_categories():
    """Generate product category records."""
    categories = []
    for i, (name, desc, parent) in enumerate(PRODUCT_CATEGORIES, 1):
        categories.append({
            "category_id": f"CAT{i:03d}",
            "category_name": name,
            "description": desc,
            "parent_category": parent or ""
        })
    return categories

# Generate product data
def generate_products(categories, count=100):
    """Generate product records."""
    products = []
    sku_base = 100000
    for i in range(1, count + 1):
        cat = random.choice(categories)
        products.append({
            "product_id": f"PRD{i:05d}",
            "category_id": cat["category_id"],
            "product_name": f"{cat['category_name']} Product {i}",
            "sku": f"SKU-{sku_base + i}",
            "unit_price": round(random.uniform(5.00, 2500.00), 2),
            "weight_kg": round(random.uniform(0.1, 150.0), 2),
            "is_active": random.choice(["true", "true", "true", "false"])
        })
    return products

# Generate warehouse data
def generate_warehouses(count=10):
    """Generate warehouse records."""
    warehouses = []
    for i in range(1, count + 1):
        city, country = random.choice(CITIES)
        warehouses.append({
            "warehouse_id": f"WH{i:03d}",
            "warehouse_name": f"{city} Distribution Center {i}",
            "city": city,
            "country": country,
            "capacity_sqm": random.randint(5000, 50000),
            "warehouse_type": random.choice(WAREHOUSE_TYPES)
        })
    return warehouses

# Generate customer data
def generate_customers(count=150):
    """Generate customer records."""
    customers = []
    for i in range(1, count + 1):
        city, country = random.choice(CITIES)
        contact = f"{random.choice(CONTACT_FIRST)} {random.choice(CONTACT_LAST)}"
        customers.append({
            "customer_id": f"CUST{i:05d}",
            "customer_name": f"Customer Corp {i}",
            "contact_name": contact,
            "email": f"orders@customer{i}.com",
            "phone": f"+{random.randint(1, 99)} {random.randint(100, 999)} {random.randint(1000000, 9999999)}",
            "city": city,
            "country": country,
            "customer_type": random.choice(CUSTOMER_TYPES),
            "credit_limit": round(random.uniform(10000, 500000), 2)
        })
    return customers

# Generate inventory data
def generate_inventory(products, warehouses, count=500):
    """Generate inventory records."""
    inventory = []
    seen = set()
    attempts = 0
    max_attempts = count * 3
    while len(inventory) < count and attempts < max_attempts:
        attempts += 1
        product = random.choice(products)
        warehouse = random.choice(warehouses)
        key = (product["product_id"], warehouse["warehouse_id"])
        if key in seen:
            continue
        seen.add(key)
        last_restock = random_date(2023, 2025) if random.random() > 0.2 else ""
        inventory.append({
            "inventory_id": f"INV{len(inventory)+1:05d}",
            "product_id": product["product_id"],
            "warehouse_id": warehouse["warehouse_id"],
            "quantity_on_hand": random.randint(0, 5000),
            "reorder_level": random.randint(50, 500),
            "last_restock_date": last_restock
        })
    return inventory

# Generate purchase order data
def generate_purchase_orders(suppliers, count=400):
    """Generate purchase order records."""
    pos = []
    for i in range(1, count + 1):
        supplier = random.choice(suppliers)
        order_date = random_date(2023, 2025)
        expected = (datetime.strptime(order_date, "%Y-%m-%d") + timedelta(days=random.randint(7, 45))).strftime("%Y-%m-%d")
        total = round(random.uniform(500, 150000), 2)
        pos.append({
            "po_id": f"PO{i:05d}",
            "supplier_id": supplier["supplier_id"],
            "order_date": order_date,
            "expected_delivery": expected,
            "status": random.choice(PO_STATUS),
            "total_amount": total
        })
    return pos

# Generate order line data (purchase order lines)
def generate_order_lines(purchase_orders, products, count=1200):
    """Generate purchase order line records."""
    lines = []
    for i in range(1, count + 1):
        po = random.choice(purchase_orders)
        product = random.choice(products)
        qty = random.randint(1, 500)
        unit_price = product["unit_price"] * random.uniform(0.85, 1.15)
        line_total = round(qty * unit_price, 2)
        lines.append({
            "line_id": f"POL{i:06d}",
            "po_id": po["po_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": round(unit_price, 2),
            "line_total": line_total
        })
    return lines

# Generate shipment data
def generate_shipments(purchase_orders, warehouses, count=600):
    """Generate shipment records."""
    shipments = []
    for i in range(1, count + 1):
        po = random.choice(purchase_orders)
        warehouse = random.choice(warehouses)
        ship_date = random_date(2023, 2025)
        eta = (datetime.strptime(ship_date, "%Y-%m-%d") + timedelta(days=random.randint(3, 21))).strftime("%Y-%m-%d")
        shipments.append({
            "shipment_id": f"SHP{i:06d}",
            "po_id": po["po_id"],
            "warehouse_id": warehouse["warehouse_id"],
            "ship_date": ship_date,
            "carrier": random.choice(CARRIERS),
            "tracking_number": f"TRK{random.randint(100000000, 999999999)}",
            "status": random.choice(SHIPMENT_STATUS),
            "estimated_arrival": eta
        })
    return shipments

# Generate delivery event data
def generate_delivery_events(shipments, count=1500):
    """Generate delivery event records."""
    events = []
    for i in range(1, count + 1):
        shipment = random.choice(shipments)
        city, country = random.choice(CITIES)
        events.append({
            "event_id": f"EVT{i:06d}",
            "shipment_id": shipment["shipment_id"],
            "event_datetime": random_datetime(2023, 2025),
            "event_type": random.choice(EVENT_TYPES),
            "location": f"{city}, {country}",
            "description": f"{random.choice(EVENT_TYPES).replace('_', ' ').title()} at {city}"
        })
    return events

# Generate sales order data
def generate_sales_orders(customers, warehouses, count=500):
    """Generate sales order records."""
    orders = []
    for i in range(1, count + 1):
        customer = random.choice(customers)
        warehouse = random.choice(warehouses)
        order_date = random_date(2023, 2025)
        ship_date = (datetime.strptime(order_date, "%Y-%m-%d") + timedelta(days=random.randint(1, 14))).strftime("%Y-%m-%d") if random.random() > 0.3 else ""
        total = round(random.uniform(100, 75000), 2)
        orders.append({
            "so_id": f"SO{i:05d}",
            "customer_id": customer["customer_id"],
            "warehouse_id": warehouse["warehouse_id"],
            "order_date": order_date,
            "ship_date": ship_date or "",
            "status": random.choice(SO_STATUS),
            "total_amount": total
        })
    return orders

# Generate sales order line data
def generate_sales_order_lines(sales_orders, products, count=1500):
    """Generate sales order line records."""
    lines = []
    for i in range(1, count + 1):
        so = random.choice(sales_orders)
        product = random.choice(products)
        qty = random.randint(1, 200)
        unit_price = product["unit_price"] * random.uniform(0.9, 1.2)
        line_total = round(qty * unit_price, 2)
        lines.append({
            "line_id": f"SOL{i:06d}",
            "so_id": so["so_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": round(unit_price, 2),
            "line_total": line_total
        })
    return lines

# Generate quality inspection data
def generate_quality_inspections(shipments, products, count=300):
    """Generate quality inspection records."""
    inspections = []
    for i in range(1, count + 1):
        shipment = random.choice(shipments)
        product = random.choice(products)
        inspections.append({
            "inspection_id": f"QIN{i:05d}",
            "shipment_id": shipment["shipment_id"],
            "product_id": product["product_id"],
            "inspection_date": random_date(2023, 2025),
            "inspector": f"INSP{random.randint(100, 200)}",
            "result": random.choice(INSPECTION_RESULTS),
            "defect_count": random.randint(0, 15) if random.random() > 0.7 else 0,
            "notes": "Routine inspection" if random.random() > 0.3 else "Follow-up required"
        })
    return inspections

# Generate return request data
def generate_return_requests(sales_orders, customers, count=200):
    """Generate return request records."""
    returns = []
    for i in range(1, count + 1):
        so = random.choice(sales_orders)
        customer = random.choice([c for c in customers if c["customer_id"] == so["customer_id"]] or customers)
        returns.append({
            "return_id": f"RET{i:05d}",
            "so_id": so["so_id"],
            "customer_id": customer["customer_id"],
            "request_date": random_date(2023, 2025),
            "reason": random.choice(RETURN_REASONS),
            "status": random.choice(RETURN_STATUS),
            "refund_amount": round(random.uniform(0, 5000), 2) if random.random() > 0.5 else 0
        })
    return returns

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
    print("Generating Supply Chain / Logistics Dataset")
    print("=" * 60)
    print()

    # Generate all data
    print("Generating data...")
    suppliers = generate_suppliers(60)
    categories = generate_product_categories()
    products = generate_products(categories, 100)
    warehouses = generate_warehouses(10)
    customers = generate_customers(150)
    inventory = generate_inventory(products, warehouses, 500)
    purchase_orders = generate_purchase_orders(suppliers, 400)
    order_lines = generate_order_lines(purchase_orders, products, 1200)
    shipments = generate_shipments(purchase_orders, warehouses, 600)
    delivery_events = generate_delivery_events(shipments, 1500)
    sales_orders = generate_sales_orders(customers, warehouses, 500)
    sales_order_lines = generate_sales_order_lines(sales_orders, products, 1500)
    quality_inspections = generate_quality_inspections(shipments, products, 300)
    return_requests = generate_return_requests(sales_orders, customers, 200)

    print()
    print("Writing CSV files...")

    # Write all CSV files
    write_csv("supplier.csv", suppliers, [
        "supplier_id", "supplier_name", "contact_name", "email", "phone",
        "country", "city", "rating", "is_active"
    ])
    write_csv("product_category.csv", categories, [
        "category_id", "category_name", "description", "parent_category"
    ])
    write_csv("product.csv", products, [
        "product_id", "category_id", "product_name", "sku", "unit_price",
        "weight_kg", "is_active"
    ])
    write_csv("warehouse.csv", warehouses, [
        "warehouse_id", "warehouse_name", "city", "country", "capacity_sqm", "warehouse_type"
    ])
    write_csv("inventory.csv", inventory, [
        "inventory_id", "product_id", "warehouse_id", "quantity_on_hand",
        "reorder_level", "last_restock_date"
    ])
    write_csv("customer.csv", customers, [
        "customer_id", "customer_name", "contact_name", "email", "phone",
        "city", "country", "customer_type", "credit_limit"
    ])
    write_csv("purchase_order.csv", purchase_orders, [
        "po_id", "supplier_id", "order_date", "expected_delivery", "status", "total_amount"
    ])
    write_csv("order_line.csv", order_lines, [
        "line_id", "po_id", "product_id", "quantity", "unit_price", "line_total"
    ])
    write_csv("shipment.csv", shipments, [
        "shipment_id", "po_id", "warehouse_id", "ship_date", "carrier",
        "tracking_number", "status", "estimated_arrival"
    ])
    write_csv("delivery_event.csv", delivery_events, [
        "event_id", "shipment_id", "event_datetime", "event_type", "location", "description"
    ])
    write_csv("sales_order.csv", sales_orders, [
        "so_id", "customer_id", "warehouse_id", "order_date", "ship_date", "status", "total_amount"
    ])
    write_csv("sales_order_line.csv", sales_order_lines, [
        "line_id", "so_id", "product_id", "quantity", "unit_price", "line_total"
    ])
    write_csv("quality_inspection.csv", quality_inspections, [
        "inspection_id", "shipment_id", "product_id", "inspection_date",
        "inspector", "result", "defect_count", "notes"
    ])
    write_csv("return_request.csv", return_requests, [
        "return_id", "so_id", "customer_id", "request_date", "reason", "status", "refund_amount"
    ])

    print()
    print("=" * 60)
    print("✅ Dataset generation complete!")
    print("=" * 60)

    # Summary
    all_data = [suppliers, categories, products, warehouses, inventory, customers,
                purchase_orders, order_lines, shipments, delivery_events,
                sales_orders, sales_order_lines, quality_inspections, return_requests]
    total_records = sum(len(d) for d in all_data)

    print(f"\n📊 Summary:")
    print(f"   • supplier.csv:          {len(suppliers):5d} records")
    print(f"   • product_category.csv:  {len(categories):5d} records")
    print(f"   • product.csv:           {len(products):5d} records")
    print(f"   • warehouse.csv:         {len(warehouses):5d} records")
    print(f"   • inventory.csv:         {len(inventory):5d} records")
    print(f"   • customer.csv:          {len(customers):5d} records")
    print(f"   • purchase_order.csv:    {len(purchase_orders):5d} records")
    print(f"   • order_line.csv:        {len(order_lines):5d} records")
    print(f"   • shipment.csv:          {len(shipments):5d} records")
    print(f"   • delivery_event.csv:    {len(delivery_events):5d} records")
    print(f"   • sales_order.csv:       {len(sales_orders):5d} records")
    print(f"   • sales_order_line.csv:  {len(sales_order_lines):5d} records")
    print(f"   • quality_inspection.csv:{len(quality_inspections):5d} records")
    print(f"   • return_request.csv:    {len(return_requests):5d} records")
    print(f"   ─────────────────────────────────")
    print(f"   Total:                  {total_records:5d} records")

if __name__ == "__main__":
    main()
