"""TPC-H domain document for realistic Ontology Evolve tests.

Eight classes over the raw TPC-H tables seeded by ``seed_tpch.py``. Mapped
relationships: Nation→Region, Customer→Nation, Supplier→Nation,
Order→Customer, Order→LineItem. Deliberately missing: LineItem→Supplier,
LineItem→Part and the many-to-many Part↔Supplier (through ``partsupp``).

Variants change only how LineItem is sourced:

* ``base`` — ``lineitem`` with the surrogate key ``l_lineid`` and declared FKs.
* ``composite`` — no surrogate: the id is computed in the mapping SQL.
* ``nofk`` / ``cast`` / ``nulls`` / ``view`` — LineItem read from
  ``lineitem_nofk`` (no constraints), ``lineitem_cast`` (``l_suppkey`` as
  string), ``lineitem_nulls`` (5% null ``l_suppkey``) or ``lineitem_v`` (view).
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

NS = "http://ontobricks.local/evo/tpch#"
VARIANTS = ("base", "composite", "nofk", "cast", "nulls", "view")

# class -> (table, id column, label column, {attribute: column}, alternate labels)
CLASSES: Dict[str, Tuple[str, str, str, Dict[str, str], List[str]]] = {
    "Region": ("region", "r_regionkey", "r_name", {"regionName": "r_name"}, []),
    "Nation": (
        "nation",
        "n_nationkey",
        "n_name",
        {"nationName": "n_name"},
        ["country"],
    ),
    "Customer": (
        "customer",
        "c_custkey",
        "c_name",
        {"customerName": "c_name", "marketSegment": "c_mktsegment"},
        ["client"],
    ),
    "Supplier": (
        "supplier",
        "s_suppkey",
        "s_name",
        {"supplierName": "s_name"},
        ["vendor"],
    ),
    "Part": (
        "part",
        "p_partkey",
        "p_name",
        {"partName": "p_name", "brand": "p_brand"},
        ["product"],
    ),
    "Order": (
        "orders",
        "o_orderkey",
        "",
        {"orderStatus": "o_orderstatus", "orderPriority": "o_orderpriority"},
        ["purchase order"],
    ),
    "LineItem": (
        "lineitem",
        "l_lineid",
        "",
        {"shipMode": "l_shipmode", "quantity": "l_quantity"},
        ["order line", "line item"],
    ),
}

# name -> (domain, range, table, source id column, target id column)
RELATIONSHIPS = {
    "inRegion": ("Nation", "Region", "nation", "n_nationkey", "n_regionkey"),
    "customerNation": ("Customer", "Nation", "customer", "c_custkey", "c_nationkey"),
    "supplierNation": ("Supplier", "Nation", "supplier", "s_suppkey", "s_nationkey"),
    "placedBy": ("Order", "Customer", "orders", "o_orderkey", "o_custkey"),
    "hasLineItem": ("Order", "LineItem", "lineitem", "l_orderkey", "l_lineid"),
}


def _label(name: str) -> str:
    out = "".join(" " + c.lower() if c.isupper() else c for c in name).strip()
    return out


def tpch_document(catalog: str, schema: str, variant: str = "base") -> Dict[str, Any]:
    if variant not in VARIANTS:
        raise ValueError(variant)
    fq = f"{catalog}.{schema}"
    line_table = {
        "nofk": "lineitem_nofk",
        "cast": "lineitem_cast",
        "nulls": "lineitem_nulls",
        "view": "lineitem_v",
    }.get(variant, "lineitem")
    classes, entities = [], []
    for name, (table, id_col, label_col, attrs, alts) in CLASSES.items():
        classes.append(
            {
                "uri": NS + name,
                "name": name,
                "label": _label(name).title(),
                "description": "",
                "parent": "",
                "alternate_labels": alts,
                "dataProperties": [
                    {"name": a, "localName": a, "label": _label(a), "uri": NS + a}
                    for a in attrs
                ],
            }
        )
        sql = f"SELECT * FROM {fq}.{table}"
        if name == "LineItem":
            table = line_table
            sql = f"SELECT * FROM {fq}.{table}"
            if variant == "composite":
                id_col = "line_key"
                sql = (
                    f"SELECT concat(CAST(l_orderkey AS STRING), '_', CAST(l_linenumber AS STRING)) "
                    f"AS line_key, * FROM {fq}.{table}"
                )
        entities.append(
            {
                "ontology_class": NS + name,
                "ontology_class_label": name,
                "sql_query": sql,
                "id_column": id_col,
                "label_column": label_col,
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "attribute_mappings": dict(attrs),
            }
        )
    properties, rels = [], []
    for name, (dom, rng, table, sid, tid) in RELATIONSHIPS.items():
        properties.append(
            {
                "uri": NS + name,
                "name": name,
                "label": _label(name),
                "description": "",
                "type": "ObjectProperty",
                "domain": dom,
                "range": rng,
                "direction": "forward",
            }
        )
        if name == "hasLineItem":
            table = line_table
            if variant == "composite":
                tid = "line_key"
                sql = (
                    f"SELECT l_orderkey, concat(CAST(l_orderkey AS STRING), '_', "
                    f"CAST(l_linenumber AS STRING)) AS line_key FROM {fq}.{table}"
                )
            else:
                sql = f"SELECT {sid}, {tid} FROM {fq}.{table}"
        else:
            sql = f"SELECT {sid}, {tid} FROM {fq}.{table}"
        rels.append(
            {
                "property": NS + name,
                "property_label": _label(name),
                "sql_query": sql,
                "source_class": NS + dom,
                "source_class_label": dom,
                "target_class": NS + rng,
                "target_class_label": rng,
                "source_table": CLASSES[dom][0],
                "target_table": CLASSES[rng][0],
                "source_id_column": sid,
                "target_id_column": tid,
                "direction": "forward",
            }
        )
    return {
        "info": {
            "name": f"EvoTpch{schema.rsplit('_', 1)[-1].upper()}",
            "description": f"TPC-H realistic Evolve fixture ({variant})",
            "author": "ontobricks-evolve fixture",
            "llm_endpoint": "",
            "graph_backend": "databricks",
        },
        "versions": {
            "1": {
                "ontology": {
                    "name": "EvoTpch",
                    "base_uri": NS,
                    "description": "TPC-H orders and suppliers",
                    "classes": classes,
                    "properties": properties,
                    "constraints": [],
                    "if_then_rules": [],
                    "axioms": [],
                    "expressions": [],
                },
                "assignment": {"entities": entities, "relationships": rels},
                "design_layout": {},
            }
        },
    }


def f(cls: str, attr: str, value: str) -> Dict[str, str]:
    return {"class": cls, "attribute": attr, "op": "eq", "value": value}


def questions(params: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """Intent per gold question id; *params* comes from ``gold_params``."""
    return {
        "orders_by_supplier": {
            "output_class": "Order",
            "mentions": ["orders", "supplier"],
            "filters": [f("Supplier", "supplierName", params["orders_by_supplier"])],
        },
        "orders_by_part": {
            "output_class": "Order",
            "mentions": ["orders", "part"],
            "filters": [f("Part", "partName", params["orders_by_part"])],
        },
        "suppliers_of_part": {
            "output_class": "Supplier",
            "mentions": ["suppliers", "part"],
            "filters": [f("Part", "partName", params["suppliers_of_part"])],
        },
        "orders_by_customer": {
            "output_class": "Order",
            "filters": [f("Customer", "customerName", params["orders_by_customer"])],
        },
        "customers_in_nation": {
            "output_class": "Customer",
            "filters": [f("Nation", "nationName", "GERMANY")],
        },
        "suppliers_in_region": {
            "output_class": "Supplier",
            "filters": [f("Region", "regionName", "EUROPE")],
        },
    }
