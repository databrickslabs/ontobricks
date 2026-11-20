"""Domain-document variants of the supply-chain fixture used by the Evolve eval rows."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict

HERE = Path(__file__).resolve().parent
NS = "http://ontobricks.local/evo/supplychain#"


def base_document(catalog: str = "cat", schema: str = "fix") -> Dict[str, Any]:
    raw = (HERE / "supply_chain_domain.json").read_text()
    return json.loads(raw.replace("__CATALOG__", catalog).replace("__SCHEMA__", schema))


def _slice(doc: Dict[str, Any]) -> Dict[str, Any]:
    return doc["versions"]["1"]


def _retable(
    doc: Dict[str, Any], cls: str, table: str, catalog: str, schema: str
) -> None:
    """Point one class's entity mapping, and every mapping SQL reading its table, at *table*."""
    sl = _slice(doc)
    for ent in sl["assignment"]["entities"]:
        if ent["ontology_class"] == NS + cls:
            old = ent["table"]
            ent["table"] = table
            ent["sql_query"] = f"SELECT * FROM {catalog}.{schema}.{table}"
    for rel in sl["assignment"]["relationships"]:
        rel["sql_query"] = rel["sql_query"].replace(
            f"{catalog}.{schema}.{old} ", f"{catalog}.{schema}.{table} "
        )
        if rel["sql_query"].endswith(f"{catalog}.{schema}.{old}"):
            rel["sql_query"] = rel["sql_query"][: -len(old)] + table


def variant_document(
    name: str, catalog: str = "cat", schema: str = "fix"
) -> Dict[str, Any]:
    doc = base_document(catalog, schema)
    if name in ("base", "restricted"):
        return doc
    if name == "nofk":
        _retable(doc, "OrderLine", "order_lines_nofk", catalog, schema)
    elif name == "orphan":
        _retable(doc, "OrderLine", "order_lines_orphan", catalog, schema)
    elif name == "dup":
        _retable(doc, "Product", "products_dup", catalog, schema)
    elif name == "ambiguous_supplier":
        _retable(doc, "Supplier", "suppliers_ambiguous", catalog, schema)
    elif name == "missing_source":
        _retable(doc, "Supplier", "suppliers_missing", catalog, schema)
    elif name == "broken_mapping":
        sl = _slice(doc)
        sl["ontology"]["properties"].append(
            {
                "uri": NS + "lineProduct",
                "name": "lineProduct",
                "label": "line product",
                "description": "The product on an order line",
                "type": "ObjectProperty",
                "domain": "OrderLine",
                "range": "Product",
                "direction": "forward",
            }
        )
        sl["assignment"]["relationships"].append(
            {
                "property": NS + "lineProduct",
                "property_label": "line product",
                "sql_query": f"SELECT line_id, alt_product_ref FROM {catalog}.{schema}.order_lines",
                "source_class": NS + "OrderLine",
                "source_class_label": "Order Line",
                "target_class": NS + "Product",
                "target_class_label": "Product",
                "source_table": "order_lines",
                "target_table": "products",
                "source_id_column": "line_id",
                "target_id_column": "alt_product_ref",
                "direction": "forward",
            }
        )
    else:
        raise ValueError(f"Unknown variant {name!r}")
    return copy.deepcopy(doc)
