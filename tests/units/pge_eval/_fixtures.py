"""Synthetic, usecase-agnostic fixtures for the PGE evaluator unit tests.

Deliberately uses a generic e-commerce-ish toy domain (Customer / Order /
Product) so the tests prove the scorer is domain-free — none of these names
appear in the scorer code.
"""

from copy import deepcopy


def clean_ontology() -> dict:
    """Agent-shape ontology that is fully structurally clean."""
    return {
        "entities": [
            {
                "uri": "ex:Customer",
                "name": "Customer",
                "attributes": ["firstName", "lastName", "email"],
            },
            {
                "uri": "ex:Order",
                "name": "Order",
                "attributes": ["orderDate", "totalAmount"],
            },
            {
                "uri": "ex:Product",
                "name": "Product",
                "attributes": ["sku", "unitPrice"],
            },
        ],
        "relationships": [
            {
                "uri": "ex:placesOrder",
                "name": "placesOrder",
                "domain": "ex:Customer",
                "range": "ex:Order",
            },
            {
                "uri": "ex:containsProduct",
                "name": "containsProduct",
                "domain": "ex:Order",
                "range": "ex:Product",
            },
        ],
    }


def clean_metadata() -> dict:
    return {
        "tables": [
            {
                "name": "customers",
                "columns": [
                    {"name": "id"},
                    {"name": "first_name"},
                    {"name": "last_name"},
                    {"name": "email"},
                    {"name": "created_at"},
                ],
            },
            {
                "name": "orders",
                "columns": [
                    {"name": "id"},
                    {"name": "order_date"},
                    {"name": "total_amount"},
                ],
            },
            {
                "name": "products",
                "columns": [
                    {"name": "id"},
                    {"name": "sku"},
                    {"name": "unit_price"},
                ],
            },
        ]
    }


def clean_artifact() -> dict:
    onto = clean_ontology()
    meta = clean_metadata()
    return {
        "success": True,
        "iterations": 3,
        "usage": {"prompt_tokens": 1000, "completion_tokens": 400},
        "stats": {"planner_reinvocations": 0},
        "mapping_run_log": [
            {"item": "ex:Customer", "kind": "entity", "attempts": [{}], "final_status": "PASS"},
            {"item": "ex:Order", "kind": "entity", "attempts": [{}], "final_status": "PASS"},
            {"item": "ex:Product", "kind": "entity", "attempts": [{}], "final_status": "PASS"},
            {"item": "ex:placesOrder", "kind": "relationship", "attempts": [{}], "final_status": "PASS"},
            {"item": "ex:containsProduct", "kind": "relationship", "attempts": [{}], "final_status": "PASS"},
        ],
        "mapping_evaluations": {
            "ex:Customer": {"metrics": {"row_count": 100, "distinct_id_count": 100, "null_id_count": 0}, "failures": []},
            "ex:Order": {"metrics": {"row_count": 500, "distinct_id_count": 500, "null_id_count": 0}, "failures": []},
            "ex:Product": {"metrics": {"row_count": 50, "distinct_id_count": 50, "null_id_count": 0}, "failures": []},
            "ex:placesOrder": {"metrics": {"total_edges": 500, "dangling_source_pct": 0.0, "dangling_target_pct": 0.0}, "failures": []},
            "ex:containsProduct": {"metrics": {"total_edges": 800, "dangling_source_pct": 0.0, "dangling_target_pct": 0.0}, "failures": []},
        },
        "entity_mappings": [
            {"ontology_class": "ex:Customer", "attribute_mappings": {"firstName": "first_name", "lastName": "last_name", "email": "email"}},
            {"ontology_class": "ex:Order", "attribute_mappings": {"orderDate": "order_date", "totalAmount": "total_amount"}},
            {"ontology_class": "ex:Product", "attribute_mappings": {"sku": "sku", "unitPrice": "unit_price"}},
        ],
        "relationship_mappings": [],
        "steps": [{"step_type": "planner", "tool_name": "", "duration_ms": 1200}],
        "ontology": onto,
        "metadata": meta,
        "elapsed_s": 42.5,
    }


def artifact_with_dangling_fk() -> dict:
    """Clean except one relationship has a dangling target FK > 5%."""
    art = clean_artifact()
    art["mapping_evaluations"]["ex:placesOrder"]["metrics"]["dangling_target_pct"] = 0.47
    return art


def artifact_with_sql_failure() -> dict:
    """Clean except one entity's SQL failed to execute."""
    art = clean_artifact()
    art["mapping_evaluations"]["ex:Order"] = {
        "metrics": {"sql_error": "UNION type mismatch"},
        "failures": [
            {
                "check": "sql_execution",
                "expected": "SQL executes without error",
                "observed": "execution error",
                "hint": "fix the SQL",
            }
        ],
    }
    # The entity drops out of PASS in the run log too (in-scope but failed).
    for entry in art["mapping_run_log"]:
        if entry["item"] == "ex:Order":
            entry["final_status"] = "FAIL"
    return art


def ontology_with_orphan() -> dict:
    """Add a class with no data properties and no relationships."""
    onto = clean_ontology()
    onto["entities"].append({"uri": "ex:Ghost", "name": "Ghost", "attributes": []})
    return onto


def artifact_with_orphan_class() -> dict:
    art = clean_artifact()
    art["ontology"] = ontology_with_orphan()
    return art


def ontology_with_dangling_range() -> dict:
    onto = clean_ontology()
    onto["relationships"].append(
        {"uri": "ex:refersTo", "name": "refersTo", "domain": "ex:Order", "range": "ex:Nonexistent"}
    )
    return onto


def ontology_with_naming_violation() -> dict:
    onto = clean_ontology()
    onto["entities"].append(
        {"uri": "ex:bad_class", "name": "bad_class", "attributes": ["someAttr"]}
    )
    return onto


def ontology_with_duplicate_class() -> dict:
    onto = clean_ontology()
    onto["entities"].append(
        {"uri": "ex:Customer2", "name": "Customer", "attributes": ["nickname"]}
    )
    return onto
