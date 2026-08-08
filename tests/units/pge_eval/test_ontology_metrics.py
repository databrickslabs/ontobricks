"""Stage-1 ontology metric tests (deterministic, no LLM)."""

import pytest

from agents.pge_eval.ontology_metrics import evaluate_ontology
from agents.pge_eval.normalize import is_surrogate_or_audit, normalize_name

from tests.units.pge_eval import _fixtures as fx


def test_clean_ontology_all_absolute_zero():
    metrics, issues, _ = evaluate_ontology(fx.clean_ontology(), fx.clean_metadata())
    assert metrics["orphan_class_count"] == 0
    assert metrics["dangling_domain_range_count"] == 0
    assert metrics["naming_violation_count"] == 0
    assert metrics["duplicate_class_count"] == 0
    assert metrics["table_footprint_coverage"] == 1.0
    assert metrics["column_footprint_coverage"] >= 0.9


def test_orphan_class_detected():
    metrics, issues, _ = evaluate_ontology(fx.ontology_with_orphan(), fx.clean_metadata())
    assert metrics["orphan_class_count"] == 1
    assert any(i["check"] == "orphan_class_count" for i in issues)


def test_dangling_range_detected():
    metrics, issues, _ = evaluate_ontology(
        fx.ontology_with_dangling_range(), fx.clean_metadata()
    )
    assert metrics["dangling_domain_range_count"] == 1
    assert any(i["check"] == "dangling_domain_range_count" for i in issues)


def test_naming_violation_detected():
    metrics, _, _ = evaluate_ontology(
        fx.ontology_with_naming_violation(), fx.clean_metadata()
    )
    assert metrics["naming_violation_count"] >= 1


def test_duplicate_class_detected():
    metrics, _, _ = evaluate_ontology(
        fx.ontology_with_duplicate_class(), fx.clean_metadata()
    )
    assert metrics["duplicate_class_count"] == 1


def test_table_coverage_drops_with_unmodelled_table():
    meta = fx.clean_metadata()
    meta["tables"].append({"name": "shipments", "columns": [{"name": "carrier"}]})
    metrics, issues, _ = evaluate_ontology(fx.clean_ontology(), meta)
    assert metrics["table_footprint_coverage"] < 1.0
    assert any(
        i["check"] == "table_footprint_coverage" for i in issues
    )


def test_surrogate_and_audit_columns_excluded():
    assert is_surrogate_or_audit("id")
    assert is_surrogate_or_audit("created_at")
    assert is_surrogate_or_audit("customer_sk")
    assert is_surrogate_or_audit("etl_load_ts")
    assert not is_surrogate_or_audit("first_name")
    assert not is_surrogate_or_audit("customer_id")  # FK can be meaningful


def test_name_normalization():
    assert normalize_name("first_name") == normalize_name("firstName") == "firstname"
    assert normalize_name("Order Date") == "orderdate"


def test_registry_shape_accepted():
    # Same ontology in registry (classes/properties) shape must score identically.
    registry = {
        "classes": [
            {"uri": "ex:A", "name": "A", "dataProperties": [{"name": "x"}]},
        ],
        "properties": [],
    }
    metrics, _, _ = evaluate_ontology(registry, {"tables": []})
    # A has a data property -> not an orphan.
    assert metrics["orphan_class_count"] == 0
