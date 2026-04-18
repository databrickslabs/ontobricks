"""Tests for SHACL data quality — service, SQL translation, in-memory evaluation, population helpers."""

import pytest
from unittest.mock import MagicMock, patch

from back.core.w3c.shacl.SHACLService import SHACLService
from back.core.w3c.shacl.constants import QUALITY_CATEGORIES
from back.objects.digitaltwin import DigitalTwin


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
BASE_URI = "http://test.org/ontology#"
TABLE = "catalog.schema.triples"


@pytest.fixture
def shacl_svc():
    return SHACLService(base_uri=BASE_URI)


def _make_shape(
    category="completeness",
    target_class="Customer",
    target_class_uri="http://test.org/ontology#Customer",
    property_path="email",
    property_uri="http://test.org/ontology/email",
    shacl_type="sh:minCount",
    parameters=None,
    **kw,
):
    return SHACLService.create_shape(
        category=category,
        target_class=target_class,
        target_class_uri=target_class_uri,
        property_path=property_path,
        property_uri=property_uri,
        shacl_type=shacl_type,
        parameters=parameters or {"sh:minCount": 1},
        **kw,
    )


def _sample_triples():
    """In-memory triples for a small Customer/Order graph."""
    return [
        {
            "subject": "http://test.org/data/c1",
            "predicate": RDF_TYPE,
            "object": "http://test.org/ontology#Customer",
        },
        {
            "subject": "http://test.org/data/c2",
            "predicate": RDF_TYPE,
            "object": "http://test.org/ontology#Customer",
        },
        {
            "subject": "http://test.org/data/c3",
            "predicate": RDF_TYPE,
            "object": "http://test.org/ontology#Customer",
        },
        {
            "subject": "http://test.org/data/c1",
            "predicate": "http://test.org/ontology/email",
            "object": "alice@example.com",
        },
        {
            "subject": "http://test.org/data/c2",
            "predicate": "http://test.org/ontology/email",
            "object": "bob@example.com",
        },
        # c3 has NO email -> completeness violation
        {
            "subject": "http://test.org/data/c1",
            "predicate": "http://test.org/ontology/status",
            "object": "active",
        },
        {
            "subject": "http://test.org/data/c2",
            "predicate": "http://test.org/ontology/status",
            "object": "inactive",
        },
        {
            "subject": "http://test.org/data/c3",
            "predicate": "http://test.org/ontology/status",
            "object": "active",
        },
        {
            "subject": "http://test.org/data/o1",
            "predicate": RDF_TYPE,
            "object": "http://test.org/ontology#Order",
        },
        {
            "subject": "http://test.org/data/c1",
            "predicate": "http://test.org/ontology/hasOrder",
            "object": "http://test.org/data/o1",
        },
    ]


# ===========================================================================
# Shape CRUD
# ===========================================================================


class TestShapeCRUD:
    def test_create_shape_defaults(self):
        shape = _make_shape()
        assert shape["category"] == "completeness"
        assert shape["target_class"] == "Customer"
        assert shape["enabled"] is True
        assert shape["id"].startswith("shape_completeness_Customer_")

    def test_create_shape_invalid_category_falls_back(self):
        shape = SHACLService.create_shape(
            category="nonexistent",
            target_class="X",
            target_class_uri="u",
        )
        assert shape["category"] == "conformance"

    def test_update_shape(self):
        shapes = [_make_shape()]
        sid = shapes[0]["id"]
        updated = SHACLService.update_shape(
            shapes, sid, {"enabled": False, "message": "updated"}
        )
        assert len(updated) == 1
        assert updated[0]["enabled"] is False
        assert updated[0]["message"] == "updated"
        assert updated[0]["id"] == sid

    def test_update_nonexistent_shape(self):
        shapes = [_make_shape()]
        updated = SHACLService.update_shape(shapes, "bogus_id", {"enabled": False})
        assert all(s["enabled"] is True for s in updated)

    def test_delete_shape(self):
        shapes = [_make_shape(), _make_shape(category="cardinality")]
        sid = shapes[0]["id"]
        remaining = SHACLService.delete_shape(shapes, sid)
        assert len(remaining) == 1
        assert remaining[0]["id"] != sid


# ===========================================================================
# SHACL → SQL translation
# ===========================================================================


class TestShapeToSQL:
    def test_min_count_sql(self):
        shape = _make_shape(shacl_type="sh:minCount", parameters={"sh:minCount": 1})
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "LEFT JOIN" in sql
        assert "COUNT" in sql
        assert "HAVING" in sql

    def test_max_count_sql(self):
        shape = _make_shape(shacl_type="sh:maxCount", parameters={"sh:maxCount": 3})
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "HAVING" in sql
        assert "> 3" in sql

    def test_exact_count_sql(self):
        shape = _make_shape(
            shacl_type="sh:minCount",
            parameters={"sh:minCount": 2, "sh:maxCount": 2},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "< 2" in sql
        assert "> 2" in sql
        assert " OR " in sql, "exact cardinality HAVING must use OR, not AND"

    def test_exact_count_one_sql(self):
        """Regression: min=1 + max=1 must produce a satisfiable HAVING clause."""
        shape = _make_shape(
            shacl_type="sh:minCount",
            parameters={"sh:minCount": 1, "sh:maxCount": 1},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "< 1" in sql
        assert "> 1" in sql
        assert " OR " in sql
        assert " AND " not in sql.split("HAVING")[1]

    def test_pattern_sql(self):
        shape = _make_shape(
            shacl_type="sh:pattern",
            parameters={"sh:pattern": "^[A-Z]"},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "RLIKE" in sql
        assert "^[A-Z]" in sql

    def test_has_value_sql(self):
        shape = _make_shape(
            shacl_type="sh:hasValue",
            parameters={"sh:hasValue": "active"},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "IS NULL" in sql
        assert "'active'" in sql

    def test_class_constraint_sql(self):
        shape = _make_shape(
            shacl_type="sh:class",
            parameters={"sh:class": "http://test.org/ontology#Order"},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "t3.subject IS NULL" in sql

    def test_datatype_string_returns_none(self):
        shape = _make_shape(
            shacl_type="sh:datatype", parameters={"sh:datatype": "xsd:string"}
        )
        assert SHACLService.shape_to_sql(shape, TABLE) is None

    def test_datatype_date_sql(self):
        shape = _make_shape(
            shacl_type="sh:datatype", parameters={"sh:datatype": "date"}
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "TRY_CAST" in sql
        assert "DATE" in sql

    def test_datatype_integer_sql(self):
        shape = _make_shape(
            shacl_type="sh:datatype", parameters={"sh:datatype": "xsd:integer"}
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "TRY_CAST" in sql
        assert "INT" in sql

    def test_datatype_boolean_sql(self):
        shape = _make_shape(
            shacl_type="sh:datatype", parameters={"sh:datatype": "boolean"}
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "TRY_CAST" in sql
        assert "BOOLEAN" in sql

    def test_sql_normalizes_hash_uri_to_slash(self):
        """Property URI with # separator must be normalized to / for SQL."""
        shape = _make_shape(
            property_uri="http://test.org/ontology#email",
            shacl_type="sh:pattern",
            parameters={"sh:pattern": ".*@.*"},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "http://test.org/ontology/email" in sql
        assert "http://test.org/ontology#email" not in sql

    def test_sparql_unknown_returns_none(self):
        shape = _make_shape(
            shacl_type="sh:sparql", parameters={"sh:select": "SELECT ..."}
        )
        assert SHACLService.shape_to_sql(shape, TABLE) is None

    def test_sparql_no_orphans_sql(self):
        """The noOrphans sh:sparql pattern must produce native SQL."""
        query = (
            "SELECT $this WHERE { "
            "$this a ?type . "
            "FILTER NOT EXISTS { $this ?p ?o . FILTER (?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) } "
            "FILTER NOT EXISTS { ?s ?p2 $this . FILTER (?p2 != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) } "
            "}"
        )
        shape = SHACLService.create_shape(
            category="structural",
            target_class="",
            target_class_uri="",
            shacl_type="sh:sparql",
            parameters={"sh:select": query},
            message="Every entity must have at least one relationship (no orphans)",
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "NOT EXISTS" in sql
        assert "rdf-syntax-ns#type" in sql

    def test_closed_returns_none(self):
        shape = SHACLService.create_shape(
            category="structural",
            target_class="Customer",
            target_class_uri="http://test.org/ontology#Customer",
            property_path="",
            property_uri="",
            shacl_type="sh:closed",
            parameters={"sh:closed": True},
        )
        assert SHACLService.shape_to_sql(shape, TABLE) is None

    def test_missing_uris_returns_none(self):
        shape = _make_shape(
            shacl_type="sh:pattern",
            parameters={"sh:pattern": ".*"},
            target_class_uri="",
            property_uri="",
        )
        assert SHACLService.shape_to_sql(shape, TABLE) is None

    def test_global_max_count_without_class(self):
        shape = SHACLService.create_shape(
            category="uniqueness",
            target_class="",
            target_class_uri="",
            property_path="someProp",
            property_uri="http://test.org/someProp",
            shacl_type="sh:maxCount",
            parameters={"sh:maxCount": 1},
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert "GROUP BY" in sql
        assert "HAVING" in sql


# ===========================================================================
# In-memory evaluation
# ===========================================================================


class TestEvaluateShapeInMemory:
    def test_min_count_violations(self):
        shape = _make_shape(shacl_type="sh:minCount", parameters={"sh:minCount": 1})
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 1
        assert "c3" in violations[0]["s"]

    def test_min_count_no_violations(self):
        shape = _make_shape(
            property_path="status",
            property_uri="http://test.org/ontology/status",
            shacl_type="sh:minCount",
            parameters={"sh:minCount": 1},
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 0

    def test_max_count_violations(self):
        triples = _sample_triples() + [
            {
                "subject": "http://test.org/data/c1",
                "predicate": "http://test.org/ontology/email",
                "object": "alice2@example.com",
            },
        ]
        shape = _make_shape(shacl_type="sh:maxCount", parameters={"sh:maxCount": 1})
        violations = SHACLService.evaluate_shape_in_memory(shape, triples)
        assert len(violations) == 1
        assert "c1" in violations[0]["s"]

    def test_pattern_violations(self):
        shape = _make_shape(
            shacl_type="sh:pattern",
            parameters={"sh:pattern": "^[A-Z]"},
            property_path="email",
            property_uri="http://test.org/ontology/email",
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 2  # alice@... and bob@... don't start with uppercase

    def test_pattern_case_insensitive(self):
        shape = _make_shape(
            shacl_type="sh:pattern",
            parameters={"sh:pattern": "^alice", "sh:flags": "i"},
            property_path="email",
            property_uri="http://test.org/ontology/email",
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 1  # only bob's email fails

    def test_has_value_violations(self):
        shape = _make_shape(
            shacl_type="sh:hasValue",
            parameters={"sh:hasValue": "active"},
            property_path="status",
            property_uri="http://test.org/ontology/status",
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 1  # c2 is "inactive"

    def test_class_constraint_violations(self):
        shape = _make_shape(
            shacl_type="sh:class",
            parameters={"sh:class": "http://test.org/ontology#Order"},
            property_path="hasOrder",
            property_uri="http://test.org/ontology/hasOrder",
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 0  # c1 -> o1 which IS an Order

    def test_class_constraint_with_missing_type(self):
        triples = _sample_triples() + [
            {
                "subject": "http://test.org/data/c2",
                "predicate": "http://test.org/ontology/hasOrder",
                "object": "http://test.org/data/x99",
            },
        ]
        shape = _make_shape(
            shacl_type="sh:class",
            parameters={"sh:class": "http://test.org/ontology#Order"},
            property_path="hasOrder",
            property_uri="http://test.org/ontology/hasOrder",
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, triples)
        assert len(violations) == 1
        assert "x99" in violations[0]["target"]

    def test_unsupported_shacl_type_returns_empty(self):
        shape = _make_shape(
            shacl_type="sh:sparql", parameters={"sh:select": "SELECT ..."}
        )
        assert SHACLService.evaluate_shape_in_memory(shape, _sample_triples()) == []

    def test_empty_triples(self):
        shape = _make_shape()
        assert SHACLService.evaluate_shape_in_memory(shape, []) == []

    def test_uri_hash_to_slash_fallback(self):
        """Shape with '#' URI must match triples that use '/' separator."""
        triples = [
            {
                "subject": "http://test.org/data/c1",
                "predicate": RDF_TYPE,
                "object": "http://test.org/ontology#Customer",
            },
            {
                "subject": "http://test.org/data/c1",
                "predicate": "http://test.org/ontology/email",
                "object": "a@b.com",
            },
        ]
        shape = _make_shape(
            property_uri="http://test.org/ontology#email",  # uses # but triples use /
            shacl_type="sh:minCount",
            parameters={"sh:minCount": 1},
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, triples)
        assert (
            len(violations) == 0
        ), "URI fallback should resolve # → / and find the value"

    def test_uri_slash_to_hash_fallback(self):
        """Shape with '/' URI must match triples that use '#' separator."""
        triples = [
            {
                "subject": "http://test.org/data/c1",
                "predicate": RDF_TYPE,
                "object": "http://test.org/ontology#Customer",
            },
            {
                "subject": "http://test.org/data/c1",
                "predicate": "http://test.org/ontology#email",
                "object": "a@b.com",
            },
        ]
        shape = _make_shape(
            property_uri="http://test.org/ontology/email",  # uses / but triples use #
            shacl_type="sh:minCount",
            parameters={"sh:minCount": 1},
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, triples)
        assert (
            len(violations) == 0
        ), "URI fallback should resolve / → # and find the value"

    def test_exact_cardinality_in_memory(self):
        """min=1, max=1: instances with exactly 1 value should pass."""
        triples = _sample_triples()
        shape = _make_shape(
            shacl_type="sh:minCount",
            parameters={"sh:minCount": 1, "sh:maxCount": 1},
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, triples)
        assert len(violations) == 1
        assert "c3" in violations[0]["s"]


# ===========================================================================
# Turtle generation & parsing round-trip
# ===========================================================================


class TestTurtleRoundTrip:
    def test_generate_turtle(self, shacl_svc):
        shapes = [_make_shape()]
        turtle = shacl_svc.generate_turtle(shapes)
        assert "sh:NodeShape" in turtle or "sh:property" in turtle

    def test_roundtrip(self, shacl_svc):
        original = _make_shape(message="email is required")
        turtle = shacl_svc.generate_turtle([original])
        parsed = shacl_svc.import_shapes(turtle)
        assert len(parsed) >= 1
        found = parsed[0]
        assert found.get("target_class_uri") == original["target_class_uri"]


# ===========================================================================
# Legacy constraint migration
# ===========================================================================


class TestLegacyMigration:
    def test_migrate_min_cardinality(self, shacl_svc):
        constraints = [
            {
                "type": "minCardinality",
                "className": "Customer",
                "classUri": "http://test.org/ontology#Customer",
                "property": "email",
                "cardinalityValue": 1,
            },
        ]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 1
        assert shapes[0]["category"] == "structural"
        assert shapes[0]["parameters"]["sh:minCount"] == 1

    def test_migrate_max_cardinality(self, shacl_svc):
        constraints = [
            {
                "type": "maxCardinality",
                "className": "Customer",
                "classUri": "http://test.org/ontology#Customer",
                "property": "phone",
                "cardinalityValue": 3,
            },
        ]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 1
        assert shapes[0]["category"] == "structural"

    def test_migrate_functional(self, shacl_svc):
        constraints = [{"type": "functional", "property": "hasId"}]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 1
        assert shapes[0]["category"] == "consistency"

    def test_migrate_value_check_not_null(self, shacl_svc):
        constraints = [
            {
                "type": "valueCheck",
                "className": "Customer",
                "attributeName": "name",
                "checkType": "notNull",
            },
        ]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 1
        assert shapes[0]["category"] == "consistency"

    def test_migrate_value_check_pattern(self, shacl_svc):
        constraints = [
            {
                "type": "valueCheck",
                "className": "Customer",
                "attributeName": "email",
                "checkType": "contains",
                "checkValue": "@",
            },
        ]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 1
        assert shapes[0]["shacl_type"] == "sh:pattern"

    def test_migrate_global_rule_no_orphans(self, shacl_svc):
        constraints = [{"type": "globalRule", "ruleName": "noOrphans"}]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 1
        assert shapes[0]["category"] == "structural"

    def test_migrate_skips_property_characteristics(self, shacl_svc):
        constraints = [
            {"type": "transitive", "property": "isPartOf"},
            {"type": "symmetric", "property": "hasSibling"},
        ]
        shapes = shacl_svc.migrate_legacy_constraints(constraints, base_uri=BASE_URI)
        assert len(shapes) == 0


# ===========================================================================
# Population counting & enrichment helpers
# ===========================================================================


class TestPopulationHelpers:
    def test_count_class_population_graph_dicts(self):
        triples = _sample_triples()
        count = DigitalTwin._count_class_population_graph(
            triples, "http://test.org/ontology#Customer"
        )
        assert count == 3

    def test_count_class_population_graph_tuples(self):
        triples = [
            ("http://test.org/data/c1", RDF_TYPE, "http://test.org/ontology#Customer"),
            ("http://test.org/data/c2", RDF_TYPE, "http://test.org/ontology#Customer"),
        ]
        count = DigitalTwin._count_class_population_graph(
            triples, "http://test.org/ontology#Customer"
        )
        assert count == 2

    def test_count_class_population_graph_caching(self):
        triples = _sample_triples()
        cache = {}
        DigitalTwin._count_class_population_graph(
            triples, "http://test.org/ontology#Customer", cache
        )
        assert "http://test.org/ontology#Customer" in cache
        assert cache["http://test.org/ontology#Customer"] == 3

    def test_count_class_population_graph_empty_uri(self):
        assert DigitalTwin._count_class_population_graph([], "") is None

    def test_count_class_population_sql(self):
        store = MagicMock()
        store.execute_query.return_value = [{"cnt": 42}]
        count = DigitalTwin._count_class_population_sql(
            store, TABLE, "http://test.org/ontology#Customer"
        )
        assert count == 42
        store.execute_query.assert_called_once()

    def test_count_class_population_sql_cached(self):
        store = MagicMock()
        cache = {(TABLE, "http://test.org/ontology#Customer"): 99}
        count = DigitalTwin._count_class_population_sql(
            store, TABLE, "http://test.org/ontology#Customer", cache
        )
        assert count == 99
        store.execute_query.assert_not_called()

    def test_count_class_population_sql_error(self):
        store = MagicMock()
        store.execute_query.side_effect = Exception("SQL error")
        count = DigitalTwin._count_class_population_sql(
            store, TABLE, "http://test.org/ontology#Customer"
        )
        assert count is None

    def test_enrich_with_population(self):
        result = {"violations": [{"s": "a"}, {"s": "b"}], "message": "original"}
        enriched = DigitalTwin._enrich_with_population(result, 10)
        assert enriched["total_population"] == 10
        assert enriched["pass_pct"] == 80.0
        assert "80.0%" in enriched["message"]

    def test_enrich_with_population_all_pass(self):
        result = {"violations": [], "message": "ok"}
        enriched = DigitalTwin._enrich_with_population(result, 5)
        assert enriched["pass_pct"] == 100.0
        assert enriched["message"] == "ok"

    def test_enrich_with_population_none(self):
        result = {"violations": [{"s": "a"}], "message": "msg"}
        enriched = DigitalTwin._enrich_with_population(result, None)
        assert "pass_pct" not in enriched

    def test_enrich_with_population_zero(self):
        result = {"violations": [], "message": "msg"}
        enriched = DigitalTwin._enrich_with_population(result, 0)
        assert "pass_pct" not in enriched


# ===========================================================================
# complete_dq_task helper
# ===========================================================================


class TestCompleteDQTask:
    def test_complete_dq_task(self):
        from back.objects.digitaltwin import complete_dq_task

        tm = MagicMock()
        task = MagicMock()
        task.id = "task-1"
        results = [
            {"status": "success"},
            {"status": "success"},
            {"status": "error"},
            {"status": "warning"},
        ]
        complete_dq_task(tm, task, results, 1.5)
        tm.complete_task.assert_called_once()
        call_kw = tm.complete_task.call_args
        summary = (
            call_kw[1]["result"]["summary"]
            if "result" in (call_kw[1] or {})
            else call_kw[0][1]["summary"]
        )
        assert summary["passed"] == 2
        assert summary["failed"] == 1
        assert summary["warnings"] == 1
