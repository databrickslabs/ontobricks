"""Tests for SHACL data quality — service, SQL translation, in-memory evaluation, population helpers."""

import pytest
from unittest.mock import MagicMock, patch

from back.core.w3c.shacl.SHACLService import SHACLService
from back.core.w3c.shacl.constants import QUALITY_CATEGORIES
from back.objects.digitaltwin import DigitalTwin
from back.objects.ontology.Ontology import Ontology


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
# Conditional shapes — the optional IF guard
# ===========================================================================

STATUS_URI = "http://test.org/ontology/status"
ORDER_URI = "http://test.org/ontology/hasOrder"


def _conditional_pattern_shape(conditions, logic="and"):
    """A conformance rule on email, guarded by *conditions*."""
    return _make_shape(
        category="conformance",
        shacl_type="sh:pattern",
        parameters={"sh:pattern": "^[A-Z]"},
        conditions=conditions,
        condition_logic=logic,
    )


class TestConditionalShapeToSQL:
    def test_conditions_wrap_the_base_query(self):
        shape = _conditional_pattern_shape(
            [{"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}]
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql is not None
        assert sql.startswith("SELECT v.* FROM (")
        assert "WHERE v.s IN (" in sql
        # The constraint half is untouched
        assert "NOT t2.object RLIKE '^[A-Z]'" in sql
        # The guard filters on the condition property
        assert STATUS_URI in sql
        assert "LOWER(c0.object) = 'active'" in sql

    def test_numeric_condition_casts_to_double(self):
        shape = _conditional_pattern_shape(
            [{
                "property": "amount",
                "property_uri": "http://test.org/ontology/amount",
                "op": "gt",
                "value": "1000",
            }]
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert "CAST(c0.object AS DOUBLE) > 1000" in sql

    def test_not_exists_condition_uses_subquery_not_join(self):
        shape = _conditional_pattern_shape(
            [{"property": "hasOrder", "property_uri": ORDER_URI, "op": "notExists", "value": ""}]
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert "NOT EXISTS (SELECT 1" in sql
        assert f"LEFT JOIN {TABLE} c0" not in sql

    def test_or_logic_left_joins_and_ors_the_predicates(self):
        shape = _conditional_pattern_shape(
            [
                {"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"},
                {
                    "property": "amount",
                    "property_uri": "http://test.org/ontology/amount",
                    "op": "gt",
                    "value": "1000",
                },
            ],
            logic="or",
        )
        sql = SHACLService.shape_to_sql(shape, TABLE)
        guard = sql.split("WHERE v.s IN (")[1]
        assert " OR " in guard
        # LEFT JOIN, otherwise a subject missing one property could never match
        # the other branch.
        assert guard.count("LEFT JOIN") == 2
        assert "INNER JOIN" not in guard

    def test_unconditional_shape_sql_is_unchanged(self):
        """Regression: a shape without conditions must not be wrapped."""
        guarded = _conditional_pattern_shape([])
        plain = dict(guarded)
        plain.pop("conditions")
        plain.pop("condition_logic")
        assert SHACLService.shape_to_sql(guarded, TABLE) == SHACLService.shape_to_sql(
            plain, TABLE
        )
        assert "SELECT v.*" not in SHACLService.shape_to_sql(plain, TABLE)

    def test_untranslatable_constraint_stays_untranslatable(self):
        shape = _make_shape(
            category="conformance",
            shacl_type="sh:closed",
            conditions=[
                {"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}
            ],
        )
        shape["parameters"] = {}
        assert SHACLService.shape_to_sql(shape, TABLE) is None


class TestConditionalShapeInMemory:
    def test_guard_filters_violations(self):
        """Both c1 and c2 fail the pattern; only c1 is active."""
        shape = _conditional_pattern_shape(
            [{"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}]
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert [v["s"] for v in violations] == ["http://test.org/data/c1"]

    def test_without_conditions_all_violations_are_kept(self):
        shape = _conditional_pattern_shape([])
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 2

    def test_exists_condition_on_a_relationship(self):
        shape = _conditional_pattern_shape(
            [{"property": "hasOrder", "property_uri": ORDER_URI, "op": "exists", "value": ""}]
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert [v["s"] for v in violations] == ["http://test.org/data/c1"]

    def test_not_exists_condition_on_a_relationship(self):
        shape = _conditional_pattern_shape(
            [{"property": "hasOrder", "property_uri": ORDER_URI, "op": "notExists", "value": ""}]
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert [v["s"] for v in violations] == ["http://test.org/data/c2"]

    def test_and_requires_every_condition(self):
        shape = _conditional_pattern_shape(
            [
                {"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"},
                {"property": "hasOrder", "property_uri": ORDER_URI, "op": "notExists", "value": ""},
            ]
        )
        assert SHACLService.evaluate_shape_in_memory(shape, _sample_triples()) == []

    def test_or_requires_a_single_condition(self):
        shape = _conditional_pattern_shape(
            [
                {"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"},
                {"property": "hasOrder", "property_uri": ORDER_URI, "op": "notExists", "value": ""},
            ],
            logic="or",
        )
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert {v["s"] for v in violations} == {
            "http://test.org/data/c1",
            "http://test.org/data/c2",
        }

    def test_incomplete_condition_is_ignored(self):
        """A half-filled row must not silently narrow the rule."""
        shape = _conditional_pattern_shape([{"property": "status", "op": "eq", "value": "active"}])
        violations = SHACLService.evaluate_shape_in_memory(shape, _sample_triples())
        assert len(violations) == 2


# ===========================================================================
# Numeric range and string length — the conformance types the modal offers
# ===========================================================================


def _range_shape(**params):
    return _make_shape(
        category="conformance", shacl_type="sh:minInclusive", parameters=params
    )


def _length_shape(**params):
    return _make_shape(
        category="conformance", shacl_type="sh:minLength", parameters=params
    )


def _fee_triples(*values):
    """One Customer per value, each carrying it on the shape's property."""
    triples = []
    for i, value in enumerate(values):
        subject = f"http://test.org/data/f{i}"
        triples.append(
            {
                "subject": subject,
                "predicate": RDF_TYPE,
                "object": "http://test.org/ontology#Customer",
            }
        )
        triples.append(
            {
                "subject": subject,
                "predicate": "http://test.org/ontology/email",
                "object": value,
            }
        )
    return triples


class TestNumericRange:
    """"Monthly fee between 1 and 10" — a rule the modal offers under
    conformance, which reported 100% pass because it had no translation."""

    def test_a_range_shape_is_translatable(self):
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        assert SHACLService.shape_to_sql(shape, TABLE) is not None

    def test_both_bounds_must_hold(self):
        sql = SHACLService.shape_to_sql(
            _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10}), TABLE
        )
        assert "TRY_CAST(t2.object AS DOUBLE) >= 1.0" in sql
        assert "TRY_CAST(t2.object AS DOUBLE) <= 10.0" in sql
        assert "AND NOT (" in sql

    def test_a_single_bound_is_enough(self):
        sql = SHACLService.shape_to_sql(_range_shape(**{"sh:minInclusive": 1}), TABLE)
        assert ">= 1.0" in sql
        assert "<=" not in sql

    def test_exclusive_bounds_are_supported(self):
        sql = SHACLService.shape_to_sql(
            _range_shape(**{"sh:minExclusive": 0, "sh:maxExclusive": 100}), TABLE
        )
        assert "> 0.0" in sql
        assert "< 100.0" in sql

    def test_a_shape_with_no_bound_is_untranslatable(self):
        shape = _make_shape(category="conformance", shacl_type="sh:minInclusive")
        shape["parameters"] = {}
        assert SHACLService.shape_to_sql(shape, TABLE) is None

    def test_a_shape_named_by_its_only_bound_is_translatable(self):
        """An imported shape takes its type from its first constraint."""
        shape = _make_shape(
            category="conformance",
            shacl_type="sh:maxInclusive",
            parameters={"sh:maxInclusive": 10},
        )
        assert SHACLService.shape_to_sql(shape, TABLE) is not None

    @pytest.mark.parametrize("value", ["0", "0.99", "10.01", "11"])
    def test_a_value_outside_the_range_is_a_violation(self, value):
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        violations = SHACLService.evaluate_shape_in_memory(shape, _fee_triples(value))
        assert [v["val"] for v in violations] == [value]

    @pytest.mark.parametrize("value", ["1", "5.5", "10"])
    def test_a_value_inside_the_range_passes(self, value):
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        assert SHACLService.evaluate_shape_in_memory(shape, _fee_triples(value)) == []

    def test_a_non_numeric_value_is_a_violation(self):
        """SHACL reports a value node it cannot compare."""
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        violations = SHACLService.evaluate_shape_in_memory(shape, _fee_triples("free"))
        assert [v["val"] for v in violations] == ["free"]

    def test_the_bound_itself_is_inside_an_inclusive_range(self):
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        assert SHACLService.evaluate_shape_in_memory(shape, _fee_triples("1", "10")) == []

    def test_the_bound_itself_is_outside_an_exclusive_range(self):
        shape = _range_shape(**{"sh:minExclusive": 1, "sh:maxExclusive": 10})
        violations = SHACLService.evaluate_shape_in_memory(shape, _fee_triples("1", "10"))
        assert {v["val"] for v in violations} == {"1", "10"}

    def test_only_the_offending_entities_are_reported(self):
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        violations = SHACLService.evaluate_shape_in_memory(
            shape, _fee_triples("5", "42", "7")
        )
        assert [v["val"] for v in violations] == ["42"]

    def test_a_range_rule_can_be_guarded_by_conditions(self):
        shape = _range_shape(**{"sh:minInclusive": 1, "sh:maxInclusive": 10})
        shape["conditions"] = [
            {"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}
        ]
        sql = SHACLService.shape_to_sql(shape, TABLE)
        assert sql.startswith("SELECT v.* FROM (")
        assert "TRY_CAST(t2.object AS DOUBLE)" in sql


class TestStringLength:
    def test_a_length_shape_is_translatable(self):
        sql = SHACLService.shape_to_sql(
            _length_shape(**{"sh:minLength": 3, "sh:maxLength": 8}), TABLE
        )
        assert "LENGTH(t2.object) >= 3" in sql
        assert "LENGTH(t2.object) <= 8" in sql

    @pytest.mark.parametrize("value", ["ab", "abcdefghi"])
    def test_a_value_of_the_wrong_length_is_a_violation(self, value):
        shape = _length_shape(**{"sh:minLength": 3, "sh:maxLength": 8})
        violations = SHACLService.evaluate_shape_in_memory(shape, _fee_triples(value))
        assert [v["val"] for v in violations] == [value]

    @pytest.mark.parametrize("value", ["abc", "abcdefgh"])
    def test_a_value_of_an_allowed_length_passes(self, value):
        shape = _length_shape(**{"sh:minLength": 3, "sh:maxLength": 8})
        assert SHACLService.evaluate_shape_in_memory(shape, _fee_triples(value)) == []

    def test_an_empty_value_violates_a_minimum(self):
        shape = _length_shape(**{"sh:minLength": 1})
        assert len(SHACLService.evaluate_shape_in_memory(shape, _fee_triples(""))) == 1


class TestInMemorySupport:
    """A check the graph engine cannot run must not be reported as passing."""

    @pytest.mark.parametrize(
        "shacl_type,params",
        [
            ("sh:minCount", {"sh:minCount": 1}),
            ("sh:maxCount", {"sh:maxCount": 1}),
            ("sh:pattern", {"sh:pattern": "^a"}),
            ("sh:hasValue", {"sh:hasValue": "x"}),
            ("sh:class", {"sh:class": "http://test.org/ontology/Order"}),
            ("sh:minInclusive", {"sh:minInclusive": 1}),
            ("sh:maxInclusive", {"sh:maxInclusive": 1}),
            ("sh:minExclusive", {"sh:minExclusive": 1}),
            ("sh:maxExclusive", {"sh:maxExclusive": 1}),
            ("sh:minLength", {"sh:minLength": 1}),
            ("sh:maxLength", {"sh:maxLength": 1}),
        ],
    )
    def test_supported_kinds(self, shacl_type, params):
        shape = _make_shape(shacl_type=shacl_type, parameters=params)
        assert SHACLService.supports_in_memory(shape) is True

    @pytest.mark.parametrize("shacl_type", ["sh:closed", "sh:sparql", "sh:datatype"])
    def test_unsupported_kinds(self, shacl_type):
        shape = _make_shape(shacl_type=shacl_type)
        shape["parameters"] = {}
        assert SHACLService.supports_in_memory(shape) is False

    @pytest.mark.parametrize("shacl_type", ["sh:closed", "sh:sparql", "sh:datatype"])
    def test_an_unsupported_kind_yields_no_violations(self, shacl_type):
        """Which is why the runner must ask supports_in_memory first."""
        shape = _make_shape(shacl_type=shacl_type)
        shape["parameters"] = {}
        assert SHACLService.evaluate_shape_in_memory(shape, _fee_triples("1")) == []

    def test_every_supported_kind_is_reachable(self):
        """The advertised set must not drift from what the dispatcher handles."""
        from back.core.w3c.shacl.SHACLService import IN_MEMORY_TYPES

        for shacl_type in IN_MEMORY_TYPES:
            shape = _make_shape(shacl_type=shacl_type)
            shape["parameters"] = {}
            assert SHACLService.supports_in_memory(shape) is True, shacl_type


class TestConditionPersistence:
    def test_a_shape_is_unconditional_by_default(self):
        shape = _make_shape()
        assert shape["conditions"] == []
        assert shape["condition_logic"] == "and"

    def test_conditions_are_stored_on_the_shape(self):
        condition = {
            "property": "status",
            "property_uri": STATUS_URI,
            "op": "eq",
            "value": "active",
        }
        shape = _conditional_pattern_shape([condition], logic="or")
        assert shape["conditions"] == [condition]
        assert shape["condition_logic"] == "or"

    def test_an_unknown_logic_falls_back_to_and(self):
        assert _conditional_pattern_shape([], logic="xor")["condition_logic"] == "and"

    def test_conditions_can_be_cleared_by_an_update(self):
        shapes = [
            _conditional_pattern_shape(
                [{"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "a"}]
            )
        ]
        updated = SHACLService.update_shape(shapes, shapes[0]["id"], {"conditions": []})
        assert updated[0]["conditions"] == []
        assert SHACLService.shape_to_sql(updated[0], TABLE) == SHACLService.shape_to_sql(
            _make_shape(
                category="conformance",
                shacl_type="sh:pattern",
                parameters={"sh:pattern": "^[A-Z]"},
            ),
            TABLE,
        )


class TestConditionValidation:
    def test_conditions_are_accepted_on_conformance(self):
        shape = _conditional_pattern_shape(
            [{"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}]
        )
        assert Ontology.validate_shape(shape) is None

    def test_conditions_are_rejected_on_completeness(self):
        shape = _make_shape(
            conditions=[
                {"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}
            ]
        )
        assert "conformance and consistency" in Ontology.validate_shape(shape)

    def test_condition_without_a_property_is_rejected(self):
        shape = _conditional_pattern_shape([{"op": "eq", "value": "active"}])
        assert Ontology.validate_shape(shape) == "Each condition needs a property"

    def test_comparison_without_a_value_is_rejected(self):
        shape = _conditional_pattern_shape(
            [{"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": ""}]
        )
        assert "needs a value" in Ontology.validate_shape(shape)

    def test_existence_operator_needs_no_value(self):
        shape = _conditional_pattern_shape(
            [{"property": "hasOrder", "property_uri": ORDER_URI, "op": "exists", "value": ""}]
        )
        assert Ontology.validate_shape(shape) is None

    def test_unknown_operator_is_rejected(self):
        shape = _conditional_pattern_shape(
            [{"property": "status", "property_uri": STATUS_URI, "op": "matches", "value": "x"}]
        )
        assert "Unknown condition operator" in Ontology.validate_shape(shape)

    def test_conditions_need_a_target_entity(self):
        shape = _conditional_pattern_shape(
            [{"property": "status", "property_uri": STATUS_URI, "op": "eq", "value": "active"}]
        )
        shape["target_class_uri"] = ""
        assert Ontology.validate_shape(shape) == "A rule with conditions must target an entity"

    def test_unconditional_shape_still_validates(self):
        assert Ontology.validate_shape(_make_shape()) is None


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
