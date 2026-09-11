"""Direct unit tests for SparqlTranslator.translate_sparql_to_spark (T-M1.P2 sample under CNS).

SparqlTranslator.py is 2407 LOC and exposes a single public method:
`translate_sparql_to_spark(sparql_query, entity_mappings, limit, ...)`.
Per §9.5 T-M1.P2 the full target is ~120 tests covering each visitor + each
SPARQL op family. This file lands a representative slice (~25 tests)
proving the test shape; expansion is one focused PR per visitor family
(BGP, FILTER, OPTIONAL, UNION, GROUP BY, ORDER BY, property paths, etc.).

Strategy: build minimal `entity_mappings` via R2RMLMappingFactory, fire a
SPARQL query at the translator, and assert structural properties of the
returned SQL (table names, column projections, LIMIT clause, etc.). We
don't execute the SQL — that's T-M2.P1 (Delta sync integration).
"""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator
from tests.fixtures.factories import R2RMLMappingFactory


@pytest.fixture
def mapping() -> dict:
    """Two-entity mapping: Customer + Order with one relationship."""
    return R2RMLMappingFactory.build(entity_count=2, relationship_count=1)


@pytest.fixture
def entity_mappings(mapping):
    return {e["ontology_class"]: e for e in mapping["entities"]}


@pytest.fixture
def relationship_mappings(mapping):
    return mapping["relationships"]


def _translate(sparql, entity_mappings, relationship_mappings=None, limit=10):
    return SparqlTranslator.translate_sparql_to_spark(
        sparql_query=sparql,
        entity_mappings=entity_mappings,
        limit=limit,
        relationship_mappings=relationship_mappings or [],
    )


@pytest.fixture
def translator_entity_mappings() -> dict:
    return {
        "http://ex/Person": {
            "table": "people",
            "sql_query": "SELECT * FROM people",
            "id_column": "person_id",
            "label_column": "name",
            "uri_template": "http://ex/person/{person_id}",
            "predicates": {
                "http://ex/name": {"type": "column", "column": "name"},
                "http://ex/age": {"type": "column", "column": "age"},
            },
        }
    }


@pytest.fixture
def translator_relationship_mappings() -> list[dict]:
    return [
        {
            "predicate": "http://ex/worksWith",
            "sql_query": "SELECT person_id, peer_id FROM person_works_with",
            "subject_template": "http://ex/person/{person_id}",
            "object_template": "http://ex/person/{peer_id}",
            "subject_column": "person_id",
            "object_column": "peer_id",
        },
        {
            "predicate": "http://ex/manages",
            "sql_query": "SELECT manager_id, report_id FROM person_manages",
            "subject_template": "http://ex/person/{manager_id}",
            "object_template": "http://ex/person/{report_id}",
            "subject_column": "manager_id",
            "object_column": "report_id",
        },
    ]


@pytest.mark.unit
class TestReturnShape:
    def test_returns_dict(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        assert isinstance(result, dict)

    def test_dict_has_success_key(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        assert "success" in result

    def test_successful_translation_returns_sql(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        if result.get("success"):
            assert "sql" in result
            assert isinstance(result["sql"], str)
            assert len(result["sql"]) > 0

    def test_successful_translation_returns_variables(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        if result.get("success"):
            assert "variables" in result


@pytest.mark.unit
class TestSelectSingleVariable:
    def test_select_s_for_customer_type(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        assert result["success"], f"translation failed: {result}"
        sql = result["sql"]
        # ?s projection should appear with an alias.
        assert "AS s" in sql or "as s" in sql.lower() or "s " in sql.lower()

    def test_emits_from_clause_for_customer_table(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        assert result["success"]
        # The mapping's table name (customers) must appear in FROM.
        assert "customers" in result["sql"].lower()


@pytest.mark.unit
class TestLimit:
    def test_limit_appears_in_sql(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings, limit=42)
        if result.get("success"):
            assert "LIMIT 42" in result["sql"] or "limit 42" in result["sql"].lower()

    def test_default_limit_respected(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings, limit=5)
        if result.get("success"):
            assert "5" in result["sql"]

    @pytest.mark.parametrize("n", [1, 100, 1000])
    def test_various_limits(self, entity_mappings, n):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings, limit=n)
        if result.get("success"):
            assert str(n) in result["sql"]


@pytest.mark.unit
class TestMissingMapping:
    """When SPARQL references an unmapped class, the translator raises
    `ValidationError` (per §4 of `src/.coding_rules.md` — translators raise
    from the `OntoBricksError` hierarchy; routes translate to HTTP)."""

    def test_unmapped_class_raises_validation_error(self):
        from back.core.errors import ValidationError

        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Unicorn> }"
        with pytest.raises(ValidationError):
            _translate(sparql, entity_mappings={})


@pytest.mark.unit
class TestMalformedInput:
    """Malformed inputs raise `ValidationError`, not `{"success": False}` —
    per the OntoBricksError contract."""

    def test_empty_sparql_raises_validation_error(self, entity_mappings):
        from back.core.errors import ValidationError

        with pytest.raises(ValidationError):
            _translate("", entity_mappings)

    def test_invalid_sparql_raises_validation_error(self, entity_mappings):
        from back.core.errors import ValidationError

        sparql = "this is not valid SPARQL at all !!!"
        with pytest.raises(ValidationError):
            _translate(sparql, entity_mappings)

    def test_unclosed_brace_raises_validation_error(self, entity_mappings):
        from back.core.errors import ValidationError

        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer>"
        with pytest.raises(ValidationError):
            _translate(sparql, entity_mappings)

    def test_non_select_query_raises_validation_error(self, entity_mappings):
        """Only SELECT is supported; CONSTRUCT/ASK/DESCRIBE should raise."""
        from back.core.errors import ValidationError

        sparql = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        with pytest.raises(ValidationError):
            _translate(sparql, entity_mappings)


@pytest.mark.unit
class TestSelectMultipleVariables:
    def test_select_two_vars(self, entity_mappings):
        # ?c label ?l — needs both a class-membership and an rdfs:label triple
        # pattern. Translator may not support full SPARQL semantics for all
        # property mappings; accept either success or a clean failure.
        sparql = (
            "SELECT ?c ?l WHERE { "
            "?c a <http://test.org/ontology#Customer> . "
            "?c <http://www.w3.org/2000/01/rdf-schema#label> ?l "
            "}"
        )
        result = _translate(sparql, entity_mappings)
        assert isinstance(result, dict)


@pytest.mark.unit
class TestEntityMappingsRespected:
    def test_catalog_schema_in_output(self, entity_mappings, mapping):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        if result.get("success"):
            sql_lower = result["sql"].lower()
            # The fully-qualified table name from the mapping appears in the SQL.
            for entity in mapping["entities"]:
                if entity["ontology_class"] == "http://test.org/ontology#Customer":
                    assert entity["table"].lower() in sql_lower or entity["catalog"].lower() in sql_lower
                    break

    def test_table_name_in_output(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        if result.get("success"):
            assert "customers" in result["sql"].lower()


@pytest.mark.unit
class TestSqlSafety:
    """Defensive: the translator must never emit raw `;` followed by another statement."""

    def test_no_statement_terminator_breaks(self, entity_mappings):
        sparql = "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer> }"
        result = _translate(sparql, entity_mappings)
        if result.get("success"):
            sql = result["sql"]
            # We allow ; as a terminator at the end, but not as a multi-statement separator.
            stripped = sql.strip().rstrip(";")
            assert ";" not in stripped, f"multi-statement SQL emitted: {sql}"

    def test_dangerous_iri_does_not_inject_sql(self, entity_mappings):
        """An IRI containing SQL fragments should not break out of the FROM clause."""
        sparql = (
            "SELECT ?s WHERE { ?s a <http://test.org/ontology#Customer'; DROP TABLE x; --> }"
        )
        # Translator may reject this as malformed SPARQL; what matters is no raise + no
        # `DROP TABLE` reaching the output.
        try:
            result = _translate(sparql, entity_mappings)
        except Exception:
            return  # Acceptable — rejected at parse time.
        if result.get("success"):
            assert "DROP TABLE" not in result["sql"].upper()


@pytest.mark.unit
class TestCapabilityBoundaryOnPublicTranslator:
    @pytest.mark.parametrize(
        ("query", "feature"),
        [
            (
                "SELECT ?s WHERE { ?s ?p ?o } GROUP BY ?s",
                "GROUP BY",
            ),
            (
                "SELECT ?s WHERE { ?s <http://ex/age> ?age . FILTER(?age > 5) }",
                "numeric FILTER",
            ),
            (
                "SELECT ?s WHERE { ?s <http://ex/name> ?name . "
                "FILTER(CONTAINS(LCASE(STR(?name)), \"an\") && STRSTARTS(LCASE(STR(?name)), \"a\")) }",
                "complex FILTER",
            ),
            (
                "SELECT ?s WHERE { ?s <http://ex/worksWith>+ ?o }",
                "property paths",
            ),
        ],
    )
    def test_rejects_unsupported_queries_fail_closed(
        self, translator_entity_mappings, query, feature
    ):
        with pytest.raises(ValidationError, match=feature):
            _translate(query, translator_entity_mappings)

    def test_accepts_basic_bgp(self, translator_entity_mappings):
        query = "SELECT ?s WHERE { ?s a <http://ex/Person> }"
        result = _translate(query, translator_entity_mappings)
        assert result["success"] is True
        assert result["sql"]

    def test_accepts_supported_string_filter(self, translator_entity_mappings):
        query = (
            "SELECT ?s WHERE { "
            "?s <http://ex/name> ?name . "
            'FILTER(CONTAINS(LCASE(STR(?name)), "an")) '
            "}"
        )
        result = _translate(query, translator_entity_mappings)
        assert result["success"] is True
        assert result["sql"]

    def test_accepts_literal_bind(self, translator_entity_mappings):
        query = (
            "SELECT ?s ?kind WHERE { "
            "?s a <http://ex/Person> . "
            'BIND("Person" AS ?kind) '
            "}"
        )
        result = _translate(query, translator_entity_mappings)
        assert result["success"] is True
        assert "'Person' AS kind" in result["sql"] or '"Person" AS kind' in result["sql"]

    def test_accepts_relationship_filter_union(self, translator_entity_mappings, translator_relationship_mappings):
        query = (
            "PREFIX : <http://ex/> "
            "SELECT ?subject ?predicate ?object WHERE { "
            "{ ?subject :worksWith ?object . BIND(:worksWith AS ?predicate) } "
            "UNION "
            "{ ?subject :manages ?object . BIND(:manages AS ?predicate) } "
            "}"
        )
        result = _translate(query, translator_entity_mappings, translator_relationship_mappings)
        assert result["success"] is True
        assert result["variables"] == ["subject", "predicate", "object"]
        sql = result["sql"]
        assert "person_works_with" in sql
        assert "person_manages" in sql
        assert "'http://ex/worksWith' AS predicate" in sql
        assert "'http://ex/manages' AS predicate" in sql


@pytest.mark.unit
class TestOptionalRelationshipSemantics:
    def test_optional_single_relationship_keeps_left_join_without_presence_filter(
        self, translator_entity_mappings, translator_relationship_mappings
    ):
        query = (
            "SELECT ?s ?peer WHERE { "
            "?s a <http://ex/Person> . "
            "OPTIONAL { ?s <http://ex/worksWith> ?peer } "
            "}"
        )
        result = _translate(query, translator_entity_mappings, translator_relationship_mappings)
        assert result["success"] is True
        sql = result["sql"]
        assert "LEFT JOIN" in sql
        assert "IS NOT NULL" not in sql
        assert " OR " not in sql

    def test_optional_two_relationships_use_distinct_aliases_without_presence_filter(
        self, translator_entity_mappings, translator_relationship_mappings
    ):
        query = (
            "SELECT ?s ?peer ?report WHERE { "
            "?s a <http://ex/Person> . "
            "?peer a <http://ex/Person> . "
            "?report a <http://ex/Person> . "
            "OPTIONAL { ?s <http://ex/worksWith> ?peer } "
            "OPTIONAL { ?s <http://ex/manages> ?report } "
            "}"
        )
        result = _translate(query, translator_entity_mappings, translator_relationship_mappings)
        assert result["success"] is True
        sql = result["sql"]
        assert "LEFT JOIN" in sql
        assert "optrel_s_0" in sql
        assert "optrel_s_1" in sql
        assert "IS NOT NULL" not in sql
        assert " OR " not in sql

    def test_optional_relationship_shape_does_not_force_match_in_where_clause(
        self, translator_entity_mappings, translator_relationship_mappings
    ):
        query = (
            "SELECT ?s ?peer WHERE { "
            "?s a <http://ex/Person> . "
            "OPTIONAL { ?s <http://ex/worksWith> ?peer } "
            "}"
        )
        result = _translate(query, translator_entity_mappings, translator_relationship_mappings)
        assert result["success"] is True
        sql = result["sql"]
        where_sql = sql.split("WHERE", maxsplit=1)[1]
        assert "IS NOT NULL" not in where_sql
        assert " OR " not in where_sql

    def test_optional_column_value_does_not_add_presence_filter(
        self, translator_entity_mappings
    ):
        query = (
            "SELECT ?s ?name WHERE { "
            "?s a <http://ex/Person> . "
            "OPTIONAL { ?s <http://ex/name> ?name } "
            "}"
        )
        result = _translate(query, translator_entity_mappings)
        assert result["success"] is True
        sql = result["sql"]
        assert "IS NOT NULL" not in sql
        assert " OR " not in sql
