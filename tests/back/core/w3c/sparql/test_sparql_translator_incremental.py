"""Translator tests for the Enzyme-safe ``incremental`` mode and ``mappings_filter``.

The SNAPSHOT_* constants below were captured from the translator BEFORE the
``TripleSqlAssembler`` extraction and the ``incremental`` work, so they pin the
default output byte for byte independently of the code under test.
"""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.core.w3c.sparql import TripleSqlAssembler as ReExportedAssembler
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator
from back.core.w3c.sparql.TripleSqlAssembler import TripleSqlAssembler

pytestmark = pytest.mark.unit

ALL_TRIPLES = (
    "SELECT DISTINCT ?subject ?predicate ?object WHERE { ?subject ?predicate ?object }"
)
ALL_TRIPLES_NON_DISTINCT = (
    "SELECT ?subject ?predicate ?object WHERE { ?subject ?predicate ?object }"
)
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


def _lines(*lines: str) -> str:
    return "\n".join(lines)


@pytest.fixture
def entity_mappings() -> dict:
    return {
        "http://ex/Person": {
            "table": "people",
            "sql_query": "SELECT * FROM people",
            "id_column": "person_id",
            "label_column": "name",
            "uri_template": "http://ex/person/{person_id}",
            "predicates": {
                "http://ex/age": {"type": "column", "column": "age"},
            },
        }
    }


@pytest.fixture
def relationship_mappings() -> list[dict]:
    return [
        {
            "predicate": "http://ex/worksWith",
            "sql_query": "SELECT person_id, peer_id FROM pww",
            "subject_template": "http://ex/person/{person_id}",
            "object_template": "http://ex/person/{peer_id}",
            "subject_column": "person_id",
            "object_column": "peer_id",
        }
    ]


def _translate(entity_mappings, relationship_mappings, sparql=ALL_TRIPLES, limit=None, **kw):
    return SparqlTranslator.translate_sparql_to_spark(
        sparql, entity_mappings, limit, relationship_mappings, dialect="spark", **kw
    )["sql"]


SNAPSHOT_DISTINCT = _lines(
    'WITH',
    '  src_0 AS (',
    '    SELECT * FROM people',
    '  ),',
    '  src_1 AS (',
    '    SELECT person_id, peer_id FROM pww',
    '  )',
    'SELECT DISTINCT subject, predicate, object FROM (',
    '            ',
    "                    SELECT CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject, stack(3, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://ex/Person', 'http://www.w3.org/2000/01/rdf-schema#label', COALESCE(TRY_CAST(name AS STRING), ''), 'http://ex/age', COALESCE(TRY_CAST(age AS STRING), '')) AS (predicate, object)",
    '                FROM src_0',
    '                 UNION ALL ',
    '                SELECT ',
    "                    CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject,",
    "                    'http://ex/worksWith' AS predicate,",
    "                    CONCAT('http://ex/person/', TRY_CAST(peer_id AS STRING)) AS object",
    '                FROM src_1 AS rel_subquery',
    '            ',
    '        ) AS triples',
    "        WHERE object IS NOT NULL AND TRIM(object) != ''",
)

SNAPSHOT_LIMIT = _lines(
    'WITH',
    '  src_0 AS (',
    '    SELECT * FROM people',
    '  ),',
    '  src_1 AS (',
    '    SELECT person_id, peer_id FROM pww',
    '  )',
    'SELECT DISTINCT subject, predicate, object FROM (',
    '            ',
    "                    SELECT CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject, stack(3, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://ex/Person', 'http://www.w3.org/2000/01/rdf-schema#label', COALESCE(TRY_CAST(name AS STRING), ''), 'http://ex/age', COALESCE(TRY_CAST(age AS STRING), '')) AS (predicate, object)",
    '                FROM src_0',
    '                 UNION ALL ',
    '                SELECT ',
    "                    CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject,",
    "                    'http://ex/worksWith' AS predicate,",
    "                    CONCAT('http://ex/person/', TRY_CAST(peer_id AS STRING)) AS object",
    '                FROM src_1 AS rel_subquery',
    '            ',
    '        ) AS triples',
    "        WHERE object IS NOT NULL AND TRIM(object) != ''",
    '    LIMIT 7',
)

SNAPSHOT_NON_DISTINCT = _lines(
    'WITH',
    '  src_0 AS (',
    '    SELECT * FROM people',
    '  ),',
    '  src_1 AS (',
    '    SELECT person_id, peer_id FROM pww',
    '  )',
    'SELECT subject, predicate, object FROM (',
    '            ',
    "                    SELECT CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject, stack(3, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://ex/Person', 'http://www.w3.org/2000/01/rdf-schema#label', COALESCE(TRY_CAST(name AS STRING), ''), 'http://ex/age', COALESCE(TRY_CAST(age AS STRING), '')) AS (predicate, object)",
    '                FROM src_0',
    '                 UNION ALL ',
    '                SELECT ',
    "                    CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject,",
    "                    'http://ex/worksWith' AS predicate,",
    "                    CONCAT('http://ex/person/', TRY_CAST(peer_id AS STRING)) AS object",
    '                FROM src_1 AS rel_subquery',
    '            ',
    '        ) AS triples',
    "        WHERE object IS NOT NULL AND TRIM(object) != ''",
)

SNAPSHOT_FILTERED = _lines(
    'WITH',
    '  src_0 AS (',
    '    SELECT * FROM people',
    '  ),  src_1 AS (',
    '    SELECT person_id, peer_id FROM pww',
    '  ),  seed_entities AS (',
    "    SELECT CONCAT('http://ex/person/', TRY_CAST(e.person_id AS STRING)) AS entity_uri FROM src_0 AS e WHERE e.age > 30",
    '  )',
    '    SELECT subject, predicate, object FROM (',
    '        ',
    "                    SELECT CONCAT('http://ex/person/', TRY_CAST(e.person_id AS STRING)) AS subject, stack(3, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://ex/Person', 'http://www.w3.org/2000/01/rdf-schema#label', COALESCE(TRY_CAST(e.name AS STRING), ''), 'http://ex/age', COALESCE(TRY_CAST(e.age AS STRING), '')) AS (predicate, object)",
    '                FROM src_0 AS e',
    "                WHERE CONCAT('http://ex/person/', TRY_CAST(e.person_id AS STRING)) IN (SELECT entity_uri FROM seed_entities) UNION ALL ",
    "                SELECT CONCAT('http://ex/person/', TRY_CAST(r.person_id AS STRING)) AS subject, 'http://ex/worksWith' AS predicate, CONCAT('http://ex/person/', TRY_CAST(r.peer_id AS STRING)) AS object",
    '                FROM src_1 AS r',
    "                WHERE CONCAT('http://ex/person/', TRY_CAST(r.person_id AS STRING)) IN (SELECT entity_uri FROM seed_entities)",
    "                   OR CONCAT('http://ex/person/', TRY_CAST(r.peer_id AS STRING)) IN (SELECT entity_uri FROM seed_entities)",
    '    ) final_results',
    '    ',
)


class TestDefaultOutputUnchanged:
    def test_all_triples_distinct_matches_snapshot(
        self, entity_mappings, relationship_mappings
    ):
        assert _translate(entity_mappings, relationship_mappings) == SNAPSHOT_DISTINCT

    def test_limit_matches_snapshot(self, entity_mappings, relationship_mappings):
        sql = _translate(entity_mappings, relationship_mappings, limit=7)
        assert sql == SNAPSHOT_LIMIT

    def test_non_distinct_matches_snapshot(self, entity_mappings, relationship_mappings):
        sql = _translate(
            entity_mappings, relationship_mappings, sparql=ALL_TRIPLES_NON_DISTINCT
        )
        assert sql == SNAPSHOT_NON_DISTINCT

    def test_filtered_cte_path_matches_snapshot(
        self, entity_mappings, relationship_mappings
    ):
        value_filters = [
            {"predicate_uri": "http://ex/age", "operator": "gt", "value": "30"}
        ]
        sql = SparqlTranslator._build_filtered_with_relationships_query(
            entity_mappings,
            None,
            False,
            None,
            relationship_mappings,
            None,
            None,
            value_filters,
            ["http://ex/worksWith"],
        )["sql"]
        assert sql == SNAPSHOT_FILTERED

    def test_default_output_keeps_stack_and_distinct(
        self, entity_mappings, relationship_mappings
    ):
        sql = _translate(entity_mappings, relationship_mappings)
        assert "stack(" in sql
        assert "SELECT DISTINCT" in sql


class TestTripleSqlAssemblerExtraction:
    def test_is_reexported_from_the_sparql_package(self):
        assert ReExportedAssembler is TripleSqlAssembler

    def test_unpivot_select_builds_the_stack_form(self):
        sql = TripleSqlAssembler.unpivot_select(
            "s_expr", [("p1", "'a'"), ("p2", "col2")], "src_0", "\n WHERE x = 1"
        )
        assert sql == (
            "SELECT s_expr AS subject, stack(2, 'p1', 'a', 'p2', col2) "
            "AS (predicate, object)\n                FROM src_0\n WHERE x = 1"
        )

    def test_unpivot_select_wrapper_delegates(self):
        args = ("s_expr", [("p1", "'a'")], "src_0")
        assert SparqlTranslator._unpivot_select(*args) == (
            TripleSqlAssembler.unpivot_select(*args)
        )

    def test_build_with_clause_is_empty_without_ctes(self):
        assert TripleSqlAssembler.build_with_clause([]) == ""

    def test_build_with_clause_wraps_select_and_bare_tables(self):
        sql = TripleSqlAssembler.build_with_clause(
            [("src_0", "SELECT 1"), ("src_1", "cat.sch.tbl")]
        )
        assert sql == (
            "WITH\n  src_0 AS (\n    SELECT 1\n  ),\n"
            "  src_1 AS (\n    SELECT * FROM cat.sch.tbl\n  )\n"
        )

    def test_build_with_clause_wrapper_delegates(self):
        ctes = [("src_0", "SELECT 1")]
        assert SparqlTranslator._build_with_clause(ctes) == (
            TripleSqlAssembler.build_with_clause(ctes)
        )

    def test_finalize_raises_when_there_are_no_branches(self):
        with pytest.raises(ValidationError):
            TripleSqlAssembler.finalize_generic_triples([], True, None, [])

    def test_finalize_wrapper_delegates(self):
        args = (["SELECT 1 AS subject"], True, 5, [])
        assert SparqlTranslator._generic_triples_finalize_sql_payload(*args) == (
            TripleSqlAssembler.finalize_generic_triples(*args)
        )

    @pytest.mark.parametrize(
        "query, expected",
        [
            ("SELECT DISTINCT ?s ?p ?o WHERE {}", (True, ["s", "p", "o"])),
            ("SELECT ?s WHERE {}", (False, ["s"])),
            ("select distinct * where {}", (True, None)),
        ],
    )
    def test_parse_select_clause(self, query, expected):
        assert TripleSqlAssembler.parse_select_clause(query) == expected

    def test_parse_select_clause_rejects_a_missing_projection(self):
        with pytest.raises(ValidationError, match="Could not parse SELECT clause"):
            TripleSqlAssembler.parse_select_clause("SELECT WHERE {}")
