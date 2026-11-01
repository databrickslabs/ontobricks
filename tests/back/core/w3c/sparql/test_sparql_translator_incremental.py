"""Translator tests for the Enzyme-safe ``incremental`` mode and ``mappings_filter``.

The SNAPSHOT_* constants below were captured from the translator BEFORE the
``TripleSQLAssembler`` extraction and the ``incremental`` work, so they pin the
default output byte for byte independently of the code under test.
"""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.core.w3c.sparql import TripleSQLAssembler as ReExportedAssembler
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator
from back.core.w3c.sparql.TripleSQLAssembler import TripleSQLAssembler

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


class TestTripleSQLAssemblerExtraction:
    def test_is_reexported_from_the_sparql_package(self):
        assert ReExportedAssembler is TripleSQLAssembler

    def test_unpivot_select_builds_the_stack_form(self):
        sql = TripleSQLAssembler.unpivot_select(
            "s_expr", [("p1", "'a'"), ("p2", "col2")], "src_0", "\n WHERE x = 1"
        )
        assert sql == (
            "SELECT s_expr AS subject, stack(2, 'p1', 'a', 'p2', col2) "
            "AS (predicate, object)\n                FROM src_0\n WHERE x = 1"
        )

    def test_unpivot_select_wrapper_delegates(self):
        args = ("s_expr", [("p1", "'a'")], "src_0")
        assert SparqlTranslator._unpivot_select(*args) == (
            TripleSQLAssembler.unpivot_select(*args)
        )

    def test_build_with_clause_is_empty_without_ctes(self):
        assert TripleSQLAssembler.build_with_clause([]) == ""

    def test_build_with_clause_wraps_select_and_bare_tables(self):
        sql = TripleSQLAssembler.build_with_clause(
            [("src_0", "SELECT 1"), ("src_1", "cat.sch.tbl")]
        )
        assert sql == (
            "WITH\n  src_0 AS (\n    SELECT 1\n  ),\n"
            "  src_1 AS (\n    SELECT * FROM cat.sch.tbl\n  )\n"
        )

    def test_build_with_clause_wrapper_delegates(self):
        ctes = [("src_0", "SELECT 1")]
        assert SparqlTranslator._build_with_clause(ctes) == (
            TripleSQLAssembler.build_with_clause(ctes)
        )

    def test_finalize_raises_when_there_are_no_branches(self):
        with pytest.raises(ValidationError):
            TripleSQLAssembler.finalize_generic_triples([], True, None, [])

    def test_finalize_wrapper_delegates(self):
        args = (["SELECT 1 AS subject"], True, 5, [])
        assert SparqlTranslator._generic_triples_finalize_sql_payload(*args) == (
            TripleSQLAssembler.finalize_generic_triples(*args)
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
        assert TripleSQLAssembler.parse_select_clause(query) == expected

    def test_parse_select_clause_rejects_a_missing_projection(self):
        with pytest.raises(ValidationError, match="Could not parse SELECT clause"):
            TripleSQLAssembler.parse_select_clause("SELECT WHERE {}")


@pytest.fixture
def two_entity_mappings(entity_mappings) -> dict:
    return {
        **entity_mappings,
        "http://ex/Company": {
            "table": "companies",
            "sql_query": "SELECT * FROM companies",
            "id_column": "company_id",
            "label_column": "title",
            "uri_template": "http://ex/company/{company_id}",
            "predicates": {},
        },
    }


@pytest.fixture
def two_relationship_mappings(relationship_mappings) -> list[dict]:
    return [
        *relationship_mappings,
        {
            "predicate": "http://ex/employedBy",
            "sql_query": "SELECT person_id, company_id FROM employment",
            "subject_template": "http://ex/person/{person_id}",
            "object_template": "http://ex/company/{company_id}",
            "subject_column": "person_id",
            "object_column": "company_id",
        },
    ]


SNAPSHOT_INCREMENTAL_DISTINCT = _lines(
    "WITH",
    "  src_0 AS (",
    "    SELECT * FROM people",
    "  ),",
    "  src_1 AS (",
    "    SELECT person_id, peer_id FROM pww",
    "  )",
    "SELECT subject, predicate, object FROM (",
    "            ",
    "                    (SELECT CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) "
    "AS subject, '" + RDF_TYPE + "' AS predicate, 'http://ex/Person' AS object",
    "                FROM src_0",
    "                UNION ALL",
    "                SELECT CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) "
    "AS subject, '" + RDFS_LABEL + "' AS predicate, "
    "COALESCE(TRY_CAST(name AS STRING), '') AS object",
    "                FROM src_0",
    "                UNION ALL",
    "                SELECT CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) "
    "AS subject, 'http://ex/age' AS predicate, "
    "COALESCE(TRY_CAST(age AS STRING), '') AS object",
    "                FROM src_0)",
    "                 UNION ALL ",
    "                SELECT ",
    "                    CONCAT('http://ex/person/', TRY_CAST(person_id AS STRING)) AS subject,",
    "                    'http://ex/worksWith' AS predicate,",
    "                    CONCAT('http://ex/person/', TRY_CAST(peer_id AS STRING)) AS object",
    "                FROM src_1 AS rel_subquery",
    "            ",
    "        ) AS triples",
    "        WHERE object IS NOT NULL AND TRIM(object) != ''",
    "        GROUP BY subject, predicate, object",
)


class TestOmittedKwargsEqualExplicitDefaults:
    def test_explicit_defaults_equal_omitted_kwargs(
        self, entity_mappings, relationship_mappings
    ):
        omitted = _translate(entity_mappings, relationship_mappings)
        explicit = _translate(
            entity_mappings,
            relationship_mappings,
            incremental=False,
            mappings_filter=None,
        )
        assert explicit == omitted == SNAPSHOT_DISTINCT


class TestIncrementalAllTriples:
    def test_matches_the_enzyme_safe_golden_sql(
        self, entity_mappings, relationship_mappings
    ):
        sql = _translate(entity_mappings, relationship_mappings, incremental=True)
        assert sql == SNAPSHOT_INCREMENTAL_DISTINCT

    def test_has_no_stack_and_no_distinct(self, entity_mappings, relationship_mappings):
        sql = _translate(entity_mappings, relationship_mappings, incremental=True)
        assert "stack(" not in sql
        assert "DISTINCT" not in sql.upper()

    def test_groups_the_outer_query_once_after_the_null_filter(
        self, entity_mappings, relationship_mappings
    ):
        sql = _translate(entity_mappings, relationship_mappings, incremental=True)
        where = "WHERE object IS NOT NULL AND TRIM(object) != ''"
        group = "GROUP BY subject, predicate, object"
        assert sql.count(group) == 1
        assert sql.index(where) < sql.index(group)
        assert sql.rstrip().endswith(group)

    def test_emits_one_select_per_predicate_and_relationship(
        self, entity_mappings, relationship_mappings
    ):
        sql = _translate(entity_mappings, relationship_mappings, incremental=True)
        # type + label + age for Person, plus the worksWith relationship.
        assert sql.count(" AS predicate") == 4

    def test_keeps_the_source_ctes(self, entity_mappings, relationship_mappings):
        sql = _translate(entity_mappings, relationship_mappings, incremental=True)
        assert sql.startswith("WITH\n  src_0 AS (")

    def test_limit_follows_the_group_by(self, entity_mappings, relationship_mappings):
        sql = _translate(
            entity_mappings, relationship_mappings, limit=7, incremental=True
        )
        assert sql.rstrip().endswith("GROUP BY subject, predicate, object\n    LIMIT 7")

    def test_non_distinct_query_is_not_grouped(
        self, entity_mappings, relationship_mappings
    ):
        sql = _translate(
            entity_mappings,
            relationship_mappings,
            sparql=ALL_TRIPLES_NON_DISTINCT,
            incremental=True,
        )
        assert "GROUP BY" not in sql
        assert "DISTINCT" not in sql.upper()
        assert "stack(" not in sql

    def test_non_distinct_query_only_differs_by_the_unpivot(
        self, entity_mappings, relationship_mappings
    ):
        sql = _translate(
            entity_mappings,
            relationship_mappings,
            sparql=ALL_TRIPLES_NON_DISTINCT,
            incremental=True,
        )
        expected = SNAPSHOT_INCREMENTAL_DISTINCT.replace(
            "\n        GROUP BY subject, predicate, object", ""
        )
        assert sql == expected


class TestIncrementalFilteredCtePath:
    @pytest.fixture
    def filtered(self, entity_mappings, relationship_mappings):
        def build(incremental):
            return SparqlTranslator._build_filtered_with_relationships_query(
                entity_mappings,
                None,
                False,
                None,
                relationship_mappings,
                None,
                None,
                [{"predicate_uri": "http://ex/age", "operator": "gt", "value": "30"}],
                ["http://ex/worksWith"],
                incremental=incremental,
            )["sql"]

        return build

    def test_incremental_replaces_stack_with_union_all(self, filtered):
        sql = filtered(True)
        assert "stack(" not in sql
        assert sql.count(" AS predicate") == 4

    def test_incremental_repeats_the_seed_filter_on_every_branch(self, filtered):
        sql = filtered(True)
        seed = "IN (SELECT entity_uri FROM seed_entities)"
        # three entity branches + the relationship branch's two conditions
        assert sql.count(seed) == 5

    def test_default_still_uses_stack(self, filtered):
        assert filtered(False) == SNAPSHOT_FILTERED


class TestUnpivotSelectIncremental:
    def test_one_select_per_predicate_wrapped_in_parentheses(self):
        sql = TripleSQLAssembler.unpivot_select(
            "s_expr",
            [("p1", "'a'"), ("p2", "col2")],
            "src_0",
            incremental=True,
        )
        assert sql == (
            "(SELECT s_expr AS subject, 'p1' AS predicate, 'a' AS object\n"
            "                FROM src_0\n"
            "                UNION ALL\n"
            "                SELECT s_expr AS subject, 'p2' AS predicate, col2 AS object\n"
            "                FROM src_0)"
        )

    def test_single_predicate_is_still_parenthesised(self):
        sql = TripleSQLAssembler.unpivot_select(
            "s", [("p1", "x")], "t", incremental=True
        )
        assert sql.startswith("(SELECT ") and sql.endswith(")")
        assert "UNION ALL" not in sql

    def test_where_clause_is_repeated_on_each_branch(self):
        sql = TripleSQLAssembler.unpivot_select(
            "s",
            [("p1", "a"), ("p2", "b")],
            "t",
            "\n WHERE s IN (SELECT 1)",
            incremental=True,
        )
        assert sql.count("WHERE s IN (SELECT 1)") == 2

    def test_default_flag_keeps_the_stack_form(self):
        assert "stack(1," in TripleSQLAssembler.unpivot_select("s", [("p", "x")], "t")


class TestFinalizeIncremental:
    BRANCHES = ["SELECT 1 AS subject"]

    def test_distinct_becomes_group_by(self):
        sql = TripleSQLAssembler.finalize_generic_triples(
            self.BRANCHES, True, None, [], incremental=True
        )["sql"]
        assert "DISTINCT" not in sql
        assert sql.endswith(
            "WHERE object IS NOT NULL AND TRIM(object) != ''\n"
            "        GROUP BY subject, predicate, object"
        )

    def test_non_distinct_is_left_alone(self):
        sql = TripleSQLAssembler.finalize_generic_triples(
            self.BRANCHES, False, None, [], incremental=True
        )["sql"]
        assert "GROUP BY" not in sql and "DISTINCT" not in sql

    def test_limit_is_kept_after_group_by(self):
        sql = TripleSQLAssembler.finalize_generic_triples(
            self.BRANCHES, True, 3, [], incremental=True
        )["sql"]
        assert sql.endswith("GROUP BY subject, predicate, object\n    LIMIT 3")

    def test_default_flag_keeps_distinct(self):
        sql = TripleSQLAssembler.finalize_generic_triples(
            self.BRANCHES, True, None, []
        )["sql"]
        assert "SELECT DISTINCT subject" in sql and "GROUP BY" not in sql


class TestMappingsFilter:
    def test_entity_filter_drops_the_excluded_branch_and_its_cte(
        self, two_entity_mappings
    ):
        keep_person = lambda m: m.get("table") == "people"  # noqa: E731
        sql = _translate(two_entity_mappings, [], mappings_filter=keep_person)
        assert "people" in sql
        assert "companies" not in sql

    def test_relationship_filter_drops_the_excluded_branch(
        self, entity_mappings, two_relationship_mappings
    ):
        sql = _translate(
            entity_mappings,
            two_relationship_mappings,
            mappings_filter=lambda m: m.get("predicate") != "http://ex/employedBy",
        )
        assert "http://ex/worksWith" in sql
        assert "employedBy" not in sql and "employment" not in sql

    def test_the_filter_sees_every_entity_and_relationship_dict(
        self, two_entity_mappings, two_relationship_mappings
    ):
        seen = []

        def record(mapping):
            seen.append(mapping)
            return True

        _translate(two_entity_mappings, two_relationship_mappings, mappings_filter=record)
        expected = [*two_entity_mappings.values(), *two_relationship_mappings]
        assert len(seen) == len(expected)
        assert all(any(s is e for e in expected) for s in seen)

    def test_accept_all_filter_equals_the_unfiltered_output(
        self, two_entity_mappings, two_relationship_mappings
    ):
        unfiltered = _translate(two_entity_mappings, two_relationship_mappings)
        filtered = _translate(
            two_entity_mappings, two_relationship_mappings, mappings_filter=lambda m: True
        )
        assert filtered == unfiltered

    def test_reject_all_filter_raises_validation_error(
        self, entity_mappings, relationship_mappings
    ):
        with pytest.raises(ValidationError):
            _translate(
                entity_mappings, relationship_mappings, mappings_filter=lambda m: False
            )

    def test_filter_composes_with_incremental(self, two_entity_mappings):
        sql = _translate(
            two_entity_mappings,
            [],
            incremental=True,
            mappings_filter=lambda m: m.get("table") == "companies",
        )
        assert "companies" in sql and "people" not in sql
        assert "stack(" not in sql

    def test_filter_does_not_mutate_the_callers_mappings(
        self, two_entity_mappings, two_relationship_mappings
    ):
        entities_before = dict(two_entity_mappings)
        rels_before = list(two_relationship_mappings)
        _translate(
            two_entity_mappings,
            two_relationship_mappings,
            mappings_filter=lambda m: m.get("table") != "companies",
        )
        assert two_entity_mappings == entities_before
        assert two_relationship_mappings == rels_before


class TestPackageWrapperForwards:
    def test_forwards_incremental_and_mappings_filter_as_keywords(self, monkeypatch):
        from unittest.mock import MagicMock

        from back.core.w3c import sparql

        fake = MagicMock(return_value={"sql": "x"})
        monkeypatch.setattr(SparqlTranslator, "translate_sparql_to_spark", fake)
        chooser = lambda m: True  # noqa: E731

        sparql.translate_sparql_to_spark(
            "Q", {"c": {}}, 5, [], incremental=True, mappings_filter=chooser
        )

        args, kwargs = fake.call_args
        assert args == ("Q", {"c": {}}, 5, [])
        assert kwargs["incremental"] is True
        assert kwargs["mappings_filter"] is chooser

    def test_defaults_are_off_and_unfiltered(self, monkeypatch):
        from unittest.mock import MagicMock

        from back.core.w3c import sparql

        fake = MagicMock(return_value={"sql": "x"})
        monkeypatch.setattr(SparqlTranslator, "translate_sparql_to_spark", fake)

        sparql.translate_sparql_to_spark("Q", {}, None)

        assert fake.call_args.kwargs["incremental"] is False
        assert fake.call_args.kwargs["mappings_filter"] is None

    def test_wrapper_output_matches_the_translator(
        self, entity_mappings, relationship_mappings
    ):
        from back.core.w3c import sparql

        result = sparql.translate_sparql_to_spark(
            ALL_TRIPLES, entity_mappings, None, relationship_mappings, incremental=True
        )
        assert result["sql"] == SNAPSHOT_INCREMENTAL_DISTINCT

    def test_keyword_only_signature(self):
        import inspect

        from back.core.w3c import sparql

        for fn in (
            sparql.translate_sparql_to_spark,
            SparqlTranslator.translate_sparql_to_spark,
        ):
            params = inspect.signature(fn).parameters
            assert params["incremental"].kind is inspect.Parameter.KEYWORD_ONLY
            assert params["mappings_filter"].kind is inspect.Parameter.KEYWORD_ONLY
            assert params["incremental"].default is False
            assert params["mappings_filter"].default is None


class TestIncrementalScope:
    def test_type_constrained_triple_pattern_is_reshaped(self, entity_mappings):
        sparql = (
            "SELECT DISTINCT ?s ?p ?o WHERE { ?s a <http://ex/Person> . ?s ?p ?o }"
        )
        sql = _translate(entity_mappings, [], sparql=sparql, incremental=True)
        assert "stack(" not in sql
        assert "DISTINCT" not in sql.upper()
        assert sql.rstrip().endswith("GROUP BY subject, predicate, object")

    def test_join_based_query_is_unaffected(self, entity_mappings):
        sparql = "SELECT ?s ?age WHERE { ?s a <http://ex/Person> . ?s <http://ex/age> ?age }"
        assert _translate(
            entity_mappings, [], sparql=sparql, incremental=True
        ) == _translate(entity_mappings, [], sparql=sparql)
