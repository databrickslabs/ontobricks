"""Regression suite: SWRL -> SHACL-AF behavioural parity.

The legacy SWRL engine was removed and SHACL-AF now executes the IF/THEN
business-rules UI.  The transition must be *behaviour preserving*: the same
rule bodies must yield the same inferred triples, the same provenance /
``stats`` contract that the UI, MCP tools, and Data-Quality pipeline depend
on, and the same cross-dialect (Spark / Postgres) SQL guarantees.

These tests pin that contract across a capability battery (unary/binary
atoms, property chains, comparison + string built-ins, closed-world
negation) so any drift away from the legacy SWRL semantics is caught.  They
complement :mod:`test_shacl_rule_engine` (which asserts *that* SQL is emitted)
by nailing down *what* that SQL means and *what* the engine returns.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Importing the package root first avoids a pre-existing circular import
# between ``back.core.reasoning`` and ``back.core.w3c`` when this module is
# collected on its own.
import back.objects  # noqa: F401
from back.core.helpers import SQL_DIALECT_POSTGRES
from back.core.reasoning.shacl_rules import (
    ShaclRuleEngine,
    SPARQL_RULE,
    SwrlToShaclTranspiler,
    TRIPLE_RULE,
)
from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import ShaclFlatSqlBackend
from back.core.reasoning.shacl_rules.rule_serializer import (
    preview_rule_shacl,
    serialize_rules_to_turtle,
)

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

ONTOLOGY = {
    "base_uri": "http://ex.org",
    "classes": [
        {"name": "Customer"},
        {"name": "Claim"},
        {"name": "Contract"},
        {"name": "HighValue"},
    ],
    "properties": [
        {"name": "hasClaim"},
        {"name": "hasContract"},
        {"name": "hasScore"},
        {"name": "status"},
        {"name": "owns"},
    ],
}

# The capability battery — identical rule bodies the legacy SWRL engine ran.
SIMPLE_CLASS = {
    "name": "simple_class",
    "antecedent": "Customer(?x)",
    "consequent": "HighValue(?x)",
}
PROPERTY_CHAIN = {
    "name": "property_chain",
    "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
    "consequent": "hasContract(?x, ?z)",
}
COMPARISON_BUILTIN = {
    "name": "comparison_builtin",
    "antecedent": "Customer(?x) \u2227 hasScore(?x, ?s) \u2227 swrlb:greaterThan(?s, 500)",
    "consequent": "HighValue(?x)",
}
STRING_BUILTIN = {
    "name": "string_builtin",
    "antecedent": (
        'Customer(?x) \u2227 Contract(?z) \u2227 hasContract(?x, ?z) '
        '\u2227 status(?z, ?st) \u2227 swrlb:contains(?st, "active")'
    ),
    "consequent": "HighValue(?x)",
}
NEGATION = {
    "name": "negation",
    "antecedent": "Customer(?x) \u2227 not(hasContract(?x, ?z))",
    "consequent": "HighValue(?x)",
}
BATTERY = [
    SIMPLE_CLASS,
    PROPERTY_CHAIN,
    COMPARISON_BUILTIN,
    STRING_BUILTIN,
    NEGATION,
]


class _CannedStore:
    """A triple store whose SELECTs return one matching row.

    Mirrors the row shape the flat-triple inference SELECT yields
    (``subject``/``predicate``/``object``), so the engine's row -> triple
    wiring (and its provenance stamping) can be exercised end to end without
    a real warehouse.  INSERTs (materialisation) return nothing.
    """

    def __init__(self) -> None:
        self.queries: List[str] = []
        self.query_dialect = ""

    def sql_table_reference(self, table: str) -> str:
        return table

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        self.queries.append(query)
        if query.lstrip().upper().startswith("SELECT"):
            return [
                {
                    "subject": "http://ex.org/e1",
                    "predicate": "http://ex.org/hasContract",
                    "object": "",
                }
            ]
        return []


def _run(rules, **kw) -> Any:
    store = _CannedStore()
    result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
        rules, store, "triples", **kw
    )
    return result, store


# -- End-to-end behavioural contract --------------------------------------


class TestEngineContractParity:
    """The result contract legacy SWRL consumers (UI / MCP / DQ) rely on."""

    def test_phase_label_stays_swrl(self):
        # DQ / MCP branch on stats["phase"] == "swrl"; the SHACL engine keeps it.
        result, _ = _run([PROPERTY_CHAIN])
        assert result.stats["phase"] == "swrl"
        assert result.stats["engine"] == "shacl-af"

    def test_inferred_triples_carry_swrl_provenance(self):
        result, _ = _run([PROPERTY_CHAIN])
        assert result.inferred_triples
        for triple in result.inferred_triples:
            assert triple.provenance == "swrl:property_chain"
            assert triple.rule_name == "property_chain"

    def test_rules_count_reflects_enabled_only(self):
        result, _ = _run(
            [PROPERTY_CHAIN, {**COMPARISON_BUILTIN, "enabled": False}]
        )
        assert result.stats["rules_count"] == 1
        assert result.stats["rules_total"] == 2

    def test_disabled_rule_emits_no_sql(self):
        result, store = _run([{**PROPERTY_CHAIN, "enabled": False}])
        assert result.inferred_triples == []
        assert store.queries == []

    def test_whole_battery_runs_without_error(self):
        result, _ = _run(BATTERY)
        assert result.stats["errors"] == 0
        assert result.stats["rules_count"] == len(BATTERY)

    def test_materialisation_marks_batch_inference(self):
        result, store = _run([PROPERTY_CHAIN], materialize=True)
        assert any(q.strip().upper().startswith("INSERT") for q in store.queries)
        assert any(
            t.predicate == "swrl:materialized" for t in result.inferred_triples
        )

    def test_focus_restricts_inference_to_entity(self):
        _, store = _run(
            [PROPERTY_CHAIN],
            focus={"vars": ["?x"], "uri": "http://ex.org/e1"},
        )
        assert any("http://ex.org/e1" in q for q in store.queries)

    def test_inference_limit_is_applied(self):
        _, store = _run([PROPERTY_CHAIN], inference_limit=7)
        assert any("LIMIT 7" in q for q in store.queries)


# -- Generated inference SQL semantics (the "what") -----------------------


class TestInferenceSqlSemantics:
    """Pin the SQL each capability compiles to, matching legacy SWRL output."""

    def setup_method(self):
        self.b = ShaclFlatSqlBackend()

    def _inference(self, rule) -> Optional[str]:
        return self.b.build_inference_sql(
            "triples",
            {
                "antecedent": rule["antecedent"],
                "consequent": rule["consequent"],
                "base_uri": "http://ex.org",
                "uri_map": {},
            },
        )

    def _violation(self, rule) -> Optional[str]:
        return self.b.build_violation_sql(
            "triples",
            {
                "antecedent": rule["antecedent"],
                "consequent": rule["consequent"],
                "base_uri": "http://ex.org",
                "uri_map": {},
            },
        )

    def test_property_chain_derives_relationship_with_dedup(self):
        sql = self._inference(PROPERTY_CHAIN)
        # Head: derive hasContract on the matched subject.
        assert "'http://ex.org#hasContract' AS predicate" in sql
        # Body joins both antecedent atoms.
        assert "'http://ex.org#hasClaim'" in sql
        assert "'http://ex.org#Claim'" in sql
        # Closed-world de-dup: don't re-derive an existing edge.
        assert "NOT EXISTS" in sql and "a4.predicate = 'http://ex.org#hasContract'" in sql

    def test_comparison_builtin_classifies_with_numeric_filter(self):
        sql = self._inference(COMPARISON_BUILTIN)
        # Classification head: rdf:type HighValue.
        assert f"'{RDF_TYPE}' AS predicate" in sql
        assert "'http://ex.org#HighValue' AS object" in sql
        # Numeric comparison uses TRY_CAST (ANSI-safe) not a bare CAST.
        assert "TRY_CAST(a2.object AS DOUBLE) > TRY_CAST(500 AS DOUBLE)" in sql

    def test_string_builtin_translates_contains_to_like(self):
        sql = self._inference(STRING_BUILTIN)
        assert "LIKE CONCAT(" in sql
        assert "'active'" in sql
        # The built-in predicate itself must not leak as a triple pattern.
        assert "contains" not in sql.replace("swrlb", "")

    def test_simple_class_inference_is_a_classification_noop(self):
        # Legacy + SHACL: a pure class->class assertion emits no inference
        # SELECT (it is materialised via the violation path, not inferred).
        assert self._inference(SIMPLE_CLASS) is None

    def test_negation_closed_world_via_nested_not_exists(self):
        # Negation has no positive inference SELECT; its closed-world body
        # lives in the violation query as a nested NOT EXISTS.
        assert self._inference(NEGATION) is None
        viol = self._violation(NEGATION)
        assert viol is not None
        # Negated atom + consequent de-dup => two NOT EXISTS subqueries.
        assert viol.count("NOT EXISTS") >= 2
        assert "a2.predicate = 'http://ex.org#hasContract'" in viol


# -- Cross-dialect parity (Spark vs Postgres) -----------------------------


class TestDialectParity:
    """Neither dialect may leak the other's syntax across the battery."""

    _SPARK_ONLY = ("TRY_CAST", "RLIKE", "CURRENT_TIMESTAMP()")
    _POSTGRES_ONLY = ("::float8", "::double precision")

    def _all_sql(self, dialect: Optional[str]) -> str:
        backend = (
            ShaclFlatSqlBackend(dialect=dialect)
            if dialect
            else ShaclFlatSqlBackend()
        )
        out = []
        for rule in BATTERY:
            params = {
                "antecedent": rule["antecedent"],
                "consequent": rule["consequent"],
                "base_uri": "http://ex.org",
                "uri_map": {},
            }
            for fn in (
                backend.build_inference_sql,
                backend.build_violation_sql,
                backend.build_materialization_sql,
            ):
                sql = fn("triples", params)
                if sql:
                    out.append(sql)
        return "\n".join(out)

    def test_postgres_has_no_spark_only_syntax(self):
        sql = self._all_sql(SQL_DIALECT_POSTGRES)
        for token in self._SPARK_ONLY:
            assert token not in sql, f"Postgres SQL leaked Spark syntax: {token}"

    def test_spark_has_no_postgres_only_syntax(self):
        sql = self._all_sql(None)  # default = Spark
        for token in self._POSTGRES_ONLY:
            assert token not in sql, f"Spark SQL leaked Postgres syntax: {token}"

    def test_both_dialects_emit_sql_for_builtin_rules(self):
        for dialect in (None, SQL_DIALECT_POSTGRES):
            sql = self._all_sql(dialect)
            assert "HighValue" in sql and "hasContract" in sql


# -- Transpiler classification stability ----------------------------------


class TestTranspilerClassificationParity:
    """Each capability must keep its SHACL-AF rule type across refactors."""

    def setup_method(self):
        self.t = SwrlToShaclTranspiler(ontology=ONTOLOGY)

    def test_rule_type_mapping_is_stable(self):
        expected = {
            "simple_class": TRIPLE_RULE,
            "property_chain": SPARQL_RULE,
            "comparison_builtin": SPARQL_RULE,
            "string_builtin": SPARQL_RULE,
            "negation": SPARQL_RULE,
        }
        for rule in BATTERY:
            shacl = self.t.transpile(rule)
            assert shacl.rule_type == expected[rule["name"]], rule["name"]

    def test_every_rule_targets_the_antecedent_class(self):
        for rule in BATTERY:
            shacl = self.t.transpile(rule)
            assert shacl.target_class_uri == "http://ex.org#Customer"

    def test_sparql_rules_carry_a_construct_body(self):
        for rule in (PROPERTY_CHAIN, COMPARISON_BUILTIN, STRING_BUILTIN, NEGATION):
            shacl = self.t.transpile(rule)
            assert shacl.construct_sparql.startswith("CONSTRUCT {")
            assert "WHERE {" in shacl.construct_sparql

    def test_negation_renders_filter_not_exists(self):
        shacl = self.t.transpile(NEGATION)
        assert "FILTER NOT EXISTS" in shacl.construct_sparql


# -- New UI SHACL surfaces cover the whole battery ------------------------


class TestShaclViewParity:
    """The read-only SHACL view/preview must render every legacy rule."""

    def test_combined_document_has_one_shape_per_rule(self):
        ttl = serialize_rules_to_turtle(BATTERY, ONTOLOGY)
        assert ttl.count("@prefix sh:") == 1  # shared header only
        assert ttl.count("sh:NodeShape") == len(BATTERY)

    def test_preview_renders_every_capability(self):
        for rule in BATTERY:
            out = preview_rule_shacl(
                rule["name"], rule["antecedent"], rule["consequent"], ONTOLOGY
            )
            assert out["turtle"].strip()
            assert out["rule_type"] in (TRIPLE_RULE, SPARQL_RULE)
