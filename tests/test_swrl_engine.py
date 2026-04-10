"""Tests for the SWRL SQL/Cypher translators and SWRLEngine."""
import pytest

from back.core.reasoning.models import InferredTriple, ReasoningResult
from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator
from back.core.reasoning.SWRLParser import SWRLParser
from back.core.reasoning.SWRLCypherTranslator import SWRLCypherTranslator
from back.core.reasoning.SWRLFlatCypherTranslator import SWRLFlatCypherTranslator


# -- Atom parser tests ----------------------------------------------------

class TestParseSwrlAtoms:
    def test_class_atom(self):
        atoms = SWRLParser.parse_atoms("Customer(?x)")
        assert len(atoms) == 1
        assert atoms[0]["name"] == "Customer"
        assert atoms[0]["args"] == ["?x"]
        assert atoms[0]["arity"] == 1

    def test_property_atom(self):
        atoms = SWRLParser.parse_atoms("hasClaim(?x, ?y)")
        assert len(atoms) == 1
        assert atoms[0]["name"] == "hasClaim"
        assert atoms[0]["args"] == ["?x", "?y"]
        assert atoms[0]["arity"] == 2

    def test_conjunction(self):
        atoms = SWRLParser.parse_atoms(
            "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)"
        )
        assert len(atoms) == 3
        assert atoms[0]["name"] == "Customer"
        assert atoms[1]["name"] == "hasClaim"
        assert atoms[2]["name"] == "Claim"

    def test_empty(self):
        assert SWRLParser.parse_atoms("") == []

    def test_dotted_names(self):
        atoms = SWRLParser.parse_atoms("ns.ClassName(?x)")
        assert atoms[0]["name"] == "ns.ClassName"


# -- URI resolution tests -------------------------------------------------

class TestResolveSwrlUri:
    def test_full_uri_passthrough(self):
        assert SWRLParser.resolve_uri("http://ex.org/Foo", "") == "http://ex.org/Foo"

    def test_uri_map_lookup(self):
        uri_map = {"customer": "http://ex.org/Customer"}
        assert SWRLParser.resolve_uri("Customer", "", uri_map) == "http://ex.org/Customer"

    def test_base_uri_fallback(self):
        assert SWRLParser.resolve_uri("Foo", "http://ex.org") == "http://ex.org#Foo"

    def test_base_uri_with_hash(self):
        assert SWRLParser.resolve_uri("Foo", "http://ex.org#") == "http://ex.org#Foo"

    def test_base_uri_with_slash(self):
        assert SWRLParser.resolve_uri("Foo", "http://ex.org/") == "http://ex.org/Foo"


# -- SQL Translator tests -------------------------------------------------

class TestSWRLSQLTranslator:
    def setup_method(self):
        self.translator = SWRLSQLTranslator()

    def test_simple_rule_violation(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        sql = self.translator.build_violation_sql("triples", params)
        assert sql is not None
        assert "SELECT DISTINCT" in sql
        assert "FROM triples" in sql
        assert "NOT EXISTS" in sql

    def test_missing_antecedent_returns_none(self):
        params = {"antecedent": "", "consequent": "Foo(?x)", "base_uri": ""}
        assert self.translator.build_violation_sql("t", params) is None

    def test_missing_consequent_returns_none(self):
        params = {"antecedent": "Foo(?x)", "consequent": "", "base_uri": ""}
        assert self.translator.build_violation_sql("t", params) is None

    def test_no_property_atoms_generates_sql(self):
        """Class-only antecedent (e.g. Customer(?x)) with consequent
        hasContract(?x,?z) now generates a NOT EXISTS violation query."""
        params = {
            "antecedent": "Customer(?x)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
        }
        sql = self.translator.build_violation_sql("t", params)
        assert sql is not None
        assert "NOT EXISTS" in sql
        assert "hasContract" in sql

    def test_materialization_sql(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        sql = self.translator.build_materialization_sql("triples", params)
        assert sql is not None
        assert "INSERT INTO triples" in sql


# -- Cypher Translator tests -----------------------------------------------

class TestSWRLCypherTranslator:
    def setup_method(self):
        self.translator = SWRLCypherTranslator(graph_schema=None)

    def test_simple_rule_violation(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        query = self.translator.build_violation_query(params)
        assert query is not None
        assert "MATCH" in query
        assert "NOT EXISTS" in query
        assert "RETURN DISTINCT" in query

    def test_missing_atoms_returns_none(self):
        params = {"antecedent": "", "consequent": "Foo(?x)", "base_uri": ""}
        assert self.translator.build_violation_query(params) is None

    def test_materialization_query(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?y)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        query = self.translator.build_materialization_query(params)
        assert query is not None
        assert "CREATE" in query

    def test_materialization_rejects_unbound_variable(self):
        """Consequent variable ?z not in antecedent → returns None (no cartesian product)."""
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        query = self.translator.build_materialization_query(params)
        assert query is None


# -- SWRLEngine tests ------------------------------------------------------

class TestSWRLEngine:
    def test_execute_rules_with_mock_store(self):
        from back.core.reasoning.SWRLEngine import SWRLEngine
        from unittest.mock import MagicMock

        store = MagicMock()
        store.execute_query.return_value = [
            {"subject": "http://ex.org/entity1", "predicate": "http://ex.org/hasContract", "object": ""},
        ]

        engine = SWRLEngine(ontology={
            "base_uri": "http://ex.org",
            "classes": [{"name": "Customer"}],
            "properties": [{"name": "hasClaim"}, {"name": "hasContract"}],
        })

        rules = [{
            "name": "test_rule",
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y)",
            "consequent": "hasContract(?x, ?z)",
        }]

        result = engine.execute_rules(rules, store, "triples")
        assert result.stats["phase"] == "swrl"
        assert result.stats["rules_count"] == 1
        assert len(result.inferred_triples) >= 1

    def test_execute_empty_rules(self):
        from back.core.reasoning.SWRLEngine import SWRLEngine
        from unittest.mock import MagicMock

        store = MagicMock()
        engine = SWRLEngine()
        result = engine.execute_rules([], store, "triples")
        assert len(result.inferred_triples) == 0
        assert result.stats["rules_count"] == 0


# -- ReasoningResult deduplication tests -----------------------------------

class TestReasoningResultDeduplication:
    def test_deduplicate_removes_cross_phase_duplicates(self):
        result = ReasoningResult(inferred_triples=[
            InferredTriple("s1", "p1", "o1", "tbox"),
            InferredTriple("s1", "p1", "o1", "swrl:Rule1"),
            InferredTriple("s1", "p1", "o1", "graph:transitive"),
            InferredTriple("s2", "p2", "o2", "tbox"),
        ])
        removed = result.deduplicate()
        assert removed == 2
        assert len(result.inferred_triples) == 2
        assert result.inferred_triples[0].provenance == "tbox"
        assert result.inferred_triples[1].subject == "s2"

    def test_deduplicate_no_duplicates(self):
        result = ReasoningResult(inferred_triples=[
            InferredTriple("s1", "p1", "o1", "tbox"),
            InferredTriple("s2", "p2", "o2", "swrl:R1"),
        ])
        removed = result.deduplicate()
        assert removed == 0
        assert len(result.inferred_triples) == 2

    def test_deduplicate_empty(self):
        result = ReasoningResult()
        assert result.deduplicate() == 0

    def test_merge_then_deduplicate(self):
        r1 = ReasoningResult(inferred_triples=[
            InferredTriple("s1", "p1", "o1", "tbox"),
            InferredTriple("s2", "p2", "o2", "tbox"),
        ])
        r2 = ReasoningResult(inferred_triples=[
            InferredTriple("s1", "p1", "o1", "swrl:R1"),
            InferredTriple("s3", "p3", "o3", "swrl:R1"),
        ])
        r1.merge(r2)
        assert len(r1.inferred_triples) == 4
        removed = r1.deduplicate()
        assert removed == 1
        assert len(r1.inferred_triples) == 3


# -- Flat Cypher Translator inference NOT EXISTS tests ---------------------

class TestSWRLFlatCypherTranslatorInference:
    def setup_method(self):
        self.translator = SWRLFlatCypherTranslator(node_table="Triple")

    def test_inference_query_contains_not_exists(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?y)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        query = self.translator.build_inference_query(params)
        assert query is not None
        assert "NOT EXISTS" in query
        assert "RETURN DISTINCT" in query

    def test_inference_class_consequent_not_exists(self):
        params = {
            "antecedent": "Person(?x) \u2227 hasAge(?x, ?a)",
            "consequent": "Adult(?x)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        query = self.translator.build_inference_query(params)
        assert query is not None
        assert "NOT EXISTS" in query
        assert "rdf:type" in query or "22-rdf-syntax-ns#type" in query


# -- Graph Cypher Translator inference NOT EXISTS tests --------------------

class TestSWRLCypherTranslatorInference:
    def setup_method(self):
        self.translator = SWRLCypherTranslator(graph_schema=None)

    def test_inference_query_contains_not_exists(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?y)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        query = self.translator.build_inference_query(params)
        assert query is not None
        assert "NOT EXISTS" in query
        assert "RETURN DISTINCT" in query
