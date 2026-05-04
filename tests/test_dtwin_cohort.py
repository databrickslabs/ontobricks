"""Tests for DigitalTwin cohort discovery domain methods (CRUD + UC helpers)."""

from typing import Dict, List
from unittest.mock import MagicMock

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.core.triplestore.constants import RDF_TYPE
from back.objects.digitaltwin import DigitalTwin


BASE_URI = "http://acme/"
PERSON = BASE_URI + "Person"
PROJECT = BASE_URI + "Project"
ASSIGNED_TO = BASE_URI + "assignedTo"
STATUS = BASE_URI + "status"


def _person_triples(p, project, status):
    return [
        {"subject": p, "predicate": RDF_TYPE, "object": PERSON},
        {"subject": p, "predicate": ASSIGNED_TO, "object": project},
        {"subject": p, "predicate": STATUS, "object": status},
    ]


def _graph() -> List[Dict[str, str]]:
    triples: List[Dict[str, str]] = [
        {"subject": "P1", "predicate": RDF_TYPE, "object": PROJECT},
        {"subject": "P2", "predicate": RDF_TYPE, "object": PROJECT},
    ]
    triples += _person_triples("Alice", "P1", "Exempt")
    triples += _person_triples("Bob", "P1", "Exempt")
    triples += _person_triples("Carol", "P1", "Non-Exempt")
    triples += _person_triples("Dave", "P2", "Exempt")
    triples += _person_triples("Eve", "P2", "Exempt")
    return triples


def _store():
    s = MagicMock()
    s.query_triples.return_value = _graph()
    s.insert_triples.return_value = 7
    s.delete_cohort_triples.return_value = -1
    s.get_entity_metadata.return_value = []
    return s


@pytest.fixture
def domain(domain_session):
    domain_session._data["ontology"]["base_uri"] = BASE_URI
    domain_session._data["domain"]["info"] = {"name": "AcmeConsulting"}
    domain_session._data["domain"]["current_version"] = "1"
    return domain_session


@pytest.fixture
def good_rule_dict():
    return {
        "id": "exempt-pool",
        "label": "Exempt staffing pool",
        "class_uri": PERSON,
        "links": [{"shared_class": PROJECT, "via": ASSIGNED_TO}],
        "compatibility": [
            {"type": "value_equals", "property": STATUS, "value": "Exempt"},
        ],
        "group_type": "connected",
        "min_size": 2,
        "output": {"graph": True},
    }


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


class TestCohortRuleCRUD:
    def test_save_and_list(self, domain, good_rule_dict):
        dt = DigitalTwin(domain)
        saved = dt.save_cohort_rule(good_rule_dict)
        assert saved["id"] == "exempt-pool"
        rules = dt.list_cohort_rules()
        assert len(rules) == 1
        assert rules[0]["label"] == "Exempt staffing pool"

    def test_save_validates(self, domain):
        dt = DigitalTwin(domain)
        with pytest.raises(ValidationError):
            dt.save_cohort_rule({"id": "bad", "label": "", "class_uri": ""})

    def test_save_upserts_existing(self, domain, good_rule_dict):
        dt = DigitalTwin(domain)
        dt.save_cohort_rule(good_rule_dict)
        good_rule_dict["label"] = "Exempt staffing pool v2"
        dt.save_cohort_rule(good_rule_dict)
        rules = dt.list_cohort_rules()
        assert len(rules) == 1
        assert rules[0]["label"] == "Exempt staffing pool v2"

    def test_delete(self, domain, good_rule_dict):
        dt = DigitalTwin(domain)
        dt.save_cohort_rule(good_rule_dict)
        assert dt.delete_cohort_rule("exempt-pool") is True
        assert dt.list_cohort_rules() == []

    def test_delete_unknown(self, domain):
        dt = DigitalTwin(domain)
        assert dt.delete_cohort_rule("does-not-exist") is False

    def test_persisted_in_session_data(self, domain, good_rule_dict):
        dt = DigitalTwin(domain)
        dt.save_cohort_rule(good_rule_dict)
        # Round-trip through export_for_save (the path the registry uses).
        snapshot = domain.export_for_save()
        ontology_export = snapshot["versions"]["1"]["ontology"]
        assert "cohort_rules" in ontology_export
        assert ontology_export["cohort_rules"][0]["id"] == "exempt-pool"


# ---------------------------------------------------------------------------
# Dry-run + materialise
# ---------------------------------------------------------------------------


class TestDryRunAndMaterialize:
    def test_dry_run_returns_cohorts(self, domain, good_rule_dict):
        store = _store()
        dt = DigitalTwin(domain)
        out = dt.dry_run_cohort(good_rule_dict, store, "graph")
        assert out["rule_id"] == "exempt-pool"
        # 4 Exempt people split into two project-cohorts of size 2.
        assert out["stats"]["cohort_count"] == 2
        sizes = sorted([c["size"] for c in out["cohorts"]], reverse=True)
        assert sizes == [2, 2]

    def test_dry_run_invalid_rule(self, domain):
        store = _store()
        dt = DigitalTwin(domain)
        with pytest.raises(Exception):
            dt.dry_run_cohort({"id": "", "label": "", "class_uri": ""}, store, "graph")

    def test_dry_run_enriches_members_with_id_and_label(
        self, domain, good_rule_dict
    ):
        """Members must be returned as ``{uri, id, label}`` so the
        preview can display all three columns without a second
        round-trip. Labels come from ``store.get_entity_metadata``.
        """
        store = _store()
        # Test fixture uses bare subject identifiers (no namespace) — they
        # already act as both URI and id, exercising the local-name fallback.
        store.get_entity_metadata.return_value = [
            {"uri": "Alice", "type": PERSON, "label": "Alice Wonder"},
            {"uri": "Bob", "type": PERSON, "label": "Bob Hope"},
            {"uri": "Dave", "type": PERSON, "label": "Dave Grohl"},
            {"uri": "Eve", "type": PERSON, "label": "Eve Online"},
        ]
        dt = DigitalTwin(domain)
        out = dt.dry_run_cohort(good_rule_dict, store, "graph")
        members = out["cohorts"][0]["members"]
        assert isinstance(members, list)
        assert all(isinstance(m, dict) for m in members)
        for m in members:
            assert set(m.keys()) == {"uri", "id", "label"}
            assert m["uri"] in {"Alice", "Bob", "Dave", "Eve"}
            # No namespace separator → id falls back to the URI itself.
            assert m["id"] == m["uri"]
            assert m["label"] in {
                "Alice Wonder", "Bob Hope", "Dave Grohl", "Eve Online"
            }

    def test_dry_run_falls_back_to_empty_label_when_metadata_missing(
        self, domain, good_rule_dict
    ):
        """If ``get_entity_metadata`` returns no rows for a URI, that
        member's ``label`` is an empty string but the dict shape is
        preserved — UI degrades to showing the id only.
        """
        store = _store()
        store.get_entity_metadata.return_value = []  # no labels indexed
        dt = DigitalTwin(domain)
        out = dt.dry_run_cohort(good_rule_dict, store, "graph")
        members = out["cohorts"][0]["members"]
        assert all(isinstance(m, dict) for m in members)
        assert all(m["label"] == "" for m in members)
        assert all(m["id"] for m in members)
        assert all(m["uri"] for m in members)

    def test_dry_run_survives_metadata_fetch_failure(
        self, domain, good_rule_dict
    ):
        """Store errors during enrichment must not crash the preview;
        members fall back to plain URI strings so the UI can still
        render something useful."""
        store = _store()
        store.get_entity_metadata.side_effect = RuntimeError("bad happened")
        dt = DigitalTwin(domain)
        out = dt.dry_run_cohort(good_rule_dict, store, "graph")
        members = out["cohorts"][0]["members"]
        # Engine ran fine; only enrichment failed → strings preserved.
        assert all(isinstance(m, str) for m in members)

    def test_materialize_unknown_rule(self, domain):
        store = _store()
        dt = DigitalTwin(domain)
        with pytest.raises(NotFoundError):
            dt.materialize_cohort("missing", store, "graph")

    def test_materialize_writes_triples(self, domain, good_rule_dict):
        dt = DigitalTwin(domain)
        dt.save_cohort_rule(good_rule_dict)
        store = _store()
        out = dt.materialize_cohort("exempt-pool", store, "graph")
        assert out["cohort_count"] == 2
        # delete_cohort_triples called once (idempotent re-run path).
        assert store.delete_cohort_triples.called
        # insert_triples called once with the cohort + membership triples.
        assert store.insert_triples.called

    def test_materialize_re_run_is_idempotent(self, domain, good_rule_dict):
        dt = DigitalTwin(domain)
        dt.save_cohort_rule(good_rule_dict)
        store = _store()
        first = dt.materialize_cohort("exempt-pool", store, "graph")
        second = dt.materialize_cohort("exempt-pool", store, "graph")
        # Same number of cohorts, same size, same URIs.
        assert first["cohort_count"] == second["cohort_count"]
        # delete_cohort_triples is called every time → idempotent.
        assert store.delete_cohort_triples.call_count == 2

    def test_materialize_with_uc_target_calls_client(self, domain, good_rule_dict):
        good_rule_dict["output"] = {
            "graph": False,
            "uc_table": {
                "catalog": "consulting",
                "schema": "cohorts",
                "table_name": "cohorts_consulting",
            },
        }
        dt = DigitalTwin(domain)
        dt.save_cohort_rule(good_rule_dict)
        store = _store()
        client = MagicMock()
        out = dt.materialize_cohort("exempt-pool", store, "graph", client)
        assert client.execute_statement.called
        executed = [c.args[0] for c in client.execute_statement.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in executed)
        assert any("DELETE FROM" in s for s in executed)
        assert any("INSERT INTO" in s for s in executed)
        assert out["uc_table"]["table_name"] == "cohorts_consulting"


# ---------------------------------------------------------------------------
# Live preview helpers
# ---------------------------------------------------------------------------


class TestPreviewHelpers:
    def test_class_stats(self, domain):
        store = _store()
        dt = DigitalTwin(domain)
        out = dt.cohort_class_stats(PERSON, store, "graph")
        assert out["instance_count"] == 5

    def test_edge_count(self, domain, good_rule_dict):
        store = _store()
        dt = DigitalTwin(domain)
        out = dt.cohort_edge_count(good_rule_dict, store, "graph")
        # P1: Alice-Bob, Alice-Carol, Bob-Carol = 3 ; P2: Dave-Eve = 1.
        assert out["edge_count"] == 4

    def test_node_count(self, domain, good_rule_dict):
        store = _store()
        dt = DigitalTwin(domain)
        out = dt.cohort_node_count(good_rule_dict, store, "graph")
        # 4 of 5 Persons are Exempt.
        assert out["matching_count"] == 4
        assert out["total_count"] == 5

    def test_sample_values(self, domain):
        store = _store()
        dt = DigitalTwin(domain)
        out = dt.cohort_sample_values(PERSON, STATUS, store, "graph", limit=10)
        assert set(out["values"]) == {"Exempt", "Non-Exempt"}

    def test_path_trace(self, domain, good_rule_dict):
        """Endpoint-level: per-hop trace flows through DigitalTwin."""
        store = _store()
        dt = DigitalTwin(domain)
        out = dt.cohort_path_trace(good_rule_dict, store, "graph")
        assert out["class_member_count"] == 5
        assert out["survivor_count"] == 4  # Compatibility filter on STATUS
        assert len(out["links"]) == 1
        link = out["links"][0]
        assert link["edge_count"] >= 1
        assert link["hops"][0]["in_frontier"] == 4

    def test_path_trace_pinpoints_collapse(self, domain):
        """Bad predicate URI → first-hop neighbours_raw == 0."""
        store = _store()
        dt = DigitalTwin(domain)
        bad_rule = {
            "class_uri": PERSON,
            "links": [{
                "path": [
                    {"via": "http://nonexistent/", "target_class": PROJECT},
                ]
            }],
            "compatibility": [],
        }
        out = dt.cohort_path_trace(bad_rule, store, "graph")
        h0 = out["links"][0]["hops"][0]
        assert h0["in_frontier"] == 5
        assert h0["neighbours_raw"] == 0
        assert h0["out_frontier"] == 0
        assert out["links"][0]["edge_count"] == 0


# ---------------------------------------------------------------------------
# UC target helpers
# ---------------------------------------------------------------------------


class TestUCTarget:
    def test_suggest_target_uses_domain_settings(self, domain):
        domain._data["settings"] = {
            "databricks": {"catalog": "main", "schema": "ai"},
        }
        out = DigitalTwin(domain).cohort_suggest_uc_target(None)
        assert out["catalog"] == "main"
        assert out["schema"] == "ai"
        assert out["table_name"].startswith("cohorts_")

    def test_suggest_target_falls_back_to_metadata_tables(self, domain):
        domain._data["settings"] = {"databricks": {}}
        domain._data["domain"]["metadata"] = {
            "tables": [{"catalog": "lake", "schema": "hr"}]
        }
        out = DigitalTwin(domain).cohort_suggest_uc_target(None)
        assert out["catalog"] == "lake"
        assert out["schema"] == "hr"

    def test_probe_no_client(self):
        out = DigitalTwin.cohort_probe_uc_write({"catalog": "c"}, None)
        assert out["ok"] is False
        assert out["checks"][0]["status"] == "error"

    def test_probe_missing_input(self):
        client = MagicMock()
        out = DigitalTwin.cohort_probe_uc_write({"catalog": "c"}, client)
        assert out["ok"] is False
        assert out["checks"][0]["status"] == "error"

    def test_probe_catalog_missing(self):
        client = MagicMock()
        client.execute_query.side_effect = Exception("catalog not found")
        out = DigitalTwin.cohort_probe_uc_write(
            {"catalog": "x", "schema": "y", "table_name": "z"}, client
        )
        assert out["ok"] is False
        names = [c["name"] for c in out["checks"]]
        assert names == ["catalog"]

    def test_probe_full_flow_table_exists_compatible(self):
        client = MagicMock()

        def _exec(query):
            q = query.lower()
            if "describe catalog" in q:
                return [{"catalog_name": "c"}]
            if "describe schema" in q:
                return [{"schema_name": "s"}]
            if "describe table" in q:
                return [
                    {"col_name": "rule_id"},
                    {"col_name": "cohort_uri"},
                    {"col_name": "member_uri"},
                    {"col_name": "cohort_size"},
                ]
            if "show grants" in q:
                return [{"grant": "USE_SCHEMA"}]
            return []

        client.execute_query.side_effect = _exec
        out = DigitalTwin.cohort_probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is True
        statuses = [c["status"] for c in out["checks"]]
        assert statuses == ["ok", "ok", "ok"]

    def test_probe_table_missing_falls_back_to_grants(self):
        client = MagicMock()

        calls = {"n": 0}

        def _exec(query):
            calls["n"] += 1
            q = query.lower()
            if "describe catalog" in q:
                return [{"catalog_name": "c"}]
            if "describe schema" in q:
                return [{"schema_name": "s"}]
            if "describe table" in q:
                raise Exception("Table not found")
            if "show grants" in q:
                return [{"grant": "CREATE_TABLE"}]
            return []

        client.execute_query.side_effect = _exec
        out = DigitalTwin.cohort_probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is True
        # Last check is the table — should be ok with the "will be created" message.
        assert out["checks"][-1]["name"] == "table"
        assert out["checks"][-1]["status"] == "ok"
        assert "created" in out["checks"][-1]["message"].lower()
