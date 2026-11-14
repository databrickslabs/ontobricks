"""Sandbox-vs-live comparison: graph projection and facade."""

from __future__ import annotations

import importlib
import sys

import pytest

from back.core.errors import InfrastructureError
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.intelligence import CompareGraph, Simulation

from tests.units.simulation.test_simulation_runner_api import (
    EX,
    _body,
    _Domain,
    _Registry,
)

sim_mod = sys.modules["back.objects.intelligence.Simulation"]

pytestmark = pytest.mark.unit


def _t(s, p, o):
    return {"subject": EX + s, "predicate": p if p.startswith("http") else EX + p, "object": o}


def _live():
    return [
        _t("a", RDF_TYPE, EX + "Customer"),
        _t("a", RDFS_LABEL, "Acme"),
        _t("a", "score", "100"),
        _t("a", "partner", EX + "b"),
        _t("a", "supplier", EX + "d"),
        _t("b", RDF_TYPE, EX + "Customer"),
    ]


def _sandbox():
    return [
        _t("a", RDF_TYPE, EX + "Customer"),
        _t("a", RDFS_LABEL, "Acme"),
        _t("a", "score", "300"),  # set patch: replaces live 100
        _t("a", "partner", EX + "b"),
        _t("a", "risk", EX + "c"),  # new link, inferred
        _t("b", RDF_TYPE, EX + "Customer"),
        _t("c", RDF_TYPE, EX + "Risk"),
    ]


def _edge(graph, predicate):
    return next(e for e in graph["edges"] if e["label"] == predicate)


def _node(graph, uri):
    return next(n for n in graph["nodes"] if n["id"] == EX + uri)


class TestCompareGraph:
    def build(self, **kw):
        patch = {(EX + "a", EX + "score", "300")}
        prov = {(EX + "a", EX + "risk", EX + "c"): "rule R1"}
        return CompareGraph.build(
            _live(), _sandbox(), patch_added=patch, provenance=prov, **kw
        )

    def test_summary_counts_are_exact(self):
        s = self.build()["summary"]
        assert s["live_count"] == 6
        assert s["sandbox_count"] == 7
        # added: score 300, risk link, c type ; removed: score 100, supplier link
        assert s["added_count"] == 3
        assert s["removed_count"] == 2
        assert s["unchanged_count"] == 4
        assert s["entity_count"] == 3
        assert s["capped"] is False
        assert "before_count" not in s and "after_count" not in s

    def test_edge_status_and_origin(self):
        graph = self.build()
        assert _edge(graph, "partner")["status"] == "unchanged"
        added = _edge(graph, "risk")
        assert added["status"] == "added"
        assert added["origin"] == "inferred"
        assert added["provenance"] == "rule R1"
        assert _edge(graph, "supplier")["status"] == "removed"

    def test_type_and_literals_are_not_edges(self):
        graph = self.build()
        labels = {e["label"] for e in graph["edges"]}
        assert labels == {"partner", "risk", "supplier"}

    def test_node_presence_and_changes(self):
        graph = self.build()
        a = _node(graph, "a")
        assert a["presence"] == "shared"
        assert a["changed"] is True
        assert a["type"] == "Customer"
        assert a["label"] == "Acme"
        changes = {(c["predicate_label"], c["object"], c["status"]) for c in a["changes"]}
        assert ("score", "300", "added") in changes
        assert ("score", "100", "removed") in changes
        assert _node(graph, "c")["presence"] == "sandbox_only"
        assert _node(graph, "d")["presence"] == "live_only"
        b = _node(graph, "b")
        assert b["presence"] == "shared" and b["changed"] is False

    def test_identical_graphs_have_no_delta(self):
        graph = CompareGraph.build(_live(), _live())
        s = graph["summary"]
        assert s["added_count"] == 0 and s["removed_count"] == 0
        assert s["unchanged_count"] == 6
        assert all(e["status"] == "unchanged" for e in graph["edges"])
        assert not any(n["changed"] for n in graph["nodes"])

    def test_empty_inputs(self):
        graph = CompareGraph.build([], [])
        assert graph["nodes"] == [] and graph["edges"] == []
        assert graph["summary"]["capped"] is False

    def test_cap_keeps_changed_nodes_first_and_flags_capped(self):
        live = [_t(f"n{i}", "next", EX + f"n{i + 1}") for i in range(30)]
        sandbox = list(live) + [_t("n5", "risk", EX + "zz")]
        graph = CompareGraph.build(live, sandbox, max_nodes=4, max_edges=3)
        ids = {n["id"] for n in graph["nodes"]}
        assert EX + "n5" in ids and EX + "zz" in ids
        assert len(ids) == 4
        assert graph["summary"]["capped"] is True
        assert graph["summary"]["node_total"] > 4
        assert graph["summary"]["added_count"] == 1
        risk = _edge(graph, "risk")
        assert risk["status"] == "added"

    def test_empty_summary_shape(self):
        s = CompareGraph.empty_summary()
        assert s["added_count"] == 0 and s["capped"] is False


class _Store:
    def __init__(self, live):
        self.live = live
        self.reads = []

    def get_triples_for_subjects(self, table, subjects):
        self.reads.append((table, list(subjects)))
        wanted = set(subjects)
        return [t for t in self.live if t["subject"] in wanted]


class _Sb:
    rows = []
    patch = set()

    def __init__(self, *a, **k):
        pass

    def triples(self):
        return list(type(self).rows)

    def load_origins(self):
        return set(type(self).patch), {}


class TestCompareToLive:
    def _sim(self, monkeypatch, *, status="ready", live=None):
        reg = _Registry()
        sim = Simulation(_Domain(), None, registry=reg)
        row = sim.create(_body())
        reg.rows[row["id"]].update(status=status)
        store = _Store(live if live is not None else _live())
        _Sb.rows = _sandbox()
        _Sb.patch = {(EX + "a", EX + "score", "300")}
        sandbox_mod = importlib.import_module("back.objects.intelligence.Sandbox")
        monkeypatch.setattr("back.core.graphdb.get_graphdb", lambda *a, **k: store)
        monkeypatch.setattr(sandbox_mod, "Sandbox", _Sb)
        seen = {}

        def _table(domain, settings, *, include_inferred=True, store=None):
            seen["include_inferred"] = include_inferred
            return "live_inferred" if include_inferred else "live_asserted"

        monkeypatch.setattr(sim_mod, "effective_graph_query_table", _table)
        return sim, row["id"], store, seen

    def test_not_ready_returns_empty_graph(self, monkeypatch):
        sim, sid, store, _ = self._sim(monkeypatch, status="draft")
        out = sim.compare_to_live(sid)
        assert out["status"] == "draft"
        assert out["nodes"] == [] and out["edges"] == []
        assert out["summary"]["live_count"] == 0
        assert store.reads == []

    def test_compares_on_sandbox_subjects(self, monkeypatch):
        sim, sid, store, seen = self._sim(monkeypatch)
        out = sim.compare_to_live(sid, include_inferred=True)
        assert out["simulation_id"] == sid
        assert out["include_inferred"] is True
        assert out["scope"]["focus"] == EX + "acme"
        assert seen["include_inferred"] is True
        table, subjects = store.reads[0]
        assert table == "live_inferred"
        assert subjects == sorted({EX + "a", EX + "b", EX + "c"})
        assert out["summary"]["added_count"] == 3
        assert out["summary"]["removed_count"] == 2
        assert {e["label"] for e in out["edges"]} == {"partner", "risk", "supplier"}

    def test_asserted_only_reads_asserted_relation(self, monkeypatch):
        sim, sid, store, seen = self._sim(monkeypatch)
        out = sim.compare_to_live(sid, include_inferred=False)
        assert out["include_inferred"] is False
        assert seen["include_inferred"] is False
        assert store.reads[0][0] == "live_asserted"

    def test_unreadable_sandbox_raises_infrastructure_error(self, monkeypatch):
        sim, sid, _, _ = self._sim(monkeypatch)

        def _boom(self):
            raise RuntimeError("table missing")

        monkeypatch.setattr(_Sb, "triples", _boom)
        with pytest.raises(InfrastructureError):
            sim.compare_to_live(sid)
