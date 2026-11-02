"""``GraphReadRelation``: the Lakehouse//RT fast path of a ``microbatch`` domain.

A Lakehouse domain whose last build ran ``microbatch`` with only row-local
mappings, read through Lakehouse//RT, reads ``…_slots`` ∪ ``…_inferred``
inline instead of the ``…_graph`` materialized view; every other domain keeps
``…_graph``.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from back.core.graphdb.delta import GraphReadRelation
from back.core.helpers import effective_graph_query_table
from back.objects.digitaltwin.build import TwinBuildRunHistory

pytestmark = pytest.mark.unit

_MODULE = importlib.import_module("back.core.graphdb.delta.GraphReadRelation")
_STEM = "cat.sch.triplestore_dom_V3"
_SLOTS, _INFERRED = f"{_STEM}_slots", f"{_STEM}_inferred"
_GRAPH, _DATA = f"{_STEM}_graph", f"{_STEM}_data"
_LIVE = (
    f"(SELECT subject, predicate, object FROM {_SLOTS} "
    "WHERE object IS NOT NULL AND TRIM(object) != '' "
    f"UNION SELECT subject, predicate, object FROM {_INFERRED})"
)
_LIVE_ASSERTED = (
    f"(SELECT DISTINCT subject, predicate, object FROM {_SLOTS} "
    "WHERE object IS NOT NULL AND TRIM(object) != '')"
)
_MICROBATCH_FACTS = {
    "materialization_mode_effective": "microbatch",
    "slots_table": _SLOTS,
    "batch_mappings": 0,
}


def _domain(mode="microbatch", backend="databricks"):
    return SimpleNamespace(
        info={"name": "Dom", "materialization_mode": mode, "graph_backend": backend},
        current_version=3,
        uc_domain_folder="dom",
        delta={"catalog": "cat", "schema": "sch"},
    )


class _Catalog:
    """Answers ``information_schema.tables`` lookups from *kinds* (leaf -> table_type)."""

    def __init__(self, kinds=None):
        self.kinds = kinds if kinds is not None else {
            "triplestore_dom_v3_slots": "STREAMING_TABLE",
            "triplestore_dom_v3_data": "MATERIALIZED_VIEW",
        }
        self.queries: list[str] = []

    def execute_query(self, sql):
        self.queries.append(sql)
        leaf = sql.rsplit("table_name = '", 1)[1].rstrip("'")
        kind = self.kinds.get(leaf)
        return [{"table_type": kind}] if kind else []


@pytest.fixture(autouse=True)
def _fresh_cache():
    GraphReadRelation.clear_cache()
    yield
    GraphReadRelation.clear_cache()


@pytest.fixture
def env(monkeypatch):
    """RT on, a microbatch build of row-local mappings only; each knob can be turned."""
    state = SimpleNamespace(rt=True, facts=dict(_MICROBATCH_FACTS), fact_reads=0)

    def facts(domain, settings):
        state.fact_reads += 1
        return state.facts

    monkeypatch.setattr(_MODULE, "resolve_lakehouse_use_sea", lambda d, s: state.rt)
    monkeypatch.setattr(TwinBuildRunHistory, "last_success_facts", staticmethod(facts))
    return state


def _decide(domain=None, catalog=None):
    return GraphReadRelation.decide(domain or _domain(), SimpleNamespace(), client=catalog or _Catalog())


class TestDecision:
    def test_microbatch_row_local_only_through_rt_reads_the_live_slots(self, env):
        assert _decide() == GraphReadRelation.SLOTS_LIVE

    def test_rt_toggle_off_keeps_the_graph(self, env):
        env.rt = False

        assert _decide() == GraphReadRelation.GRAPH

    def test_an_incremental_domain_keeps_the_graph_without_any_read(self, env):
        catalog = _Catalog()

        assert _decide(_domain("incremental"), catalog) == GraphReadRelation.GRAPH
        assert env.fact_reads == 0 and catalog.queries == []

    def test_a_full_domain_keeps_the_graph_without_any_read(self, env):
        assert _decide(_domain("full")) == GraphReadRelation.GRAPH
        assert env.fact_reads == 0

    def test_another_backend_keeps_its_store(self, env):
        assert _decide(_domain(backend="lakebase")) == GraphReadRelation.GRAPH
        assert env.fact_reads == 0

    def test_a_microbatch_request_that_ran_incremental_keeps_the_graph(self, env):
        env.facts = {**_MICROBATCH_FACTS, "materialization_mode_effective": "incremental"}

        assert _decide() == GraphReadRelation.GRAPH

    def test_batch_mappings_keep_the_graph(self, env):
        env.facts = {**_MICROBATCH_FACTS, "batch_mappings": 2}

        assert _decide() == GraphReadRelation.GRAPH

    def test_a_build_without_the_batch_fact_counts_as_batch(self, env):
        env.facts = {k: v for k, v in _MICROBATCH_FACTS.items() if k != "batch_mappings"}

        assert _decide() == GraphReadRelation.GRAPH

    def test_a_build_without_slots_keeps_the_graph(self, env):
        env.facts = {**_MICROBATCH_FACTS, "slots_table": ""}

        assert _decide() == GraphReadRelation.GRAPH

    def test_no_build_yet_falls_back_to_the_requested_mode_and_keeps_the_graph(self, env):
        env.facts = None

        assert _decide() == GraphReadRelation.GRAPH

    def test_a_missing_slots_table_keeps_the_graph(self, env):
        catalog = _Catalog({"triplestore_dom_v3_data": "MATERIALIZED_VIEW"})

        assert _decide(catalog=catalog) == GraphReadRelation.GRAPH

    def test_a_data_relation_that_is_no_longer_the_view_keeps_the_graph(self, env):
        catalog = _Catalog(
            {"triplestore_dom_v3_slots": "STREAMING_TABLE", "triplestore_dom_v3_data": "MANAGED"}
        )

        assert _decide(catalog=catalog) == GraphReadRelation.GRAPH

    def test_a_failing_read_keeps_the_graph(self, env, monkeypatch):
        def broken(domain, settings):
            raise RuntimeError("registry down")

        monkeypatch.setattr(TwinBuildRunHistory, "last_success_facts", staticmethod(broken))

        assert _decide() == GraphReadRelation.GRAPH

    def test_without_a_client_the_build_warehouse_is_probed(self, env, monkeypatch):
        calls = []

        def factory(domain, settings=None, *, for_write=False, warehouse_id=None):
            calls.append(for_write)
            return _Catalog()

        monkeypatch.setattr(_MODULE, "create_databricks_client", factory)

        assert GraphReadRelation.decide(_domain(), SimpleNamespace()) == GraphReadRelation.SLOTS_LIVE
        assert calls == [True]


class TestCache:
    def test_a_decision_is_reused_within_the_ttl(self, env, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(GraphReadRelation, "_clock", staticmethod(lambda: now[0]))
        catalog = _Catalog()

        _decide(catalog=catalog)
        now[0] += GraphReadRelation.TTL_S - 1
        env.rt = False

        assert _decide(catalog=catalog) == GraphReadRelation.SLOTS_LIVE
        assert env.fact_reads == 1 and len(catalog.queries) == 2

    def test_a_decision_is_read_again_after_the_ttl(self, env, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(GraphReadRelation, "_clock", staticmethod(lambda: now[0]))

        _decide()
        now[0] += GraphReadRelation.TTL_S
        env.rt = False

        assert _decide() == GraphReadRelation.GRAPH

    def test_the_cache_is_per_domain_version(self, env):
        other = _domain()
        other.current_version = 4

        _decide()
        _decide(other)

        assert env.fact_reads == 2


class TestRelationSQL:
    def test_the_live_relation_unions_the_non_blank_slots_and_the_inferred(self):
        assert GraphReadRelation.relation_sql(_SLOTS, _INFERRED) == _LIVE

    def test_without_inferred_the_distinct_non_blank_slots(self):
        assert GraphReadRelation.relation_sql(_SLOTS, _INFERRED, include_inferred=False) == (
            _LIVE_ASSERTED
        )

    def test_it_is_recognised_as_inline_and_maps_back_to_its_physical_relation(self):
        domain = _domain()

        assert GraphReadRelation.is_inline(_LIVE) and not GraphReadRelation.is_inline(_GRAPH)
        assert GraphReadRelation.physical(_LIVE, domain, None) == _GRAPH
        assert GraphReadRelation.physical(_LIVE_ASSERTED, domain, None) == _DATA
        assert GraphReadRelation.physical(_GRAPH, domain, None) == _GRAPH

    def test_an_unsafe_name_never_becomes_inline_sql(self, env, monkeypatch):
        monkeypatch.setattr(
            _MODULE._table_naming, "slots_table_fqn", lambda d, s=None: "cat.sch.x; DROP"
        )

        assert GraphReadRelation.read_relation(_domain(), SimpleNamespace()) == ""


class TestEffectiveGraphQueryTable:
    def test_slots_live_returns_the_inline_relation(self, monkeypatch):
        monkeypatch.setattr(
            GraphReadRelation, "decide", staticmethod(lambda d, s, client=None: "slots_live")
        )

        assert effective_graph_query_table(_domain(), SimpleNamespace()) == _LIVE
        assert effective_graph_query_table(
            _domain(), SimpleNamespace(), include_inferred=False
        ) == _LIVE_ASSERTED

    def test_graph_returns_the_graph_relation(self, monkeypatch):
        monkeypatch.setattr(
            GraphReadRelation, "decide", staticmethod(lambda d, s, client=None: "graph")
        )

        assert effective_graph_query_table(_domain(), SimpleNamespace()) == _GRAPH

    def test_physical_names_the_graph_relation_without_deciding(self, monkeypatch):
        def never(domain, settings, client=None):
            raise AssertionError("no decision for a physical name")

        monkeypatch.setattr(GraphReadRelation, "decide", staticmethod(never))

        assert effective_graph_query_table(_domain(), SimpleNamespace(), physical=True) == _GRAPH
        assert effective_graph_query_table(
            _domain(), SimpleNamespace(), include_inferred=False, physical=True
        ) == _DATA


class _Reads:
    """Read client recording each statement; *fail_first* raises once (an RT guardrail)."""

    def __init__(self, fail_first=None):
        self.queries: list[str] = []
        self._fail = fail_first

    def execute_query(self, query):
        self.queries.append(query)
        if self._fail is not None:
            exc, self._fail = self._fail, None
            raise exc
        return [{"subject": "s", "predicate": "p", "object": "o", "cnt": 1}]


class TestDeltaFlatStoreReads:
    """The store interpolates the inline relation as ``FROM {relation}`` and aliases it itself."""

    @staticmethod
    def _store(client):
        from back.core.graphdb.delta import DeltaFlatStore

        return DeltaFlatStore(client, domain=_domain(), settings=SimpleNamespace())

    def test_a_paginated_read_selects_from_the_inline_relation(self):
        client = _Reads()

        self._store(client).paginated_triples(_LIVE, ["predicate = 'p'"], 10, 0)

        assert client.queries == [
            f"SELECT subject, predicate, object FROM {_LIVE} WHERE predicate = 'p' "
            "LIMIT 10 OFFSET 0"
        ]

    def test_an_aliasing_read_aliases_the_subquery(self):
        client = _Reads()

        self._store(client).query_triples_for_analysis(_LIVE, predicate_filter=["p"])

        assert f"FROM {_LIVE} t WHERE" in client.queries[0]

    def test_the_rt_guardrail_fallback_retries_the_same_inline_read(self, monkeypatch):
        from back.core.errors import InfrastructureError

        store_module = importlib.import_module("back.core.graphdb.delta.DeltaFlatStore")
        build = _Reads()
        monkeypatch.setattr(store_module, "resolve_build_warehouse_id", lambda d, s: "wh-build")
        monkeypatch.setattr(store_module, "create_databricks_client", lambda *a, **k: build)
        query = _Reads(fail_first=InfrastructureError("RT guardrail hit"))
        store = self._store(query)

        store.paginated_count(_LIVE, [])

        assert build.queries == query.queries == [f"SELECT COUNT(*) AS cnt FROM {_LIVE}"]
        assert store.last_read_route == "build-fallback"

    def test_a_cache_rebuild_reads_the_graph_relation(self, monkeypatch):
        from back.core.graphdb.delta import materialize

        sources = []
        monkeypatch.setattr(
            materialize, "build_adj_ctas_sql", lambda rel, fqn, d: sources.append(rel) or "SELECT 1"
        )
        monkeypatch.setattr(materialize, "build_props_ctas_sql", lambda rel, fqn: "SELECT 1")
        monkeypatch.setattr(materialize, "build_entity_search_ctas_sql", lambda rel, fqn: "SELECT 1")
        client = SimpleNamespace(execute_statement=lambda sql: None, execute_query=lambda sql: [])

        self._store(client).rebuild_adjacency(_LIVE)

        assert sources == [_GRAPH, _GRAPH]


class TestAssertedOnlyRelation:
    def test_the_inline_relation_without_inferred_is_asserted_only(self):
        from back.core.graphdb.entity_search import is_asserted_only_relation

        assert is_asserted_only_relation(_LIVE_ASSERTED) is True
        assert is_asserted_only_relation(_LIVE) is False


class TestBridgeTarget:
    def test_a_bridge_reads_the_inline_relation_but_no_other_sql(self):
        from back.objects.digitaltwin.node import NodeContextService

        assert NodeContextService._readable_relation(_LIVE) is True
        assert NodeContextService._readable_relation(_GRAPH) is True
        assert NodeContextService._readable_relation("cat.sch.t; DROP TABLE x") is False


class TestReported:
    """``read_relation`` in the incremental status and the readiness preflight."""

    def test_the_status_reports_the_live_slots(self, env, monkeypatch):
        from back.objects.digitaltwin.build import TwinIncrementalStatus

        for section in ("relation", "slot_pipeline", "lakebase_sync", "last_build"):
            monkeypatch.setattr(TwinIncrementalStatus, section, staticmethod(lambda *a, **k: {}))

        report = TwinIncrementalStatus.report(_domain(), SimpleNamespace(), _Catalog())

        assert report["read_relation"] == GraphReadRelation.SLOTS_LIVE

    def test_a_full_domain_s_status_reads_the_graph(self, env):
        from back.objects.digitaltwin.build import TwinIncrementalStatus

        report = TwinIncrementalStatus.report(_domain("full"), SimpleNamespace(), _Catalog())

        assert report["read_relation"] == GraphReadRelation.GRAPH and env.fact_reads == 0

    def test_the_readiness_preflight_reports_it(self, env, monkeypatch):
        from back.objects.digitaltwin.build import TwinIncrementalBuild, TwinIncrementalReadiness

        monkeypatch.setattr(TwinIncrementalBuild, "mappings_for", staticmethod(lambda d: None))
        monkeypatch.setattr(
            TwinIncrementalReadiness, "realtime_facts", staticmethod(lambda d, s: {})
        )
        monkeypatch.setattr(
            importlib.import_module("back.objects.digitaltwin.build.TwinBuildPostMaterialize")
            .TwinBuildPostMaterialize,
            "serverless_available",
            staticmethod(lambda s: True),
        )

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), _Catalog())

        assert report["read_relation"] == GraphReadRelation.SLOTS_LIVE


class TestBuildRunFacts:
    def test_the_newest_successful_main_build_is_the_one_read(self, monkeypatch):
        def run(status, kind="manual", **facts):
            return {"status": status, "build_kind": kind, "stats": {"materialization": facts}}

        runs = [
            run("success", "scheduled", reconcile_drift=False),
            run("failed", materialization_mode_effective="full"),
            run("success", materialization_mode_effective="microbatch", batch_mappings=0),
            run("success", materialization_mode_effective="full"),
        ]
        monkeypatch.setattr(TwinBuildRunHistory, "recent_runs_for", staticmethod(lambda d, s: runs))

        facts = TwinBuildRunHistory.last_success_facts(_domain(), SimpleNamespace())

        assert facts == {"materialization_mode_effective": "microbatch", "batch_mappings": 0}

    def test_no_successful_build_is_none(self, monkeypatch):
        monkeypatch.setattr(TwinBuildRunHistory, "recent_runs_for", staticmethod(lambda d, s: []))

        assert TwinBuildRunHistory.last_success_facts(_domain(), SimpleNamespace()) is None
