"""``GraphReadRelation``: the Lakehouse//RT fast path of a ``microbatch`` domain.

A Lakehouse domain read through Lakehouse//RT whose ``…_data`` is the
``microbatch`` materialized view over the slots alone (its stored fingerprint
says so) reads ``…_graph`` / ``…_data`` as inline subqueries over ``…_slots``
(∪ ``…_inferred``); every other domain keeps the views. The swap happens in
``DeltaFlatStore._sql_relation``, on the store's own client.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
import sqlglot

from back.core.errors import InfrastructureError
from back.core.graphdb.delta import DeltaFlatStore, GraphReadRelation, MaterializedViewBuilder
from back.core.graphdb.delta.MaterializedViewBuilder import TRIGGER_ON_UPDATE
from back.core.helpers import effective_graph_query_table

pytestmark = pytest.mark.unit

_MODULE = importlib.import_module("back.core.graphdb.delta.GraphReadRelation")
_STORE_MODULE = importlib.import_module("back.core.graphdb.delta.DeltaFlatStore")
_STEM = "cat.sch.triplestore_dom_V3"
_SLOTS, _INFERRED = f"{_STEM}_slots", f"{_STEM}_inferred"
_GRAPH, _DATA = f"{_STEM}_graph", f"{_STEM}_data"
_LIVE = (
    f"(SELECT DISTINCT subject, predicate, object FROM {_SLOTS} "
    "WHERE object IS NOT NULL AND TRIM(object) != '' "
    f"UNION ALL SELECT subject, predicate, object FROM {_INFERRED})"
)
_LIVE_ASSERTED = (
    f"(SELECT DISTINCT subject, predicate, object FROM {_SLOTS} "
    "WHERE object IS NOT NULL AND TRIM(object) != '')"
)
_MICROBATCH_SHA = MaterializedViewBuilder.data_mv_sha(
    slots_fqn=_SLOTS, batch_branch_sql=None, refresh_clause=TRIGGER_ON_UPDATE
)


def _domain(mode="microbatch", backend="databricks", version=3):
    return SimpleNamespace(
        info={"name": "Dom", "materialization_mode": mode, "graph_backend": backend},
        current_version=version,
        uc_domain_folder="dom",
        delta={"catalog": "cat", "schema": "sch"},
    )


class _Warehouse:
    """The store's client: ``…_data``'s fingerprint, companion probes and graph reads.

    *sha* is what ``SHOW TBLPROPERTIES`` holds (``None``: no such property, as a
    CTAS table); *probe_error* fails that lookup; *read_error* fails the first
    SELECT (an RT guardrail); *companions* answers ``DESCRIBE DETAIL``.
    """

    def __init__(self, sha=_MICROBATCH_SHA, *, probe_error=None, read_error=None, companions=False):
        self.sha, self.probe_error, self.read_error = sha, probe_error, read_error
        self.companions = companions
        self.queries: list[str] = []

    @property
    def reads(self):
        return [q for q in self.queries if not q.startswith(("SHOW", "DESCRIBE"))]

    @property
    def probes(self):
        return [q for q in self.queries if q.startswith("SHOW TBLPROPERTIES")]

    def execute_query(self, sql):
        self.queries.append(sql)
        if sql.startswith("SHOW TBLPROPERTIES"):
            if self.probe_error is not None:
                raise self.probe_error
            missing = f"Table {_DATA} does not have property: ontobricks.mv_sql_sha256"
            return [{"value": self.sha if self.sha is not None else missing}]
        if sql.startswith("DESCRIBE DETAIL"):
            if not self.companions:
                raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] not there")
            return [{}]
        if self.read_error is not None:
            exc, self.read_error = self.read_error, None
            raise exc
        return [
            {"subject": "s", "predicate": "p", "object": "o", "cnt": 1, "entity": "e", "uri": "u"}
        ]

    def execute_statement(self, sql):
        self.queries.append(sql)


@pytest.fixture(autouse=True)
def _fresh_cache():
    GraphReadRelation.clear_cache()
    yield
    GraphReadRelation.clear_cache()


@pytest.fixture
def rt(monkeypatch):
    """The RT toggle, on unless a test turns it off; counts its reads."""
    state = SimpleNamespace(on=True, reads=0)

    def toggle(domain, settings):
        state.reads += 1
        return state.on

    monkeypatch.setattr(_MODULE, "resolve_lakehouse_use_sea", toggle)
    return state


def _decide(warehouse, domain=None):
    return GraphReadRelation.decide(domain or _domain(), SimpleNamespace(), warehouse)


def _store(warehouse, domain=None):
    return DeltaFlatStore(warehouse, domain=domain or _domain(), settings=SimpleNamespace())


class TestDecision:
    def test_the_microbatch_view_over_the_slots_alone_reads_the_live_slots(self, rt):
        warehouse = _Warehouse()

        assert _decide(warehouse) == GraphReadRelation.SLOTS_LIVE
        assert warehouse.probes == [f"SHOW TBLPROPERTIES {_DATA} ('ontobricks.mv_sql_sha256')"]

    def test_the_incremental_view_has_another_fingerprint(self, rt):
        incremental = MaterializedViewBuilder.data_mv_sha(
            slots_fqn=_SLOTS, batch_branch_sql=None, refresh_clause=""
        )

        assert incremental != _MICROBATCH_SHA
        assert _decide(_Warehouse(incremental)) == GraphReadRelation.GRAPH

    def test_a_view_with_a_batch_branch_keeps_the_views(self, rt):
        with_batch = MaterializedViewBuilder.data_mv_sha(
            slots_fqn=_SLOTS,
            batch_branch_sql="SELECT 's' AS subject, 'p' AS predicate, 'o' AS object",
            refresh_clause=TRIGGER_ON_UPDATE,
        )

        assert _decide(_Warehouse(with_batch)) == GraphReadRelation.GRAPH

    def test_a_ctas_table_without_the_property_keeps_the_views(self, rt):
        assert _decide(_Warehouse(sha=None)) == GraphReadRelation.GRAPH

    def test_no_fingerprint_keeps_the_views(self, rt, monkeypatch):
        monkeypatch.setattr(
            MaterializedViewBuilder, "existing_mv_sha", staticmethod(lambda c, f, strict=False: "")
        )

        assert _decide(_Warehouse()) == GraphReadRelation.GRAPH

    @pytest.fixture
    def warnings(self, monkeypatch):
        """The module logger's warnings (the app's logging setup may stop propagation)."""
        seen: list[str] = []
        monkeypatch.setattr(
            _MODULE.logger, "warning", lambda msg, *args, **kw: seen.append(msg % args)
        )
        return seen

    def test_an_absent_data_relation_keeps_the_views_quietly(self, rt, warnings):
        warehouse = _Warehouse(probe_error=RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] gone"))

        assert _decide(warehouse) == GraphReadRelation.GRAPH
        assert warnings == []

    def test_a_failing_probe_keeps_the_views_and_warns(self, rt, warnings):
        warehouse = _Warehouse(probe_error=RuntimeError("PERMISSION_DENIED on information"))

        assert _decide(warehouse) == GraphReadRelation.GRAPH
        assert len(warnings) == 1 and "PERMISSION_DENIED" in warnings[0]

    def test_rt_toggle_off_keeps_the_views_without_a_probe(self, rt):
        rt.on = False
        warehouse = _Warehouse()

        assert _decide(warehouse) == GraphReadRelation.GRAPH
        assert warehouse.queries == []

    @pytest.mark.parametrize(
        ("mode", "backend"),
        [("full", "databricks"), ("incremental", "databricks"), ("microbatch", "lakebase")],
    )
    def test_a_domain_that_cannot_run_microbatch_is_answered_without_any_read(
        self, rt, mode, backend
    ):
        warehouse = _Warehouse()

        assert _decide(warehouse, _domain(mode, backend)) == GraphReadRelation.GRAPH
        assert warehouse.queries == [] and rt.reads == 0


class TestCache:
    def test_a_decision_is_reused_within_the_ttl(self, rt, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(GraphReadRelation, "_clock", staticmethod(lambda: now[0]))
        warehouse = _Warehouse()

        _decide(warehouse)
        now[0] += GraphReadRelation.TTL_S - 1
        warehouse.sha = None

        assert _decide(warehouse) == GraphReadRelation.SLOTS_LIVE
        assert len(warehouse.probes) == 1

    def test_a_decision_is_read_again_after_the_ttl(self, rt, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(GraphReadRelation, "_clock", staticmethod(lambda: now[0]))
        warehouse = _Warehouse()

        _decide(warehouse)
        now[0] += GraphReadRelation.TTL_S
        warehouse.sha = None

        assert _decide(warehouse) == GraphReadRelation.GRAPH

    def test_clearing_the_cache_reads_a_changed_definition_at_once(self, rt):
        warehouse = _Warehouse()
        _decide(warehouse)

        warehouse.sha = None
        GraphReadRelation.clear_cache()

        assert _decide(warehouse) == GraphReadRelation.GRAPH

    def test_a_warm_fast_path_ends_with_a_switch_away_from_microbatch(self, rt):
        warehouse = _Warehouse()
        _decide(warehouse)

        assert _decide(warehouse, _domain("full")) == GraphReadRelation.GRAPH
        assert _decide(warehouse, _domain("incremental")) == GraphReadRelation.GRAPH

    def test_the_cache_is_per_domain_version(self, rt):
        warehouse = _Warehouse()

        _decide(warehouse)
        _decide(warehouse, _domain(version=4))

        assert len(warehouse.probes) == 2


class TestRelationSQL:
    def test_the_graph_relation_is_the_distinct_slots_union_all_the_inferred(self):
        assert GraphReadRelation.relation_sql(_SLOTS, _INFERRED) == _LIVE

    def test_the_data_relation_is_the_distinct_non_blank_slots(self):
        assert GraphReadRelation.asserted_relation_sql(_SLOTS) == _LIVE_ASSERTED

    @pytest.mark.parametrize("relation", [_LIVE, _LIVE_ASSERTED])
    def test_it_parses_bare_and_aliased(self, relation):
        for sql in (f"SELECT * FROM {relation}", f"SELECT t.subject FROM {relation} t"):
            assert len(sqlglot.parse(sql, read="databricks")) == 1

    def test_effective_graph_query_table_stays_a_table_name(self, rt):
        assert effective_graph_query_table(_domain(), SimpleNamespace()) == _GRAPH
        assert effective_graph_query_table(_domain(), SimpleNamespace(), include_inferred=False) == _DATA
        assert rt.reads == 0


class TestStoreSwap:
    def test_graph_and_data_become_the_live_relations(self, rt):
        store = _store(_Warehouse())

        assert store._sql_relation(_GRAPH) == _LIVE
        assert store._sql_relation(_DATA) == _LIVE_ASSERTED
        assert store.sql_table_reference(_GRAPH) == _LIVE

    @pytest.mark.parametrize(
        "name",
        [
            _INFERRED,
            _SLOTS,
            _STEM,
            f"{_STEM}_adj_out",
            f"{_STEM}_entity_search",
            "cat.sch.triplestore_other_V3_graph",
            "cat.sch.triplestore_dom_V4_data",
        ],
    )
    def test_every_other_name_is_left_alone(self, rt, name):
        warehouse = _Warehouse()

        assert _store(warehouse)._sql_relation(name) == name
        assert warehouse.probes == []

    def test_a_logical_name_still_resolves_to_the_view(self, rt):
        assert _store(_Warehouse(companions=True))._sql_relation("Dom_V3") == _GRAPH

    def test_without_the_fast_path_the_views_are_read(self, rt):
        store = _store(_Warehouse(sha=None))

        assert store._sql_relation(_GRAPH) == _GRAPH and store._sql_relation(_DATA) == _DATA

    def test_a_raw_store_never_swaps(self, rt):
        store = DeltaFlatStore(_Warehouse())

        assert store._sql_relation(_GRAPH) == _GRAPH

    def test_a_paginated_read_selects_from_the_live_relation(self, rt):
        warehouse = _Warehouse()

        _store(warehouse).paginated_triples(_GRAPH, ["predicate = 'p'"], 10, 0)

        assert warehouse.reads == [
            f"SELECT subject, predicate, object FROM {_LIVE} WHERE predicate = 'p' "
            "LIMIT 10 OFFSET 0"
        ]

    def test_an_aliasing_read_aliases_the_subquery(self, rt):
        warehouse = _Warehouse()

        _store(warehouse).query_triples_for_analysis(_GRAPH, predicate_filter=["p"])

        assert f"FROM {_LIVE} t WHERE" in warehouse.reads[0]

    def test_a_dump_goes_through_execute_query_unbounded(self, rt):
        timeouts = []

        class _Bounded(_Warehouse):
            sql = SimpleNamespace(
                execute_query=lambda q, statement_timeout_s=None: timeouts.append(q) or []
            )

        warehouse = _Bounded()
        store = _store(warehouse)

        store.query_triples(_DATA)
        store.count_triples(_GRAPH)

        assert warehouse.reads == [
            f"SELECT subject, predicate, object FROM {_LIVE_ASSERTED}",
            f"SELECT COUNT(*) AS cnt FROM {_LIVE}",
        ]
        assert timeouts == [] and store.last_read_route == "query"

    def test_a_dump_refused_by_rt_is_retried_on_the_build_warehouse(self, rt, monkeypatch):
        build = _Warehouse()
        monkeypatch.setattr(_STORE_MODULE, "resolve_build_warehouse_id", lambda d, s: "wh-build")
        monkeypatch.setattr(_STORE_MODULE, "create_databricks_client", lambda *a, **k: build)
        query = _Warehouse(read_error=InfrastructureError("RT guardrail hit"))
        store = _store(query)

        store.query_triples(_GRAPH)

        assert build.reads == query.reads == [f"SELECT subject, predicate, object FROM {_LIVE}"]
        assert store.last_read_route == "build-fallback"

    def test_a_cache_rebuild_reads_the_same_relation_as_the_reads(self, rt, monkeypatch):
        from back.core.graphdb.delta import materialize

        sources = []
        monkeypatch.setattr(
            materialize, "build_adj_ctas_sql", lambda rel, fqn, d: sources.append(rel) or "SELECT 1"
        )
        monkeypatch.setattr(materialize, "build_props_ctas_sql", lambda rel, fqn: "SELECT 1")
        monkeypatch.setattr(materialize, "build_entity_search_ctas_sql", lambda rel, fqn: "SELECT 1")

        _store(_Warehouse()).rebuild_adjacency(_GRAPH)

        assert sources == [_LIVE, _LIVE]


def _read_statements(warehouse, table):
    """Run the store's read templates over *table*; the SQL each one sent."""
    store = _store(warehouse)
    calls = [
        lambda: store.paginated_triples(table, ["predicate = 'p'"], 10, 0),
        lambda: store.paginated_count(table, []),
        lambda: store.query_triples(table),
        lambda: store.count_triples(table),
        lambda: store.query_triples_for_analysis(
            table, class_filter=["http://x#C"], predicate_filter=["p"]
        ),
        lambda: store.get_aggregate_stats(table),
        lambda: store.get_type_distribution(table),
        lambda: store.get_predicate_distribution(table),
        lambda: store.find_subjects_by_type(table, "http://x#C", limit=5, offset=0),
        lambda: store.resolve_subject_by_id(table, "http://x#C", "1"),
        lambda: store.get_entity_metadata(table, ["http://x/1"]),
        lambda: store.get_triples_for_subjects(table, ["http://x/1"]),
        lambda: store.get_predicates_for_type(table, "http://x#C"),
        lambda: store.bfs_traversal(table, " WHERE subject = 'x'", depth=2),
        lambda: store.find_seed_subjects(table, entity_type="http://x#C", value="a"),
        lambda: store.find_subjects_by_patterns(table, ["%x%"]),
        lambda: store.expand_entity_neighbors(table, {"http://x/1"}),
        lambda: store.transitive_closure(table, "http://x#p"),
        lambda: store.symmetric_expand(table, "http://x#p"),
        lambda: store.expand_and_fetch_subgraph(table, ["http://x/1"], 2, 10, 100),
    ]
    for call in calls:
        call()
    return warehouse.reads


class TestReadTemplatesParse:
    """Every read template is valid SQL with either live relation in it (sqlglot, databricks)."""

    @pytest.mark.parametrize(("table", "relation"), [(_GRAPH, _LIVE), (_DATA, _LIVE_ASSERTED)])
    @pytest.mark.parametrize("companions", [False, True])
    def test_the_read_templates_parse(self, rt, table, relation, companions):
        statements = _read_statements(_Warehouse(companions=companions), table)

        assert any(relation in sql for sql in statements)
        for sql in statements:
            assert len(sqlglot.parse(sql, read="databricks")) == 1, sql


class TestReported:
    """``read_relation`` in the incremental status and the readiness preflight."""

    def test_the_status_reports_the_live_slots(self, rt, monkeypatch):
        from back.objects.digitaltwin.build import TwinIncrementalStatus

        for section in ("relation", "slot_pipeline", "lakebase_sync", "last_build"):
            monkeypatch.setattr(TwinIncrementalStatus, section, staticmethod(lambda *a, **k: {}))

        report = TwinIncrementalStatus.report(_domain(), SimpleNamespace(), _Warehouse())

        assert report["read_relation"] == GraphReadRelation.SLOTS_LIVE

    def test_a_full_domain_s_status_reads_the_views_without_a_probe(self, rt):
        from back.objects.digitaltwin.build import TwinIncrementalStatus

        warehouse = _Warehouse()
        report = TwinIncrementalStatus.report(_domain("full"), SimpleNamespace(), warehouse)

        assert report["read_relation"] == GraphReadRelation.GRAPH and warehouse.queries == []

    def test_the_readiness_preflight_reports_it(self, rt, monkeypatch):
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

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), _Warehouse())

        assert report["read_relation"] == GraphReadRelation.SLOTS_LIVE
