"""Materialization facts persisted per build run, and the builds that read them back.

* **Persisted trace.** ``build_runs.stats["materialization"]`` carries the ten
  flat trace keys plus ``data_table_version``, ``fingerprint``, ``strategy``,
  ``apply_mode`` and ``apply_fallback_reason``; a later build finds its base
  run through ``TwinBuildRunHistory.previous_materialization_stats``.
* **Net change apply.** An incremental Neo4j / app-managed Lakebase build
  applies only the net changes of the ``…_data`` change feed since the base
  run's version (deletes first), unless there is no base, the change feed is
  unreadable, the changes exceed 10 % of the graph or the store cannot drop
  its inferences: then it streams the whole graph as before, with the reason.
* **MERGE fallback.** An ``incremental`` domain that cannot run serverless
  pipelines keeps ``…_data`` as a MERGE-maintained table (effective mode
  ``incremental``, strategy ``merge``) and skips the build entirely when the
  sources and the mapping are unchanged since the base run.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb import ModeDecision
from back.objects.digitaltwin.build import (
    TwinBuildRunHistory,
    TwinMergeFallback,
    TwinNetChangeApply,
)
from back.objects.registry.RegistryService import RegistryService
from tests.units.objects import test_build_pipeline_modes as _modes
from tests.units.objects.test_build_pipeline_materialize import _DATA, _VIEW, _bare_pipeline

pytestmark = pytest.mark.unit

_TRACE_KEYS = {
    "triples_source",
    "inferred_reset",
    "materialization_mode_requested",
    "materialization_mode_effective",
    "mode_fallback_reason",
    "refresh_technique",
    "slot_pipeline_id",
    "slots_table",
    "sync_source",
    "sync_mode_effective",
}
_FACT_KEYS = {
    "data_table_version",
    "data_table_id",
    "fingerprint",
    "strategy",
    "apply_mode",
    "apply_fallback_reason",
}


@pytest.fixture(autouse=True)
def _no_databricks_sdk(monkeypatch):
    """Keep every build here off the Databricks SDK (shape of the compute-once guard).

    The slot pipeline runner is the scripted fake from the modes tests, and the
    SDK constructor records any attempt so the teardown can fail loudly
    (``SlotPipelineRunner._w`` would swallow a raising guard).
    """
    _modes._FakeRunner.script = _modes._Script([])
    monkeypatch.setattr("back.core.streaming.SlotPipelineRunner", _modes._FakeRunner)
    attempts: list = []
    monkeypatch.setattr(
        "databricks.sdk.WorkspaceClient",
        lambda *a, **kw: attempts.append((a, kw)) or MagicMock(),
    )
    yield
    assert attempts == [], "unit tests must not build a WorkspaceClient"


@pytest.fixture(autouse=True)
def registry(monkeypatch):
    """The registry every build reads and writes: no Lakebase connection, ever."""
    reg = MagicMock()
    reg.load_build_runs.return_value = []
    monkeypatch.setattr(RegistryService, "from_context", staticmethod(lambda *a, **kw: reg))
    return reg


def _snap() -> SimpleNamespace:
    return SimpleNamespace(current_version=3, ontology={}, assignment={})


def _entry(registry, pipe) -> dict:
    """Run *pipe* (real count + stream, real trace writer); return the entry it persisted."""
    del pipe._record_build_run
    del pipe._apply_full_rebuild
    pipe.domain_snap = _snap()
    pipe.source_client = _modes._Warehouse([])
    pipe.store.bulk_load_into_sync = MagicMock(return_value=5)
    pipe.run()
    return registry.record_build_run.call_args_list[0][0][1]


def _run_record(*, status="success", apply_mode="full_stream", version=5, **identity) -> dict:
    """A persisted build run of the bare pipeline's store (``G_V3`` on app-managed Lakebase)."""
    run = {
        "status": status,
        "graph_engine": "lakebase",
        "sync_mode": "app_managed",
        "graph_name": "G_V3",
        "triple_count": 100,
        "stats": {
            "materialization": {
                "apply_mode": apply_mode,
                "data_table_version": version,
                "fingerprint": f"fp-{version}",
                "strategy": "mv",
            }
        },
    }
    run.update(identity)
    return run


def _previous(registry, runs) -> dict:
    registry.load_build_runs.return_value = runs
    pipe = _bare_pipeline()
    pipe.domain_snap = _snap()
    return TwinBuildRunHistory.previous_materialization_stats(pipe)


class TestBuildRunStats:
    def test_the_cockpit_counts_stay_next_to_the_materialization_facts(self):
        pipe = SimpleNamespace(
            view_table=_VIEW,
            graph_name="G_V3",
            domain_snap=SimpleNamespace(ontology={"classes": [{}, {}]}, assignment={}),
        )

        stats = TwinBuildRunHistory.build_run_stats(pipe)

        assert stats["ontology"]["classes"] == 2
        assert set(stats["materialization"]) == _TRACE_KEYS | _FACT_KEYS

    def test_a_pipeline_that_did_nothing_reports_empty_facts(self):
        facts = TwinBuildRunHistory.build_run_stats(SimpleNamespace(view_table=_VIEW))[
            "materialization"
        ]

        assert {key: facts[key] for key in _FACT_KEYS} == {
            "data_table_version": None,
            "data_table_id": "",
            "fingerprint": "",
            "strategy": "",
            "apply_mode": "",
            "apply_fallback_reason": "",
        }

    @pytest.mark.parametrize(
        ("attrs", "strategy"),
        [
            ({"data_table": _DATA, "data_relation_mode": "table"}, "ctas"),
            ({"data_table": _DATA, "data_relation_mode": "materialized_view"}, "mv"),
            ({"data_table": _DATA, "data_relation_mode": "view"}, ""),
            ({"data_table": _DATA, "data_relation_mode": "table", "strategy": "merge"}, "merge"),
            ({"data_table": "", "data_relation_mode": "table"}, ""),
        ],
        ids=["ctas", "mv", "pass-through", "merge", "nothing-built"],
    )
    def test_the_strategy_says_how_the_data_relation_was_built(self, attrs, strategy):
        pipe = SimpleNamespace(view_table=_VIEW, **attrs)

        facts = TwinBuildRunHistory.build_run_stats(pipe)["materialization"]

        assert facts["strategy"] == strategy

    @pytest.mark.parametrize("value", [True, "7", 7.0, MagicMock()])
    def test_a_version_that_is_not_an_integer_is_dropped(self, value):
        pipe = SimpleNamespace(view_table=_VIEW, data_table_version=value)

        facts = TwinBuildRunHistory.build_run_stats(pipe)["materialization"]

        assert facts["data_table_version"] is None


class TestPersistedEntry:
    def test_a_lakebase_full_build_persists_a_full_stream_over_a_ctas(self, registry):
        entry = _entry(registry, _bare_pipeline())

        facts = entry["stats"]["materialization"]
        assert (facts["strategy"], facts["apply_mode"], facts["data_table_version"]) == (
            "ctas",
            "full_stream",
            None,
        )
        assert facts["triples_source"] == entry["triples_source"] == _DATA

    def test_a_databricks_build_persists_no_store_apply(self, registry):
        entry = _entry(registry, _bare_pipeline(info={"graph_backend": "databricks"}))

        assert entry["stats"]["materialization"]["apply_mode"] == "none"

    def test_the_store_identity_and_the_flat_trace_keys_stay_top_level(self, registry):
        entry = _entry(registry, _bare_pipeline())

        assert (entry["graph_engine"], entry["sync_mode"], entry["graph_name"]) == (
            "lakebase",
            "app_managed",
            "G_V3",
        )
        assert _TRACE_KEYS <= set(entry)
        assert entry["version"] == "3"


class TestBuildRunKey:
    def test_the_uc_folder_and_the_snapshot_version(self):
        pipe = _bare_pipeline()
        pipe.domain_snap = _snap()

        assert TwinBuildRunHistory.build_run_key(pipe) == ("dom", "3")

    def test_without_a_uc_folder_the_sanitised_name(self):
        pipe = _bare_pipeline()
        pipe.domain.uc_domain_folder = ""
        pipe.domain_name = "My Domain"
        pipe.domain_snap = SimpleNamespace(current_version=None)
        pipe.domain.current_version = None

        folder, version = TwinBuildRunHistory.build_run_key(pipe)

        assert folder and folder != "My Domain"
        assert version == ""


class TestPreviousMaterializationStats:
    def test_the_newest_success_on_the_same_store_is_the_base(self, registry):
        facts = _previous(registry, [_run_record(version=9), _run_record(version=4)])

        assert facts["data_table_version"] == 9
        registry.load_build_runs.assert_called_once_with("dom", version="3", limit=50)

    @pytest.mark.parametrize("apply_mode", ["net_changes", "full_stream", "lakeflow_sync"])
    def test_every_apply_that_synced_the_store_is_a_base(self, registry, apply_mode):
        assert _previous(registry, [_run_record(apply_mode=apply_mode)])["apply_mode"] == apply_mode

    @pytest.mark.parametrize(
        "identity",
        [{"graph_engine": "neo4j"}, {"graph_name": "G_V2"}],
        ids=["engine", "graph"],
    )
    def test_a_success_on_another_store_is_passed_over(self, registry, identity):
        facts = _previous(registry, [_run_record(version=9, **identity), _run_record(version=4)])

        assert facts["data_table_version"] == 4

    def test_the_same_graph_synced_another_way_leaves_no_base(self, registry):
        """A managed-synced build of this graph rewrote ``<graph>_sync``."""
        runs = [_run_record(version=9, sync_mode="managed_synced"), _run_record(version=4)]

        assert _previous(registry, runs) == {}

    def test_a_skipped_delta_engine_build_is_a_base_there(self, registry):
        """On the Delta engine ``…_data`` is the graph: a build that skipped the store is a base."""
        registry.load_build_runs.return_value = [_run_record(version=9, apply_mode="none")]
        pipe = _bare_pipeline(info={"graph_backend": "databricks"})
        pipe.domain_snap = _snap()

        facts = TwinBuildRunHistory.previous_materialization_stats(pipe)

        assert facts["apply_mode"] == "none"

    @pytest.mark.parametrize("apply_mode", ["none", ""], ids=["skipped", "empty"])
    def test_a_success_that_left_the_store_alone_is_passed_over(self, registry, apply_mode):
        facts = _previous(
            registry, [_run_record(version=9, apply_mode=apply_mode), _run_record(version=4)]
        )

        assert facts["data_table_version"] == 4

    def test_a_failure_before_the_apply_is_passed_over(self, registry):
        facts = _previous(
            registry, [_run_record(status="error", apply_mode=""), _run_record(version=4)]
        )

        assert facts["data_table_version"] == 4

    @pytest.mark.parametrize("status", ["error", "cancelled"])
    def test_a_failure_during_the_apply_leaves_no_base(self, registry, status):
        runs = [_run_record(status=status, apply_mode="full_stream"), _run_record(version=4)]

        assert _previous(registry, runs) == {}

    def test_a_run_without_materialization_facts_leaves_no_base(self, registry):
        scheduled = {"status": "success", "graph_engine": "", "sync_mode": "", "stats": {}}

        assert _previous(registry, [scheduled, _run_record(version=4)]) == {}

    def test_no_runs_no_base(self, registry):
        assert _previous(registry, []) == {}

    def test_a_registry_failure_reads_as_no_base(self, registry):
        registry.load_build_runs.side_effect = RuntimeError("registry down")
        pipe = _bare_pipeline()
        pipe.domain_snap = _snap()

        with patch("back.objects.digitaltwin.build.TwinBuildRunHistory.logger") as logger:
            assert TwinBuildRunHistory.previous_materialization_stats(pipe) == {}
        logger.debug.assert_called_once()

    def test_an_unexpected_answer_reads_as_no_base(self, registry):
        assert _previous(registry, MagicMock()) == {}

    def test_an_unknown_version_reads_nothing(self, registry):
        pipe = _bare_pipeline()
        pipe.domain_snap = SimpleNamespace(current_version=None)
        pipe.domain.current_version = None

        assert TwinBuildRunHistory.previous_materialization_stats(pipe) == {}
        registry.load_build_runs.assert_not_called()


# ----------------------------------------------------------------------
# Net change apply
# ----------------------------------------------------------------------


class _CDFWarehouse(_modes._Warehouse):
    """The modes warehouse, plus ``DESCRIBE HISTORY`` (*version*) and the change feed.

    The change-feed COUNT answers ``len(changes)`` and the stream yields *changes*;
    *cdf_error* is raised by any change-feed read.
    """

    def __init__(
        self,
        log,
        *,
        version=9,
        data_version=None,
        table_id="",
        changes=(),
        cdf_error=None,
        stream_error=None,
        count=100,
        data_exists=False,
        **kw,
    ):
        super().__init__(log, count=count, **kw)
        self._version = version
        self._data_version = version if data_version is None else data_version
        self._table_id = table_id
        self._changes = list(changes)
        self._cdf_error = cdf_error
        self._stream_error = stream_error
        self._data_exists = data_exists

    def execute_query(self, sql: str):
        if sql.startswith("SHOW TABLES") and "_data'" in sql:
            self._record(sql)
            return [{"tableName": "triplestore_dom_v3_data"}] if self._data_exists else []
        if sql.startswith("DESCRIBE DETAIL"):
            self._record(sql)
            return [{"id": self._table_id}] if self._table_id else []
        if sql.startswith("DESCRIBE HISTORY"):
            self._record(sql)
            return [{"version": self._data_version if _DATA in sql else self._version}]
        if "table_changes(" in sql:
            self._record(sql)
            if self._cdf_error is not None:
                raise self._cdf_error
            return [{"cnt": len(self._changes)}]
        return super().execute_query(sql)

    def iter_rows(self, sql: str, batch_size: int = 5000):
        if "table_changes(" not in sql:
            return super().iter_rows(sql, batch_size)
        self._record(sql)
        return self._stream()

    def _stream(self):
        """The changes, then *stream_error* (a feed lost half-way through)."""
        yield from self._changes[:1] if self._stream_error else self._changes
        if self._stream_error is not None:
            raise self._stream_error


def _change(subject: str, op: str) -> dict:
    return {"subject": subject, "predicate": "http://ex.org/p", "object": "o", "op": op}


_TWO_CHANGES = [_change("http://ex.org/old", "delete"), _change("http://ex.org/new", "insert")]


def _store_writes(store, *names) -> list:
    """Make each store write method consume its triples into a shared, ordered record."""
    writes: list = []

    def _writer(name):
        def _write(graph, triples, **kwargs):
            rows = list(triples)
            writes.append((name, graph, rows, kwargs.get("batch_size")))
            return len(rows)

        return _write

    for name in names:
        getattr(store, name).side_effect = _writer(name)
    return writes


def _net_pipeline(registry, backend="lakebase", *, base_version=5, purge=True):
    """An incremental build whose previous run left the store at *base_version*."""
    pipe = _modes._pipeline("incremental", backend=backend)
    # The defaults ``_BuildPipeline.__init__`` sets (the bare pipeline skips it).
    pipe.data_table_version, pipe.apply_mode, pipe.apply_fallback_reason = None, "", ""
    pipe.data_table_id = ""
    del pipe._complete_task  # the real completion message and build mode
    pipe._graph_engine = "neo4j" if backend == "neo4j" else "lakebase"
    pipe.store.supports_materialized_inference_purge = purge
    if base_version is not None:
        registry.load_build_runs.return_value = [
            _run_record(version=base_version, graph_engine=pipe._graph_engine)
        ]
    return pipe


def _streamed(log) -> bool:
    return ("apply_full_rebuild",) in log


class TestNetChangeAppliesTo:
    @staticmethod
    def _pipe(**attrs):
        pipe = _bare_pipeline(info={"graph_backend": attrs.pop("backend", "lakebase")})
        pipe.mode_decision = attrs.pop(
            "decision", ModeDecision("incremental", "incremental", "")
        )
        pipe.data_table = attrs.pop("data_table", _DATA)
        pipe.data_relation_mode = attrs.pop("data_relation_mode", "materialized_view")
        for key, value in attrs.items():
            setattr(pipe, key, value)
        return pipe

    @pytest.mark.parametrize("backend", ["lakebase", "neo4j"])
    def test_an_incremental_build_on_an_app_written_store(self, backend):
        assert TwinNetChangeApply.applies(self._pipe(backend=backend)) is True

    def test_a_merge_maintained_table_also_has_a_change_feed(self):
        pipe = self._pipe(data_relation_mode="table", strategy="merge")

        assert TwinNetChangeApply.applies(pipe) is True

    @pytest.mark.parametrize("relation_mode", ["table", "view"], ids=["ctas", "pass-through"])
    def test_not_for_a_relation_without_an_incremental_change_feed(self, relation_mode):
        """A CTAS table is replaced wholesale; a pass-through view has no feed at all."""
        assert TwinNetChangeApply.applies(self._pipe(data_relation_mode=relation_mode)) is False

    @pytest.mark.parametrize(
        "attrs",
        [
            {"decision": ModeDecision("incremental", "full", "serverless_unavailable")},
            {"decision": None},
            {"data_table": ""},
            {"backend": "databricks"},
            {"_is_lakebase_synced": True},
        ],
        ids=["full", "no-decision", "no-data-table", "delta-engine", "managed-synced"],
    )
    def test_not_for(self, attrs):
        assert TwinNetChangeApply.applies(self._pipe(**attrs)) is False


class TestLakebaseNetApply:
    def test_deletes_then_inserts_reach_the_sync_table_only(self, registry):
        pipe = _net_pipeline(registry)
        writes = _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert [(name, graph) for name, graph, *_ in writes] == [
            ("delete_from_sync_iter", "G_V3"),
            ("bulk_load_into_sync", "G_V3"),
        ]
        assert writes[0][2] == [
            {"subject": "http://ex.org/old", "predicate": "http://ex.org/p", "object": "o"}
        ]
        assert not _streamed(log)
        for companion_write in ("delete_triples", "bulk_delete_iter", "bulk_insert_iter"):
            getattr(pipe.store, companion_write).assert_not_called()
        pipe.store.drop_table.assert_not_called()

    def test_the_feed_is_read_after_the_base_version_up_to_the_recorded_one(self, registry):
        pipe = _net_pipeline(registry, base_version=5)

        _, _, warehouse = _modes._run(
            pipe, warehouse=_CDFWarehouse([], version=9, changes=_TWO_CHANGES)
        )

        feed_reads = [s for s in warehouse.calls if "table_changes(" in s]
        assert len(feed_reads) == 2  # the count and the stream read the same window
        assert all(f"table_changes('{_DATA}', 6, 9)" in s for s in feed_reads)

    def test_the_facts_are_recorded(self, registry):
        pipe = _net_pipeline(registry)

        _modes._run(pipe, warehouse=_CDFWarehouse([], version=9, changes=_TWO_CHANGES))

        assert (pipe.apply_mode, pipe.apply_fallback_reason, pipe.data_table_version) == (
            "net_changes",
            "",
            9,
        )
        assert pipe.triple_count == 100
        assert pipe.mode_decision == ModeDecision("incremental", "incremental", "")

    def test_inferences_are_purged_after_the_writes(self, registry):
        pipe = _net_pipeline(registry)
        _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")

        _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        names = [call[0] for call in pipe.store.mock_calls]
        assert names.index("bulk_load_into_sync") < names.index("purge_materialized_triples")
        pipe.store.purge_materialized_triples.assert_called_once_with("G_V3")
        pipe.store.optimize_table.assert_called_once_with("G_V3")

    def test_the_purge_is_the_only_graph_cache_rebuild(self, registry):
        """``purge_materialized_triples`` rebuilds the cache; the build does not do it again."""
        pipe = _net_pipeline(registry)
        _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")
        pipe.store.sql_flavor.return_value = "postgres"

        with patch("back.core.graphdb.search_cache.rebuild_graph_cache_if_enabled") as rebuild:
            _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        rebuild.assert_not_called()

    def test_the_task_reports_a_net_change_apply(self, registry):
        pipe = _net_pipeline(registry)
        _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")

        _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert pipe.tm.messages[0].startswith("Net change apply: 100 triples in ")
        assert pipe.tm.results[0]["build_mode"] == "incremental"

    def test_no_new_version_reads_no_feed_and_writes_nothing(self, registry):
        pipe = _net_pipeline(registry, base_version=9)

        log, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([], version=9))

        assert not any("table_changes(" in s for s in warehouse.calls)
        pipe.store.delete_from_sync_iter.assert_not_called()
        pipe.store.bulk_load_into_sync.assert_not_called()
        assert not _streamed(log)
        assert pipe.apply_mode == "net_changes"

    def test_the_progress_names_the_net_changes(self, registry):
        pipe = _net_pipeline(registry)
        _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")

        _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert any("net change" in step.lower() for step in pipe.tm.steps)

    def test_a_store_without_an_existence_probe_skips_that_guard(self, registry):
        pipe = _net_pipeline(registry)
        del pipe.store.table_exists

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert not _streamed(log)


class TestNeo4jNetApply:
    def test_without_separable_inferences_the_graph_is_streamed(self, registry):
        pipe = _net_pipeline(registry, backend="neo4j", purge=False)

        log, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert _streamed(log)
        assert pipe.apply_fallback_reason == "inferred_not_separable"
        assert not any("table_changes(" in s for s in warehouse.calls)
        pipe.store.delete_triples.assert_not_called()

    def test_a_store_that_purges_inferences_gets_deletes_then_inserts_in_lists(self, registry):
        pipe = _net_pipeline(registry, backend="neo4j", purge=True)
        writes = _store_writes(pipe.store, "delete_triples", "insert_triples")

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert [(name, size) for name, _, _, size in writes] == [
            ("delete_triples", 500),
            ("insert_triples", 500),
        ]
        assert not _streamed(log)
        pipe.store.purge_materialized_triples.assert_called_once_with("G_V3")


class TestNetApplyFallsBackToTheFullStream:
    def _fallback(self, registry, warehouse, **pipe_kw):
        pipe = _net_pipeline(registry, **pipe_kw)
        log, _, _ = _modes._run(pipe, warehouse=warehouse)
        assert _streamed(log)
        pipe.store.delete_from_sync_iter.assert_not_called()
        pipe.store.bulk_load_into_sync.assert_not_called()
        assert pipe.apply_mode == "full_stream"
        assert pipe.mode_decision.reason == ""  # the mode decision is not the apply's
        return pipe

    def test_a_changeset_above_ten_percent(self, registry):
        changes = [_change(f"http://ex.org/{i}", "insert") for i in range(11)]

        pipe = self._fallback(registry, _CDFWarehouse([], changes=changes, count=100))

        assert pipe.apply_fallback_reason == "changeset_too_large"
        assert pipe.data_table_version == 9  # the full stream reads that version

    def test_ten_percent_still_applies_net_changes(self, registry):
        pipe = _net_pipeline(registry)
        changes = [_change(f"http://ex.org/{i}", "insert") for i in range(10)]

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=changes, count=100))

        assert not _streamed(log)

    def test_no_base_run(self, registry):
        pipe = self._fallback(registry, _CDFWarehouse([]), base_version=None)

        assert pipe.apply_fallback_reason == "no_previous_version"

    def test_a_store_that_lost_its_graph(self, registry):
        pipe = _net_pipeline(registry)
        pipe.store.table_exists.return_value = False

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert _streamed(log)
        assert pipe.apply_fallback_reason == "no_previous_version"

    def test_an_unreadable_change_feed(self, registry):
        warehouse = _CDFWarehouse([], cdf_error=RuntimeError("change data feed not enabled"))

        pipe = self._fallback(registry, warehouse)

        assert pipe.apply_fallback_reason == "cdf_unavailable"

    def test_an_unreadable_history(self, registry):
        pipe = self._fallback(registry, _CDFWarehouse([], fail_on="DESCRIBE HISTORY"))

        assert pipe.apply_fallback_reason == "cdf_unavailable"
        assert pipe.data_table_version is None

    def test_a_base_newer_than_the_relation(self, registry):
        pipe = self._fallback(registry, _CDFWarehouse([], version=3), base_version=5)

        assert pipe.apply_fallback_reason == "cdf_unavailable"

    def test_a_base_from_another_relation_kind(self, registry):
        """MV -> MERGE table (or back) restarts the history: equal versions mean nothing."""
        pipe = _net_pipeline(registry)
        registry.load_build_runs.return_value[0]["stats"]["materialization"]["strategy"] = "merge"

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], version=5))

        assert _streamed(log)
        assert pipe.apply_fallback_reason == "no_previous_version"

    def test_a_base_on_another_table_id(self, registry):
        pipe = _net_pipeline(registry)
        registry.load_build_runs.return_value[0]["stats"]["materialization"]["data_table_id"] = "a"

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], table_id="b"))

        assert _streamed(log)
        assert pipe.apply_fallback_reason == "no_previous_version"

    def test_the_same_table_id_applies_and_is_recorded(self, registry):
        pipe = _net_pipeline(registry)
        registry.load_build_runs.return_value[0]["stats"]["materialization"]["data_table_id"] = "a"
        _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], table_id="a", changes=_TWO_CHANGES))

        assert not _streamed(log)
        assert pipe.data_table_id == "a"

    def test_a_change_feed_lost_half_way_streams_the_whole_graph(self, registry):
        pipe = _net_pipeline(registry)
        writes = _store_writes(pipe.store, "delete_from_sync_iter", "bulk_load_into_sync")
        warehouse = _CDFWarehouse(
            [], changes=_TWO_CHANGES, stream_error=RuntimeError("connection reset")
        )

        log, _, _ = _modes._run(pipe, warehouse=warehouse)

        assert _streamed(log)
        assert pipe.apply_fallback_reason == "cdf_unavailable"
        assert pipe.apply_mode == "full_stream"
        assert pipe.tm.failed is False
        assert [name for name, *_ in writes] == []  # the half-read batch was never written

    def test_the_fallback_reports_a_full_rebuild(self, registry):
        pipe = self._fallback(registry, _CDFWarehouse([]), base_version=None)

        assert pipe.tm.results[0]["build_mode"] == "full"
        assert pipe.tm.messages[0].startswith("Full rebuild: ")


class TestNetApplyFailures:
    def test_a_failed_write_fails_the_build(self, registry):
        pipe = _net_pipeline(registry)
        pipe.store.delete_from_sync_iter.side_effect = InfrastructureError("Lakebase down")

        log, _, _ = _modes._run(pipe, warehouse=_CDFWarehouse([], changes=_TWO_CHANGES))

        assert pipe.tm.failed is True
        assert not _streamed(log)
        assert pipe.apply_mode == "net_changes"  # so the next build streams the whole graph


class TestNetApplyIsPersisted:
    def test_the_entry_records_the_net_change_apply(self, registry):
        pipe = _net_pipeline(registry)
        del pipe._record_build_run

        _modes._run(pipe, warehouse=_CDFWarehouse([], version=9, changes=_TWO_CHANGES))

        entry = registry.record_build_run.call_args_list[0][0][1]
        facts = entry["stats"]["materialization"]
        assert (facts["apply_mode"], facts["data_table_version"], facts["strategy"]) == (
            "net_changes",
            9,
            "mv",
        )
        assert entry["materialization_mode_effective"] == "incremental"


# ----------------------------------------------------------------------
# MERGE fallback
# ----------------------------------------------------------------------

_SKIPPED = "no source changes (skipped)"


def _merge_pipeline(backend="databricks", mode="incremental", **kw):
    pipe = _modes._pipeline(mode, backend=backend, **kw)
    pipe.settings.serverless_pipelines_enabled = False
    # The defaults ``_BuildPipeline.__init__`` sets (the bare pipeline skips it).
    pipe.data_table_version, pipe.fingerprint, pipe.strategy = None, "", ""
    pipe.apply_mode, pipe.apply_fallback_reason, pipe.mode_fallback_detail = "", "", ""
    pipe.data_table_id = ""
    del pipe._complete_task  # the real completion message and build mode
    pipe.store.supports_materialized_inference_purge = False
    return pipe


def _merged(warehouse) -> bool:
    return any(s.startswith(f"MERGE INTO {_DATA}") for s in warehouse.calls)


def _ctas(warehouse) -> bool:
    return any(s.startswith(f"CREATE OR REPLACE TABLE {_DATA} ") for s in warehouse.calls)


class TestMergeAppliesTo:
    @staticmethod
    def _pipe(decision, detail=""):
        return SimpleNamespace(mode_decision=decision, mode_fallback_detail=detail)

    def test_an_incremental_domain_without_serverless_pipelines(self):
        decision = ModeDecision("incremental", "full", "serverless_unavailable")

        assert TwinMergeFallback.applies(self._pipe(decision)) is True

    @pytest.mark.parametrize(
        "detail",
        ["Serverless compute is not enabled", "Lakeflow pipelines API call failed (create)"],
    )
    def test_a_slot_pipeline_infrastructure_failure(self, detail):
        decision = ModeDecision("incremental", "full", "incremental_failed:InfrastructureError")

        assert TwinMergeFallback.applies(self._pipe(decision, detail)) is True

    @pytest.mark.parametrize(
        ("decision", "detail"),
        [
            (ModeDecision("incremental", "full", "incremental_failed:InfrastructureError"), "quota"),
            (ModeDecision("incremental", "full", "incremental_failed:ValidationError"), "pipeline"),
            (ModeDecision("incremental", "full", "row_tracking_missing"), ""),
            (ModeDecision("microbatch", "full", "serverless_unavailable"), ""),
            (ModeDecision("full", "full", ""), ""),
            (ModeDecision("incremental", "incremental", ""), ""),
            (None, ""),
        ],
        ids=[
            "other-infra-failure",
            "validation-failure",
            "untracked-sources",
            "microbatch",
            "full",
            "running-incrementally",
            "no-decision",
        ],
    )
    def test_not_for(self, decision, detail):
        assert TwinMergeFallback.applies(self._pipe(decision, detail)) is False


class TestMergePromotes:
    """The predicate the main build and the scheduled tick share."""

    def test_a_serverless_unavailable_incremental_request_is_promoted(self):
        decision = ModeDecision("incremental", "full", "serverless_unavailable")

        assert TwinMergeFallback.promotes(decision) is True

    def test_a_runtime_failure_needs_its_detail(self):
        decision = ModeDecision("incremental", "full", "incremental_failed:InfrastructureError")

        assert TwinMergeFallback.promotes(decision) is False
        assert TwinMergeFallback.promotes(decision, "Slot pipeline update FAILED") is True

    @pytest.mark.parametrize("decision", [None, "incremental", ModeDecision("full", "full", "")])
    def test_anything_else_is_not(self, decision):
        assert TwinMergeFallback.promotes(decision) is False


class TestMergeInsteadOfCtas:
    def test_without_serverless_the_data_table_is_merged_not_replaced(self):
        pipe = _merge_pipeline()

        log, _, warehouse = _modes._run(pipe)

        ensure = next(i for i, s in enumerate(warehouse.calls) if s.startswith(
            f"CREATE TABLE IF NOT EXISTS {_DATA} "
        ))
        merge = next(i for i, s in enumerate(warehouse.calls) if s.startswith(f"MERGE INTO {_DATA}"))
        assert ensure < merge
        assert not _ctas(warehouse)
        assert ("apply_full_rebuild",) in log
        assert pipe.tm.failed is False

    def test_the_build_reports_incremental_by_merge(self):
        pipe = _merge_pipeline()

        _modes._run(pipe)

        assert pipe.mode_decision == ModeDecision(
            "incremental", "incremental", "serverless_unavailable"
        )
        assert (pipe.strategy, pipe.data_table, pipe.data_relation_mode) == (
            "merge",
            _DATA,
            "table",
        )

    def test_the_merge_build_syncs_on_the_incremental_schedule(self):
        """The scheduled tick registers TRIGGERED for this domain (TwinMergeFallback.promotes)."""
        pipe = _merge_pipeline(backend="lakebase")
        pipe.store.sync_table_mode = "triggered"

        _modes._run(pipe)

        assert pipe.sync_schedule_mode == "incremental"
        assert _modes.TwinLakebaseSync.sync_plan(pipe).sync_mode == "triggered"

    def test_a_slot_pipeline_failure_mentioning_serverless_also_merges(self):
        pipe = _merge_pipeline()
        pipe.settings.serverless_pipelines_enabled = True
        error = InfrastructureError("Serverless compute is not enabled in this workspace")

        _, _, warehouse = _modes._run(pipe, fail_on="ensure", error=error)

        assert _merged(warehouse) and not _ctas(warehouse)
        assert pipe.mode_decision == ModeDecision(
            "incremental", "incremental", "incremental_failed:InfrastructureError"
        )

    def test_another_infrastructure_failure_keeps_the_ctas(self):
        pipe = _merge_pipeline()
        pipe.settings.serverless_pipelines_enabled = True

        _, _, warehouse = _modes._run(pipe, fail_on="ensure", error=InfrastructureError("quota"))

        assert _ctas(warehouse) and not _merged(warehouse)
        assert pipe.mode_decision.effective == "full"

    def test_microbatch_keeps_the_ctas(self):
        pipe = _merge_pipeline(mode="microbatch")

        _, _, warehouse = _modes._run(pipe)

        assert _ctas(warehouse) and not _merged(warehouse)

    def test_a_lakeflow_synced_domain_merges_the_object_hash_too(self):
        pipe = _merge_pipeline(backend="lakebase")
        pipe._is_lakebase_synced = True

        _, _, warehouse = _modes._run(pipe)

        merge = next(s for s in warehouse.calls if s.startswith(f"MERGE INTO {_DATA}"))
        assert "object_hash" in merge

    def test_a_failed_merge_falls_back_to_the_ctas(self):
        pipe = _merge_pipeline()

        with patch("back.objects.digitaltwin.build.TwinMergeFallback.logger") as logger:
            _, _, warehouse = _modes._run(pipe, warehouse=_modes._Warehouse([], fail_on="MERGE INTO"))

        assert _ctas(warehouse)
        assert pipe.mode_decision == ModeDecision("incremental", "full", "serverless_unavailable")
        assert pipe.strategy == ""
        assert pipe.tm.failed is False
        logger.warning.assert_called_once()


class TestMergeFingerprint:
    """Only an all-row-local domain can be fingerprinted: batch SQL sources are not versioned."""

    @staticmethod
    def _row_local_pipeline():
        return _merge_pipeline(backend="lakebase", entities={_modes._CUSTOMER: _modes._row_local()})

    def test_the_fingerprint_covers_the_row_local_source_versions(self, registry):
        first, second = self._row_local_pipeline(), self._row_local_pipeline()

        _modes._run(first, warehouse=_CDFWarehouse([], version=4))
        _modes._run(second, warehouse=_CDFWarehouse([], version=5))

        assert len(first.fingerprint) == 64
        assert first.fingerprint != second.fingerprint

    def test_batch_mappings_are_never_fingerprinted(self, registry):
        pipe = _merge_pipeline()

        _, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([]))

        assert pipe.fingerprint == ""
        assert _merged(warehouse)
        registry.load_build_runs.assert_not_called()


class TestMergeSkipsAnUnchangedBuild:
    @staticmethod
    def _pipeline():
        return _merge_pipeline(backend="lakebase", entities={_modes._CUSTOMER: _modes._row_local()})

    def _base(
        self, registry, *, fingerprint=None, strategy="merge", apply_mode="full_stream", version=7
    ):
        """Build once to learn the fingerprint; record it as the base run (``…_data`` at *version*)."""
        if fingerprint is None:
            first = self._pipeline()
            _modes._run(first, warehouse=_CDFWarehouse([], version=4))
            fingerprint = first.fingerprint
        run = _run_record(version=version)
        run["stats"]["materialization"].update(
            fingerprint=fingerprint, strategy=strategy, apply_mode=apply_mode
        )
        registry.load_build_runs.return_value = [run]
        return fingerprint

    def test_unchanged_sources_and_mapping_complete_the_build_as_skipped(self, registry):
        self._base(registry)
        pipe = self._pipeline()

        log, _, warehouse = _modes._run(
            pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True, count=42)
        )

        assert not _merged(warehouse) and not _ctas(warehouse)
        assert ("apply_full_rebuild",) not in log
        assert pipe.tm.failed is False
        assert pipe.tm.messages == [_SKIPPED]
        assert pipe.tm.results[0]["triple_count"] == 42
        assert pipe.tm.results[0]["build_mode"] == "incremental"
        assert pipe._record_build_run.call_args_list[0] == (("success",), {"message": _SKIPPED})

    def test_a_skipped_build_records_merge_and_no_apply(self, registry):
        fingerprint = self._base(registry)
        pipe = self._pipeline()

        _modes._run(pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True))

        assert (pipe.strategy, pipe.apply_mode, pipe.fingerprint) == ("merge", "none", fingerprint)
        assert pipe.mode_decision.effective == "incremental"
        assert pipe.triple_count == 100

    def test_the_skip_is_persisted_once_as_a_success(self, registry):
        self._base(registry)
        pipe = self._pipeline()
        del pipe._record_build_run

        _modes._run(pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True))

        assert registry.record_build_run.call_count == 1
        entry = registry.record_build_run.call_args[0][1]
        assert (entry["status"], entry["message"]) == ("success", _SKIPPED)
        assert entry["stats"]["materialization"]["apply_mode"] == "none"

    def test_a_changed_source_version_merges(self, registry):
        self._base(registry)
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([], version=5, data_version=7, data_exists=True))

        assert _merged(warehouse)

    def test_a_missing_data_table_merges(self, registry):
        self._base(registry)
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=False))

        assert _merged(warehouse)

    def test_a_data_table_written_since_the_base_merges(self, registry):
        self._base(registry, version=7)
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(
            pipe, warehouse=_CDFWarehouse([], version=4, data_version=8, data_exists=True)
        )

        assert _merged(warehouse)

    def test_a_store_that_lost_its_graph_merges(self, registry):
        """A store dropped outside a build must never read as "no source changes"."""
        self._base(registry)
        pipe = self._pipeline()
        pipe.store.table_exists.return_value = False

        _, _, warehouse = _modes._run(
            pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True)
        )

        assert _merged(warehouse)
        assert pipe.tm.messages != [_SKIPPED]

    def test_the_delta_engine_skips_after_a_build_that_left_the_store_alone(self, registry):
        """On the Delta engine the store is ``…_data``: apply_mode none is a base and the
        store probe is irrelevant."""
        self._base(registry, apply_mode="none", version=None)
        pipe = _merge_pipeline(entities={_modes._CUSTOMER: _modes._row_local()})
        pipe.store.table_exists.return_value = False

        _, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([], version=4, data_exists=True))

        assert not _merged(warehouse)
        assert pipe.tm.messages == [_SKIPPED]

    def test_a_base_that_was_not_merged_merges(self, registry):
        self._base(registry, strategy="mv")
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True))

        assert _merged(warehouse)


class TestMergeRecordsTheDataVersion:
    """The MERGE build records the version (and table id) its ``…_data`` ended at."""

    def test_a_merge_records_the_data_version_and_table_id(self):
        pipe = _merge_pipeline()

        _modes._run(pipe, warehouse=_CDFWarehouse([], data_version=8, table_id="t-1"))

        assert (pipe.data_table_version, pipe.data_table_id) == (8, "t-1")

    def test_an_unreadable_history_records_no_version(self):
        pipe = _merge_pipeline()

        _modes._run(pipe, warehouse=_CDFWarehouse([], fail_on="DESCRIBE HISTORY"))

        assert pipe.data_table_version is None
        assert pipe.strategy == "merge"


class TestDeltaMergeSkipChecksTheDataVersion:
    """On the Delta engine ``…_data`` is the served graph: a skip needs it unchanged."""

    @staticmethod
    def _pipeline():
        return _merge_pipeline(entities={_modes._CUSTOMER: _modes._row_local()})

    def _base(self, registry, *, version=7, table_id=""):
        first = self._pipeline()
        _modes._run(first, warehouse=_CDFWarehouse([], version=4))
        run = _run_record(version=version)
        run["stats"]["materialization"].update(
            fingerprint=first.fingerprint,
            strategy="merge",
            apply_mode="none",
            data_table_id=table_id,
        )
        registry.load_build_runs.return_value = [run]

    def test_a_data_table_merged_since_the_base_is_merged_again(self, registry):
        """A later failed build merged other content into ``…_data``: never skip over it."""
        self._base(registry, version=7)
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(
            pipe, warehouse=_CDFWarehouse([], version=4, data_version=8, data_exists=True)
        )

        assert _merged(warehouse)

    def test_the_data_table_at_the_base_version_skips(self, registry):
        self._base(registry, version=7)
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(
            pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True)
        )

        assert not _merged(warehouse)
        assert pipe.tm.messages == [_SKIPPED]

    def test_a_data_table_with_another_id_is_merged_again(self, registry):
        self._base(registry, version=7, table_id="t-1")
        pipe = self._pipeline()

        _, _, warehouse = _modes._run(
            pipe,
            warehouse=_CDFWarehouse(
                [], version=4, data_version=7, table_id="t-2", data_exists=True
            ),
        )

        assert _merged(warehouse)

    def test_the_skip_carries_the_base_version_and_id_forward(self, registry):
        self._base(registry, version=7, table_id="t-1")
        pipe = self._pipeline()
        del pipe._record_build_run

        _modes._run(
            pipe,
            warehouse=_CDFWarehouse(
                [], version=4, data_version=7, table_id="t-1", data_exists=True
            ),
        )

        facts = registry.record_build_run.call_args[0][1]["stats"]["materialization"]
        assert (facts["apply_mode"], facts["data_table_version"], facts["data_table_id"]) == (
            "none",
            7,
            "t-1",
        )


class TestMergeSkipStoreProbe:
    def test_a_failing_store_probe_merges_instead_of_failing(self, registry):
        TestMergeSkipsAnUnchangedBuild()._base(registry)
        pipe = TestMergeSkipsAnUnchangedBuild._pipeline()
        pipe.store.table_exists.side_effect = RuntimeError("Lakebase unreachable")

        with patch("back.objects.digitaltwin.build.TwinMergeFallback.logger") as logger:
            _, _, warehouse = _modes._run(
                pipe, warehouse=_CDFWarehouse([], version=4, data_version=7, data_exists=True)
            )

        assert _merged(warehouse)
        assert pipe.tm.failed is False
        logger.warning.assert_called_once()


class TestMergeIsPersisted:
    def test_the_entry_reports_incremental_by_merge(self, registry):
        pipe = _merge_pipeline()
        del pipe._record_build_run

        _modes._run(pipe)

        entry = registry.record_build_run.call_args_list[0][0][1]
        assert entry["materialization_mode_effective"] == "incremental"
        assert entry["mode_fallback_reason"] == "serverless_unavailable"
        assert entry["stats"]["materialization"]["strategy"] == "merge"
