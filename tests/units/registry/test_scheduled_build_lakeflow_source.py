"""The scheduled build syncs the same Lakeflow source the main full build does.

``registry/schedule/build.py`` is a full rebuild: it materializes ``…_data``
(with ``object_hash`` and row tracking, like a Lakeflow-synced main build) and
registers the synced table on it through ``TwinLakebaseSync.sync_plan_for``, so
a scheduled run neither re-registers the main build's table on the VIEW nor
syncs a stale ``…_data``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb import ModeDecision
from back.core.streaming import SlotProjectionCompiler, SourceReadiness
from back.objects.digitaltwin.build import TwinBuildPostMaterialize
from back.objects.registry.schedule import build as scheduled_build

pytestmark = pytest.mark.unit

_VIEW = "cat.sch.triplestore_dom_V3"
_DATA = "cat.sch.triplestore_dom_V3_data"
_SYNCED_UC = "cat.ontobricks_graph.triplestore_dom_v3_sync"


@pytest.fixture(autouse=True)
def _no_registry(monkeypatch):
    """A tick reads no build-run history unless a test scripts one: no registry, ever."""
    from back.objects.digitaltwin.build import TwinBuildRunHistory

    monkeypatch.setattr(TwinBuildRunHistory, "recent_runs_for", staticmethod(lambda d, s: []))


def _domain(mode: str | None = None):
    info = {"name": "dom"}
    if mode:
        info["materialization_mode"] = mode
    return SimpleNamespace(
        delta={"catalog": "cat", "schema": "sch"},
        info=info,
        current_version="3",
    )


def _store(log: list, *, sync_table_mode: str = "triggered"):
    store = MagicMock()
    store.is_synced = True
    store.sync_table_mode = sync_table_mode
    store.sync_timeout_s = 600
    store.sync_uc_catalog = ""
    store.graph_schema = "ontobricks_graph"
    store.synced_uc_name.return_value = _SYNCED_UC
    mgr = MagicMock()
    mgr.ensure.side_effect = lambda name, **kw: log.append(("ensure", name, kw))
    mgr.trigger_and_wait.return_value = "ONLINE"
    store.synced_manager.return_value = mgr
    store.drop_union_view.side_effect = lambda graph: log.append(("drop_view", graph))
    store.ensure_synced_union_view.side_effect = lambda graph, **kw: log.append(
        ("union_view", graph, kw)
    )
    return store, mgr


def _warehouse(log: list):
    src = MagicMock()
    src.execute_statement.side_effect = lambda sql: log.append(("sql", sql)) or True
    return src


def _apply(store, src, *, domain=None, settings=None, source_tables=None, mappings=None):
    with patch(
        "back.core.graphdb.lakebase.LakebaseFlatStore.resolve_sync_uc_fallback_catalog",
        return_value="cat",
    ), patch(
        "back.core.graphdb.lakebase._sync_uc_schema.ensure_uc_schema_for_synced_table_fqn"
    ):
        scheduled_build._apply_synced_pipeline(
            store,
            src,
            {"catalog": "cat", "schema": "sch"},
            "triplestore_dom_V3",
            _VIEW,
            full=True,
            domain_name="dom",
            domain=domain,
            settings=settings,
            source_tables=source_tables,
            mappings=mappings,
        )


def _ensures(log):
    return [event for event in log if event[0] == "ensure"]


class TestScheduledLakeflowSource:
    def test_the_data_table_is_materialized_before_the_synced_table_is_registered(self):
        log: list = []
        store, _ = _store(log)

        _apply(store, _warehouse(log), domain=_domain())

        ctas = next(
            i for i, e in enumerate(log) if e[0] == "sql" and "CREATE OR REPLACE TABLE" in e[1]
        )
        ensure = next(i for i, e in enumerate(log) if e[0] == "ensure")
        assert ctas < ensure
        sql = log[ctas][1]
        assert sql.startswith(f"CREATE OR REPLACE TABLE {_DATA} ")
        assert "delta.enableRowTracking = true" in sql
        assert sql.endswith(f"SELECT subject, predicate, object, object_hash FROM {_VIEW}")

    def test_the_synced_table_reads_the_data_table_as_a_snapshot(self):
        """Scheduled builds are full: the configured cap never lifts them off snapshots."""
        log: list = []
        store, _ = _store(log, sync_table_mode="continuous")

        _apply(store, _warehouse(log), domain=_domain())

        [(_, name, kwargs)] = _ensures(log)
        assert name == _SYNCED_UC
        kwargs.pop("before_delete")
        assert kwargs == {
            "source_table_full_name": _DATA,
            "primary_key_columns": ["subject", "predicate", "object_hash"],
            "sync_mode": "snapshot",
        }

    def test_the_union_view_is_built_in_the_triples_shape(self):
        log: list = []
        store, _ = _store(log)

        _apply(store, _warehouse(log), domain=_domain())

        [view] = [e for e in log if e[0] == "union_view"]
        assert view[2] == {}

    def test_ensure_drops_the_union_view_only_through_its_delete_hook(self):
        log: list = []
        store, mgr = _store(log)

        _apply(store, _warehouse(log), domain=_domain())

        assert not any(e[0] == "drop_view" for e in log)
        [(_, _, kwargs)] = _ensures(log)
        kwargs["before_delete"](_SYNCED_UC)
        assert log[-1] == ("drop_view", "triplestore_dom_V3")

    def test_without_a_domain_the_gateway_view_is_synced_and_nothing_is_copied(self):
        log: list = []
        store, _ = _store(log)

        _apply(store, _warehouse(log))

        assert not any(e[0] == "sql" for e in log)
        [(_, _, kwargs)] = _ensures(log)
        assert kwargs["source_table_full_name"] == _VIEW
        assert kwargs["sync_mode"] == "snapshot"


_SOURCES = ["main.sales.customers", "main.sales.orders"]


def _readiness(*untracked: str):
    """``SourceReadiness.check`` stand-in: every source tracked except *untracked*."""
    return MagicMock(side_effect=lambda client, tables: {t: t not in untracked for t in tables})


class TestScheduledSyncFollowsTheDomainMode:
    """A scheduled tick probes row tracking like the main build and lands on the same mode.

    A tick on another schedule than the main build would be a drift: union-view
    drop, purge and a full resync on every alternation.
    """

    @staticmethod
    def _sync_mode(mode, *, cap="continuous", settings=None, source_tables=_SOURCES, check=None):
        log: list = []
        store, _ = _store(log, sync_table_mode=cap)
        with patch.object(SourceReadiness, "check", check or _readiness()):
            _apply(
                store,
                _warehouse(log),
                domain=_domain(mode),
                settings=settings,
                source_tables=source_tables,
            )
        [(_, _, kwargs)] = _ensures(log)
        return kwargs["sync_mode"]

    def test_an_incremental_domain_with_tracked_sources_syncs_triggered(self):
        assert self._sync_mode("incremental") == "triggered"

    def test_an_incremental_domain_with_an_untracked_source_syncs_a_snapshot(self):
        assert self._sync_mode("incremental", check=_readiness("main.sales.orders")) == "snapshot"

    def test_a_microbatch_domain_with_tracked_sources_syncs_continuously(self):
        assert self._sync_mode("microbatch") == "continuous"

    def test_a_realtime_domain_runs_as_microbatch(self):
        assert self._sync_mode("realtime") == "continuous"

    def test_a_full_domain_syncs_a_snapshot_without_probing(self):
        check = _readiness()

        assert self._sync_mode(None, check=check) == "snapshot"
        check.assert_not_called()

    def test_unknown_sources_are_not_probed_and_sync_a_snapshot(self):
        check = _readiness()

        assert self._sync_mode("incremental", source_tables=None, check=check) == "snapshot"
        check.assert_not_called()

    def test_no_row_local_source_counts_as_tracked(self):
        check = MagicMock(return_value={})

        assert self._sync_mode("incremental", source_tables=[], check=check) == "triggered"

    def test_the_configured_cap_still_applies(self):
        assert self._sync_mode("microbatch", cap="triggered") == "triggered"

    def test_without_serverless_pipelines_an_incremental_domain_syncs_like_its_merge_build(self):
        """The main build keeps such a domain incremental by MERGE (triggered): a
        snapshot tick would delete and recreate the synced table every time."""
        settings = SimpleNamespace(serverless_pipelines_enabled=False)

        assert self._sync_mode("incremental", settings=settings) == "triggered"

    def test_without_serverless_pipelines_a_microbatch_domain_is_full(self):
        """MERGE never stands in for a continuous pipeline."""
        settings = SimpleNamespace(serverless_pipelines_enabled=False)

        assert self._sync_mode("microbatch", settings=settings) == "snapshot"

    def test_a_failed_probe_fails_the_sync_step(self):
        log: list = []
        store, mgr = _store(log)
        failing = MagicMock(side_effect=InfrastructureError("SHOW TBLPROPERTIES failed"))

        with patch.object(SourceReadiness, "check", failing):
            with pytest.raises(InfrastructureError, match="SHOW TBLPROPERTIES"):
                _apply(
                    store, _warehouse(log), domain=_domain("incremental"), source_tables=_SOURCES
                )

        mgr.ensure.assert_not_called()


@pytest.fixture
def slot_runner(monkeypatch):
    """The scripted slot pipeline runner of the modes tests; no WorkspaceClient, ever.

    The pipeline exists and its stored SQL fingerprint is the current one.
    """
    from tests.units.objects import test_build_pipeline_modes as _modes

    script = _modes._Script([], existing="pipe-1", stored="fp-current")
    monkeypatch.setattr(
        "back.core.streaming.SlotPipelineRunner",
        type("_BoundRunner", (_modes._FakeRunner,), {"script": script}),
    )
    attempts: list = []
    monkeypatch.setattr(
        "databricks.sdk.WorkspaceClient", lambda *a, **kw: attempts.append(a) or MagicMock()
    )
    yield script
    assert attempts == [], "unit tests must not build a WorkspaceClient"


def _mappings(*, batch: bool = True, row_local: bool = True) -> dict:
    """What the tick hands down: the domain's mappings as a build derives them."""
    from tests.units.objects import test_build_pipeline_modes as _modes

    entities = {}
    if row_local:
        entities[_modes._CUSTOMER] = _modes._row_local()
    if batch:
        entities[_modes._ORDER] = _modes._batch()
    from back.objects.digitaltwin.build import TwinIncrementalBuild

    spec = TwinIncrementalBuild.compile_for(
        _domain("incremental"), None, entities, [], base_uri="http://ex.org/"
    )
    return {"entity_mappings": entities, "relationship_mappings": [], "spec": spec}


def _current_sha(mode: str, mappings: dict) -> str:
    """The ``ontobricks.mv_sql_sha256`` a main build would give ``_data`` for *mappings*."""
    from back.core.graphdb.delta import MaterializedViewBuilder
    from back.objects.digitaltwin.build import TwinIncrementalBuild

    ent, rels = mappings["entity_mappings"], mappings["relationship_mappings"]
    definition = TwinIncrementalBuild.data_definition_for(mappings["spec"], ent, rels, mode)
    return MaterializedViewBuilder.data_mv_sha(**definition)


def _mv_warehouse(log: list, *, mv_sha: str):
    """A warehouse whose ``_data`` already is a materialized view with *mv_sha*."""
    src = _warehouse(log)

    def _query(sql):
        log.append(("query", sql))
        if sql.startswith("SHOW TBLPROPERTIES") and "ontobricks.mv_sql_sha256" in sql:
            return [{"key": "ontobricks.mv_sql_sha256", "value": mv_sha}]
        return []

    src.execute_query.side_effect = _query
    return src


def _statements(log) -> list:
    return [event[1] for event in log if event[0] == "sql"]


def _ctas(log) -> bool:
    return any(s.startswith(f"CREATE OR REPLACE TABLE {_DATA}") for s in _statements(log))


class TestScheduledTickKeepsTheIncrementalView:
    """A tick refreshes the MV a main build left instead of replacing it by a CTAS,
    as long as that view still has the domain's current definition."""

    @staticmethod
    def _tick(mode, log, *, mappings=None, stored_for=None, settings=None, source_tables=_SOURCES):
        """Tick a *mode* domain whose ``_data`` MV was built for *stored_for* mappings."""
        mappings = _mappings() if mappings is None else mappings
        stored = _current_sha(mode or "incremental", stored_for or mappings)
        store, _ = _store(log, sync_table_mode="continuous")
        with patch.object(SourceReadiness, "check", _readiness()):
            _apply(
                store,
                _mv_warehouse(log, mv_sha=stored),
                domain=_domain(mode),
                settings=settings,
                source_tables=source_tables,
                mappings=mappings,
            )

    def test_an_incremental_tick_updates_the_slots_then_refreshes_the_view(self, slot_runner):
        log: list = []

        self._tick("incremental", log)

        statements = _statements(log)
        assert not any(s.startswith("CREATE OR REPLACE TABLE") for s in statements)
        assert not any(s.startswith("DROP") for s in statements)
        assert f"REFRESH MATERIALIZED VIEW {_DATA}" in statements
        assert [event[0] for event in slot_runner.log] == ["find", "start", "wait"]
        assert slot_runner.log[1] == ("start", "pipe-1", False)
        assert slot_runner.log[2][2]["until_running"] is False
        [(_, _, kwargs)] = _ensures(log)
        assert kwargs["sync_mode"] == "triggered"

    def test_a_graph_materialized_view_is_refreshed_right_after_the_data(self, slot_runner):
        log: list = []
        graph = _DATA.removesuffix("_data") + "_graph"

        self._tick("incremental", log)

        statements = _statements(log)
        data = statements.index(f"REFRESH MATERIALIZED VIEW {_DATA}")
        assert statements.index(f"REFRESH MATERIALIZED VIEW {graph}") == data + 1

    def test_a_microbatch_tick_only_refreshes_the_view(self, slot_runner):
        log: list = []

        self._tick("microbatch", log)

        assert f"REFRESH MATERIALIZED VIEW {_DATA}" in _statements(log)
        assert [event[0] for event in slot_runner.log] == ["find"]  # its SQL is checked

    def test_an_incremental_view_without_row_local_mappings_needs_no_slot_update(
        self, slot_runner
    ):
        log: list = []

        self._tick("incremental", log, mappings=_mappings(row_local=False), source_tables=[])

        assert f"REFRESH MATERIALIZED VIEW {_DATA}" in _statements(log)
        assert slot_runner.log == []

    def test_a_changed_batch_mapping_rebuilds_by_ctas(self, slot_runner):
        """The view was built before a batch mapping was added: refreshing it would
        silently keep the old definition."""
        log: list = []

        self._tick("incremental", log, stored_for=_mappings(batch=False))

        assert _ctas(log)
        assert not any(s.startswith("REFRESH") for s in _statements(log))
        assert slot_runner.log == []

    def test_a_changed_slot_pipeline_sql_rebuilds_by_ctas(self, slot_runner):
        slot_runner.stored = "fp-old"
        log: list = []

        self._tick("incremental", log)

        assert _ctas(log)
        assert [event[0] for event in slot_runner.log] == ["find"]

    def test_a_definition_change_is_logged(self, slot_runner):
        log: list = []

        with patch("back.objects.digitaltwin.build.TwinScheduledRefresh.logger") as logger:
            self._tick("incremental", log, stored_for=_mappings(batch=False))

        call = logger.info.call_args
        assert "definition" in (call.args[0] % call.args[1:])

    def test_the_tick_compiled_spec_is_reused_not_compiled_again(self, slot_runner):
        from back.objects.digitaltwin.build import TwinIncrementalBuild

        mappings = _mappings()
        log: list = []
        with patch.object(
            TwinIncrementalBuild, "compile_for", side_effect=AssertionError("compiled twice")
        ):
            self._tick("incremental", log, mappings=mappings, stored_for=mappings)

        assert f"REFRESH MATERIALIZED VIEW {_DATA}" in _statements(log)

    def test_unknown_mappings_rebuild_by_ctas(self, slot_runner):
        log: list = []
        store, _ = _store(log)
        with patch.object(SourceReadiness, "check", _readiness()):
            _apply(
                store,
                _mv_warehouse(log, mv_sha=_current_sha("incremental", _mappings())),
                domain=_domain("incremental"),
                source_tables=_SOURCES,
            )

        assert _ctas(log)

    def test_a_missing_slot_pipeline_falls_back_to_the_ctas(self, slot_runner):
        slot_runner.existing = None
        log: list = []

        self._tick("incremental", log)

        assert _ctas(log)

    def test_a_data_table_that_is_not_a_view_is_rebuilt_by_ctas(self, slot_runner):
        log: list = []
        store, _ = _store(log)
        with patch.object(SourceReadiness, "check", _readiness()):
            _apply(
                store,
                _mv_warehouse(log, mv_sha=""),
                domain=_domain("incremental"),
                source_tables=_SOURCES,
                mappings=_mappings(),
            )

        assert _ctas(log)
        assert slot_runner.log == []

    def test_a_full_domain_keeps_the_ctas(self, slot_runner):
        log: list = []

        self._tick(None, log)

        assert _ctas(log)
        assert not any(s.startswith("REFRESH") for s in _statements(log))

    def test_a_merge_promoted_domain_keeps_the_ctas(self, slot_runner):
        log: list = []
        settings = SimpleNamespace(serverless_pipelines_enabled=False)

        self._tick("incremental", log, settings=settings)

        assert _ctas(log)
        assert slot_runner.log == []

    def test_a_failed_probe_drops_nothing(self, slot_runner):
        log: list = []
        store, mgr = _store(log)
        failing = MagicMock(side_effect=InfrastructureError("SHOW TBLPROPERTIES failed"))

        with patch.object(SourceReadiness, "check", failing), pytest.raises(InfrastructureError):
            _apply(
                store,
                _mv_warehouse(log, mv_sha=_current_sha("incremental", _mappings())),
                domain=_domain("incremental"),
                source_tables=_SOURCES,
                mappings=_mappings(),
            )

        assert _statements(log) == []
        mgr.ensure.assert_not_called()


class TestDecideModeFor:
    """The probe-and-resolve core shared by the main build and the scheduled mirror."""

    @staticmethod
    def _decide(mode, *, source_tables=_SOURCES, check=None, **domain_info):
        domain = _domain(mode)
        domain.info.update(domain_info)
        with patch.object(SourceReadiness, "check", check or _readiness()) as probe:
            decision = TwinBuildPostMaterialize.decide_mode_for(
                domain, None, MagicMock(), source_tables=source_tables
            )
        return decision, probe

    def test_tracked_sources_keep_the_requested_mode(self):
        decision, probe = self._decide("incremental")

        assert decision == ModeDecision("incremental", "incremental", "")
        assert probe.call_args.args[1] == _SOURCES

    def test_an_untracked_source_falls_back_to_full(self):
        decision, _ = self._decide("incremental", check=_readiness("main.sales.customers"))

        assert decision == ModeDecision("incremental", "full", "row_tracking_missing")

    def test_unknown_sources_count_as_untracked_without_a_probe(self):
        decision, probe = self._decide("incremental", source_tables=None)

        assert decision == ModeDecision("incremental", "full", "row_tracking_missing")
        probe.assert_not_called()

    def test_no_source_tables_count_as_tracked_without_a_probe(self):
        decision, _ = self._decide(
            "microbatch", source_tables=[], check=MagicMock(return_value={})
        )

        assert decision.effective == "microbatch"

    def test_a_gated_mode_is_not_probed(self):
        decision, probe = self._decide("incremental", graph_backend="none")

        assert decision.effective == "full"
        probe.assert_not_called()

    def test_realtime_resolves_to_microbatch(self):
        decision, _ = self._decide("realtime")

        assert decision == ModeDecision("realtime", "microbatch", "realtime_not_available")

    def test_a_downgrade_without_a_task_is_logged_at_debug(self):
        with patch("back.objects.digitaltwin.build.TwinBuildPostMaterialize.logger") as logger:
            self._decide("incremental", check=_readiness("main.sales.orders"))

        logger.info.assert_not_called()
        call = logger.debug.call_args
        assert "row_tracking_missing" in call.args[0] % call.args[1:]


class TestSlotSpec:
    """The tick's one compile: its source tables are what the probe reads."""

    _ROW_LOCAL = {
        "table": "main.sales.customers",
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": "http://ex.org/Customer",
        "predicates": {
            "http://www.w3.org/2000/01/rdf-schema#label": {"type": "column", "column": "name"}
        },
    }

    def test_the_row_local_source_tables_are_compiled_from_the_mappings(self):
        spec = scheduled_build._slot_spec(
            _domain("incremental"),
            SimpleNamespace(databricks_triplestore_table=""),
            {"http://ex.org/Customer": self._ROW_LOCAL},
            [],
            "http://ex.org/",
            "dom",
        )

        assert spec.source_tables == ["main.sales.customers"]
        assert len(spec.row_local_map_ids) == 1

    def test_a_full_domain_is_not_compiled(self):
        with patch.object(SlotProjectionCompiler, "compile") as compile_:
            tables = scheduled_build._slot_spec(
                _domain(None), SimpleNamespace(), {}, [], "http://ex.org/", "dom"
            )

        assert tables is None
        compile_.assert_not_called()

    def test_without_settings_the_sources_are_unknown(self):
        assert (
            scheduled_build._slot_spec(
                _domain("incremental"), None, {}, [], "http://ex.org/", "dom"
            )
            is None
        )

    def test_an_unsupported_mapping_never_fails_the_tick(self):
        with patch.object(
            SlotProjectionCompiler, "compile", side_effect=ValidationError("unsupported mapping")
        ), patch.object(scheduled_build, "logger") as logger:
            tables = scheduled_build._slot_spec(
                _domain("incremental"), SimpleNamespace(), {}, [], "http://ex.org/", "dom"
            )

        assert tables is None
        call = logger.warning.call_args
        assert "unsupported mapping" in call.args[0] % call.args[1:]

    def test_write_graph_triples_forwards_the_source_tables(self):
        store = MagicMock()
        store.is_synced = True
        with patch.object(scheduled_build, "_apply_synced_pipeline") as apply_, patch.object(
            scheduled_build, "_count_view_triples", return_value=3
        ):
            scheduled_build._write_graph_triples(
                store, MagicMock(), "g", _VIEW, "dom", source_tables=["main.sales.customers"]
            )

        assert apply_.call_args.kwargs["source_tables"] == ["main.sales.customers"]


_CUSTOMERS = "main.sales.customers"


def _history_warehouse(log: list, *, mv_sha: str, version=7, history=(), fail_history=False):
    """The MV warehouse, also answering the sources' ``DESCRIBE HISTORY``."""
    src = _mv_warehouse(log, mv_sha=mv_sha)
    base = src.execute_query.side_effect

    def _query(sql):
        if sql.startswith("DESCRIBE HISTORY") and sql.endswith(" LIMIT 1"):
            log.append(("query", sql))
            return [{"version": version}]
        if sql.startswith("DESCRIBE HISTORY"):
            log.append(("query", sql))
            if fail_history:
                raise RuntimeError("PERMISSION_DENIED")
            return list(history)
        return base(sql)

    src.execute_query.side_effect = _query
    return src


class TestScheduledTickPlansTheSlotUpdate:
    """An incremental tick decides its slot update like a build and records the facts."""

    @staticmethod
    def _tick(log, monkeypatch, *, runs=(), mode="incremental", facts=None, **warehouse_kw):
        from back.objects.digitaltwin.build import TwinBuildRunHistory

        monkeypatch.setattr(
            TwinBuildRunHistory, "recent_runs_for", staticmethod(lambda d, s: list(runs))
        )
        mappings = {**_mappings(), "facts": {} if facts is None else facts}
        stored = _current_sha(mode, mappings)
        store, _ = _store(log, sync_table_mode="continuous")
        with patch.object(SourceReadiness, "check", _readiness()):
            _apply(
                store,
                _history_warehouse(log, mv_sha=stored, **warehouse_kw),
                domain=_domain(mode),
                source_tables=_SOURCES,
                mappings=mappings,
            )
        return mappings["facts"]

    @staticmethod
    def _run(**facts) -> dict:
        return {"status": "success", "stats": {"materialization": facts}}

    def test_a_completed_update_records_the_versions_read_before_it(
        self, slot_runner, monkeypatch
    ):
        log: list = []

        facts = self._tick(log, monkeypatch, version=7)

        assert facts == {
            "source_versions": {_CUSTOMERS: 7},
            "source_table_ids": {},
            "slot_full_refresh_pending": False,
            "slot_full_refresh_reason": "",
        }
        assert slot_runner.log[1] == ("start", "pipe-1", False)

    def test_a_rewritten_source_makes_the_update_a_full_refresh(self, slot_runner, monkeypatch):
        log: list = []
        runs = [self._run(source_versions={_CUSTOMERS: 5})]

        facts = self._tick(
            log, monkeypatch, runs=runs, history=[{"version": 6, "operation": "TRUNCATE"}]
        )

        assert slot_runner.log[1] == ("start", "pipe-1", True)
        assert facts["slot_full_refresh_reason"] == "source_overwritten"
        assert facts["slot_full_refresh_pending"] is False

    def test_an_owed_drift_refreshes_the_slots_and_the_view_in_full(
        self, slot_runner, monkeypatch
    ):
        log: list = []

        self._tick(log, monkeypatch, runs=[self._run(reconcile_drift=True)])

        assert slot_runner.log[1] == ("start", "pipe-1", True)
        assert f"REFRESH MATERIALIZED VIEW {_DATA} FULL" in _statements(log)

    def test_a_failed_update_leaves_the_refresh_pending(self, slot_runner, monkeypatch):
        log: list = []
        slot_runner.fail_on = ("wait",)
        runs = [self._run(source_versions={_CUSTOMERS: 5})]
        facts: dict = {}

        with pytest.raises(InfrastructureError):
            self._tick(
                log,
                monkeypatch,
                runs=runs,
                facts=facts,
                history=[{"version": 6, "operation": "TRUNCATE"}],
            )

        assert facts == {
            "source_versions": {_CUSTOMERS: 5},
            "source_table_ids": {},
            "slot_full_refresh_pending": True,
            "slot_full_refresh_reason": "source_overwritten",
        }

    def test_an_unreadable_source_history_leaves_the_view_as_is(self, slot_runner, monkeypatch):
        """A transient failure must not drop the view for a CTAS (every later tick would CTAS)."""
        log: list = []
        runs = [self._run(source_versions={_CUSTOMERS: 5})]

        with patch("back.objects.digitaltwin.build.TwinScheduledRefresh.logger") as logger:
            facts = self._tick(log, monkeypatch, runs=runs, fail_history=True)

        statements = _statements(log)
        assert not _ctas(log)
        assert not any(s.startswith(("REFRESH", "DROP")) for s in statements)
        assert slot_runner.log == []
        assert facts == {}
        logger.warning.assert_called_once()

    def test_a_microbatch_tick_plans_nothing(self, slot_runner, monkeypatch):
        log: list = []

        facts = self._tick(log, monkeypatch, mode="microbatch")

        assert facts == {}


def _lakehouse_domain(mode: str | None = None):
    domain = _domain(mode)
    domain.info["graph_backend"] = "databricks"
    return domain


class TestALakehouseTickStreamsNothing:
    """On the Lakehouse engine ``…_data`` is the graph: a tick refreshes it, never streams."""

    _GRAPH = _DATA.removesuffix("_data") + "_graph"
    _INFERRED = _DATA.removesuffix("_data") + "_inferred"

    @staticmethod
    def _tick(src, domain, mappings=None, source_tables=None) -> tuple:
        store = MagicMock()
        store.is_synced = False
        with patch.object(scheduled_build, "_count_view_triples", return_value=7) as count:
            written = scheduled_build._write_graph_triples(
                store,
                src,
                "dom_V3",
                _VIEW,
                "dom",
                domain=domain,
                settings=SimpleNamespace(databricks_triplestore_table=""),
                source_tables=source_tables,
                mappings=mappings,
            )
        return store, written, count

    @staticmethod
    def _no_store_write(store) -> None:
        for name in ("drop_table", "create_table", "insert_triples", "bulk_insert_iter"):
            getattr(store, name).assert_not_called()

    def test_a_full_domain_rebuilds_data_from_the_view(self):
        log: list = []

        store, written, count = self._tick(_mv_warehouse(log, mv_sha=""), _lakehouse_domain())

        self._no_store_write(store)
        assert _ctas(log)
        assert written == 7 and count.call_args.args[1] == _DATA

    def test_an_incremental_domain_refreshes_its_views_in_place(self, slot_runner):
        log: list = []
        mappings = _mappings()
        src = _mv_warehouse(log, mv_sha=_current_sha("incremental", mappings))

        with patch.object(SourceReadiness, "check", _readiness()):
            store, _, _ = self._tick(src, _lakehouse_domain("incremental"), mappings, _SOURCES)

        self._no_store_write(store)
        statements = _statements(log)
        assert not _ctas(log)
        data = statements.index(f"REFRESH MATERIALIZED VIEW {_DATA}")
        assert statements.index(f"REFRESH MATERIALIZED VIEW {self._GRAPH}") > data
        assert statements.count(f"REFRESH MATERIALIZED VIEW {self._GRAPH}") == 1

    def test_the_inferences_of_the_previous_data_are_discarded_first(self, slot_runner):
        """As the main build's reset: ``…_inferred`` is emptied before ``…_data`` / ``…_graph`` move."""
        log: list = []
        mappings = _mappings()
        src = _mv_warehouse(log, mv_sha=_current_sha("incremental", mappings))

        with patch.object(SourceReadiness, "check", _readiness()):
            self._tick(src, _lakehouse_domain("incremental"), mappings, _SOURCES)

        statements = _statements(log)
        truncate = statements.index(f"TRUNCATE TABLE {self._INFERRED}")
        assert truncate < statements.index(f"REFRESH MATERIALIZED VIEW {_DATA}")

    def test_a_rebuilt_data_refreshes_a_graph_view_once(self):
        log: list = []

        self._tick(_mv_warehouse(log, mv_sha="a" * 64), _lakehouse_domain())

        statements = _statements(log)
        assert _ctas(log)
        assert statements.count(f"REFRESH MATERIALIZED VIEW {self._GRAPH}") == 1
        assert statements.index(f"TRUNCATE TABLE {self._INFERRED}") < statements.index(
            f"REFRESH MATERIALIZED VIEW {self._GRAPH}"
        )

    def test_a_lakebase_store_still_streams_in_full(self):
        """Not Lakehouse (and not synced): the drop-and-rebuild of the store stays."""
        store = MagicMock()
        store.is_synced = False
        src = MagicMock()
        with patch.object(scheduled_build, "_count_view_triples", return_value=2):
            scheduled_build._write_graph_triples(store, src, "dom_V3", _VIEW, "dom")

        store.drop_table.assert_called_once_with("dom_V3")
