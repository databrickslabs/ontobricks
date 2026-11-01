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


def _apply(store, src, *, domain=None, settings=None, source_tables=None):
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

    def test_the_union_view_is_built_for_a_triples_source(self):
        log: list = []
        store, _ = _store(log)

        _apply(store, _warehouse(log), domain=_domain())

        [view] = [e for e in log if e[0] == "union_view"]
        assert view[2] == {"source_kind": "triples"}

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

    def test_without_serverless_pipelines_an_incremental_domain_is_full(self):
        settings = SimpleNamespace(serverless_pipelines_enabled=False)

        assert self._sync_mode("incremental", settings=settings) == "snapshot"

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


class TestSlotSourceTables:
    """What the scheduled tick probes: the source tables of the row-local mappings."""

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
        tables = scheduled_build._slot_source_tables(
            _domain("incremental"),
            SimpleNamespace(databricks_triplestore_table=""),
            {"http://ex.org/Customer": self._ROW_LOCAL},
            [],
            "http://ex.org/",
            "dom",
        )

        assert tables == ["main.sales.customers"]

    def test_a_full_domain_is_not_compiled(self):
        with patch.object(SlotProjectionCompiler, "compile") as compile_:
            tables = scheduled_build._slot_source_tables(
                _domain(None), SimpleNamespace(), {}, [], "http://ex.org/", "dom"
            )

        assert tables is None
        compile_.assert_not_called()

    def test_without_settings_the_sources_are_unknown(self):
        assert (
            scheduled_build._slot_source_tables(
                _domain("incremental"), None, {}, [], "http://ex.org/", "dom"
            )
            is None
        )

    def test_an_unsupported_mapping_never_fails_the_tick(self):
        with patch.object(
            SlotProjectionCompiler, "compile", side_effect=ValidationError("unsupported mapping")
        ), patch.object(scheduled_build, "logger") as logger:
            tables = scheduled_build._slot_source_tables(
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
