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

from back.objects.registry.schedule import build as scheduled_build

pytestmark = pytest.mark.unit

_VIEW = "cat.sch.triplestore_dom_V3"
_DATA = "cat.sch.triplestore_dom_V3_data"
_SYNCED_UC = "cat.ontobricks_graph.triplestore_dom_v3_sync"


def _domain():
    return SimpleNamespace(
        delta={"catalog": "cat", "schema": "sch"},
        info={"name": "dom"},
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


def _apply(store, src, *, domain=None):
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
            settings=None,
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
