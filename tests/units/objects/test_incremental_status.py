"""``TwinIncrementalStatus``: the read-only status report of an incremental domain."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.delta import _table_naming
from back.core.streaming import SlotPipelineRunner
from back.objects.digitaltwin.build import TwinIncrementalStatus

pytestmark = pytest.mark.unit

_DATA = "cat.sch.t_v3_data"
_SLOTS = "cat.sch.t_v3_slots"
_NOW = datetime(2026, 11, 2, 12, 0, 0, tzinfo=timezone.utc)


def _domain(mode="incremental"):
    return SimpleNamespace(
        info={"name": "Dom", "materialization_mode": mode, "graph_backend": "lakebase"},
        current_version=3,
        uc_domain_folder="dom",
        delta={"catalog": "cat", "schema": "sch"},
    )


class _Warehouse:
    """Answers the catalog lookup and ``DESCRIBE HISTORY`` per relation."""

    def __init__(self, kinds=None, history=None, error_on=()):
        self.kinds = kinds or {}
        self.history = history or {}
        self.error_on = tuple(error_on)
        self.queries: list[str] = []

    def execute_query(self, sql):
        self.queries.append(sql)
        if any(token in sql for token in self.error_on):
            raise RuntimeError("PERMISSION_DENIED")
        if sql.startswith("SELECT table_type"):
            leaf = sql.rsplit("table_name = '", 1)[1].rstrip("'")
            kind = self.kinds.get(leaf)
            return [{"table_type": kind}] if kind else []
        if sql.startswith("DESCRIBE HISTORY"):
            return [self.history[sql.split()[2]]]
        raise AssertionError(sql)


class _Runner(SlotPipelineRunner):
    pipelines: dict = {}
    error: Exception | None = None

    def __init__(self, *args, **kwargs):
        pass

    def find_pipeline_id(self, name):
        if _Runner.error is not None:
            raise _Runner.error
        return _Runner.pipelines.get(name)

    def get_state(self, pipeline_id):
        return "RUNNING"


class _Registry:
    def __init__(self, runs=(), error=None):
        self.runs, self.error, self.calls = list(runs), error, []

    def load_build_runs(self, folder, version=None, limit=None):
        self.calls.append((folder, version, limit))
        if self.error is not None:
            raise self.error
        return self.runs


@pytest.fixture
def env(monkeypatch):
    import importlib

    import back.core.streaming as streaming
    from back.objects.registry.RegistryService import RegistryService

    flat_store = importlib.import_module("back.core.graphdb.lakebase.LakebaseFlatStore")

    registry = _Registry()
    _Runner.pipelines, _Runner.error = {}, None
    monkeypatch.setattr(_table_naming, "data_table_fqn", lambda d, s=None: _DATA)
    monkeypatch.setattr(_table_naming, "slots_table_fqn", lambda d, s=None: _SLOTS)
    monkeypatch.setattr(streaming, "SlotPipelineRunner", _Runner)
    monkeypatch.setattr(
        RegistryService, "from_context", classmethod(lambda cls, d, s: registry)
    )
    monkeypatch.setattr(
        flat_store, "resolve_sync_uc_fallback_catalog", lambda d, s, c=None: "cat"
    )
    return registry


def _report(warehouse, store=None, domain=None):
    return TwinIncrementalStatus.report(
        domain or _domain(), SimpleNamespace(), warehouse, get_store=lambda d, s: store, now=_NOW
    )


class TestTables:
    def test_an_mv_data_and_a_streaming_slots_table_with_their_last_commits(self, env):
        warehouse = _Warehouse(
            kinds={"t_v3_data": "MATERIALIZED_VIEW", "t_v3_slots": "STREAMING_TABLE"},
            history={
                _DATA: {"version": 9, "timestamp": datetime(2026, 11, 2, 11, 55, tzinfo=timezone.utc)},
                _SLOTS: {"version": 4, "timestamp": "2026-11-02T11:59:00+00:00"},
            },
        )

        report = _report(warehouse)

        assert report["data"] == {
            "table": _DATA,
            "kind": "mv",
            "version": 9,
            "committed_at": "2026-11-02T11:55:00+00:00",
        }
        assert report["slots"]["kind"] == "streaming_table" and report["slots"]["version"] == 4
        assert report["freshness_lag_s"] == 300

    def test_absent_relations_have_no_history_and_no_lag(self, env):
        warehouse = _Warehouse()

        report = _report(warehouse)

        assert report["data"]["kind"] == "absent" and report["slots"]["kind"] == "absent"
        assert report["freshness_lag_s"] is None
        assert not any(q.startswith("DESCRIBE HISTORY") for q in warehouse.queries)

    def test_a_view_has_no_history(self, env):
        warehouse = _Warehouse(kinds={"t_v3_data": "VIEW"})

        assert _report(warehouse)["data"]["version"] is None

    def test_a_failed_lookup_is_unknown_not_absent(self, env):
        report = _report(_Warehouse(error_on=("information_schema",)))

        assert report["data"]["kind"] == "unknown"
        assert "error" in report["data"]

    def test_a_failed_history_keeps_the_kind(self, env):
        warehouse = _Warehouse(kinds={"t_v3_data": "MANAGED"}, error_on=("DESCRIBE HISTORY",))

        data = _report(warehouse)["data"]

        assert data["kind"] == "table" and data["version"] is None and "error" in data


class TestSlotPipeline:
    def test_the_version_pipeline_and_its_state(self, env):
        name = SlotPipelineRunner.pipeline_name("Dom", "3")
        _Runner.pipelines = {name: "p-1"}

        assert _report(_Warehouse())["slot_pipeline"] == {"name": name, "id": "p-1", "state": "RUNNING"}

    def test_no_pipeline_is_absent(self, env):
        assert _report(_Warehouse())["slot_pipeline"]["state"] == "absent"

    def test_an_unreachable_api_is_unknown(self, env):
        _Runner.error = InfrastructureError("pipelines API unreachable")

        pipeline = _report(_Warehouse())["slot_pipeline"]

        assert pipeline["state"] == "unknown" and "unreachable" in pipeline["error"]


class _Manager:
    def __init__(self, synced=None, error=None):
        self.synced, self.error, self.names = synced, error, []

    def get(self, name):
        self.names.append(name)
        if self.error is not None:
            raise self.error
        return self.synced


class _SyncedStore:
    is_synced = True

    def __init__(self, manager):
        self._manager = manager

    def synced_uc_name(self, graph_name, fallback_catalog=""):
        return f"{fallback_catalog}.graphs.{graph_name}_sync"

    def synced_manager(self):
        return self._manager


class TestLakebaseSync:
    def test_an_unsynced_store_has_no_section(self, env):
        assert _report(_Warehouse(), store=SimpleNamespace(is_synced=False))["lakebase_sync"] is None

    def test_the_synced_table_state_and_message(self, env):
        status = SimpleNamespace(detailed_state="SYNCED_TABLE_ONLINE_TRIGGERED_UPDATE", message="ok")
        manager = _Manager(SimpleNamespace(data_synchronization_status=status))

        sync = _report(_Warehouse(), store=_SyncedStore(manager))["lakebase_sync"]

        assert sync["state"] == "ONLINE_TRIGGERED_UPDATE" and sync["message"] == "ok"
        assert sync["synced_table"] == manager.names[0]

    def test_a_missing_synced_table(self, env):
        sync = _report(_Warehouse(), store=_SyncedStore(_Manager()))["lakebase_sync"]

        assert sync["state"] == "MISSING"

    def test_an_unreadable_synced_table_is_unknown(self, env):
        store = _SyncedStore(_Manager(error=RuntimeError("503")))

        sync = _report(_Warehouse(), store=store)["lakebase_sync"]

        assert sync["state"] == "UNKNOWN" and sync["error"] == "503"


class TestLastBuild:
    def test_the_newest_run_and_its_materialization_facts(self, env):
        env.runs = [
            {
                "status": "success",
                "build_kind": "scheduled",
                "finished_at": "2026-11-02T11:00:00+00:00",
                "stats": {"materialization": {"strategy": "mv", "apply_mode": "lakeflow_sync"}},
            }
        ]

        last = _report(_Warehouse())["last_build"]

        assert last == {
            "status": "success",
            "build_kind": "scheduled",
            "finished_at": "2026-11-02T11:00:00+00:00",
            "materialization": {"strategy": "mv", "apply_mode": "lakeflow_sync"},
        }
        assert env.calls == [("dom", "3", 1)]

    def test_no_run_yet(self, env):
        assert _report(_Warehouse())["last_build"] is None

    def test_an_unreadable_registry_is_reported(self, env):
        env.error = RuntimeError("registry down")

        assert _report(_Warehouse())["last_build"] == {"error": "registry down"}


def test_a_full_domain_is_answered_without_any_probe(env, monkeypatch):
    """Both build panels ask on page load: a ``full`` domain must not wake a warehouse."""
    warehouse = _Warehouse()
    _Runner.error = AssertionError("no pipelines API call for a full domain")

    report = _report(warehouse, store=object(), domain=_domain("full"))

    assert report == {
        "mode_requested": "full",
        "read_relation": "graph",
        "slot_pipeline": None,
        "data": None,
        "slots": None,
        "freshness_lag_s": None,
        "lakebase_sync": None,
        "last_build": None,
        "realtime": None,
    }
    assert warehouse.queries == [] and env.calls == []


def test_a_realtime_domain_reports_its_stream(env, monkeypatch):
    from back.objects.digitaltwin.build import TwinRealtimeStream

    monkeypatch.setattr(
        TwinRealtimeStream, "status", lambda self, domain: {"run_id": 9, "state": "RUNNING"}
    )

    report = _report(_Warehouse(), domain=_domain("realtime"))

    assert report["realtime"] == {"run_id": 9, "state": "RUNNING"}


def test_another_mode_reports_no_stream(env):
    assert _report(_Warehouse(), domain=_domain("microbatch"))["realtime"] is None


def test_the_requested_mode_is_reported(env):
    assert _report(_Warehouse(), domain=_domain("microbatch"))["mode_requested"] == "microbatch"
