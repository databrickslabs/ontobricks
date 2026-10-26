"""TwinStoreCache: per-section TTL cache, pending existence skeleton,
schedule → last_build sync, and the live Lakebase / Neo4j existence probes."""

from __future__ import annotations

import importlib
import time
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.cache.TwinStoreCache import TwinStoreCache

pytestmark = pytest.mark.unit

TTL = importlib.import_module(
    "back.objects.digitaltwin.cache.TwinStoreCache"
)._TS_STATS_CACHE_TTL_SECONDS


def _domain(**over) -> SimpleNamespace:
    base = dict(
        triplestore={},
        save=MagicMock(),
        last_build="",
        last_update="",
        info={},
        domain_folder="sales",
    )
    base.update(over)
    return SimpleNamespace(**base)


async def _run_blocking(fn, *args, **kwargs):
    return fn(*args, **kwargs)


# ---------------------------------------------------------------------------
# get_ts_cache / set_ts_cache / clear_ts_cache
# ---------------------------------------------------------------------------


class TestSectionCache:
    def test_missing_triplestore_returns_none(self):
        assert TwinStoreCache(_domain(triplestore=None)).get_ts_cache("status") is None

    def test_non_dict_stats_returns_none(self):
        dom = _domain(triplestore={"stats": "corrupt"})
        assert TwinStoreCache(dom).get_ts_cache("status") is None

    def test_non_dict_section_returns_none(self):
        dom = _domain(triplestore={"stats": {"status": ["x"]}})
        assert TwinStoreCache(dom).get_ts_cache("status") is None

    def test_fresh_section_is_served_without_internal_timestamp(self):
        dom = _domain()
        cache = TwinStoreCache(dom)
        cache.set_ts_cache("status", {"has_data": True, "count": 3})

        assert cache.get_ts_cache("status") == {"has_data": True, "count": 3}
        dom.save.assert_called_once()

    def test_expired_section_timestamp_is_a_miss(self):
        dom = _domain()
        cache = TwinStoreCache(dom)
        cache.set_ts_cache("status", {"has_data": True})
        dom.triplestore["stats"]["status"]["_ts"] = time.time() - TTL - 5

        assert cache.get_ts_cache("status") is None

    def test_sections_expire_independently(self):
        """Refreshing ``status`` must not extend the TTL of ``dt_existence``."""
        dom = _domain()
        cache = TwinStoreCache(dom)
        cache.set_ts_cache("dt_existence", {"view_exists": True})
        dom.triplestore["stats"]["dt_existence"]["_ts"] = time.time() - TTL - 5
        cache.set_ts_cache("status", {"has_data": True})

        assert cache.get_ts_cache("status") is not None
        assert cache.get_ts_cache("dt_existence") is None

    def test_legacy_entry_without_section_ts_and_without_shared_ts_is_a_miss(self):
        dom = _domain(triplestore={"stats": {"status": {"has_data": True}}})
        assert TwinStoreCache(dom).get_ts_cache("status") is None

    def test_legacy_entry_uses_fresh_shared_timestamp(self):
        dom = _domain(
            triplestore={
                "stats": {"status": {"has_data": True}},
                "_ts_cache_timestamp": time.time(),
            }
        )
        assert TwinStoreCache(dom).get_ts_cache("status") == {"has_data": True}

    def test_legacy_entry_with_expired_shared_timestamp_is_a_miss(self):
        dom = _domain(
            triplestore={
                "stats": {"status": {"has_data": True}},
                "_ts_cache_timestamp": time.time() - TTL - 1,
            }
        )
        assert TwinStoreCache(dom).get_ts_cache("status") is None

    def test_clear_removes_only_the_named_section_and_saves(self):
        dom = _domain()
        cache = TwinStoreCache(dom)
        cache.set_ts_cache("stats", {"total": 1})
        cache.set_ts_cache("status", {"has_data": True})
        dom.save.reset_mock()

        cache.clear_ts_cache("stats")

        assert cache.get_ts_cache("stats") is None
        assert cache.get_ts_cache("status") is not None
        dom.save.assert_called_once()

    def test_clear_of_absent_section_does_not_save(self):
        dom = _domain()
        TwinStoreCache(dom).clear_ts_cache("stats")
        dom.save.assert_not_called()


# ---------------------------------------------------------------------------
# get_or_fetch_dt_existence
# ---------------------------------------------------------------------------


class TestDtExistenceCaching:
    async def test_cache_hit_skips_the_live_probe(self):
        dom = _domain()
        cache = TwinStoreCache(dom)
        cache.set_ts_cache("dt_existence", {"lakebase_table_exists": True})
        with patch.object(
            TwinStoreCache, "fetch_digital_twin_existence"
        ) as live:
            result = await cache.get_or_fetch_dt_existence(MagicMock())
        live.assert_not_called()
        assert result == {"lakebase_table_exists": True}

    async def test_confident_answer_is_cached_on_miss(self):
        dom = _domain()
        cache = TwinStoreCache(dom)

        async def _live(self, settings):
            return {"lakebase_table_exists": False, "view_exists": True}

        with patch.object(TwinStoreCache, "fetch_digital_twin_existence", _live):
            await cache.get_or_fetch_dt_existence(MagicMock())

        assert cache.get_ts_cache("dt_existence")["lakebase_table_exists"] is False

    async def test_inconclusive_probe_is_not_cached(self):
        dom = _domain()
        cache = TwinStoreCache(dom)

        async def _live(self, settings):
            return {"lakebase_table_exists": None, "lakebase_check_error": "timeout"}

        with patch.object(TwinStoreCache, "fetch_digital_twin_existence", _live):
            result = await cache.get_or_fetch_dt_existence(MagicMock())

        assert result["lakebase_check_error"] == "timeout"
        assert cache.get_ts_cache("dt_existence") is None

    async def test_force_refresh_bypasses_a_cached_false(self):
        dom = _domain()
        cache = TwinStoreCache(dom)
        cache.set_ts_cache("dt_existence", {"lakebase_table_exists": False})

        async def _live(self, settings):
            return {"lakebase_table_exists": True}

        with patch.object(TwinStoreCache, "fetch_digital_twin_existence", _live):
            result = await cache.get_or_fetch_dt_existence(
                MagicMock(), force_refresh=True
            )

        assert result["lakebase_table_exists"] is True
        assert cache.get_ts_cache("dt_existence")["lakebase_table_exists"] is True


# ---------------------------------------------------------------------------
# fetch_graph_triplestore_status (complements test_graph_status_probe_failure)
# ---------------------------------------------------------------------------


class TestGraphStatusExtras:
    @contextmanager
    def _patched(self, store):
        with patch(
            "back.core.helpers.effective_graph_query_table", return_value="g"
        ), patch("back.core.helpers.effective_view_table", return_value="c.s.v"), patch(
            "back.core.helpers.run_blocking", _run_blocking
        ), patch("back.core.graphdb.get_graphdb", return_value=store):
            yield

    async def test_build_stamp_and_path_are_reported_when_graph_has_data(self):
        store = MagicMock()
        store.table_exists.return_value = True
        store.get_status.return_value = {"count": 4, "path": "/Volumes/x"}
        dom = _domain(triplestore={"build_last_update": "2026-10-01T00:00:00"})
        with self._patched(store):
            result = await TwinStoreCache(dom).fetch_graph_triplestore_status(None)

        assert result["last_modified"] == "2026-10-01T00:00:00"
        assert result["path"] == "/Volumes/x"
        assert "reason" not in result

    async def test_unexpected_failure_raises_infrastructure_error(self):
        with patch(
            "back.core.helpers.effective_graph_query_table",
            side_effect=RuntimeError("bad config"),
        ):
            with pytest.raises(InfrastructureError):
                await TwinStoreCache(_domain()).fetch_graph_triplestore_status(None)


# ---------------------------------------------------------------------------
# pending_dt_existence
# ---------------------------------------------------------------------------


@contextmanager
def _names(engine: str, *, graph_name="sales_graph", engine_cfg=None, schema="ob",
           fallback_catalog="main"):
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(DigitalTwin, "resolve_graph_engine", return_value=engine)
        )
        stack.enter_context(
            patch("back.core.helpers.effective_view_table", return_value="c.s.view")
        )
        stack.enter_context(
            patch(
                "back.core.helpers.effective_graph_query_table",
                return_value=graph_name,
            )
        )
        stack.enter_context(
            patch("back.core.helpers.effective_graph_name", return_value="fallback_g")
        )
        stack.enter_context(
            patch.object(
                GraphDBFactory,
                "_resolve_graph_engine_config",
                return_value=engine_cfg if engine_cfg is not None else {},
            )
        )
        stack.enter_context(
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.resolve_lakebase_graph_schema",
                return_value=schema,
            )
        )
        stack.enter_context(
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.resolve_sync_uc_fallback_catalog",
                return_value=fallback_catalog,
            )
        )
        yield


class TestPendingExistence:
    def test_skeleton_has_unknown_flags_and_pending_true(self):
        dom = _domain(last_build="2026-01-01", last_update="2026-01-02")
        with _names("neo4j"):
            result = TwinStoreCache(dom).pending_dt_existence(MagicMock())

        assert result["pending"] is True
        assert result["view_exists"] is None
        assert result["lakebase_table_exists"] is None
        assert result["graph_has_data"] is None
        assert result["graph_engine"] == "neo4j"
        assert result["view_table"] == "c.s.view"
        assert result["last_built"] == "2026-01-01"
        assert result["last_update"] == "2026-01-02"
        assert result["lakebase_table"] == ""

    def test_empty_query_table_falls_back_to_graph_name(self):
        with _names("neo4j", graph_name=""):
            result = TwinStoreCache(_domain()).pending_dt_existence(MagicMock())
        assert result["graph_name"] == "fallback_g"

    def test_lakebase_app_managed_resolves_schema_and_table_only(self):
        with _names("lakebase", engine_cfg={"lakebase": {"sync_mode": "app_managed"}}):
            result = TwinStoreCache(_domain()).pending_dt_existence(MagicMock())

        assert result["lakebase_schema"] == "ob"
        assert result["lakebase_table"] == "sales_graph"
        assert result["lakebase_synced_uc"] == ""

    def test_lakebase_managed_synced_uses_configured_catalog(self):
        cfg = {"lakebase": {"sync_mode": "managed_synced", "sync_uc_catalog": "gold"}}
        with _names("lakebase", engine_cfg=cfg):
            result = TwinStoreCache(_domain()).pending_dt_existence(MagicMock())

        assert result["lakebase_synced_uc"] == "gold.ob.sales_graph_sync"

    def test_lakebase_managed_synced_falls_back_to_registry_catalog(self):
        cfg = {"lakebase": {"sync_mode": "managed_synced"}}
        with _names("lakebase", engine_cfg=cfg, fallback_catalog="main"):
            result = TwinStoreCache(_domain()).pending_dt_existence(MagicMock())

        assert result["lakebase_synced_uc"] == "main.ob.sales_graph_sync"

    def test_lakebase_name_resolution_failure_keeps_the_skeleton(self):
        with _names("lakebase"), patch.object(
            GraphDBFactory,
            "_resolve_graph_engine_config",
            side_effect=RuntimeError("no config"),
        ):
            result = TwinStoreCache(_domain()).pending_dt_existence(MagicMock())

        assert result["pending"] is True
        assert result["lakebase_schema"] == ""


# ---------------------------------------------------------------------------
# sync_last_build_from_schedule
# ---------------------------------------------------------------------------


@contextmanager
def _schedule(*, started=True, host="https://h", catalog="main", schedules=None):
    scheduler = SimpleNamespace(_started=started)
    rcfg = MagicMock()
    rcfg.as_dict.return_value = {"catalog": catalog}
    gcs = MagicMock()
    gcs.load.return_value = {"schedules": schedules or {}}
    with patch("back.objects.registry.get_scheduler", return_value=scheduler), patch(
        "back.objects.registry.RegistryCfg.RegistryCfg.from_domain", return_value=rcfg
    ), patch(
        "back.core.helpers.get_databricks_host_and_token", return_value=(host, "tok")
    ), patch("back.objects.session.global_config_service", gcs):
        yield gcs


class TestSyncLastBuildFromSchedule:
    def test_newer_successful_schedule_run_updates_last_build(self):
        dom = _domain(last_build="2026-01-01T00:00:00")
        sched = {"sales": {"last_status": "success", "last_run": "2026-02-01T00:00:00"}}
        with _schedule(schedules=sched):
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())

        assert dom.last_build == "2026-02-01T00:00:00"
        dom.save.assert_called_once()

    def test_older_schedule_run_does_not_overwrite(self):
        dom = _domain(last_build="2026-03-01T00:00:00")
        sched = {"sales": {"last_status": "success", "last_run": "2026-02-01T00:00:00"}}
        with _schedule(schedules=sched):
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())

        assert dom.last_build == "2026-03-01T00:00:00"
        dom.save.assert_not_called()

    def test_failed_schedule_run_is_ignored(self):
        dom = _domain(last_build="")
        sched = {"sales": {"last_status": "error", "last_run": "2026-02-01T00:00:00"}}
        with _schedule(schedules=sched):
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        assert dom.last_build == ""

    def test_domain_without_schedule_is_ignored(self):
        dom = _domain(last_build="")
        with _schedule(schedules={"other": {"last_status": "success", "last_run": "x"}}):
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        assert dom.last_build == ""

    def test_stopped_scheduler_skips_registry_lookup(self):
        dom = _domain()
        with _schedule(started=False) as gcs:
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        gcs.load.assert_not_called()

    def test_missing_host_or_catalog_skips_registry_lookup(self):
        dom = _domain()
        with _schedule(host="") as gcs:
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        gcs.load.assert_not_called()
        with _schedule(catalog="") as gcs:
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        gcs.load.assert_not_called()

    def test_domain_without_folder_returns_immediately(self):
        dom = _domain(domain_folder="")
        with _schedule() as gcs:
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        gcs.load.assert_not_called()

    def test_registry_failure_is_swallowed(self):
        dom = _domain(last_build="keep")
        with _schedule() as gcs:
            gcs.load.side_effect = RuntimeError("registry down")
            TwinStoreCache(dom).sync_last_build_from_schedule(MagicMock())
        assert dom.last_build == "keep"


# ---------------------------------------------------------------------------
# fetch_digital_twin_existence — Lakebase path
# ---------------------------------------------------------------------------


class _LakebaseStore:
    def __init__(self, *, exists=True, count=10, status=None, synced=False,
                 table_exists_exc=None):
        self.graph_schema = "live_schema"
        self.is_synced = synced
        self._exists = exists
        self._count = count
        self._status = status
        self._exc = table_exists_exc

    def physical_table_id(self, name):
        return f"{name}_phy"

    def _effective_database_display(self):
        return "db_display"

    def synced_uc_name(self, name, fallback_catalog=""):
        return f"{fallback_catalog}.live_schema.{name}_sync"

    def table_exists(self, name):
        if self._exc:
            raise self._exc
        return self._exists

    def get_status(self, name):
        return self._status or {"count": self._count}


@contextmanager
def _existence(engine, *, graph_store, view_store, engine_cfg=None,
               view_table="c.s.view", graph_name="sales_graph"):
    def _get_graphdb(domain, settings, engine=None, **kw):
        return view_store if engine == "view" else graph_store

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(DigitalTwin, "resolve_graph_engine", return_value=engine)
        )
        stack.enter_context(
            patch("back.core.helpers.effective_view_table", return_value=view_table)
        )
        stack.enter_context(
            patch(
                "back.core.helpers.effective_graph_query_table",
                return_value=graph_name,
            )
        )
        stack.enter_context(patch("back.core.helpers.run_blocking", _run_blocking))
        stack.enter_context(
            patch("back.core.graphdb.get_graphdb", side_effect=_get_graphdb)
        )
        stack.enter_context(
            patch.object(
                GraphDBFactory,
                "_resolve_graph_engine_config",
                return_value=engine_cfg or {},
            )
        )
        stack.enter_context(
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.resolve_lakebase_graph_schema",
                return_value="cfg_schema",
            )
        )
        stack.enter_context(
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.resolve_sync_uc_fallback_catalog",
                return_value="main",
            )
        )
        yield


def _view_store(exists=True, exc=None):
    vs = MagicMock()
    if exc:
        vs.table_exists.side_effect = exc
    else:
        vs.table_exists.return_value = exists
    return vs


class TestLakebaseExistence:
    async def test_live_store_enriches_names_and_counts(self):
        store = _LakebaseStore(count=12, status={"count": 12, "database": "pg", "schema": "s1"})
        with _existence("lakebase", graph_store=store, view_store=_view_store(True)):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["view_exists"] is True
        assert result["view_check_error"] is None
        assert result["lakebase_table_exists"] is True
        assert result["graph_has_data"] is True
        assert result["triple_count"] == 12
        assert result["lakebase_database"] == "pg"
        assert result["lakebase_schema"] == "s1"
        assert result["lakebase_table"] == "sales_graph_phy"
        assert result["graph_display"] == "pg · s1 · sales_graph_phy"
        assert result["lakebase_sync_mode"] == "app_managed"
        assert result["lakebase_synced_uc_exists"] is None

    async def test_live_store_without_status_names_keeps_live_schema(self):
        store = _LakebaseStore(count=0)
        with _existence("lakebase", graph_store=store, view_store=_view_store(True)):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["lakebase_schema"] == "live_schema"
        assert result["lakebase_database"] == "db_display"
        assert result["graph_has_data"] is False
        assert result["graph_display"] == "sales_graph_phy"

    async def test_missing_table_is_a_definite_false(self):
        store = _LakebaseStore(exists=False)
        with _existence("lakebase", graph_store=store, view_store=_view_store(False)):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["lakebase_table_exists"] is False
        assert result["graph_has_data"] is False
        assert result["view_exists"] is False

    async def test_postgres_failure_yields_unknown_and_error(self):
        store = _LakebaseStore(table_exists_exc=TimeoutError("pg asleep"))
        with _existence("lakebase", graph_store=store, view_store=_view_store(True)):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["lakebase_table_exists"] is None
        assert result["graph_has_data"] is None
        assert result["lakebase_check_error"] == "pg asleep"

    async def test_configured_managed_synced_fqn_is_probed_on_the_view_warehouse(self):
        cfg = {"lakebase": {"sync_mode": "managed_synced", "sync_uc_catalog": "gold"}}
        vs = _view_store(True)
        with _existence(
            "lakebase", graph_store=_LakebaseStore(), view_store=vs, engine_cfg=cfg
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["lakebase_sync_mode"] == "managed_synced"
        assert result["lakebase_synced_uc"] == "gold.cfg_schema.sales_graph_sync"
        assert result["lakebase_synced_uc_exists"] is True
        probed = [c.args[0] for c in vs.table_exists.call_args_list]
        assert "gold.cfg_schema.sales_graph_sync" in probed
        assert "c.s.view" in probed

    async def test_managed_synced_without_catalog_uses_fallback(self):
        cfg = {"lakebase": {"sync_mode": "managed_synced"}}
        with _existence(
            "lakebase", graph_store=_LakebaseStore(), view_store=_view_store(), engine_cfg=cfg
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert result["lakebase_synced_uc"] == "main.cfg_schema.sales_graph_sync"

    async def test_live_discovered_synced_name_gets_a_follow_up_probe(self):
        vs = _view_store(False)
        with _existence(
            "lakebase", graph_store=_LakebaseStore(synced=True), view_store=vs
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["lakebase_synced_uc"] == "main.live_schema.sales_graph_sync"
        assert result["lakebase_synced_uc_exists"] is False
        vs.table_exists.assert_any_call("main.live_schema.sales_graph_sync")

    async def test_synced_name_lookup_failure_skips_the_follow_up_probe(self):
        store = _LakebaseStore(synced=True)
        store.synced_uc_name = MagicMock(side_effect=RuntimeError("no catalog"))
        vs = _view_store(True)
        with _existence("lakebase", graph_store=store, view_store=vs):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["lakebase_synced_uc"] == ""
        assert result["lakebase_synced_uc_exists"] is None
        assert result["lakebase_table_exists"] is True
        vs.table_exists.assert_called_once_with("c.s.view")

    async def test_uc_probe_failure_yields_unknown(self):
        cfg = {"lakebase": {"sync_mode": "managed_synced", "sync_uc_catalog": "gold"}}
        vs = MagicMock()
        vs.table_exists.side_effect = lambda name: (
            True if name == "c.s.view" else (_ for _ in ()).throw(RuntimeError("x"))
        )
        with _existence(
            "lakebase", graph_store=_LakebaseStore(), view_store=vs, engine_cfg=cfg
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert result["lakebase_synced_uc_exists"] is None
        assert result["view_exists"] is True

    async def test_view_probe_reports_missing_view_name(self):
        with _existence(
            "lakebase", graph_store=None, view_store=_view_store(), view_table=""
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert result["view_exists"] is None
        assert "No view name resolved" in result["view_check_error"]

    async def test_view_probe_rejects_unqualified_name(self):
        with _existence(
            "lakebase", graph_store=None, view_store=_view_store(), view_table="bare"
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert "not fully qualified" in result["view_check_error"]

    async def test_view_probe_without_warehouse(self):
        with _existence("lakebase", graph_store=None, view_store=None):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert "No SQL warehouse available" in result["view_check_error"]
        assert result["graph_has_data"] is None

    async def test_view_probe_failure_is_reported(self):
        with _existence(
            "lakebase",
            graph_store=None,
            view_store=_view_store(exc=RuntimeError("warehouse cold")),
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert result["view_exists"] is None
        assert "warehouse cold" in result["view_check_error"]

    async def test_config_resolution_failure_still_probes(self):
        with _existence(
            "lakebase", graph_store=_LakebaseStore(), view_store=_view_store()
        ), patch.object(
            GraphDBFactory,
            "_resolve_graph_engine_config",
            side_effect=RuntimeError("cfg"),
        ):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)
        assert result["lakebase_table_exists"] is True
        assert result["lakebase_sync_mode"] == "app_managed"


# ---------------------------------------------------------------------------
# fetch_digital_twin_existence — Neo4j path
# ---------------------------------------------------------------------------


class Neo4jStore:
    def __init__(self, *, exists=True, count=5, exc=None, close_exc=None):
        self._database = "neo_db"
        self._exists = exists
        self._count = count
        self._exc = exc
        self._close_exc = close_exc
        self.closed = False

    def table_exists(self, name):
        if self._exc:
            raise self._exc
        return self._exists

    def count_triples(self, name):
        return self._count

    def close(self):
        self.closed = True
        if self._close_exc:
            raise self._close_exc


class TestNeo4jExistence:
    async def test_existing_graph_reports_count_marker_and_closes(self):
        store = Neo4jStore(count=7)
        with _existence("neo4j", graph_store=store, view_store=None, graph_name="my-graph"):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["neo4j_graph_exists"] is True
        assert result["triple_count"] == 7
        assert result["graph_has_data"] is True
        assert result["neo4j_database"] == "neo_db"
        assert result["neo4j_marker_label"] == "my_graph"
        assert result["graph_display"] == "neo_db · my_graph"
        assert result["view_exists"] is None
        assert store.closed is True

    async def test_configured_database_wins_over_connection_default(self):
        dom = _domain(info={"neo4j_database": "configured"})
        with _existence("neo4j", graph_store=Neo4jStore(exists=False), view_store=None):
            result = await TwinStoreCache(dom).fetch_digital_twin_existence(None)

        assert result["neo4j_database"] == "configured"
        assert result["graph_has_data"] is False

    async def test_non_neo4j_store_is_reported_unavailable(self):
        with _existence("neo4j", graph_store=MagicMock(), view_store=None):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert "Neo4j backend unavailable" in result["neo4j_check_error"]
        assert result["neo4j_graph_exists"] is None

    async def test_probe_failure_is_reported_and_store_still_closed(self):
        store = Neo4jStore(exc=RuntimeError("bolt down"), close_exc=RuntimeError("x"))
        with _existence("neo4j", graph_store=store, view_store=None):
            result = await TwinStoreCache(_domain()).fetch_digital_twin_existence(None)

        assert result["neo4j_check_error"] == "bolt down"
        assert store.closed is True
        assert result["neo4j_marker_label"] == "sales_graph"
