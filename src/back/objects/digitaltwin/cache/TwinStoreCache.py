"""Session-cached graph status and digital-twin artefact existence.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

logger = get_logger(__name__)

# Session TTL for ``domain.triplestore.stats`` sections (status, dt_existence, aggregate stats).
_TS_STATS_CACHE_TTL_SECONDS = 300


def _digital_twin():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class TwinStoreCache:
    """Per-section TTL cache plus live graph / Lakebase / Neo4j existence probes."""

    def __init__(self, domain) -> None:
        self._domain = domain

    def get_ts_cache(self, section: str) -> Optional[dict]:
        """Read a cached triplestore section (e.g. ``'stats'``, ``'status'``) from the domain.

        Returns ``None`` when the section is missing or the cache is older than
        :data:`_TS_STATS_CACHE_TTL_SECONDS`.

        Each section now carries its own ``_ts`` timestamp so that refreshing
        one section (e.g. ``status``) does not inadvertently extend the TTL of
        another (e.g. ``dt_existence``), which would cause stale cross-section
        data to appear fresh and produce contradictory UI badges.
        """
        ts = self._domain.triplestore or {}
        stats = ts.get("stats", {})
        if not isinstance(stats, dict):
            return None
        entry = stats.get(section)
        if not isinstance(entry, dict):
            return None

        # Per-section timestamp (preferred).
        section_ts = entry.get("_ts")
        if section_ts is not None:
            if (time.time() - float(section_ts)) > _TS_STATS_CACHE_TTL_SECONDS:
                return None
            return {k: v for k, v in entry.items() if k != "_ts"}

        # Fallback: shared timestamp written by older code paths.
        shared_ts = ts.get("_ts_cache_timestamp")
        if shared_ts is None:
            return None
        if (time.time() - float(shared_ts)) > _TS_STATS_CACHE_TTL_SECONDS:
            return None
        return entry

    def set_ts_cache(self, section: str, data: dict):
        """Write a cached triplestore section and persist to session.

        Each section is stored with its own ``_ts`` timestamp so that sections
        expire independently (prevents stale ``dt_existence`` from surviving a
        fresh ``status`` write that bumps the shared clock).
        """
        ts = self._domain.triplestore
        if "stats" not in ts:
            ts["stats"] = {}
        ts["stats"][section] = {**data, "_ts": time.time()}
        # Keep the shared key for any legacy readers.
        ts["_ts_cache_timestamp"] = time.time()
        self._domain.save()

    def clear_ts_cache(self, section: str) -> None:
        """Drop a cached triplestore section so the next read recomputes it.

        The TTL alone cannot cover a setting that changes what a section
        *reports*: ``stats`` carries the analytics job availability, so an admin
        flipping the Settings → Global toggle would otherwise keep reading the
        pre-change answer until the entry expired.
        """
        ts = self._domain.triplestore or {}
        stats = ts.get("stats")
        if not isinstance(stats, dict) or section not in stats:
            return
        del stats[section]
        self._domain.save()

    async def get_or_fetch_graph_status(
        self, settings, force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Return graph triplestore status from session cache, or fetch live and cache.

        Inconclusive results (``has_data is None`` — the probe could not reach the
        engine) are never cached, for the same reason as
        :meth:`get_or_fetch_dt_existence`: this section drives the "Graph not
        built" badge on every KG sub-page, so caching a transient engine timeout
        as a confirmed absence contradicts the Build page — which force-refreshes
        its own live probe — for the whole TTL.

        ``force_refresh=True`` bypasses the cache so a page can re-establish the
        truth without waiting the TTL out.
        """
        if not force_refresh:
            cached = self.get_ts_cache("status")
            if cached:
                logger.debug("get_or_fetch_graph_status: serving from cache")
                return cached
            logger.debug("get_or_fetch_graph_status: cache miss — fetching live")
        else:
            logger.debug("get_or_fetch_graph_status: force_refresh — fetching live")

        result = await self.fetch_graph_triplestore_status(settings)
        if result.get("has_data") is not None:
            self.set_ts_cache("status", result)
        else:
            logger.debug(
                "get_or_fetch_graph_status: probe inconclusive (%s) — not caching",
                result.get("graph_check_error") or "unknown",
            )
        return result

    async def get_or_fetch_dt_existence(
        self, settings, force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Return DT artefact existence from session cache, or fetch live and cache.

        ``force_refresh=True`` bypasses the session cache. Pages that must show
        the *current* state of Lakebase (Build, Cockpit) pass this so a stale
        ``lakebase_table_exists=False`` poisoned by a transient Postgres
        timeout does not survive across navigations.

        Unknown results (``lakebase_table_exists is None`` — probe failed) are
        never cached: caching a transient failure as if it were a confirmed
        absence is the bug this whole pathway exists to prevent.
        """
        if not force_refresh:
            cached = self.get_ts_cache("dt_existence")
            if cached:
                logger.debug("get_or_fetch_dt_existence: serving from cache")
                return cached
            logger.debug("get_or_fetch_dt_existence: cache miss — fetching live")
        else:
            logger.debug("get_or_fetch_dt_existence: force_refresh — fetching live")

        result = await self.fetch_digital_twin_existence(settings)
        if result.get("lakebase_table_exists") is not None:
            self.set_ts_cache("dt_existence", result)
        else:
            logger.debug(
                "get_or_fetch_dt_existence: probe inconclusive (%s) — not caching",
                result.get("lakebase_check_error") or "unknown",
            )
        return result

    def pending_dt_existence(self, settings) -> Dict[str, Any]:
        """Cheap, non-blocking existence skeleton for the first page paint.

        Resolves artefact *names* from config (no network round-trip) and
        leaves every existence flag as ``None`` with ``pending=True``. The Build
        page renders this instantly, then confirms the live Lakebase/UC state via
        a non-blocking follow-up to ``/dtwin/sync/dt-existence``. This keeps the
        cold SQL-warehouse / Lakebase wake-up entirely off the request path.
        """
        from back.core.helpers import (
            effective_graph_name,
            effective_graph_query_table,
            effective_view_table,
        )

        domain = self._domain
        graph_engine = _digital_twin().resolve_graph_engine(domain, settings)
        view_table = effective_view_table(domain)
        graph_name = effective_graph_query_table(domain, settings)

        result: Dict[str, Any] = {
            "view_exists": None,
            "graph_engine": graph_engine,
            "graph_has_data": None,
            "lakebase_table_exists": None,
            "lakebase_synced_uc_exists": None,
            "lakebase_check_error": None,
            "view_table": view_table,
            "graph_name": graph_name or effective_graph_name(domain),
            "graph_display": "",
            "last_update": domain.last_update or None,
            "last_built": domain.last_build or None,
            "view_check_error": None,
            "triple_count": 0,
            "pending": True,
            "lakebase_database": "",
            "lakebase_schema": "",
            "lakebase_table": "",
            "lakebase_synced_uc": "",
        }

        # Resolve Lakebase artefact names from engine config only — no probes.
        if graph_engine == "lakebase":
            try:
                from back.core.graphdb import GraphDBFactory
                from back.core.graphdb.engine_config import lakebase_section
                from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase
                from back.core.graphdb.lakebase.LakebaseFlatStore import (
                    resolve_lakebase_graph_schema,
                    resolve_sync_uc_fallback_catalog,
                )
                from back.core.graphdb.lakebase._companion_ddl import synced_phy

                engine_config = lakebase_section(
                    GraphDBFactory._resolve_graph_engine_config(domain, settings) or {}
                )
                sync_mode = (
                    str(engine_config.get("sync_mode") or "app_managed").strip()
                    or "app_managed"
                )
                schema_raw = str(engine_config.get("schema") or "").strip()
                lk_schema = resolve_lakebase_graph_schema(domain, settings, schema_raw)
                lk_table = (
                    LakebaseBase.physical_table_id(graph_name) if graph_name else ""
                )
                result["lakebase_schema"] = lk_schema
                result["lakebase_table"] = lk_table
                # Database display needs a live connection; leave blank while pending.
                if sync_mode == "managed_synced" and lk_schema and graph_name:
                    catalog = str(engine_config.get("sync_uc_catalog") or "").strip()
                    if not catalog:
                        catalog = resolve_sync_uc_fallback_catalog(domain, settings)
                    if catalog:
                        result["lakebase_synced_uc"] = (
                            f"{catalog}.{lk_schema}.{synced_phy(graph_name)}"
                        )
            except Exception as exc:  # noqa: BLE001 — keep skeleton best-effort
                logger.debug(
                    "pending_dt_existence: lakebase name resolution failed: %s", exc
                )

        return result

    def sync_last_build_from_schedule(self, settings) -> None:
        """Pull the latest successful scheduled-build timestamp into the session."""
        domain = self._domain
        try:
            folder = domain.domain_folder
            if not folder:
                return
            from back.objects.registry import get_scheduler, RegistryCfg

            scheduler = get_scheduler()
            if not scheduler._started:
                return
            from back.core.helpers import get_databricks_host_and_token

            host, token = get_databricks_host_and_token(domain, settings)
            registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
            if not host or not registry_cfg.get("catalog"):
                return
            from back.objects.session import global_config_service

            cfg = global_config_service.load(host, token, registry_cfg)
            schedules = cfg.get("schedules") or {}
            sched = schedules.get(folder)
            if not sched:
                return
            if sched.get("last_status") != "success":
                return
            sched_ts = sched.get("last_run", "")
            if sched_ts and sched_ts > (domain.last_build or ""):
                logger.info(
                    "Syncing last_build from schedule: %s -> %s",
                    domain.last_build or "(empty)",
                    sched_ts,
                )
                domain.last_build = sched_ts
                domain.save()
        except Exception as exc:
            logger.debug("sync_last_build_from_schedule: %s", exc)

    async def fetch_graph_triplestore_status(self, settings) -> Dict[str, Any]:
        """Live graph backend row count and paths.

        ``has_data`` is tri-state: ``True``/``False`` once the probe has answered,
        and ``None`` when the engine could not be reached. A failed probe must
        never be reported as ``False`` — callers cache this answer and render the
        KG readiness badge from it, so one timeout against a remote engine would
        otherwise tell users to rebuild a graph they already have.
        ``graph_check_error`` carries the reason when ``has_data`` is ``None``.
        """
        from back.core.helpers import (
            effective_graph_name,
            effective_graph_query_table,
            effective_view_table,
            run_blocking,
        )
        from back.core.graphdb import get_graphdb

        domain = self._domain
        try:
            graph_name = effective_graph_query_table(domain, settings)
            view_table = effective_view_table(domain)
            graph_store = get_graphdb(domain, settings)
            graph_exists = False
            graph_count = 0
            graph_path = None
            probe_error = None
            if graph_store:
                try:
                    graph_exists = bool(
                        await run_blocking(graph_store.table_exists, graph_name)
                    )
                    if graph_exists:
                        gs = await run_blocking(graph_store.get_status, graph_name)
                        graph_count = int(gs.get("count", 0) or 0)
                        graph_path = gs.get("path")
                except Exception as e:
                    logger.warning("Graph status check failed: %s", e)
                    probe_error = str(e) or e.__class__.__name__

            graph_ok = None if probe_error else (graph_exists and graph_count > 0)
            build_stamp = (domain.triplestore or {}).get("build_last_update")
            result: Dict[str, Any] = {
                "success": True,
                "has_data": graph_ok,
                "count": graph_count,
                "view_table": view_table,
                "graph_name": graph_name,
                "graph_check_error": probe_error,
            }
            if build_stamp and graph_ok:
                result["last_modified"] = build_stamp
            if graph_path:
                result["path"] = graph_path
            if graph_ok is None:
                result["reason"] = "Could not check the graph status"
            elif not graph_ok:
                # Keyed on existence, not on the count: an existing-but-empty
                # graph used to be reported as never built.
                result["reason"] = (
                    "Graph is empty" if graph_exists else "Graph does not exist yet"
                )
            return result
        except Exception as e:
            logger.exception("fetch_graph_triplestore_status failed: %s", e)
            raise InfrastructureError(
                "Could not load graph triplestore status.",
                detail=str(e),
            ) from e

    async def fetch_digital_twin_existence(self, settings) -> Dict[str, Any]:
        """Live checks for SQL view, snapshot table, and graph artefacts.

        Lakebase: Postgres triple table existence/count (no Volume archive).

        The three live checks — SQL-warehouse view existence, Lakebase Postgres
        table existence/count, and UC synced-table existence — are independent
        network round-trips and run concurrently, so the slowest probe (not
        their sum) bounds the latency. On a cold SQL warehouse or a sleeping
        Lakebase instance this collapses three serial wake-ups into one.
        """
        import asyncio

        from back.core.helpers import (
            effective_graph_name,
            effective_graph_query_table,
            effective_view_table,
            run_blocking,
        )
        from back.core.graphdb import get_graphdb

        domain = self._domain
        graph_engine = _digital_twin().resolve_graph_engine(domain, settings)
        view_table = effective_view_table(domain)
        graph_name = effective_graph_query_table(domain, settings)
        last_built = domain.last_build or None
        last_update = domain.last_update or None

        result: Dict[str, Any] = {
            "view_exists": None,
            "graph_engine": graph_engine,
            "graph_has_data": None,
            "lakebase_table_exists": None,
            "lakebase_synced_uc_exists": None,
            "lakebase_check_error": None,
            "view_table": view_table,
            "graph_name": graph_name,
            "graph_display": "",
            "last_update": last_update,
            "last_built": last_built,
            "view_check_error": None,
            "triple_count": 0,
        }

        # --- Neo4j engine: no SQL VIEW / UC-sync / Postgres — probe the graph
        #     directly over Bolt and return engine-specific fields. The Neo4j
        #     path writes a typed property graph, so there is no view_table /
        #     synced-UC bridge to check; the "Triple Store" card still shows the
        #     source Delta view name for context but its existence is N/A. ---
        if graph_engine == "neo4j":
            return await self._fetch_neo4j_existence(
                settings, result, graph_name, run_blocking
            )

        # --- Resolve config without needing a Postgres connection (sync) ---
        lk_sync_mode = "app_managed"
        lk_schema = ""
        lk_table = ""
        lk_synced_uc_cfg = ""
        try:
            from back.core.graphdb import GraphDBFactory
            from back.core.graphdb.engine_config import lakebase_section
            from back.core.graphdb.lakebase.LakebaseFlatStore import (
                resolve_sync_uc_fallback_catalog,
                resolve_lakebase_graph_schema,
            )
            from back.core.graphdb.lakebase._companion_ddl import synced_phy

            engine_config = lakebase_section(
                GraphDBFactory._resolve_graph_engine_config(domain, settings) or {}
            )
            lk_sync_mode = str(engine_config.get("sync_mode") or "app_managed").strip() or "app_managed"

            # Populate schema/table from config so the card shows values even without Postgres
            schema_raw = str(engine_config.get("schema") or "").strip()
            lk_schema = resolve_lakebase_graph_schema(domain, settings, schema_raw)
            if graph_name:
                from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase
                lk_table = LakebaseBase.physical_table_id(graph_name)

            # Compute UC sync FQN (managed_synced only)
            if lk_sync_mode == "managed_synced":
                catalog = str(engine_config.get("sync_uc_catalog") or "").strip()
                if not catalog:
                    catalog = resolve_sync_uc_fallback_catalog(domain, settings)
                uc_schema = lk_schema  # always equals the graph schema
                if catalog and uc_schema:
                    lk_synced_uc_cfg = f"{catalog}.{uc_schema}.{synced_phy(graph_name)}"
        except Exception as e:
            logger.warning("DT existence: lakebase config resolution failed: %s", e)

        # --- Probe 1: SQL view existence (SQL warehouse) ---
        async def _view_probe():
            if not view_table:
                return None, "No view name resolved (domain.delta.catalog/schema/name missing)"
            if "." not in view_table:
                return None, f"Resolved view name is not fully qualified: {view_table}"
            try:
                view_store = get_graphdb(domain, settings, engine="view")
                if not view_store:
                    return None, (
                        "No SQL warehouse available "
                        "(set domain.databricks.sql_warehouse_id or settings.databricks_warehouse_id)"
                    )
                exists = await run_blocking(view_store.table_exists, view_table)
                logger.info("DT existence: VIEW %s -> exists=%s", view_table, exists)
                return exists, None
            except Exception as e:
                logger.warning("DT existence: VIEW %s check failed: %s", view_table, e)
                return None, f"View check failed: {e}"

        # --- Probe 2: live Lakebase Postgres check (enriches display) ---
        async def _postgres_probe():
            data = {
                "exists_tbl": None,
                "cnt": 0,
                "display": "",
                "lk_database": "",
                "lk_schema": lk_schema,
                "lk_table": lk_table,
                "lk_synced_uc": "",
                "lk_check_error": None,
            }
            try:
                from back.core.graphdb.lakebase.LakebaseFlatStore import (
                    resolve_sync_uc_fallback_catalog,
                )

                graph_store = get_graphdb(domain, settings)
                if graph_store:
                    lk_schema_live = getattr(graph_store, "graph_schema", "") or ""
                    tbl_fn = getattr(graph_store, "physical_table_id", None)
                    lk_table_live = tbl_fn(graph_name) if callable(tbl_fn) else ""
                    db_fn = getattr(graph_store, "_effective_database_display", None)
                    if lk_schema_live:
                        data["lk_schema"] = lk_schema_live
                    if lk_table_live:
                        data["lk_table"] = lk_table_live
                    if callable(db_fn):
                        data["lk_database"] = db_fn() or ""
                    if getattr(graph_store, "is_synced", False) and not lk_synced_uc_cfg:
                        try:
                            fallback_cat = resolve_sync_uc_fallback_catalog(domain, settings)
                            data["lk_synced_uc"] = graph_store.synced_uc_name(
                                graph_name, fallback_catalog=fallback_cat
                            )
                        except Exception:
                            pass
                    exists_tbl = await run_blocking(graph_store.table_exists, graph_name)
                    data["exists_tbl"] = exists_tbl
                    if exists_tbl:
                        gs = await run_blocking(graph_store.get_status, graph_name)
                        data["cnt"] = int(gs.get("count", 0) or 0)
                        dbpart = str(gs.get("database") or "").strip()
                        schpart = str(gs.get("schema") or "").strip()
                        if dbpart:
                            data["lk_database"] = dbpart
                        if schpart:
                            data["lk_schema"] = schpart
                        parts = [p for p in (dbpart, schpart, data["lk_table"]) if p]
                        data["display"] = " · ".join(parts) if parts else data["lk_table"]
            except Exception as e:
                logger.warning("DT existence: lakebase graph check failed: %s", e)
                data["lk_check_error"] = str(e)
            return data

        # --- Probe 3: UC synced-table existence (SQL warehouse) ---
        async def _uc_probe(uc_fqn):
            if not uc_fqn:
                return None
            try:
                view_store_uc = get_graphdb(domain, settings, engine="view")
                if view_store_uc:
                    exists = await run_blocking(view_store_uc.table_exists, uc_fqn)
                    logger.info(
                        "DT existence: synced UC table %s -> exists=%s", uc_fqn, exists
                    )
                    return exists
            except Exception as e:
                logger.warning(
                    "DT existence: synced UC table %s check failed: %s", uc_fqn, e
                )
            return None

        view_res, pg, uc_cfg_exists = await asyncio.gather(
            _view_probe(),
            _postgres_probe(),
            _uc_probe(lk_synced_uc_cfg),
        )

        view_ok, view_err = view_res
        result["view_exists"] = view_ok
        result["view_check_error"] = view_err

        result["triple_count"] = pg["cnt"]
        # Preserve the tri-state: True=present, False=absent, None=unknown
        # (probe failed/timed out). Caller must NOT cache unknown results.
        if pg["exists_tbl"] is None:
            result["graph_has_data"] = None
        else:
            result["graph_has_data"] = bool(pg["exists_tbl"] and pg["cnt"] > 0)
        result["lakebase_table_exists"] = pg["exists_tbl"]
        result["lakebase_check_error"] = pg["lk_check_error"]
        result["graph_display"] = pg["display"] or ""
        result["lakebase_database"] = pg["lk_database"]
        result["lakebase_schema"] = pg["lk_schema"]
        result["lakebase_table"] = pg["lk_table"]
        result["lakebase_sync_mode"] = lk_sync_mode

        lk_synced_uc = lk_synced_uc_cfg or pg["lk_synced_uc"] or ""
        result["lakebase_synced_uc"] = lk_synced_uc

        if lk_synced_uc_cfg:
            # FQN known from config — its probe already ran in the gather above.
            result["lakebase_synced_uc_exists"] = uc_cfg_exists
        elif lk_synced_uc:
            # FQN discovered live via Postgres — confirm with a follow-up probe.
            result["lakebase_synced_uc_exists"] = await _uc_probe(lk_synced_uc)

        return result

    async def _fetch_neo4j_existence(
        self, settings, result: Dict[str, Any], graph_name: str, run_blocking
    ) -> Dict[str, Any]:
        """Neo4j existence probe for the Build page (typed property graph).

        Populates ``neo4j_*`` fields: whether the graph's marker label exists,
        the reconstructed triple count, the configured database, and a display
        FQN ``<database> · <marker>``. No SQL VIEW / UC-sync / Postgres probes
        run on this path (Neo4j writes over Bolt at build time).
        """
        from back.core.graphdb import get_graphdb

        domain = self._domain
        result["neo4j_graph_exists"] = None
        result["neo4j_database"] = str(
            (domain.info or {}).get("neo4j_database") or ""
        ).strip()
        result["neo4j_check_error"] = None
        # The SQL VIEW existence is not part of the Neo4j path; mark N/A so the
        # UI does not show a misleading "Not found".
        result["view_exists"] = None

        try:
            # Use the AUTO path (engine=None): it resolves the engine from the
            # per-domain backend AND loads the saved connection config from
            # GlobalConfigService. Passing engine="neo4j" explicitly would hand
            # _create_neo4j an empty engine_config (no URI) → None, which is why
            # this previously reported "Neo4j backend unavailable" even though
            # the connection (Settings → Neo4j → Test/Health) works.
            store = get_graphdb(domain, settings)
            if store is None or store.__class__.__name__ != "Neo4jStore":
                result["neo4j_check_error"] = (
                    "Neo4j backend unavailable (check the connection in Settings → Neo4j)."
                )
                return result
            try:
                exists = await run_blocking(store.table_exists, graph_name)
                result["neo4j_graph_exists"] = exists
                if not result["neo4j_database"]:
                    # Fall back to the connection's effective database.
                    result["neo4j_database"] = getattr(store, "_database", "") or ""
                if exists:
                    cnt = await run_blocking(store.count_triples, graph_name)
                    result["triple_count"] = int(cnt or 0)
                    result["graph_has_data"] = result["triple_count"] > 0
                else:
                    result["graph_has_data"] = False
            finally:
                try:
                    store.close()
                except Exception:  # noqa: BLE001
                    pass
        except Exception as e:  # noqa: BLE001
            logger.warning("DT existence: neo4j graph check failed: %s", e)
            result["neo4j_check_error"] = str(e)

        from back.core.graphdb.neo4j.Neo4jWriteOps import sanitise_label

        marker = sanitise_label(graph_name) if graph_name else ""
        db = result["neo4j_database"]
        result["neo4j_marker_label"] = marker
        result["graph_display"] = " · ".join([p for p in (db, marker) if p]) or marker
        return result
