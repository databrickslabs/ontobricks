"""Where a domain's incremental materialization stands: pipeline, tables, sync, last build."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

logger = get_logger(__name__)

#: Relation kinds with a Delta history (``DESCRIBE HISTORY``).
_VERSIONED_KINDS = frozenset({"mv", "table", "streaming_table"})

#: Newest runs per domain the overview searches for a main build (ticks pile up).
_OVERVIEW_RUNS_LIMIT = 50

#: The report's sections besides ``mode_requested``.
_SECTIONS = (
    "slot_pipeline",
    "data",
    "slots",
    "freshness_lag_s",
    "lakebase_sync",
    "last_build",
    "realtime",
)


class TwinIncrementalStatus:
    """Read-only status report of a domain's incremental materialization.

    Every section is read on its own and best-effort: a read that fails puts
    ``error`` (and an ``unknown`` state) in its section instead of failing the
    report, so one unreachable service does not hide the others. Nothing is
    written.
    """

    @staticmethod
    def report(
        domain: Any,
        settings: Any,
        client: Any,
        *,
        get_store: Callable[..., Any] | None = None,
        now: datetime | None = None,
    ) -> dict:
        """``mode_requested``, ``read_relation``, ``slot_pipeline``, ``data``, ``slots``,
        ``freshness_lag_s``, ``lakebase_sync``, ``last_build``, ``realtime`` (a
        ``realtime`` domain's stream run).

        ``read_relation`` is what Lakehouse graph reads take (``GraphReadRelation``):
        ``graph`` (``…_graph``) or ``slots_live`` (the Lakehouse//RT fast path).
        ``freshness_lag_s`` is *now* (default: the current UTC time) minus the
        last ``_data`` commit, ``None`` when that commit is unknown. A ``full``
        domain (the default; both build panels ask on page load) is answered
        without any warehouse, pipelines or registry call: every section ``None``.
        """
        from back.core.graphdb.delta import GraphReadRelation, _table_naming
        from back.core.graphdb.GraphDBFactory import (
            GraphDBFactory,
            normalize_materialization_mode,
        )

        requested = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(domain, settings)
        )
        read_relation = GraphReadRelation.decide(domain, settings, client=client)
        if requested == "full":
            return {
                "mode_requested": requested,
                "read_relation": read_relation,
                **dict.fromkeys(_SECTIONS),
            }
        data = TwinIncrementalStatus.relation(client, _table_naming.data_table_fqn(domain, settings))
        slots = TwinIncrementalStatus.relation(
            client, _table_naming.slots_table_fqn(domain, settings)
        )
        return {
            "mode_requested": requested,
            "read_relation": read_relation,
            "slot_pipeline": TwinIncrementalStatus.slot_pipeline(domain),
            "data": data,
            "slots": slots,
            "freshness_lag_s": TwinIncrementalStatus._lag_s(data.get("committed_at"), now),
            "lakebase_sync": TwinIncrementalStatus.lakebase_sync(domain, settings, get_store),
            "last_build": TwinIncrementalStatus.last_build(domain, settings),
            "realtime": (
                TwinIncrementalStatus.realtime_stream(domain, settings)
                if requested == "realtime"
                else None
            ),
        }

    @staticmethod
    def overview(registry: Any) -> dict:
        """``domains_per_mode``, ``fallbacks_by_reason``, ``domains`` and ``domains_skipped``.

        Every domain *registry* lists (``list_domain_details_cached``) counts
        with its newest main build run among its newest 50 runs of any version
        (:meth:`TwinBuildRunHistory.newest_main_run`: scheduled ticks are
        skipped, status and version are not looked at): ``{name, version,
        mode_requested, mode_effective, strategy, last_build_at,
        fallback_reason}`` (``strategy`` — ``mv`` / ``merge`` / ``ctas`` — tells
        a MERGE-maintained ``incremental`` from an MV one; ``""`` when not
        recorded). A domain without one (never built, only ticks lately, or its
        runs unreadable: the store answers ``[]`` on an error) is left out and
        counted in ``domains_skipped``. ``domains_per_mode`` counts the domains
        per effective mode, ``fallbacks_by_reason`` the domains whose newest
        main run fell back, per ``mode_fallback_reason``. One
        ``load_build_runs`` per domain: an admin KPI, not a hot path. Nothing is
        written; a registry that cannot list its domains raises
        :class:`InfrastructureError`.
        """
        ok, details, message = registry.list_domain_details_cached()
        if not ok:
            raise InfrastructureError("Could not list the registry domains", detail=message)
        names = [str(d.get("name") or "") for d in details if isinstance(d, dict)]
        domains = []
        for name in names:
            runs = registry.load_build_runs(name, limit=_OVERVIEW_RUNS_LIMIT) if name else []
            run = TwinBuildRunHistory.newest_main_run(runs)
            if run is not None:
                domains.append(TwinIncrementalStatus._overview_row(name, run))
        return {
            "domains_per_mode": TwinIncrementalStatus._tally(domains, "mode_effective"),
            "fallbacks_by_reason": TwinIncrementalStatus._tally(domains, "fallback_reason"),
            "domains": domains,
            "domains_skipped": len(names) - len(domains),
        }

    @staticmethod
    def _overview_row(name: str, run: dict) -> dict:
        """One :meth:`overview` domain row from the domain's newest main *run*."""
        facts = TwinBuildRunHistory.facts_of(run) or {}
        return {
            "name": name,
            "version": str(run.get("version") or ""),
            "mode_requested": str(facts.get("materialization_mode_requested") or ""),
            "mode_effective": str(facts.get("materialization_mode_effective") or ""),
            "strategy": str(facts.get("strategy") or ""),
            "last_build_at": run.get("finished_at") or run.get("started_at") or "",
            "fallback_reason": str(facts.get("mode_fallback_reason") or ""),
        }

    @staticmethod
    def _tally(domains: list, key: str) -> dict:
        """``{value: number of domains}`` of *key*'s non-empty values."""
        return dict(Counter(d[key] for d in domains if d[key]))

    @staticmethod
    def realtime_stream(domain: Any, settings: Any) -> dict:
        """``{run_id, state}`` of the domain version's stream job (``TwinRealtimeStream.status``)."""
        from back.objects.digitaltwin.build.TwinRealtimeStream import TwinRealtimeStream

        return TwinRealtimeStream(settings).status(domain)

    @staticmethod
    def relation(client: Any, fqn: str) -> dict:
        """``{table, kind, version, committed_at}`` of *fqn*; a history only when it has one."""
        from back.core.graphdb.delta import ChangeFeedReader, materialize

        status: dict = {"table": fqn, "kind": "absent", "version": None, "committed_at": None}
        if not fqn:
            return status
        try:
            status["kind"] = materialize.relation_kind(client, fqn)
        except OntoBricksError as exc:
            return {**status, "kind": "unknown", "error": str(exc)}
        if status["kind"] in _VERSIONED_KINDS:
            try:
                status.update(ChangeFeedReader.last_commit(client, fqn))
            except OntoBricksError as exc:
                status["error"] = str(exc)
        return status

    @staticmethod
    def slot_pipeline(domain: Any) -> dict:
        """``{name, id, state}`` of the domain version's slot pipeline (``absent`` without)."""
        from back.core.streaming import SlotPipelineRunner

        _folder, version = TwinBuildRunHistory.run_key_of(domain)
        domain_name = (getattr(domain, "info", None) or {}).get("name", "")
        name = SlotPipelineRunner.pipeline_name(domain_name, version)
        try:
            runner = SlotPipelineRunner()
            pipeline_id = runner.find_pipeline_id(name)
            state = runner.get_state(pipeline_id) if pipeline_id else "absent"
        except OntoBricksError as exc:
            return {"name": name, "id": None, "state": "unknown", "error": str(exc)}
        return {"name": name, "id": pipeline_id, "state": state}

    @staticmethod
    def lakebase_sync(
        domain: Any, settings: Any, get_store: Callable[..., Any] | None = None
    ) -> dict | None:
        """``{synced_table, state, message}`` of a ``managed_synced`` store, else ``None``."""
        from back.core.graphdb import get_graphdb
        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )
        from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager
        from back.core.helpers import effective_graph_name

        name = ""
        try:
            store = (get_store or get_graphdb)(domain, settings)
            if getattr(store, "is_synced", False) is not True:
                return None
            fallback = resolve_sync_uc_fallback_catalog(
                domain, settings, getattr(domain, "delta", None) or {}
            )
            name = store.synced_uc_name(effective_graph_name(domain), fallback_catalog=fallback)
            synced = store.synced_manager().get(name)
        except Exception as exc:  # noqa: BLE001 — SDK errors arrive raw; reported in the section
            logger.warning("Incremental status: synced table %s unreadable: %s", name, exc)
            return {"synced_table": name, "state": "UNKNOWN", "error": str(exc)}
        if synced is None:
            return {"synced_table": name, "state": "MISSING", "message": ""}
        return {
            "synced_table": name,
            "state": SyncedTableManager._extract_state(synced) or "UNKNOWN",
            "message": SyncedTableManager._extract_status_message(synced),
        }

    @staticmethod
    def last_build(domain: Any, settings: Any) -> dict | None:
        """``{status, build_kind, finished_at, materialization}`` of the newest run, or ``None``."""
        from back.objects.registry.RegistryService import RegistryService

        folder, version = TwinBuildRunHistory.run_key_of(domain)
        if not (folder and version):
            return None
        try:
            runs = RegistryService.from_context(domain, settings).load_build_runs(
                folder, version=version, limit=1
            )
        except Exception as exc:  # noqa: BLE001 — registry backends raise their own errors
            logger.warning("Incremental status: build runs of %s unreadable: %s", folder, exc)
            return {"error": str(exc)}
        run = runs[0] if isinstance(runs, list) and runs and isinstance(runs[0], dict) else None
        if run is None:
            return None
        stats = run.get("stats") if isinstance(run.get("stats"), dict) else {}
        return {
            "status": run.get("status", ""),
            "build_kind": run.get("build_kind", ""),
            "finished_at": run.get("finished_at", ""),
            "materialization": stats.get("materialization") or {},
        }

    @staticmethod
    def _lag_s(committed_at: Any, now: datetime | None) -> int | None:
        """Whole seconds from *committed_at* (ISO 8601; naive is UTC) to *now*, never negative."""
        if not committed_at:
            return None
        try:
            stamp = datetime.fromisoformat(str(committed_at))
        except ValueError:
            return None
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0, int((current - stamp).total_seconds()))
