"""The build-run trace a build writes, and the earlier runs a build continues from.

Extracted from :class:`TwinBuildPostMaterialize` (Fowler Extract Class), in the
same style: static methods that take the running pipeline (*pipe*).
:meth:`TwinBuildRunHistory.build_run_stats` is what ``_record_build_run``
persists under ``build_runs.stats``; :meth:`previous_materialization_stats`
reads those facts back for the net change apply and the MERGE fingerprint skip;
:meth:`persist_last_build_to_registry` stamps the registry's ``last_build``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from back.core.graphdb.ModeDecision import ModeDecision
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)

logger = get_logger(__name__)

#: ``apply_mode`` values after which the store matches ``…_data``: a run that
#: ended with one of them is a base a later build can continue from. On the
#: Delta engine ``…_data`` is the graph, so ``none`` (no store write) is one too.
_BASE_APPLY_MODES = ("net_changes", "full_stream", "lakeflow_sync")
_DELTA_BASE_APPLY_MODES = _BASE_APPLY_MODES + ("none",)

#: How many recent runs are searched for that base (skipped builds pile up).
_PREVIOUS_RUNS_LIMIT = 50


class TwinBuildRunHistory:
    """The registry side of a build: its run trace, ``last_build``, and the next build's base."""

    @staticmethod
    def build_run_stats(pipe: Any) -> dict:
        """``build_runs.stats``: the Cockpit's ontology / mapping counts and ``materialization``.

        ``materialization`` persists :meth:`TwinBuildPostMaterialize.trace_fields`
        and the facts a later build continues from
        (:meth:`previous_materialization_stats`):
        ``data_table_version`` (the ``…_data`` version the store matches, or
        ``None``), ``fingerprint`` (MERGE sources + mapping), ``strategy``
        (``mv`` / ``merge`` / ``ctas`` / ``""``), ``apply_mode``
        (``net_changes`` / ``full_stream`` / ``lakeflow_sync`` / ``none``;
        ``""`` when the apply phase was never reached),
        ``apply_fallback_reason``, ``data_table_id`` (the Delta table id the
        version belongs to, ``""`` when unknown) and ``source_versions`` (the
        Delta version of each slot-pipeline source, for the next build's
        overwrite check).
        """
        from back.objects.digitaltwin.build._build_pipeline import collect_domain_stats

        snap = getattr(pipe, "domain_snap", None)
        stats = collect_domain_stats(
            getattr(snap, "ontology", {}),
            getattr(snap, "assignment", {}),
            constraints=getattr(snap, "constraints", None),
            swrl_rules=getattr(snap, "swrl_rules", None),
            axioms=getattr(snap, "axioms", None),
            shacl_shapes=getattr(snap, "shacl_shapes", None),
        )
        version = getattr(pipe, "data_table_version", None)
        text = TwinBuildPostMaterialize.text_attr
        materialization = {
            **TwinBuildPostMaterialize.trace_fields(pipe),
            "data_table_version": TwinBuildRunHistory._version_or_none(version),
            "fingerprint": text(pipe, "fingerprint"),
            "strategy": TwinBuildRunHistory.strategy(pipe),
            "apply_mode": text(pipe, "apply_mode"),
            "apply_fallback_reason": text(pipe, "apply_fallback_reason"),
            "data_table_id": text(pipe, "data_table_id"),
            "source_versions": TwinBuildRunHistory._source_versions(pipe),
        }
        return {**stats, "materialization": materialization}

    @staticmethod
    def build_run_key(pipe: Any) -> tuple[str, str]:
        """``(folder, version)`` of the build-run trace.

        :meth:`run_key_of` the build's domain; the build's snapshot version
        wins over the live domain's.
        """
        return TwinBuildRunHistory.run_key_of(
            pipe.domain, domain_name=pipe.domain_name, snapshot=pipe.domain_snap
        )

    @staticmethod
    def run_key_of(
        domain: Any, *, domain_name: str | None = None, snapshot: Any = None
    ) -> tuple[str, str]:
        """``(folder, version)`` of *domain*'s build runs.

        The folder is the UC domain folder, else the sanitised domain name
        (*domain_name*, by default the domain's own); the version is
        *snapshot*'s ``current_version``, else the domain's (``""`` when unknown).
        """
        from back.objects.session import sanitize_domain_folder

        if domain_name is None:
            domain_name = (getattr(domain, "info", None) or {}).get("name", "")
        folder = getattr(domain, "uc_domain_folder", "") or sanitize_domain_folder(domain_name)
        version = (
            getattr(snapshot, "current_version", None)
            or getattr(domain, "current_version", None)
            or ""
        )
        return folder, str(version)

    @staticmethod
    def store_identity(pipe: Any) -> dict:
        """``graph_engine`` / ``sync_mode`` / ``graph_name`` of the store the build writes."""
        synced = getattr(pipe, "_is_lakebase_synced", False) is True
        return {
            "graph_engine": getattr(pipe, "_graph_engine", ""),
            "sync_mode": "managed_synced" if synced else "app_managed",
            "graph_name": pipe.graph_name,
        }

    @staticmethod
    def previous_materialization_stats(pipe: Any) -> dict:
        """``stats["materialization"]`` of the run this build continues from, or ``{}``.

        Over the newest runs of this folder + version: a success on the same
        store (:meth:`store_identity`) whose apply left it matching ``…_data``
        (``net_changes`` / ``full_stream`` / ``lakeflow_sync``, and ``none`` on
        the Delta engine) is the base. Other successes (another engine or graph;
        empty or skipped builds) and failures before the apply phase
        (``apply_mode == ""``) are passed over. Anything else ends the search
        with ``{}``: a failure that may have half-written the store, a run
        without materialization facts (a scheduled tick, an older build) or a
        build of the same graph in another sync mode, which rewrote it.
        Best-effort: any error reads as ``{}`` (debug log), i.e. "no base".
        """
        try:
            from back.objects.registry.RegistryService import RegistryService

            folder, version = TwinBuildRunHistory.build_run_key(pipe)
            if not (folder and version):
                return {}
            runs = RegistryService.from_context(pipe.domain, pipe.settings).load_build_runs(
                folder, version=version, limit=_PREVIOUS_RUNS_LIMIT
            )
            return TwinBuildRunHistory._base_materialization(pipe, runs)
        except Exception as exc:  # noqa: BLE001 — best-effort read; no base means a full apply
            logger.debug(
                "[DT-BUILD %s] could not read the previous build runs: %s", pipe.task_id, exc
            )
            return {}

    @staticmethod
    def _base_materialization(pipe: Any, runs: Any) -> dict:
        """The base run's materialization facts in *runs* (newest first); see above."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        if not isinstance(runs, list):
            return {}
        identity = TwinBuildRunHistory.store_identity(pipe)
        delta = GraphDBFactory._resolve_graph_backend(pipe.domain) == "databricks"
        accepted = _DELTA_BASE_APPLY_MODES if delta else _BASE_APPLY_MODES
        for run in runs:
            stats = run.get("stats") if isinstance(run, dict) else None
            facts = stats.get("materialization") if isinstance(stats, dict) else None
            if not isinstance(facts, dict):
                return {}
            if run.get("status") != "success":
                if facts.get("apply_mode") == "":
                    continue
                return {}
            same_graph = all(run.get(key) == identity[key] for key in ("graph_engine", "graph_name"))
            if same_graph and run.get("sync_mode") != identity["sync_mode"]:
                return {}
            if same_graph and facts.get("apply_mode") in accepted:
                return dict(facts)
        return {}

    @staticmethod
    def persist_last_build_to_registry(pipe: Any) -> None:
        """Stamp ``last_build`` on the registry row without rewriting JSONB.

        The session/UI build path stamps domain.last_build before the build
        thread starts; the API path does not. The timestamp must reach the
        ``domain_versions.last_build`` column so ReviewService.submit() can
        unblock the Submit-for-Review gate.

        A full ``write_version`` here is unsafe: the worker's live session
        can have empty ``ontology.classes`` (request-scoped DomainSession
        after Generate/complete, or a snapshot that only carried mappings).
        Rewriting the document would wipe a previously saved ontology while
        keeping mappings — SHACL suggest then returns nothing.

        Best-effort: a failure is logged but never propagates.
        """
        try:
            from back.objects.registry.RegistryService import RegistryService

            folder, version = TwinBuildRunHistory.build_run_key(pipe)
            if not folder or not version:
                logger.warning(
                    "[DT-BUILD %s] _persist_last_build_to_registry: "
                    "cannot resolve folder=%r version=%r — skipping",
                    pipe.task_id,
                    folder,
                    version,
                )
                return

            # API build path never stamps last_build before starting; do it now.
            if not getattr(pipe.domain, "last_build", None):
                pipe.domain.last_build = datetime.now(timezone.utc).isoformat()

            svc = RegistryService.from_context(pipe.domain, pipe.settings)
            ok, msg = svc._store.stamp_last_build(
                folder, str(version), pipe.domain.last_build
            )
            if ok:
                logger.info(
                    "[DT-BUILD %s] stamped last_build=%s in registry "
                    "(folder=%s version=%s)",
                    pipe.task_id,
                    pipe.domain.last_build,
                    folder,
                    version,
                )
            else:
                logger.error(
                    "[DT-BUILD %s] stamp_last_build failed when persisting "
                    "last_build: %s",
                    pipe.task_id,
                    msg,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] could not persist last_build to registry "
                "(non-fatal — Submit for Review gate may remain blocked): %s",
                pipe.task_id,
                exc,
            )

    @staticmethod
    def completion_summary(pipe: Any) -> tuple[str, str]:
        """``(label, build_mode)`` the completed task reports.

        ``build_mode`` is ``full`` when the mode resolved to ``full`` or the
        store was dropped and rewritten (``full_stream``), else
        ``incremental``. The label names a net change apply, an incremental
        build or a full rebuild.
        """
        decision = getattr(pipe, "mode_decision", None)
        effective = decision.effective if isinstance(decision, ModeDecision) else "full"
        apply_mode = TwinBuildPostMaterialize.text_attr(pipe, "apply_mode")
        full = effective == "full" or apply_mode == "full_stream"
        build_mode = "full" if full else "incremental"
        if apply_mode == "net_changes":
            return "Net change apply", build_mode
        return ("Full rebuild" if full else "Incremental build"), build_mode

    @staticmethod
    def strategy(pipe: Any) -> str:
        """How ``…_data`` was built: ``merge`` (recorded), else ``ctas`` / ``mv`` / ``""``."""
        if TwinBuildPostMaterialize.text_attr(pipe, "strategy") == "merge":
            return "merge"
        if not TwinBuildPostMaterialize.text_attr(pipe, "data_table"):
            return ""
        mode = getattr(pipe, "data_relation_mode", None)
        if mode == "table":
            return "ctas"
        return "mv" if mode == TwinBuildPostMaterialize.MATERIALIZED_VIEW else ""

    @staticmethod
    def _source_versions(pipe: Any) -> dict:
        """``{source table: Delta version}`` the slot pipeline read (``{}`` when none)."""
        value = getattr(pipe, "source_versions", None)
        if not isinstance(value, dict):
            return {}
        return {
            str(table): version
            for table, version in value.items()
            if TwinBuildRunHistory._version_or_none(version) is not None
        }

    @staticmethod
    def _version_or_none(value: Any) -> int | None:
        return value if isinstance(value, int) and not isinstance(value, bool) else None

