"""The build-run trace a build writes, and the earlier runs a build continues from.

Extracted from :class:`TwinBuildPostMaterialize` (Fowler Extract Class), in the
same style: static methods that take the running pipeline (*pipe*).
:meth:`TwinBuildRunHistory.build_run_stats` is what ``_record_build_run``
persists under ``build_runs.stats``; :meth:`previous_materialization_stats`
reads those facts back for the net change apply and the MERGE fingerprint skip.
"""

from __future__ import annotations

from typing import Any

from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)

logger = get_logger(__name__)

#: ``apply_mode`` values after which the store matches ``…_data``: a run that
#: ended with one of them is a base a later build can continue from.
_BASE_APPLY_MODES = ("net_changes", "full_stream", "lakeflow_sync")

#: How many recent runs are searched for that base.
_PREVIOUS_RUNS_LIMIT = 10


class TwinBuildRunHistory:
    """Build-run trace keys and stats, and the base run of the next build."""

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
        ``""`` when the apply phase was never reached) and
        ``apply_fallback_reason``.
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
        }
        return {**stats, "materialization": materialization}

    @staticmethod
    def build_run_key(pipe: Any) -> tuple[str, str]:
        """``(folder, version)`` of the build-run trace.

        The folder is the UC domain folder, else the sanitised domain name; the
        version is the build's ``current_version`` (``""`` when unknown).
        """
        from back.objects.session import sanitize_domain_folder

        folder = getattr(pipe.domain, "uc_domain_folder", "") or (
            sanitize_domain_folder(pipe.domain_name)
        )
        version = (
            getattr(pipe.domain_snap, "current_version", None)
            or getattr(pipe.domain, "current_version", None)
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
        (``net_changes`` / ``full_stream`` / ``lakeflow_sync``) is the base.
        Other successes (another store; empty, skipped or Delta-engine builds)
        and failures before the apply phase (``apply_mode == ""``) are passed
        over. Anything else ends the search with ``{}``: a failure that may have
        half-written the store, or a run without materialization facts (a
        scheduled tick, an older build) that may have rewritten it.
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
        if not isinstance(runs, list):
            return {}
        identity = TwinBuildRunHistory.store_identity(pipe)
        for run in runs:
            stats = run.get("stats") if isinstance(run, dict) else None
            facts = stats.get("materialization") if isinstance(stats, dict) else None
            if not isinstance(facts, dict):
                return {}
            if run.get("status") != "success":
                if facts.get("apply_mode") == "":
                    continue
                return {}
            same_store = all(run.get(key) == value for key, value in identity.items())
            if same_store and facts.get("apply_mode") in _BASE_APPLY_MODES:
                return dict(facts)
        return {}

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
    def _version_or_none(value: Any) -> int | None:
        return value if isinstance(value, int) and not isinstance(value, bool) else None

