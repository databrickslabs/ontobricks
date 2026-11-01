"""Fallback ladder that turns a requested materialization mode into the one that runs.

A domain *requests* a mode (``full``, ``incremental``, ``microbatch``,
``realtime``); whether it can run depends on what the workspace and the
sources offer. Every mode degrades to the next-slower one and records why, so
a build never fails or ends empty because a faster mode was unavailable.

The steps are applied one at a time, in order, each on the mode the previous
step left behind. The reason on the decision is the *first* downgrade's.
"""

from __future__ import annotations

from typing import Mapping

from back.core.graphdb.GraphDBFactory import normalize_materialization_mode
from back.core.graphdb.ModeDecision import ModeDecision


class MaterializationModeResolver:
    """Resolve a requested materialization mode against readiness probes."""

    REASON_REALTIME_NOT_AVAILABLE = "realtime_not_available"
    REASON_SERVERLESS_UNAVAILABLE = "serverless_unavailable"
    REASON_BACKEND_NOT_ELIGIBLE = "backend_not_eligible"
    REASON_ROW_TRACKING_MISSING = "row_tracking_missing"

    #: Backends that can host an incremental build (domain backend values).
    ELIGIBLE_BACKENDS = ("databricks", "lakebase", "neo4j")

    @staticmethod
    def resolve(
        requested: str,
        *,
        serverless_available: bool,
        sources_row_tracked: bool,
        backend: str,
        previews: Mapping[str, bool],
        realtime_topic_available: bool = False,
    ) -> ModeDecision:
        """Return the requested mode and the mode that will actually run.

        Ladder, in order:

        1. ``realtime`` -> ``microbatch`` (``realtime_not_available``).
        2. ``incremental`` / ``microbatch`` -> ``full`` when serverless compute
           is unavailable (``serverless_unavailable``) or the backend is not
           one of :attr:`ELIGIBLE_BACKENDS` (``backend_not_eligible``).
        3. ``microbatch`` -> ``incremental`` when the sources do not track row
           changes (``row_tracking_missing``).
        4. ``incremental`` -> ``full`` when the sources do not track row
           changes (``row_tracking_missing``).

        ``previews`` (workspace preview flags) is accepted so the signature is
        stable; no step consults it yet.
        """
        # Realtime is reserved for a later release: ``realtime_topic_available``
        # is deliberately ignored, so it always resolves to ``microbatch`` first.
        cls = MaterializationModeResolver
        requested = normalize_materialization_mode(requested)
        mode, reason = requested, ""

        if mode == "realtime":
            mode, reason = "microbatch", cls.REASON_REALTIME_NOT_AVAILABLE

        if mode in ("incremental", "microbatch"):
            blocker = cls._gate_blocker(serverless_available, backend)
            if blocker:
                mode, reason = "full", reason or blocker

        if mode == "microbatch" and not sources_row_tracked:
            mode, reason = "incremental", reason or cls.REASON_ROW_TRACKING_MISSING

        if mode == "incremental" and not sources_row_tracked:
            mode, reason = "full", reason or cls.REASON_ROW_TRACKING_MISSING

        return ModeDecision(requested=requested, effective=mode, reason=reason)

    @staticmethod
    def _gate_blocker(serverless_available: bool, backend: str) -> str:
        """Reason the serverless/backend gate refuses an incremental mode, or ``""``."""
        if not serverless_available:
            return MaterializationModeResolver.REASON_SERVERLESS_UNAVAILABLE
        if backend not in MaterializationModeResolver.ELIGIBLE_BACKENDS:
            return MaterializationModeResolver.REASON_BACKEND_NOT_ELIGIBLE
        return ""
