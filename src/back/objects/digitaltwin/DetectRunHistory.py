"""Run-history rows for the community and pattern Detect tasks.

Each Detect task writes one row when it finishes, completed or failed, into
``community_runs`` / ``pattern_runs`` (Settings → Automation → Runs). Writes
are best-effort: a failure is logged and never changes the task outcome.

Extracted from :class:`TwinBackgroundTasks` to keep that class inside the
architecture size budget.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger("back.objects.digitaltwin.DetectRunHistory")


def _count(stats: Optional[Dict[str, Any]], key: str, default: int = 0) -> int:
    return int((stats or {}).get(key, default) or 0)


def _text(stats: Optional[Dict[str, Any]], key: str) -> str:
    return str((stats or {}).get(key, "") or "")


def _duration_ms(stats: Optional[Dict[str, Any]], started: float) -> int:
    """The job's own elapsed time when reported, else wall-clock since *started*."""
    elapsed = (stats or {}).get("elapsed_ms")
    if elapsed is not None:
        return int(elapsed)
    return int((time.time() - started) * 1000)


class DetectRunHistory:
    """Appends one community or pattern Detect run row per finished task."""

    @staticmethod
    def record_community(
        domain,
        settings,
        *,
        task_id: str,
        class_filter: List[str],
        started: float,
        stats: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> None:
        """Record a community run; ``stats`` is the job summary (completed only)."""
        entry = {
            "status": "failed" if error else "completed",
            "class_filter": list(class_filter or []),
            "node_count": _count(stats, "node_count"),
            "cluster_count": _count(stats, "cluster_count"),
            "converged": bool((stats or {}).get("converged", True)),
            "run_page_url": _text(stats, "run_page_url"),
            "error": error,
        }
        DetectRunHistory._write("community", domain, settings, entry, task_id, started, stats)

    @staticmethod
    def record_pattern(
        domain,
        settings,
        *,
        task_id: str,
        anchor_type: str,
        hops: int,
        min_support: int,
        started: float,
        stats: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> None:
        """Record a pattern run; the requested parameters back-fill missing stats."""
        entry = {
            "status": "failed" if error else "completed",
            "anchor_type": anchor_type,
            "hop_count": _count(stats, "hop_count", hops),
            "min_support": _count(stats, "min_support", min_support),
            "detail_level": _text(stats, "detail_level"),
            "anchor_count": _count(stats, "anchor_count"),
            "pattern_count": _count(stats, "pattern_count"),
            "run_page_url": _text(stats, "run_page_url"),
            "error": error,
        }
        DetectRunHistory._write("pattern", domain, settings, entry, task_id, started, stats)

    @staticmethod
    def _write(
        kind: str,
        domain,
        settings,
        entry: Dict[str, Any],
        task_id: str,
        started: float,
        stats: Optional[Dict[str, Any]],
    ) -> None:
        """Stamp the common fields and append the row for ``(folder, version)``."""
        from back.objects.registry.RegistryService import RegistryService

        folder = getattr(domain, "uc_domain_folder", "") or ""
        version = str(getattr(domain, "current_version", "") or "")
        if not (folder and version):
            logger.warning(
                "%s run not recorded: missing folder/version (%r/%r)",
                kind,
                folder,
                version,
            )
            return
        entry.update(
            task_id=task_id,
            duration_ms=_duration_ms(stats, started),
            computed_at=datetime.now(timezone.utc).isoformat(),
        )
        try:
            store = RegistryService.from_context(domain, settings).store
            writer = store.record_community_run if kind == "community" else store.record_pattern_run
            writer(folder, version, entry)
        except Exception as exc:  # noqa: BLE001
            logger.warning("record %s run (%s) failed: %s", kind, folder, exc)
