"""Background TaskManager runners for pattern Detect and catalog save.

Extracted from :class:`TwinBackgroundTasks` to keep that class inside the
architecture size budget. The Detect task runs the Databricks pattern job and,
in the same background task, snapshots the top pattern into the saved catalog.
"""

from __future__ import annotations

from typing import Any

from back.core.logging import get_logger
from back.objects.digitaltwin.TwinBackgroundTasks import _analytics, _error_text

logger = get_logger(__name__)


class PatternBackgroundTasks:
    """TaskManager runners for pattern Detect (with auto-save) and manual save."""

    @staticmethod
    def _pattern_auto_save_label(class_filter: str) -> str:
        """``Customer — 2026/11/16 16:40:00`` — same shape the UI used to build."""
        from datetime import datetime

        from back.core.graph_analysis.pattern.pattern_diagram import local_label

        entity = local_label((class_filter or "").strip()) or "Pattern"
        stamp = datetime.now().astimezone().strftime("%Y/%m/%d %H:%M:%S")
        return f"{entity} — {stamp}"

    @staticmethod
    def run_pattern_task(
        tm,
        task_id: str,
        domain,
        settings,
        graph_name: str,
        *,
        class_filter: str,
        hops: int = 3,
        bins: int = 8,
        min_support: int = 3,
        refine_top: int = 0,
        auto_save: bool = True,
    ) -> None:
        """Detect patterns on Databricks, then snapshot the catalog in one task."""
        import time as _time

        from back.objects.digitaltwin.DetectRunHistory import DetectRunHistory

        anchor_type = (class_filter or "").strip()
        t0 = _time.time()
        detect_recorded = False
        try:
            tm.start_task(task_id, "Detecting patterns...")
            tm.update_progress(task_id, 20, "Starting the Databricks pattern detection job")
            analytics = _analytics()(domain)
            stats = analytics.detect_patterns(
                graph_name,
                class_filter=class_filter,
                hops=hops,
                bins=bins,
                min_support=min_support,
                refine_top=refine_top,
                settings=settings,
                on_progress=lambda pct, msg: tm.update_progress(task_id, pct, msg),
            )
            patterns = int(stats.get("pattern_count", 0) or 0)
            anchors = int(stats.get("anchor_count", 0) or 0)
            elapsed_ms = stats.get("elapsed_ms")
            DetectRunHistory.record_pattern(
                domain, settings, task_id=task_id, anchor_type=anchor_type,
                hops=hops, min_support=min_support, started=t0, stats=stats,
            )
            detect_recorded = True
            saved: dict[str, Any] = {}
            if auto_save and patterns > 0:
                tm.update_progress(task_id, 90, "Saving pattern catalog")
                top = analytics.build_job_patterns(
                    domain, settings, graph_name
                ).top_patterns(1)
                focus_id = str((top[0] if top else {}).get("id") or "")
                if not focus_id:
                    raise RuntimeError("Detect found patterns but none were readable")
                label = PatternBackgroundTasks._pattern_auto_save_label(anchor_type)
                saved = analytics.save_pattern(
                    graph_name, focus_id, label, settings=settings
                )
            result = {
                "pattern_count": patterns,
                "anchor_count": anchors,
                "run_page_url": stats.get("run_page_url", ""),
                "elapsed_ms": elapsed_ms,
            }
            if saved:
                result["saved_id"] = saved.get("saved_id", "")
                result["saved_label"] = saved.get("label", "")
                result["pattern_id"] = saved.get("pattern_id", "")
            message = (
                f"{patterns:,} patterns over {anchors:,} anchors "
                f"(computed on Databricks"
                + (f" in {int(elapsed_ms)} ms" if elapsed_ms is not None else "")
                + ")"
            )
            if saved.get("saved_id"):
                message += f"; saved as '{saved.get('label', '')}'"
            tm.complete_task(task_id, result=result, message=message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Pattern detection task failed: %s", exc)
            if not detect_recorded:
                DetectRunHistory.record_pattern(
                    domain, settings, task_id=task_id, anchor_type=anchor_type,
                    hops=hops, min_support=min_support, started=t0,
                    error=_error_text(exc),
                )
            tm.fail_task(task_id, _error_text(exc))

    @staticmethod
    def run_pattern_save_task(
        tm,
        task_id: str,
        domain,
        settings,
        graph_name: str,
        *,
        pattern_id: str,
        label: str,
    ) -> None:
        """Snapshot one discovered pattern into its saved-pattern table."""
        try:
            tm.start_task(task_id, "Saving pattern...")
            tm.update_progress(task_id, 10, "Copying pattern members")
            result = _analytics()(domain).save_pattern(
                graph_name, pattern_id, label, settings=settings
            )
            tm.complete_task(
                task_id,
                result=result,
                message=(
                    f"Saved pattern '{result.get('label', label)}' "
                    f"({result.get('size', 0):,} members)"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Pattern save task failed: %s", exc)
            tm.fail_task(task_id, _error_text(exc))
