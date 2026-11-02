"""The rows one slot pipeline update wrote, summed from its ``flow_progress`` events."""

from __future__ import annotations

from typing import Any

from back.core.logging import get_logger

logger = get_logger(__name__)

#: ``details.flow_progress.metrics`` key -> ``update_metrics`` key.
_ROW_METRICS = {
    "num_output_rows": "rows_output",
    "num_upserted_rows": "rows_upserted",
    "num_deleted_rows": "rows_deleted",
}
#: Events per page, and the pages read before giving up on reaching the update.
_PAGE_SIZE = 250
_MAX_PAGES = 20


class SlotUpdateMetrics:
    """Reads a pipeline's event log and sums the row metrics of one update's flows.

    Called by :meth:`SlotPipelineRunner.update_metrics` on its SDK client,
    under its API timeout and error translation.
    """

    @staticmethod
    def read(w: Any, pipeline_id: str, update_id: str) -> dict | None:
        """``{rows_output, rows_upserted, rows_deleted}`` of *update_id*, or ``None``.

        ``num_output_rows`` of every flow, and ``num_upserted_rows`` /
        ``num_deleted_rows`` of the AUTO CDC flows, over the update's
        ``flow_progress`` events (``origin.update_id``); a metric no event has
        is ``None``. Verified live (2026-11-02): an AUTO CDC slot flow carries
        its upserted / deleted counts on one ``RUNNING`` event and no
        ``num_output_rows``; ``QUEUED`` / ``STARTING`` / ``COMPLETED`` carry
        none, so nothing is counted twice. ``None`` when no event carries any
        metric or the update's events could not all be read
        (:meth:`_update_events`). Counts arrive as integers or digit strings.
        The events REST endpoint is read directly: the SDK's ``PipelineEvent``
        drops ``details``, and its ``filter`` cannot select an update.
        """
        events = SlotUpdateMetrics._update_events(w, pipeline_id, update_id)
        totals: dict = dict.fromkeys(_ROW_METRICS.values())
        for event in events or []:
            if event.get("event_type") != "flow_progress":
                continue
            metrics = ((event.get("details") or {}).get("flow_progress") or {}).get("metrics") or {}
            for name, key in _ROW_METRICS.items():
                value = SlotUpdateMetrics._count(metrics.get(name))
                if value is not None:
                    totals[key] = (totals[key] or 0) + value
        return None if all(value is None for value in totals.values()) else totals

    @staticmethod
    def _count(value: Any) -> int | None:
        """*value* as a row count: an ``int`` (not a ``bool``) or a digit string, else ``None``."""
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isascii() and value.isdigit():
            return int(value)
        return None

    @staticmethod
    def _update_events(w: Any, pipeline_id: str, update_id: str) -> list | None:
        """The events of *update_id*, newest first; ``None`` when they could not all be read.

        The log lists newest first and one pipeline runs one update at a time,
        so the update's events are one run of the list: reading stops at the
        first event of *another* update after them (events without an update
        id, such as user actions, are skipped). Reaching :data:`_MAX_PAGES`
        before that is ``None`` — partial counts would read as complete; a
        warning says so when the update's events had begun.
        """
        path = f"/api/2.0/pipelines/{pipeline_id}/events"
        query: dict = {"max_results": _PAGE_SIZE}
        found: list = []
        for _page in range(_MAX_PAGES):
            page = w.api_client.do("GET", path, query=query) or {}
            for event in page.get("events") or []:
                event_update = (event.get("origin") or {}).get("update_id")
                if event_update == update_id:
                    found.append(event)
                elif event_update and found:
                    return found
            token = page.get("next_page_token")
            if not token:
                return found
            query = {"max_results": _PAGE_SIZE, "page_token": token}
        if found:
            logger.warning(
                "Slot pipeline %s update %s: events not read to their end within %d pages; "
                "no row metrics recorded",
                pipeline_id,
                update_id,
                _MAX_PAGES,
            )
        return None
