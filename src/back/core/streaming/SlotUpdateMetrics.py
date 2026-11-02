"""The rows one slot pipeline update wrote, summed from its ``flow_progress`` events."""

from __future__ import annotations

from typing import Any

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
    def read(w: Any, pipeline_id: str, update_id: str) -> dict:
        """``{rows_output, rows_upserted, rows_deleted}`` of *update_id* (``None`` per metric no event has).

        ``num_output_rows`` of every flow, and ``num_upserted_rows`` /
        ``num_deleted_rows`` of the AUTO CDC flows, over the update's
        ``flow_progress`` events (``origin.update_id``). The events REST
        endpoint is read directly: the SDK's ``PipelineEvent`` drops
        ``details``, and its ``filter`` cannot select an update.
        """
        totals: dict = dict.fromkeys(_ROW_METRICS.values())
        for event in SlotUpdateMetrics._update_events(w, pipeline_id, update_id):
            if event.get("event_type") != "flow_progress":
                continue
            metrics = ((event.get("details") or {}).get("flow_progress") or {}).get("metrics") or {}
            for name, key in _ROW_METRICS.items():
                value = metrics.get(name)
                if isinstance(value, int) and not isinstance(value, bool):
                    totals[key] = (totals[key] or 0) + value
        return totals

    @staticmethod
    def _update_events(w: Any, pipeline_id: str, update_id: str) -> list:
        """The events of *update_id*, newest first.

        The log lists newest first and one pipeline runs one update at a time,
        so the update's events are one run of the list: reading stops at the
        first older event after them, or after :data:`_MAX_PAGES` pages.
        """
        path = f"/api/2.0/pipelines/{pipeline_id}/events"
        query: dict = {"max_results": _PAGE_SIZE}
        found: list = []
        for _page in range(_MAX_PAGES):
            page = w.api_client.do("GET", path, query=query) or {}
            for event in page.get("events") or []:
                if (event.get("origin") or {}).get("update_id") == update_id:
                    found.append(event)
                elif found:
                    return found
            token = page.get("next_page_token")
            if not token:
                break
            query = {"max_results": _PAGE_SIZE, "page_token": token}
        return found
