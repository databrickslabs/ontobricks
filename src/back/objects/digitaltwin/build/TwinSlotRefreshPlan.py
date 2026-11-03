"""Whether a slot pipeline update must be a full refresh, and the facts that carry it."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from back.core.errors import InfrastructureError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

logger = get_logger(__name__)

#: ``slot_full_refresh_reason`` values.
REASON_SOURCE_OVERWRITTEN = "source_overwritten"
REASON_RECONCILE_DRIFT = "reconcile_drift"
REASON_SLOT_UPDATE_FAILED = "slot_update_failed"


class TwinSlotRefreshPlan:
    """Decides, from the run history and the sources, whether a slot update is a full refresh.

    The slot pipeline reads its sources' change data feed, which a rewrite
    (``INSERT OVERWRITE``, ``CREATE OR REPLACE``, a ``DROP`` + ``CREATE``…)
    breaks, and a reconciliation that found drift asks for a recompute. Both
    are carried across runs — main builds and scheduled ticks — in their
    ``stats.materialization`` facts:

    * ``source_versions`` / ``source_table_ids``: each source's Delta version
      and table id as read *before* the last update that completed (carried
      forward unchanged by a run whose update did not complete);
    * ``slot_full_refresh_pending``: true from the moment a full refresh is
      owed until an update with it completed — sticky across failures;
    * ``slot_full_refresh_reason``: ``source_overwritten``, ``reconcile_drift``
      or ``slot_update_failed`` (a slot update that failed resumes from the
      same checkpoint and fails again — the next one starts over), kept apart
      from the mode's ``mode_fallback_reason``.

    Runs that did not reach a slot update (full builds, MERGE builds, older
    runs) record none of these and are passed over.
    """

    @staticmethod
    def plan(client: Any, runs: Any, source_tables: Iterable[str]) -> dict:
        """``{"reason", "before", "after"}`` for an update reading *source_tables*.

        ``reason`` is ``""`` (an incremental update), ``reconcile_drift`` or
        ``source_overwritten``; ``before`` is what to record until the update
        (and the ``…_data`` refresh after it) completed — the previous source
        state, the full refresh pending when a reason applies — and ``after``
        what to record once it did: the source state read now, nothing pending.
        A source history that cannot be read raises :class:`InfrastructureError`.
        """
        tables = list(source_tables)
        previous = TwinSlotRefreshPlan.previous_source_state(runs)
        current = TwinSlotRefreshPlan.source_state(client, tables)
        rewritten = TwinSlotRefreshPlan.rewritten(client, tables, previous, current)
        reason = TwinSlotRefreshPlan.owed(runs) or (REASON_SOURCE_OVERWRITTEN if rewritten else "")
        if rewritten:
            logger.info(
                "%s rewritten since the last slot update: full refresh", ", ".join(rewritten)
            )
        if reason:
            logger.info("Slot pipeline update runs as a full refresh (%s)", reason)
        return {
            "reason": reason,
            "before": {
                **previous,
                "slot_full_refresh_pending": bool(reason),
                "slot_full_refresh_reason": reason,
            },
            "after": {
                **current,
                "slot_full_refresh_pending": False,
                "slot_full_refresh_reason": reason,
            },
        }

    @staticmethod
    def owed(runs: Any) -> str:
        """The full refresh the newest deciding run left owed, ``""`` for none.

        Newest first: a run that recorded ``reconcile_drift`` owes one; a run
        that recorded ``slot_full_refresh_pending`` decides (its reason when
        true, none when false); any other run is passed over.
        """
        for run in runs if isinstance(runs, list) else []:
            facts = TwinBuildRunHistory.facts_of(run) or {}
            if facts.get("reconcile_drift") is True:
                return REASON_RECONCILE_DRIFT
            pending = facts.get("slot_full_refresh_pending")
            if pending is True:
                return str(facts.get("slot_full_refresh_reason") or REASON_SOURCE_OVERWRITTEN)
            if pending is False:
                return ""
        return ""

    @staticmethod
    def previous_source_state(runs: Any) -> dict:
        """``source_versions`` / ``source_table_ids`` of the newest run that recorded versions."""
        for run in runs if isinstance(runs, list) else []:
            facts = TwinBuildRunHistory.facts_of(run) or {}
            versions = facts.get("source_versions")
            if isinstance(versions, dict) and versions:
                ids = facts.get("source_table_ids")
                return {
                    "source_versions": {
                        str(t): v
                        for t, v in versions.items()
                        if isinstance(v, int) and not isinstance(v, bool)
                    },
                    "source_table_ids": {
                        str(t): str(i) for t, i in (ids if isinstance(ids, dict) else {}).items()
                    },
                }
        return {"source_versions": {}, "source_table_ids": {}}

    @staticmethod
    def source_state(client: Any, source_tables: Iterable[str]) -> dict:
        """Each source's latest Delta version and table id (unreadable ones left out)."""
        from back.core.graphdb.delta import ChangeFeedReader

        versions: dict = {}
        ids: dict = {}
        for table in source_tables:
            try:
                versions[table] = ChangeFeedReader.latest_version(client, table)
            except InfrastructureError as exc:
                logger.debug("No version of %s: %s", table, exc)
            table_id = ChangeFeedReader.table_id(client, table)
            if table_id:
                ids[table] = table_id
        return {"source_versions": versions, "source_table_ids": ids}

    @staticmethod
    def rewritten(client: Any, source_tables: Iterable[str], previous: dict, current: dict) -> list:
        """Sources rewritten since *previous*: another table id, or a rewrite in their history."""
        from back.core.streaming import SourceReadiness

        recorded = previous["source_versions"]
        recorded_ids = previous["source_table_ids"]
        rewritten = []
        for table in source_tables:
            if table not in recorded:
                continue
            old_id, new_id = recorded_ids.get(table), current["source_table_ids"].get(table)
            if (old_id and new_id and old_id != new_id) or SourceReadiness.source_overwritten_since(
                client, table, recorded[table], current["source_versions"].get(table)
            ):
                rewritten.append(table)
        return rewritten
