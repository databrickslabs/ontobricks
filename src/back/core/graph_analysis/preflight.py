"""Whether Graph Analytics can run for a domain, and why not.

Analytics has three prerequisites — the admin toggle, a resolvable
Databricks job name, and a mapped-triples snapshot with rows in it — and
each failure has a different remedy. Reporting *which* one is missing is
the whole point, and it cuts both ways: telling someone to rebuild a
domain because the warehouse was asleep, or to go and wake a warehouse
that is running because the snapshot was never built, both send them off
to fix something that was never broken.

The check is split in two on cost. :func:`analytics_job_configured` is
the three free checks, for callers that only need to know whether
analytics *could* run (the stats payload renders on every page).
:func:`analytics_job_status` adds a warehouse round-trip and is for the
caller about to spend far more than that on a job run — the interactive
endpoint and the scheduler.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from back.core.graph_analysis.JobMetrics import resolve_analytics_source
from back.core.helpers import (
    resolve_analytics_job_enabled,
    resolve_analytics_job_name,
)
from back.core.logging import get_logger

logger = get_logger(__name__)


# A query that fails because the name is not there has answered the
# question: the snapshot holds no rows because it does not exist. Only a
# failure that leaves the question *unanswered* — auth, network, a
# warehouse that will not start — is a probe failure.
_ABSENT_MARKERS = (
    "table_or_view_not_found",
    "schema_not_found",
    "catalog_not_found",
    "42p01",
    "42704",
)


def _names_a_missing_object(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in _ABSENT_MARKERS)


def probe_data_table(
    domain: Any, settings: Any, table: str
) -> Tuple[Optional[bool], str]:
    """Whether the mapped snapshot exists and holds at least one triple.

    Returns ``(answer, failure_detail)``. A ``…_data`` that is absent and
    one that is empty have the same remedy — build the domain — so both
    answer ``False``, including when the absence arrives as a
    ``TABLE_OR_VIEW_NOT_FOUND`` error rather than an empty result set.

    ``None`` is reserved for a probe that never got an answer, and then
    the detail carries the engine's own words. Guessing there instead
    ("the warehouse must be asleep") sends people off to check a
    warehouse that was running all along.
    """
    from back.core.databricks.DatabricksClient import DatabricksClient
    from back.core.helpers import (
        get_databricks_host_and_token,
        resolve_delta_warehouse_id,
    )

    try:
        host, token = get_databricks_host_and_token(domain, settings)
        client = DatabricksClient(
            host=host,
            token=token,
            warehouse_id=resolve_delta_warehouse_id(domain, settings),
        )
        rows = client.execute_query(f"SELECT 1 AS ok FROM {table} LIMIT 1")
        return bool(rows), ""
    except Exception as exc:  # noqa: BLE001
        if _names_a_missing_object(exc):
            logger.info("mapped snapshot %s does not exist yet: %s", table, exc)
            return False, ""
        logger.warning("mapped-snapshot probe failed for %s: %s", table, exc)
        return None, str(exc).strip()


def analytics_job_configured(domain: Any, settings: Any) -> Tuple[bool, str]:
    """Return ``(configured, reason_it_is_not)`` without touching the warehouse.

    The reason is empty whenever the toggle is off, because then nothing
    is broken: not using the job is the configured behaviour.
    """
    if not resolve_analytics_job_enabled(domain, settings):
        return False, ""

    if not resolve_analytics_job_name(settings):
        return False, (
            "No Databricks job name could be determined. Set "
            "ONTOBRICKS_ANALYTICS_JOB_NAME, or deploy the bundle so the name can "
            "be derived from the app name."
        )

    source, reason = resolve_analytics_source(domain, settings)
    if not source:
        return False, reason or (
            "No mapped-triples table could be resolved for this domain."
        )

    return True, ""


def analytics_job_status(domain: Any, settings: Any) -> Tuple[bool, str]:
    """Return ``(analytics_available, reason_it_is_not)`` for this domain.

    :func:`analytics_job_configured` plus a probe that the snapshot
    actually holds triples.
    """
    configured, reason = analytics_job_configured(domain, settings)
    if not configured:
        return False, reason

    source, _ = resolve_analytics_source(domain, settings)
    has_rows, detail = probe_data_table(domain, settings, source)
    if has_rows is None:
        return False, (
            f"The mapped-triples table {source} could not be probed: "
            f"{detail or 'no detail was reported'}"
        )
    if not has_rows:
        return False, (
            f"The mapped-triples table {source} is missing or empty. Run "
            f"Knowledge Graph → Build to materialise it, then retry."
        )

    return True, ""
