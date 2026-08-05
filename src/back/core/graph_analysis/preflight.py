"""Whether Graph Analytics can run for a domain, and why not.

Analytics has three prerequisites — the admin toggle, a resolvable
Databricks job name, and a mapped-triples snapshot with rows in it — and
each failure has a different remedy. Reporting *which* one is missing is
the whole point: telling someone to rebuild a domain because the
warehouse was asleep sends them off to fix something that was never
broken.

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


def data_table_has_rows(domain: Any, settings: Any, table: str) -> Optional[bool]:
    """Whether the mapped snapshot exists and holds at least one triple.

    A ``…_data`` that is absent and one that is empty have the same
    remedy — build the domain — so they are one check. ``None`` means the
    probe could not reach the warehouse at all, which is a different
    situation entirely.
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
        return bool(rows)
    except Exception as exc:  # noqa: BLE001
        logger.warning("mapped-snapshot probe failed for %s: %s", table, exc)
        return None


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
    has_rows = data_table_has_rows(domain, settings, source)
    if has_rows is None:
        return False, (
            f"The mapped-triples table {source} could not be reached. The SQL "
            f"warehouse may be starting up — retry in a moment."
        )
    if not has_rows:
        return False, (
            f"The mapped-triples table {source} is missing or empty. Run "
            f"Knowledge Graph → Build to materialise it, then retry."
        )

    return True, ""
