"""Scheduled Graph Analytics run.

Submits the serverless Lakeflow analytics job for the domain's mapped
snapshot. The heavy lifting — job submission, progress forwarding, and
persistence to the ``graph_analytics`` cache and the
``graph_analytics_runs`` trace — already lives in
:meth:`DigitalTwin.run_metrics_task`, so a scheduled run is the same
code path the Analytics page uses and shows up on the Runs pages
without any extra wiring.

Because that delegate owns the TaskManager task (it completes or fails
it itself), this type is registered with ``delegates_task_lifecycle``
and returns no outcome — the harness reads the finished task back.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.errors import ValidationError
from back.core.logging import get_logger

from .context import TaskContext

logger = get_logger(__name__)


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Analytics runs take no options: filters are an interactive choice."""
    del config
    return {}


def run(ctx: TaskContext) -> None:
    """Run graph analytics for the scheduled domain and version."""
    from back.core.graph_analysis import analytics_job_status
    from back.objects.digitaltwin import DigitalTwin

    ctx.progress(5, "Loading domain from registry...")
    domain = ctx.domain

    # Same preflight the interactive endpoint runs, so a misconfigured
    # domain fails with the actionable reason rather than an opaque job
    # error minutes later.
    available, reason = analytics_job_status(domain, ctx.settings)
    if not available:
        raise ValidationError(
            reason or "Graph analytics is not available for this domain"
        )

    DigitalTwin.run_metrics_task(
        ctx.tm,
        ctx.task_id,
        domain,
        ctx.settings,
        ctx.graph_name,
        top_n=getattr(ctx.settings, "analytics_top_n", 100),
    )
