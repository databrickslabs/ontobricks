"""Scheduled graph cache refresh."""

from __future__ import annotations

from typing import Any, Dict

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb import get_graphdb
from back.core.graphdb.GraphDBFactory import GraphDBFactory

from back.objects.registry.schedule.RunOutcome import RunOutcome
from back.objects.registry.schedule.TaskContext import TaskContext

_SUPPORTED_BACKENDS = {"lakebase", "databricks"}


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Cache refresh takes no type-specific options."""
    del config
    return {}


def run(ctx: TaskContext) -> RunOutcome:
    """Rebuild graph companion indexes for the scheduled domain."""
    ctx.progress(5, "Loading domain from registry...")
    backend = GraphDBFactory._resolve_graph_backend(ctx.domain)
    if backend not in _SUPPORTED_BACKENDS:
        raise ValidationError(
            "Cache refresh is only available for lakebase and databricks "
            "graph backends."
        )

    ctx.progress(20, "Opening graph backend")
    store = get_graphdb(
        ctx.snapshot,
        ctx.settings,
        for_write=backend == "databricks",
    )
    if store is None:
        raise InfrastructureError("Graph backend is not configured")
    if not getattr(store, "supports_adjacency", False):
        raise ValidationError(f"{backend} backend does not support cache refresh")

    from back.core.graphdb.search_cache import (
        CACHE_DISABLED_REFRESH_MESSAGE,
        graph_cache_rebuild_allowed,
        rebuild_graph_cache_if_enabled,
    )

    if not graph_cache_rebuild_allowed(ctx.domain):
        raise ValidationError(CACHE_DISABLED_REFRESH_MESSAGE)

    graph_name = ctx.graph_name.strip()
    if not graph_name:
        raise ValidationError("Graph name is not configured")

    ctx.progress(70, f"Rebuilding graph indexes for {graph_name}")
    rebuild_graph_cache_if_enabled(store, graph_name, ctx.domain)
    return RunOutcome(
        status="success",
        message="Graph cache refresh completed",
        count=0,
        task_result={"mode": "adjacency_only", "backend": backend},
    )
