"""Which ``…_graph`` relation an incremental build keeps, and keeping it current after ``…_data``."""

from __future__ import annotations

from typing import Any

from back.core.logging import get_logger

logger = get_logger(__name__)


class TwinGraphRelation:
    """The ``…_graph`` relation of an incremental build: a materialized view, or the union VIEW.

    A Lakehouse domain (the ``databricks`` engine — the only one whose readers,
    Lakehouse//RT included, read ``…_graph``) gets a graph materialized view
    over ``…_data`` ∪ ``…_inferred`` in every incremental mode (``incremental``,
    ``microbatch``, ``realtime``): created with ``…_data`` or on the first build
    that finds the VIEW or an older definition, refreshed right after
    ``…_data``, and self-refreshing (``TRIGGER ON UPDATE``, two upstreams)
    when ``…_data`` is. Other engines read their own store and keep the VIEW.
    The build records the relation as ``graph_relation`` (``mv`` / ``view``).
    """

    @staticmethod
    def materialized(pipe: Any) -> bool:
        """True when the build's ``…_graph`` is a materialized view (a Lakehouse domain)."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        return GraphDBFactory._resolve_triple_store_backend(pipe.domain, pipe.settings) == "databricks"

    @staticmethod
    def refresh_clause(definition: dict) -> str:
        """The graph MV's own refresh clause: ``TRIGGER ON UPDATE`` when ``…_data`` refreshes itself."""
        from back.core.graphdb.delta.MaterializedViewBuilder import TRIGGER_ON_UPDATE

        return TRIGGER_ON_UPDATE if (definition or {}).get("refresh_clause") else ""

    @staticmethod
    def after_data(
        pipe: Any, *, data: str, graph: str, inferred: str, definition: dict, created: bool
    ) -> None:
        """Step d': bring ``…_graph`` up to date once ``…_data`` is refreshed.

        *created*: ``…_data`` was just (re)created with ``…_graph``
        (``MaterializedViewBuilder.apply``), which already holds the current
        graph relation. Otherwise a graph MV is created when it is missing or
        has another definition, and an engine that keeps the VIEW replaces a
        leftover MV. A graph MV is then refreshed synchronously. Warehouse
        errors raise :class:`InfrastructureError` (the build falls back).
        """
        from back.core.graphdb.delta import GraphRelation
        from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

        client = pipe.source_client
        step = TwinIncrementalBuild.warehouse_step
        if not TwinGraphRelation.materialized(pipe):
            if not created and GraphRelation.is_materialized(client, graph):
                step(
                    f"replace {graph} by its view",
                    lambda: GraphRelation.replace_with_view(client, graph, data, inferred),
                )
            pipe.graph_relation = GraphRelation.VIEW
            return
        if not created:
            step(
                f"create the materialized view {graph}",
                lambda: GraphRelation.ensure_materialized(
                    client,
                    graph_fqn=graph,
                    data_fqn=data,
                    inferred_fqn=inferred,
                    refresh_clause=TwinGraphRelation.refresh_clause(definition),
                ),
            )
        pipe.tm.update_progress(pipe.task_id, 34, f"Refreshing materialized view {graph}...")
        step(f"refresh {graph}", lambda: GraphRelation.refresh_materialized(client, graph))
        pipe.graph_relation = GraphRelation.MATERIALIZED
        logger.info("[DT-BUILD %s] %s is a materialized view, refreshed", pipe.task_id, graph)
