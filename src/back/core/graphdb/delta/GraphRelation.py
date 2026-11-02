"""A Lakehouse domain's ``…_graph`` relation: the union VIEW or a materialized view."""

from __future__ import annotations

from typing import Any

from back.core.graphdb.delta import materialize
from back.core.graphdb.delta.MaterializedViewBuilder import MaterializedViewBuilder
from back.core.logging import get_logger

logger = get_logger(__name__)


class GraphRelation:
    """``…_graph`` = ``…_data`` ∪ ``…_inferred``, as a union VIEW or a materialized view.

    The ``full`` mode keeps the union VIEW; the incremental modes make it a
    materialized view on the Lakehouse engine, which Lakehouse//RT reads. A
    graph MV carries its definition fingerprint (``ontobricks.mv_sql_sha256``),
    which is also how it is told from the VIEW. Writers of ``…_inferred`` keep
    the VIEW as it is (:meth:`ensure_view`) and refresh the MV
    (:meth:`refresh_after_write`), so inferred triples reach the MV's readers
    — and the graph caches rebuilt from it — once the write returns.

    The kind is probed by the fingerprint (:meth:`is_materialized`), which
    reads as "not an MV" when the probe fails. A transient failure therefore
    skips a refresh (the next write, build or trigger catches up) and makes
    :meth:`ensure_view` try ``CREATE OR REPLACE VIEW``, which a real MV refuses:
    the write fails, nothing is replaced.
    """

    VIEW = "view"
    MATERIALIZED = "mv"

    @staticmethod
    def is_materialized(client: Any, graph_fqn: str) -> bool:
        """True when *graph_fqn* is a graph materialized view (it has a fingerprint); False on a failed probe."""
        return bool(graph_fqn) and bool(MaterializedViewBuilder.existing_mv_sha(client, graph_fqn))

    @staticmethod
    def ensure_materialized(
        client: Any, *, graph_fqn: str, data_fqn: str, inferred_fqn: str, refresh_clause: str = ""
    ) -> bool:
        """Create the graph MV unless it exists with the current definition; True when created.

        Like :meth:`MaterializedViewBuilder.apply` with ``with_graph_mv``: the
        union VIEW is dropped first (``CREATE OR REPLACE MATERIALIZED VIEW``
        cannot replace a VIEW), the ``…_inferred`` companion is ensured and
        given row tracking, then the view is created.
        """
        current = MaterializedViewBuilder.graph_mv_sha(
            data_fqn, inferred_fqn, refresh_clause=refresh_clause
        )
        if MaterializedViewBuilder.existing_mv_sha(client, graph_fqn) == current:
            return False
        create = MaterializedViewBuilder.graph_mv_sql(
            graph_fqn, data_fqn, inferred_fqn, refresh_clause=refresh_clause
        )
        materialize.drop_relation(client, graph_fqn, kind="view")
        materialize.ensure_inferred_table(client, inferred_fqn)
        materialize.run_sql(client, materialize.build_enable_row_tracking_sql(inferred_fqn))
        logger.info("Creating the graph materialized view %s", graph_fqn)
        materialize.run_sql(client, create)
        return True

    @staticmethod
    def ensure_view(client: Any, graph_fqn: str, data_fqn: str, inferred_fqn: str) -> None:
        """The union VIEW, unless ``…_graph`` is a materialized view (left as it is)."""
        if not GraphRelation.is_materialized(client, graph_fqn):
            materialize.ensure_graph_view(client, graph_fqn, data_fqn, inferred_fqn)

    @staticmethod
    def replace_with_view(client: Any, graph_fqn: str, data_fqn: str, inferred_fqn: str) -> None:
        """The union VIEW in place of a graph MV (a ``full`` path; the drop is best-effort)."""
        materialize.drop_relation(client, graph_fqn, kind="materialized_view")
        materialize.ensure_graph_view(client, graph_fqn, data_fqn, inferred_fqn)

    @staticmethod
    def refresh(client: Any, graph_fqn: str) -> bool:
        """:meth:`refresh_materialized` when *graph_fqn* is a graph MV; False (nothing run) for a VIEW."""
        if not GraphRelation.is_materialized(client, graph_fqn):
            return False
        GraphRelation.refresh_materialized(client, graph_fqn)
        return True

    @staticmethod
    def refresh_materialized(client: Any, graph_fqn: str) -> None:
        """``REFRESH MATERIALIZED VIEW <graph>`` (synchronous), for a caller that knows it is one."""
        materialize.run_sql(client, MaterializedViewBuilder.refresh_sql(graph_fqn))

    @staticmethod
    def refresh_after_write(client: Any, written_fqn: str) -> bool:
        """:meth:`refresh` the ``…_graph`` over *written_fqn* after a write; never raises.

        Every writer of an ``…_inferred`` companion calls this with the table it
        wrote; any other table has no graph relation (nothing is run). The
        refresh is synchronous: the writers (background tasks) rebuild the
        graph caches from ``…_graph`` right after, and back-to-back writes
        must not leave the last one waiting for a later refresh. The write
        itself succeeded, so a refresh that fails is logged and the MV serves
        the new triples after its next refresh (the next write, build or
        trigger): the writer is never failed for it.
        """
        from back.core.graphdb.delta import _table_naming

        suffix = _table_naming.inferred_suffix()
        if not (written_fqn or "").endswith(suffix):
            return False
        graph = written_fqn[: -len(suffix)] + _table_naming.graph_suffix()
        try:
            return GraphRelation.refresh(client, graph)
        except Exception as exc:  # noqa: BLE001 — best-effort, see above
            logger.warning("Graph materialized view %s not refreshed after a write: %s", graph, exc)
            return False
