"""Materialized-view DDL for the incremental ``_data`` / ``_graph`` relations."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import Any

from back.core.errors import ValidationError
from back.core.graphdb.delta import materialize
from back.core.helpers import validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)

_REFRESH_POLICIES = ("AUTO", "INCREMENTAL", "INCREMENTAL STRICT", "FULL")

_MV_HEADER = (
    "CREATE OR REPLACE MATERIALIZED VIEW {fqn}\n"
    "  CLUSTER BY (predicate, subject)\n"
    "  REFRESH POLICY {policy}\n"
    "  TBLPROPERTIES ('pipelines.channel' = 'preview', "
    "'delta.enableRowTracking' = 'true')\n"
)

_NON_BLANK_OBJECT = "WHERE object IS NOT NULL AND TRIM(object) != ''"

_INCREMENTAL_TECHNIQUES = re.compile(
    r"\b(?:ROW_BASED|GROUP_AGGREGATE|APPEND_ONLY|PARTITION_OVERWRITE"
    r"|WINDOW_FUNCTION|GENERIC_AGGREGATE|NO_OP)\b",
    re.IGNORECASE,
)
_FULL_RECOMPUTE = re.compile(r"\bFULL_RECOMPUTE\b", re.IGNORECASE)
_PLANNED_TECHNIQUE = re.compile(r"executed as ([A-Z_]+)")
UNKNOWN_TECHNIQUE = "UNKNOWN"


class MaterializedViewBuilder:
    """Builds and applies the ``_data`` / ``_graph`` materialized views.

    The views are *standalone* materialized views created from a SQL
    warehouse, hence ``CREATE OR REPLACE MATERIALIZED VIEW`` (``CREATE OR
    REFRESH`` is pipeline-only). Their SQL is shaped so Enzyme can refresh
    them incrementally: plain projections, ``UNION ALL`` and a ``GROUP BY``.

    The only I/O is the ``client`` handed to :meth:`apply` and
    :meth:`last_refresh_technique` (``execute_statement`` / ``execute_query``).
    """

    @staticmethod
    def data_mv_sql(
        data_fqn: str,
        *,
        slots_fqn: str | None = None,
        batch_branch_sql: str | None = None,
        refresh_policy: str = "INCREMENTAL",
    ) -> str:
        """``CREATE OR REPLACE MATERIALIZED VIEW`` for ``_data``.

        The view unions up to two sides, either of which may be omitted by
        passing ``None`` (``ValidationError`` when both are):

        * the **slots** table, ``slots_fqn``: row-local mappings streamed
          through AUTO CDC;
        * the **batch branch**, ``batch_branch_sql``: the translator's
          ``incremental=True`` SQL for the join / aggregate mappings. It is
          parenthesised so a leading ``WITH`` stays valid after ``UNION ALL``.

        Absorbs three facts about the slots table:

        * It is an AUTO CDC SCD TYPE 1 target: deletes are *physical*, so
          there is no ``deleted`` column and the slots side reads with no
          tombstone filter.
        * Classes that share one TriplesMap each emit their own copy of the
          attribute slots. The outer ``GROUP BY subject, predicate, object``
          is what collapses those copies into one triple.
        * Attribute objects are never NULL (the reused helper wraps them in
          ``COALESCE(..., '')``), so an unset attribute arrives as ``''``.
          The ``TRIM(object) != ''`` filter therefore stays in the slots
          select, ahead of the grouping, to drop those empty objects (the
          gateway VIEW does the same). The batch SQL carries its own filter.

        ``object_hash`` (``sha2(object, 256)``) is the Lakeflow sync key.
        """
        validate_table_name(data_fqn)
        policy = MaterializedViewBuilder._policy(refresh_policy)
        batch = (batch_branch_sql or "").strip()
        if slots_fqn is None and not batch:
            raise ValidationError(
                "A data materialized view needs a slots table, a batch branch, "
                "or both"
            )
        branches = []
        if slots_fqn is not None:
            validate_table_name(slots_fqn)
            branches.append(
                f"  SELECT subject, predicate, object FROM {slots_fqn}\n"
                f"  {_NON_BLANK_OBJECT}"
            )
        if batch:
            branches.append(f"  ({batch})" if slots_fqn is not None else f"  {batch}")
        return (
            _MV_HEADER.format(fqn=data_fqn, policy=policy)
            + "AS SELECT subject, predicate, object, sha2(object, 256) AS object_hash\n"
            "FROM (\n" + "\n  UNION ALL\n".join(branches) + "\n) t\n"
            "GROUP BY subject, predicate, object"
        )

    @staticmethod
    def graph_mv_sql(
        graph_fqn: str,
        data_fqn: str,
        inferred_fqn: str,
        *,
        refresh_policy: str = "INCREMENTAL",
    ) -> str:
        """``CREATE OR REPLACE MATERIALIZED VIEW`` for ``_graph``.

        A plain ``UNION ALL`` of ``_data`` and ``_inferred``, as the union
        VIEW it replaces. ``_inferred`` needs row tracking for Enzyme to read
        it incrementally (see :meth:`apply`).
        """
        validate_table_name(graph_fqn)
        validate_table_name(data_fqn)
        validate_table_name(inferred_fqn)
        policy = MaterializedViewBuilder._policy(refresh_policy)
        return (
            _MV_HEADER.format(fqn=graph_fqn, policy=policy)
            + f"AS SELECT subject, predicate, object FROM {data_fqn}\n"
            "UNION ALL\n"
            f"SELECT subject, predicate, object FROM {inferred_fqn}"
        )

    @staticmethod
    def explain_sql(mv_sql: str) -> str:
        """``EXPLAIN`` the statement to preview the refresh technique."""
        return "EXPLAIN " + mv_sql

    @staticmethod
    def refresh_sql(fqn: str, *, full: bool = False) -> str:
        """``REFRESH MATERIALIZED VIEW`` (``… FULL`` forces a full recompute)."""
        validate_table_name(fqn)
        return f"REFRESH MATERIALIZED VIEW {fqn}" + (" FULL" if full else "")

    @staticmethod
    def refresh_technique_sql(fqn: str) -> str:
        """Latest ``planning_information`` event of the view's event log."""
        validate_table_name(fqn)
        return (
            "SELECT timestamp, message "
            f"FROM event_log(TABLE({fqn})) "
            "WHERE event_type = 'planning_information' "
            "ORDER BY timestamp DESC LIMIT 1"
        )

    @staticmethod
    def plan_is_incremental(explain_rows: Any) -> bool:
        """True when an ``EXPLAIN`` result plans an incremental refresh.

        Scans every value of every row. ``FULL_RECOMPUTE`` anywhere means
        false, even next to an incremental technique (the conservative
        reading); otherwise any incremental technique means true. An empty or
        unrecognised plan is false.
        """
        text = "\n".join(MaterializedViewBuilder._texts(explain_rows))
        if _FULL_RECOMPUTE.search(text):
            return False
        return _INCREMENTAL_TECHNIQUES.search(text) is not None

    @staticmethod
    def apply(
        client: Any,
        *,
        data_fqn: str,
        graph_fqn: str,
        inferred_fqn: str,
        slots_fqn: str | None,
        batch_branch_sql: str | None,
        drop_existing_table: bool,
        refresh_policy: str = "INCREMENTAL",
    ) -> dict:
        """Create or replace the ``_data`` and ``_graph`` materialized views.

        Both statements are built (and validated) before anything runs.
        Then, in order:

        1. ``DROP TABLE IF EXISTS <data>`` when *drop_existing_table*: an
           existing ``_data`` TABLE blocks ``CREATE MATERIALIZED VIEW``.
        2. ``DROP VIEW IF EXISTS <graph>``: a plain VIEW blocks it as well.
           Both drops are best-effort (``materialize.drop_relation``): when
           the relation is already a materialized view the drop is refused
           and ``CREATE OR REPLACE`` takes over.
        3. the ``_data`` view;
        4. the ``_inferred`` companion (``CREATE TABLE IF NOT EXISTS``);
        5. ``ALTER TABLE <inferred> SET TBLPROPERTIES`` for row tracking and
           deletion vectors, so a companion created before incremental mode
           becomes readable incrementally too;
        6. the ``_graph`` view.

        Returns ``{"data_mv": data_fqn, "graph_mv": graph_fqn}``.
        """
        data_sql = MaterializedViewBuilder.data_mv_sql(
            data_fqn,
            slots_fqn=slots_fqn,
            batch_branch_sql=batch_branch_sql,
            refresh_policy=refresh_policy,
        )
        graph_sql = MaterializedViewBuilder.graph_mv_sql(
            graph_fqn, data_fqn, inferred_fqn, refresh_policy=refresh_policy
        )
        enable_tracking_sql = materialize.build_enable_row_tracking_sql(inferred_fqn)

        if drop_existing_table:
            materialize.drop_relation(client, data_fqn, kind="table")
        materialize.drop_relation(client, graph_fqn, kind="view")
        logger.info("Creating the data materialized view %s", data_fqn)
        materialize.run_sql(client, data_sql)
        materialize.ensure_inferred_table(client, inferred_fqn)
        materialize.run_sql(client, enable_tracking_sql)
        logger.info("Creating the graph materialized view %s", graph_fqn)
        materialize.run_sql(client, graph_sql)
        return {"data_mv": data_fqn, "graph_mv": graph_fqn}

    @staticmethod
    def last_refresh_technique(client: Any, fqn: str) -> str:
        """Technique of the latest planned refresh, e.g. ``ROW_BASED``.

        Reads the ``planning_information`` message (``"... planned to be
        executed as ROW_BASED ..."``). Returns ``"UNKNOWN"`` when there is no
        event, the message does not parse, or anything at all goes wrong:
        the technique is diagnostic, so this never raises.
        """
        try:
            rows = client.execute_query(MaterializedViewBuilder.refresh_technique_sql(fqn))
            row = rows[0] if rows else None
            message = row.get("message") if isinstance(row, Mapping) else None
            match = _PLANNED_TECHNIQUE.search(str(message)) if message else None
            return match.group(1) if match else UNKNOWN_TECHNIQUE
        except Exception as exc:  # noqa: BLE001 — contract: never raises
            logger.debug("No refresh technique for %s: %s", fqn, exc)
            return UNKNOWN_TECHNIQUE

    @staticmethod
    def _policy(refresh_policy: str) -> str:
        """Upper-cased policy, restricted to the documented set (it is inlined)."""
        policy = " ".join(str(refresh_policy).upper().split())
        if policy not in _REFRESH_POLICIES:
            raise ValidationError(
                f"Unsupported refresh policy {refresh_policy!r}; "
                f"expected one of {', '.join(_REFRESH_POLICIES)}"
            )
        return policy

    @staticmethod
    def _texts(value: Any) -> Iterator[str]:
        """Every string inside *value*, descending into rows (dict / list / tuple)."""
        if isinstance(value, str):
            yield value
        elif isinstance(value, Mapping):
            for item in value.values():
                yield from MaterializedViewBuilder._texts(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from MaterializedViewBuilder._texts(item)
