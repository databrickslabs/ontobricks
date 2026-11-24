"""CTAS and table lifecycle SQL for the Databricks Delta triple store.

The ``…_data`` relation comes in two shapes, chosen per domain (Domain →
General → Knowledge Graph):

* ``table`` — CTAS the gateway VIEW into a clustered Delta TABLE. A copy of
  the mapped triples, which is what makes graph reads cheap and Graph
  Analytics scannable.
* ``view`` — a pass-through VIEW over the gateway VIEW. No copy at all: reads
  re-run the R2RML SQL against the source tables and always see live data.

Only the inferred companion is a TABLE in both modes — reasoning and cohort
triples have no source to be derived from.
"""

from __future__ import annotations

from typing import Any, Literal

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.graphdb.adjacency import typed_in_select, typed_out_select
from back.core.graphdb.entity_search import (
    entity_search_select,
    is_missing_relation_error,
    is_view_not_table_error,
)
from back.core.graphdb.overrides import OP_ADD, OP_DEL, ORIGIN_USER
from back.core.graphdb.props import props_select
from back.core.helpers import sql_escape, validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)


def run_sql(client: Any, sql: str) -> Any:
    """Execute DDL/DML on a warehouse client.

    Classic ``SQLWarehouse`` exposes ``execute_statement``. Databricks Apps
    use ``StatementExecutionWarehouse``, which historically only had
    ``execute_query`` — both now implement ``execute_statement``. Fall back
    to ``execute_query`` for older/test doubles.
    """
    stmt = getattr(client, "execute_statement", None)
    if callable(stmt):
        return stmt(sql)
    query = getattr(client, "execute_query", None)
    if callable(query):
        return query(sql)
    raise AttributeError(
        f"{type(client).__name__} has neither execute_statement nor execute_query"
    )


def build_ctas_sql(view_fqn: str, table_fqn: str) -> str:
    """Spark SQL to materialize triples from a VIEW into a clustered Delta TABLE.

    Databricks RTAS does not allow an explicit column schema alongside ``AS SELECT``;
    types are inferred from the source VIEW.
    """
    validate_table_name(view_fqn)
    validate_table_name(table_fqn)
    return (
        f"CREATE OR REPLACE TABLE {table_fqn} USING DELTA "
        "CLUSTER BY (predicate, subject) "
        f"AS SELECT subject, predicate, object FROM {view_fqn}"
    )


def build_data_view_sql(view_fqn: str, data_fqn: str) -> str:
    """Spark SQL for a pass-through VIEW over the gateway VIEW (no data copy).

    The columns are listed rather than ``SELECT *`` so the relation keeps the
    same three-column shape as its materialized counterpart whatever the
    gateway VIEW grows.
    """
    validate_table_name(view_fqn)
    validate_table_name(data_fqn)
    return (
        f"CREATE OR REPLACE VIEW {data_fqn} AS "
        f"SELECT subject, predicate, object FROM {view_fqn}"
    )


def build_adj_ctas_sql(
    spo_fqn: str, adj_fqn: str, direction: Literal["out", "in"]
) -> str:
    """Spark SQL to materialize an adjacency table from an SPO relation."""
    validate_table_name(spo_fqn)
    validate_table_name(adj_fqn)
    if direction == "out":
        select_sql = typed_out_select(spo_fqn)
        cluster_key = "src, predicate"
    elif direction == "in":
        select_sql = typed_in_select(spo_fqn)
        cluster_key = "dst, predicate"
    else:
        raise ValueError(f"Unsupported adjacency direction: {direction}")
    return (
        f"CREATE OR REPLACE TABLE {adj_fqn} USING DELTA "
        f"CLUSTER BY ({cluster_key}) "
        f"AS {select_sql}"
    )


def build_entity_search_ctas_sql(spo_fqn: str, search_fqn: str) -> str:
    """Spark SQL to materialize the entity-search companion."""
    validate_table_name(spo_fqn)
    validate_table_name(search_fqn)
    return (
        f"CREATE OR REPLACE TABLE {search_fqn} USING DELTA "
        "CLUSTER BY (type_uri, label_lc) "
        f"AS {entity_search_select(spo_fqn)}"
    )


def set_bloom_filter_columns(client: Any, table_fqn: str, columns: str) -> None:
    """Best-effort Delta Bloom filters on lowercase search columns."""
    validate_table_name(table_fqn)
    try:
        run_sql(
            client,
            f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ("
            f"'delta.bloomFilter.columns' = '{columns}')",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Bloom filter TBLPROPERTIES failed for %s: %s", table_fqn, exc
        )


def build_props_ctas_sql(spo_fqn: str, props_fqn: str) -> str:
    """Spark SQL to materialize the typed-subject property companion."""
    validate_table_name(spo_fqn)
    validate_table_name(props_fqn)
    return (
        f"CREATE OR REPLACE TABLE {props_fqn} USING DELTA "
        "CLUSTER BY (subject) "
        f"AS {props_select(spo_fqn)}"
    )


OVERRIDE_COLUMNS = (
    "subject, predicate, object, op, origin, change_id, author, created_at"
)


def build_ensure_overrides_sql(table_fqn: str) -> str:
    """Empty ``…_overrides`` TABLE: add / del rows applied over ``…_data``."""
    validate_table_name(table_fqn)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn} "
        "(subject STRING, predicate STRING, object STRING, "
        "op STRING, origin STRING, change_id STRING, author STRING, "
        "created_at TIMESTAMP) USING DELTA "
        "CLUSTER BY (predicate, subject)"
    )


def _row_key_match(left: str, right: str) -> str:
    return (
        f"{left}.subject = {right}.subject "
        f"AND {left}.predicate = {right}.predicate "
        f"AND {left}.object = {right}.object"
    )


def _overrides_rows(overrides_fqn: str, where: str) -> str:
    return (
        f"SELECT subject, predicate, object FROM {overrides_fqn} WHERE {where}"
    )


def build_union_view_sql(graph_fqn: str, data_fqn: str, overrides_fqn: str) -> str:
    """Effective graph: mapped triples plus ``add`` rows, minus ``del`` rows."""
    validate_table_name(graph_fqn)
    validate_table_name(data_fqn)
    validate_table_name(overrides_fqn)
    return (
        f"CREATE OR REPLACE VIEW {graph_fqn} AS "
        "SELECT u.subject, u.predicate, u.object FROM ("
        f"SELECT subject, predicate, object FROM {data_fqn} "
        "UNION ALL "
        f"{_overrides_rows(overrides_fqn, f'op = {_lit(OP_ADD)}')}"
        ") u "
        "LEFT ANTI JOIN ("
        f"{_overrides_rows(overrides_fqn, f'op = {_lit(OP_DEL)}')}"
        ") d ON "
        f"{_row_key_match('u', 'd')}"
    )


def build_asserted_view_sql(
    asserted_fqn: str, data_fqn: str, overrides_fqn: str
) -> str:
    """Asserted graph: mapped triples plus *user* ``add`` rows, minus ``del`` rows.

    Derived triples (reasoning, cohorts, communities) are left out, which is the
    scope Graph Analytics and Data Quality score.
    """
    validate_table_name(asserted_fqn)
    validate_table_name(data_fqn)
    validate_table_name(overrides_fqn)
    user_adds = _overrides_rows(
        overrides_fqn, f"op = {_lit(OP_ADD)} AND origin = {_lit(ORIGIN_USER)}"
    )
    return (
        f"CREATE OR REPLACE VIEW {asserted_fqn} AS "
        "SELECT u.subject, u.predicate, u.object FROM ("
        f"SELECT subject, predicate, object FROM {data_fqn} "
        "UNION ALL "
        f"{user_adds}"
        ") u "
        "LEFT ANTI JOIN ("
        f"{_overrides_rows(overrides_fqn, f'op = {_lit(OP_DEL)}')}"
        ") d ON "
        f"{_row_key_match('u', 'd')}"
    )


def _lit(value: str) -> str:
    """Single-quoted SQL string literal."""
    return f"'{sql_escape(value)}'"


def _nullable_lit(value: str) -> str:
    return _lit(value) if value else "NULL"


def build_values_sql(batch: list[dict[str, str]]) -> str:
    """``VALUES`` rows for *batch* (subject, predicate, object)."""
    return ", ".join(
        f"({_lit(t.get('subject', '') or '')}, "
        f"{_lit(t.get('predicate', '') or '')}, "
        f"{_lit(t.get('object', '') or '')})"
        for t in batch
    )


def _values_source(batch: list[dict[str, str]]) -> str:
    return f"(VALUES {build_values_sql(batch)}) AS v(subject, predicate, object)"


def build_clear_derived_sql(overrides_fqn: str) -> str:
    """Delete every row a Build or purge may rewrite (anything not ``user``)."""
    validate_table_name(overrides_fqn)
    return (
        f"DELETE FROM {overrides_fqn} "
        f"WHERE COALESCE(origin, '') <> {_lit(ORIGIN_USER)}"
    )


def build_clear_origin_sql(
    overrides_fqn: str, origin: str, subject_prefix: str, member_predicate: str
) -> str:
    """Delete the rows of one cohort / community partition (subject-prefix shape)."""
    validate_table_name(overrides_fqn)
    prefix = sql_escape(subject_prefix)
    return (
        f"DELETE FROM {overrides_fqn} "
        f"WHERE origin = {_lit(origin)} "
        f"AND (subject LIKE '{prefix}%' "
        f"OR (predicate = {_lit(member_predicate)} AND object LIKE '{prefix}%'))"
    )


def build_restore_sql(overrides_fqn: str, batch: list[dict[str, str]]) -> str:
    """Delete the ``del`` rows of *batch*: the hidden triples become visible again."""
    validate_table_name(overrides_fqn)
    return (
        f"MERGE INTO {overrides_fqn} t USING {_values_source(batch)} "
        f"ON {_row_key_match('t', 'v')} AND t.op = {_lit(OP_DEL)} "
        "WHEN MATCHED THEN DELETE"
    )


def build_drop_user_adds_sql(overrides_fqn: str, batch: list[dict[str, str]]) -> str:
    """Delete the user ``add`` rows of *batch*."""
    validate_table_name(overrides_fqn)
    return (
        f"MERGE INTO {overrides_fqn} t USING {_values_source(batch)} "
        f"ON {_row_key_match('t', 'v')} AND t.op = {_lit(OP_ADD)} "
        f"AND t.origin = {_lit(ORIGIN_USER)} "
        "WHEN MATCHED THEN DELETE"
    )


def build_add_user_sql(
    overrides_fqn: str,
    graph_fqn: str,
    batch: list[dict[str, str]],
    *,
    author: str = "",
    change_id: str = "",
) -> str:
    """Insert user ``add`` rows for the triples of *batch* absent from the graph."""
    validate_table_name(overrides_fqn)
    validate_table_name(graph_fqn)
    return (
        f"INSERT INTO {overrides_fqn} ({OVERRIDE_COLUMNS}) "
        f"SELECT v.subject, v.predicate, v.object, {_lit(OP_ADD)}, "
        f"{_lit(ORIGIN_USER)}, {_nullable_lit(change_id)}, "
        f"{_nullable_lit(author)}, current_timestamp() "
        f"FROM {_values_source(batch)} "
        f"WHERE NOT EXISTS (SELECT 1 FROM {graph_fqn} g "
        f"WHERE {_row_key_match('g', 'v')})"
    )


def build_hide_sql(
    overrides_fqn: str,
    data_fqn: str,
    batch: list[dict[str, str]],
    *,
    author: str = "",
    change_id: str = "",
) -> str:
    """Insert ``del`` rows for the triples of *batch* still visible without user adds.

    A triple only needs a ``del`` row when it comes from the mapped data or from a
    derived ``add`` row. A triple that exists solely as a user ``add`` row is
    removed by :func:`build_drop_user_adds_sql` and leaves no ``del`` behind.
    """
    validate_table_name(overrides_fqn)
    validate_table_name(data_fqn)
    return (
        f"INSERT INTO {overrides_fqn} ({OVERRIDE_COLUMNS}) "
        f"SELECT v.subject, v.predicate, v.object, {_lit(OP_DEL)}, "
        f"{_lit(ORIGIN_USER)}, {_nullable_lit(change_id)}, "
        f"{_nullable_lit(author)}, current_timestamp() "
        f"FROM {_values_source(batch)} "
        f"WHERE (EXISTS (SELECT 1 FROM {data_fqn} d WHERE {_row_key_match('d', 'v')}) "
        f"OR EXISTS (SELECT 1 FROM {overrides_fqn} a WHERE a.op = {_lit(OP_ADD)} "
        f"AND COALESCE(a.origin, '') <> {_lit(ORIGIN_USER)} "
        f"AND {_row_key_match('a', 'v')})) "
        f"AND NOT EXISTS (SELECT 1 FROM {overrides_fqn} x WHERE x.op = {_lit(OP_DEL)} "
        f"AND {_row_key_match('x', 'v')})"
    )


def build_migrate_inferred_sql(
    overrides_fqn: str, inferred_fqn: str, origin_expr: str
) -> str:
    """Copy the legacy ``…_inferred`` rows into ``…_overrides`` as derived ``add`` rows."""
    validate_table_name(overrides_fqn)
    validate_table_name(inferred_fqn)
    return (
        f"INSERT INTO {overrides_fqn} ({OVERRIDE_COLUMNS}) "
        f"SELECT subject, predicate, object, {_lit(OP_ADD)}, {origin_expr}, "
        "NULL, NULL, current_timestamp() "
        f"FROM {inferred_fqn}"
    )


def build_truncate_sql(table_fqn: str) -> str:
    validate_table_name(table_fqn)
    return f"TRUNCATE TABLE {table_fqn}"


def materialize_from_view(client: Any, view_fqn: str, table_fqn: str) -> None:
    """Replace *table_fqn* with rows from *view_fqn*."""
    sql = build_ctas_sql(view_fqn, table_fqn)
    logger.info("Materializing Delta triple store: %s from %s", table_fqn, view_fqn)
    run_sql(client, sql)


def create_data_view(client: Any, view_fqn: str, data_fqn: str) -> None:
    """Replace *data_fqn* with a pass-through VIEW over *view_fqn*."""
    sql = build_data_view_sql(view_fqn, data_fqn)
    logger.info("Creating pass-through Delta view: %s over %s", data_fqn, view_fqn)
    run_sql(client, sql)


def drop_relation(client: Any, fqn: str, *, kind: str) -> None:
    """Best-effort ``DROP {TABLE,VIEW} IF EXISTS`` on *fqn*.

    Failure is logged and swallowed: the only caller is
    :func:`apply_data_relation`, clearing a relation of the *other* kind whose
    usual state is "not there at all".
    """
    validate_table_name(fqn)
    statement = "TABLE" if kind == "table" else "VIEW"
    try:
        run_sql(client, f"DROP {statement} IF EXISTS {fqn}")
    except Exception as exc:  # noqa: BLE001
        logger.debug("DROP %s %s failed (may not exist): %s", statement, fqn, exc)


def apply_data_relation(
    client: Any, view_fqn: str, data_fqn: str, *, mode: str
) -> None:
    """Build the ``…_data`` relation for *mode* (``"table"`` or ``"view"``).

    Databricks will not let ``CREATE OR REPLACE TABLE`` overwrite a VIEW, nor
    the reverse, so switching a domain between the two modes needs the stale
    relation of the other kind dropped first. Doing it unconditionally keeps
    the two branches symmetric and costs one no-op statement.
    """
    if mode == "view":
        drop_relation(client, data_fqn, kind="table")
        create_data_view(client, view_fqn, data_fqn)
        return
    drop_relation(client, data_fqn, kind="view")
    materialize_from_view(client, view_fqn, data_fqn)


def relation_exists(client: Any, fqn: str) -> bool:
    """``True`` when *fqn* resolves in Unity Catalog (table or view)."""
    validate_table_name(fqn)
    try:
        client.execute_query(f"DESCRIBE DETAIL {fqn}")
    except Exception as exc:  # noqa: BLE001
        if is_view_not_table_error(exc):
            return True  # a view has no Delta detail, but it exists
        if is_missing_relation_error(exc):
            return False
        raise
    return True


def ensure_overrides_table(client: Any, table_fqn: str) -> bool:
    """Create the overrides TABLE if missing. Returns ``True`` when it was created."""
    existed = relation_exists(client, table_fqn)
    logger.info("Ensuring Delta overrides table: %s", table_fqn)
    run_sql(client, build_ensure_overrides_sql(table_fqn))
    return not existed


def ensure_graph_view(
    client: Any, graph_fqn: str, data_fqn: str, overrides_fqn: str
) -> None:
    """Create or refresh the effective-graph VIEW used for graph read queries."""
    sql = build_union_view_sql(graph_fqn, data_fqn, overrides_fqn)
    logger.info(
        "Ensuring Delta effective graph view: %s (data=%s, overrides=%s)",
        graph_fqn,
        data_fqn,
        overrides_fqn,
    )
    run_sql(client, sql)


def ensure_asserted_view(
    client: Any, asserted_fqn: str, data_fqn: str, overrides_fqn: str
) -> None:
    """Create or refresh the asserted VIEW (mapped + user overrides, no derived)."""
    sql = build_asserted_view_sql(asserted_fqn, data_fqn, overrides_fqn)
    logger.info(
        "Ensuring Delta asserted view: %s (data=%s, overrides=%s)",
        asserted_fqn,
        data_fqn,
        overrides_fqn,
    )
    run_sql(client, sql)


def migrate_inferred(
    client: Any, overrides_fqn: str, inferred_fqn: str, origin_expr: str
) -> bool:
    """Copy the legacy ``…_inferred`` rows into the freshly created overrides table.

    Returns ``True`` when a legacy table was found and copied. The legacy table
    is left untouched so a rollback to the previous version still finds its data.
    """
    if not inferred_fqn or not relation_exists(client, inferred_fqn):
        return False
    logger.info("Migrating legacy inferred rows: %s -> %s", inferred_fqn, overrides_fqn)
    run_sql(client, build_migrate_inferred_sql(overrides_fqn, inferred_fqn, origin_expr))
    return True


def clear_derived(client: Any, overrides_fqn: str) -> None:
    """Delete the derived rows of *overrides_fqn*, keeping every ``user`` row.

    Best-effort like :func:`truncate_table`: the table may not exist on a first
    build, which is not an error.
    """
    try:
        run_sql(client, build_clear_derived_sql(overrides_fqn))
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "clear_derived %s failed (may not exist yet): %s", overrides_fqn, exc
        )


def truncate_table(client: Any, table_fqn: str) -> None:
    """Clear all rows from *table_fqn* (best-effort)."""
    try:
        run_sql(client, build_truncate_sql(table_fqn))
    except Exception as exc:  # noqa: BLE001
        logger.debug("truncate_table %s failed (may not exist yet): %s", table_fqn, exc)


def optimize_table(client: Any, table_fqn: str) -> None:
    validate_table_name(table_fqn)
    logger.info("Optimizing Delta table: %s", table_fqn)
    run_sql(client, f"OPTIMIZE {table_fqn}")


def count_triples(client: Any, relation_fqn: str) -> int:
    """Return ``COUNT(*)`` of *relation_fqn*.

    Raises :class:`InfrastructureError` when the count cannot run. A failed
    count must never read as an empty graph: builds that lost the warehouse
    connection mid-count were reported as "no triples generated (check your
    mappings)", which sent users debugging mappings that were fine.
    """
    validate_table_name(relation_fqn)
    try:
        rows = client.execute_query(f"SELECT COUNT(*) AS cnt FROM {relation_fqn}")
    except OntoBricksError:
        raise
    except Exception as exc:  # noqa: BLE001 — re-raised with the relation named
        raise InfrastructureError(
            f"Could not count the triples in {relation_fqn}: {exc}"
        ) from exc
    raw = rows[0].get("cnt") if rows else None
    if raw is None:
        raise InfrastructureError(
            f"Could not count the triples in {relation_fqn}: "
            "COUNT(*) returned no row"
        )
    return int(raw)
