"""CTAS and table lifecycle SQL for the Databricks Delta triple store.

The ``…_data`` relation comes in two shapes, chosen per domain (Domain →
Information → Knowledge Graph):

* ``table`` — CTAS the gateway VIEW into a clustered Delta TABLE. A copy of
  the mapped triples, which is what makes graph reads cheap and Graph
  Analytics scannable.
* ``view`` — a pass-through VIEW over the gateway VIEW. No copy at all: reads
  re-run the R2RML SQL against the source tables and always see live data.

Only the inferred companion is a TABLE in both modes — reasoning and cohort
triples have no source to be derived from.
"""

from __future__ import annotations

from typing import Any

from back.core.helpers import validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)


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


def build_ensure_inferred_sql(table_fqn: str) -> str:
    """Empty companion TABLE for reasoning / cohort writes (same shape as ``_data``)."""
    validate_table_name(table_fqn)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn} "
        "(subject STRING, predicate STRING, object STRING) USING DELTA "
        "CLUSTER BY (predicate, subject)"
    )


def build_union_view_sql(graph_fqn: str, data_fqn: str, inferred_fqn: str) -> str:
    """VIEW that merges bulk materialized triples with app-written inferred rows."""
    validate_table_name(graph_fqn)
    validate_table_name(data_fqn)
    validate_table_name(inferred_fqn)
    return (
        f"CREATE OR REPLACE VIEW {graph_fqn} AS "
        f"SELECT subject, predicate, object FROM {data_fqn} "
        f"UNION ALL "
        f"SELECT subject, predicate, object FROM {inferred_fqn}"
    )


def build_truncate_sql(table_fqn: str) -> str:
    validate_table_name(table_fqn)
    return f"TRUNCATE TABLE {table_fqn}"


def materialize_from_view(client: Any, view_fqn: str, table_fqn: str) -> None:
    """Replace *table_fqn* with rows from *view_fqn*."""
    sql = build_ctas_sql(view_fqn, table_fqn)
    logger.info("Materializing Delta triple store: %s from %s", table_fqn, view_fqn)
    client.execute_statement(sql)


def create_data_view(client: Any, view_fqn: str, data_fqn: str) -> None:
    """Replace *data_fqn* with a pass-through VIEW over *view_fqn*."""
    sql = build_data_view_sql(view_fqn, data_fqn)
    logger.info("Creating pass-through Delta view: %s over %s", data_fqn, view_fqn)
    client.execute_statement(sql)


def drop_relation(client: Any, fqn: str, *, kind: str) -> None:
    """Best-effort ``DROP {TABLE,VIEW} IF EXISTS`` on *fqn*.

    Failure is logged and swallowed: the only caller is
    :func:`apply_data_relation`, clearing a relation of the *other* kind whose
    usual state is "not there at all".
    """
    validate_table_name(fqn)
    statement = "TABLE" if kind == "table" else "VIEW"
    try:
        client.execute_statement(f"DROP {statement} IF EXISTS {fqn}")
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


def ensure_inferred_table(client: Any, table_fqn: str) -> None:
    """Create the writable companion TABLE if it does not exist yet."""
    sql = build_ensure_inferred_sql(table_fqn)
    logger.info("Ensuring Delta inferred companion table: %s", table_fqn)
    client.execute_statement(sql)


def ensure_graph_view(
    client: Any, graph_fqn: str, data_fqn: str, inferred_fqn: str
) -> None:
    """Create or refresh the union VIEW used for graph read queries."""
    sql = build_union_view_sql(graph_fqn, data_fqn, inferred_fqn)
    logger.info(
        "Ensuring Delta graph union view: %s (data=%s, inferred=%s)",
        graph_fqn,
        data_fqn,
        inferred_fqn,
    )
    client.execute_statement(sql)


def truncate_table(client: Any, table_fqn: str) -> None:
    """Clear all rows from *table_fqn* (best-effort)."""
    try:
        client.execute_statement(build_truncate_sql(table_fqn))
    except Exception as exc:  # noqa: BLE001
        logger.debug("truncate_table %s failed (may not exist yet): %s", table_fqn, exc)


def optimize_table(client: Any, table_fqn: str) -> None:
    validate_table_name(table_fqn)
    logger.info("Optimizing Delta table: %s", table_fqn)
    client.execute_statement(f"OPTIMIZE {table_fqn}")
