"""CTAS and table lifecycle SQL for the Databricks Delta triple store."""

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
