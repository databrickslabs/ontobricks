"""SourceProbe backed by a Databricks SQL warehouse."""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ValidationError
from back.core.helpers.SQLHelpers import SQLHelpers
from back.core.logging import get_logger
from back.objects.ontology.evolution.SourceAccessDenied import SourceAccessDenied
from back.objects.ontology.evolution.SourceProbe import SourceProbe
from back.objects.ontology.evolution.SourceUnavailable import SourceUnavailable

logger = get_logger(__name__)

_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DENIED = (
    "PERMISSION_DENIED",
    "INSUFFICIENT_PERMISSIONS",
    "ACCESS_DENIED",
    "UNAUTHORIZED",
)
_MISSING = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "CATALOG_NOT_FOUND",
    "NOT_FOUND",
)
_READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH)\b", re.I)


def split_fqn(table: str) -> List[str]:
    parts = str(table or "").split(".")
    if len(parts) != 3 or not all(_PART.match(p) for p in parts):
        raise ValidationError(
            "Expected a catalog.schema.table name", detail=str(table)[:200]
        )
    return parts


def quote_fqn(table: str) -> str:
    return ".".join(f"`{p}`" for p in split_fqn(table))


def quote_column(column: str) -> str:
    if not _PART.match(str(column or "")):
        raise ValidationError("Invalid column name", detail=str(column)[:200])
    return f"`{column}`"


class WarehouseSourceProbe(SourceProbe):
    """Runs fixed, server-built SQL through *execute* (``sql -> rows``).

    Only identifiers that pass ``[A-Za-z_][A-Za-z0-9_]*`` reach the SQL text,
    and values go through :meth:`SQLHelpers.sql_literal`. Existing mapping
    SQL is only ever wrapped in a CTE and must start with SELECT or WITH.
    """

    def __init__(self, execute: Callable[[str], List[Dict[str, Any]]]):
        self._execute = execute

    def _run(self, sql: str, table: str = "") -> List[Dict[str, Any]]:
        try:
            return self._execute(sql)
        except Exception as exc:  # noqa: BLE001 — classified below
            text = str(exc).upper()
            if any(code in text for code in _DENIED):
                raise SourceAccessDenied(
                    "You do not have access to a mapped source", detail=table
                )
            if any(code in text for code in _MISSING):
                raise SourceUnavailable(
                    "A mapped source table was not found", detail=table
                )
            logger.warning("Evolve source probe failed for %s: %s", table, exc)
            raise SourceUnavailable(
                "A mapped source could not be inspected", detail=table
            )

    def columns(self, table: str) -> Dict[str, str]:
        catalog, schema, name = split_fqn(table)
        self._run(f"SELECT 1 FROM {quote_fqn(table)} LIMIT 0", table)
        rows = self._run(
            f"SELECT column_name, data_type FROM `{catalog}`.information_schema.columns "
            f"WHERE table_schema = {SQLHelpers.sql_literal(schema)} "
            f"AND table_name = {SQLHelpers.sql_literal(name)}",
            table,
        )
        return {str(r["column_name"]): str(r["data_type"]) for r in rows}

    def foreign_keys(self, table: str) -> List[Dict[str, str]]:
        catalog, schema, name = split_fqn(table)
        isc = f"`{catalog}`.information_schema"
        rows = self._run(
            f"""
            SELECT kcu.column_name AS column_name,
                   rk.table_catalog AS ref_catalog, rk.table_schema AS ref_schema,
                   rk.table_name AS ref_table, rk.column_name AS ref_column
            FROM {isc}.referential_constraints rc
            JOIN {isc}.key_column_usage kcu
              ON rc.constraint_catalog = kcu.constraint_catalog
             AND rc.constraint_schema = kcu.constraint_schema
             AND rc.constraint_name = kcu.constraint_name
            JOIN {isc}.key_column_usage rk
              ON rc.unique_constraint_catalog = rk.constraint_catalog
             AND rc.unique_constraint_schema = rk.constraint_schema
             AND rc.unique_constraint_name = rk.constraint_name
             AND kcu.position_in_unique_constraint = rk.ordinal_position
            WHERE kcu.table_schema = {SQLHelpers.sql_literal(schema)}
              AND kcu.table_name = {SQLHelpers.sql_literal(name)}
            """,
            table,
        )
        return [
            {
                "column": str(r["column_name"]),
                "ref_table": f"{r['ref_catalog']}.{r['ref_schema']}.{r['ref_table']}",
                "ref_column": str(r["ref_column"]),
            }
            for r in rows
        ]

    def count_value(self, table: str, column: str, value: str) -> int:
        rows = self._run(
            f"SELECT count(*) AS n FROM {quote_fqn(table)} "
            f"WHERE {quote_column(column)} = {SQLHelpers.sql_literal(str(value))}",
            table,
        )
        return int(rows[0]["n"]) if rows else 0

    def key_is_unique(self, table: str, column: str) -> bool:
        col = quote_column(column)
        rows = self._run(
            f"SELECT count({col}) AS n, count(DISTINCT {col}) AS d FROM {quote_fqn(table)}",
            table,
        )
        return bool(rows) and int(rows[0]["n"]) == int(rows[0]["d"])

    def join_profile(
        self, source: str, source_column: str, target: str, target_column: str
    ) -> Dict[str, object]:
        sc, tc = quote_column(source_column), quote_column(target_column)
        rows = self._run(
            f"""
            SELECT
              (SELECT count(*) FROM {quote_fqn(source)} WHERE {sc} IS NOT NULL) AS source_rows,
              (SELECT count(*) FROM {quote_fqn(source)} s
                 WHERE s.{sc} IN (SELECT {tc} FROM {quote_fqn(target)})) AS matched,
              (SELECT count({tc}) = count(DISTINCT {tc}) FROM {quote_fqn(target)})
                 AS target_column_unique
            """,
            f"{source},{target}",
        )
        row = rows[0] if rows else {}
        return {
            "source_rows": int(row.get("source_rows") or 0),
            "matched": int(row.get("matched") or 0),
            "target_column_unique": bool(row.get("target_column_unique")),
        }

    def mapping_profile(
        self,
        sql: str,
        source_column: str,
        target_column: str,
        source_entity: Dict[str, str],
        target_entity: Dict[str, str],
    ) -> Dict[str, int]:
        if not _READ_ONLY_SQL.match(sql or "") or ";" in sql:
            raise ValidationError("Mapping SQL must be a single SELECT statement")
        sc, tc = quote_column(source_column), quote_column(target_column)
        sid, tid = quote_column(source_entity["id_column"]), quote_column(
            target_entity["id_column"]
        )
        rows = self._run(
            f"""
            WITH m AS ({sql})
            SELECT count(*) AS rows,
                   count_if(m.{sc} NOT IN (SELECT {sid} FROM {quote_fqn(source_entity['table'])}))
                     AS dangling_source,
                   count_if(m.{tc} NOT IN (SELECT {tid} FROM {quote_fqn(target_entity['table'])}))
                     AS dangling_target
            FROM m
            """,
            f"{source_entity['table']},{target_entity['table']}",
        )
        row = rows[0] if rows else {}
        return {
            k: int(row.get(k) or 0)
            for k in ("rows", "dangling_source", "dangling_target")
        }

    def table_version(self, table: str) -> Optional[int]:
        try:
            rows = self._run(f"DESCRIBE HISTORY {quote_fqn(table)} LIMIT 1", table)
        except SourceUnavailable:
            if self._table_type(table) == "VIEW":
                return None
            raise
        if not rows:
            raise SourceUnavailable("The table has no Delta history", detail=table)
        return int(rows[0]["version"])

    def _table_type(self, table: str) -> str:
        catalog, schema, name = split_fqn(table)
        rows = self._run(
            f"SELECT table_type FROM `{catalog}`.information_schema.tables "
            f"WHERE table_schema = {SQLHelpers.sql_literal(schema)} "
            f"AND table_name = {SQLHelpers.sql_literal(name)}",
            table,
        )
        return str(rows[0]["table_type"]).upper() if rows else ""

    def gold_ids(self, table: str, question_id: str) -> List[str]:
        from back.core.errors import NotFoundError

        rows = self._run(
            f"SELECT expected_ids FROM {quote_fqn(table)} "
            f"WHERE question_id = {SQLHelpers.sql_literal(str(question_id))} LIMIT 2",
            table,
        )
        if len(rows) != 1:
            raise NotFoundError(
                "The question is not in the gold table (or is listed twice)",
                detail=str(question_id),
            )
        ids = rows[0]["expected_ids"]
        if isinstance(ids, str):
            import json

            ids = json.loads(ids)
        # The SQL connector returns ARRAY columns as numpy arrays, whose truth
        # value is ambiguous, so never test the value itself for emptiness.
        return sorted(str(i) for i in (list(ids) if ids is not None else []))
