"""SQL assembly helpers for the SPARQL -> Spark triple translation.

Extracted from :class:`~back.core.w3c.sparql.SparqlTranslator.SparqlTranslator`
so the translator stays inside its size budget. The class owns the pieces that
turn per-mapping branches into the final ``subject, predicate, object`` query:
the SELECT-clause parse, the ``stack()`` unpivot, the ``WITH`` preamble and the
outer projection.
"""

import re

from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.core.w3c.sparql.constants import DIALECT_SPARK

logger = get_logger(__name__)


class TripleSQLAssembler:
    """Static builders for the unpivot, ``WITH`` and outer-projection SQL."""

    @staticmethod
    def parse_select_clause(query_body: str) -> tuple[bool, list[str] | None]:
        """Parse ``SELECT [DISTINCT] ?a ?b | *`` into ``(is_distinct, select_vars)``.

        ``select_vars`` is ``None`` for ``SELECT *``.
        """
        select_match = re.search(
            r"SELECT\s+(DISTINCT\s+)?((?:\?\w+\s*)+|\*)", query_body, re.IGNORECASE
        )
        if not select_match:
            raise ValidationError("Could not parse SELECT clause.")

        is_distinct = bool(select_match.group(1))
        select_vars_str = select_match.group(2).strip()

        if select_vars_str == "*":
            return is_distinct, None
        return is_distinct, [
            v.strip()[1:] for v in select_vars_str.split() if v.strip().startswith("?")
        ]

    @staticmethod
    def unpivot_select(
        subject_expr: str,
        predicates: list,
        from_clause: str,
        where_clause: str = "",
        dialect: str = DIALECT_SPARK,
        alias: str = "",
        *,
        incremental: bool = False,
    ) -> str:
        """Build a SELECT that unpivots predicate/object pairs.

        By default the pairs are unpivoted with Spark ``stack()``. With
        *incremental* the same rows come out of a parenthesised ``UNION ALL``
        of one single-predicate SELECT per pair (``where_clause`` repeated on
        each): ``stack()`` is a generator Enzyme cannot refresh incrementally,
        while a ``UNION ALL`` of plain projections is row-based.
        """
        if incremental:
            branches = [
                f"SELECT {subject_expr} AS subject, '{p[0]}' AS predicate, "
                f"{p[1]} AS object\n                FROM {from_clause}{where_clause}"
                for p in predicates
            ]
            return "(" + "\n                UNION ALL\n                ".join(branches) + ")"
        stack_args = ", ".join([f"'{p[0]}', {p[1]}" for p in predicates])
        return (
            f"SELECT {subject_expr} AS subject, "
            f"stack({len(predicates)}, {stack_args}) AS (predicate, object)\n"
            f"                FROM {from_clause}{where_clause}"
        )

    @staticmethod
    def build_with_clause(cte_defs) -> str:
        """Render the ``WITH ...`` preamble from a list of (alias, sql) tuples."""
        if not cte_defs:
            return ""
        parts = []
        for alias, sql_text in cte_defs:
            if "SELECT" in sql_text.upper():
                parts.append(f"  {alias} AS (\n    {sql_text}\n  )")
            else:
                parts.append(f"  {alias} AS (\n    SELECT * FROM {sql_text}\n  )")
        return "WITH\n" + ",\n".join(parts) + "\n"

    @staticmethod
    def finalize_generic_triples(
        table_queries, is_distinct, limit, cte_defs, *, incremental: bool = False
    ) -> dict:
        """Wrap the per-mapping branches into the final triples query payload.

        With *incremental*, a distinct query is deduplicated by a ``GROUP BY
        subject, predicate, object`` on the outer query instead of
        ``SELECT DISTINCT`` (Enzyme refreshes a group aggregate incrementally,
        not a bare DISTINCT). The ``object IS NOT NULL`` filter still runs
        first, ahead of the grouping, and ``LIMIT`` stays last. A non-distinct
        query is returned unchanged.
        """
        if not table_queries:
            raise ValidationError("No tables found in R2RML mapping.")
        distinct_str = "DISTINCT " if is_distinct and not incremental else ""
        group_by = (
            "\n        GROUP BY subject, predicate, object"
            if is_distinct and incremental
            else ""
        )
        limit_clause = f"\n    LIMIT {limit}" if limit else ""
        with_clause = TripleSQLAssembler.build_with_clause(cte_defs)
        sql = f"""{with_clause}SELECT {distinct_str}subject, predicate, object FROM (
            {' UNION ALL '.join(table_queries)}
        ) AS triples
        WHERE object IS NOT NULL AND TRIM(object) != ''{group_by}{limit_clause}"""
        logger.debug(
            "Generated SQL with %d CTEs (length: %s chars):", len(cte_defs), len(sql)
        )
        for i, tq in enumerate(table_queries):
            logger.debug("Subquery %s: %s...", i, tq[:300])
        return {
            "success": True,
            "sql": sql,
            "variables": ["subject", "predicate", "object"],
        }
