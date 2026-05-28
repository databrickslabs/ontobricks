"""
Planner tools – used by the mapping-PGE Planner agent (Sprint 2+).

Exposes four OpenAI function-calling tools that let the Planner LLM probe
source tables and submit a validated ``SourceModel`` artefact:

* ``sample_table``         — N random rows from a table (n capped at 100).
* ``column_value_overlap`` — one-sided distinct-value overlap between two columns.
* ``distinct_count``       — uniqueness / completeness of a candidate canonical id.
* ``submit_source_model``  — terminal tool: validates the candidate SourceModel
  JSON against :class:`agents.agent_mapping_pge.contracts.SourceModel` and stores
  the dataclass instance on :attr:`ToolContext.source_model`.

All handlers return JSON strings (same convention as ``agents.tools.sql``)
and stringify scalar values for the LLM-facing surface.
"""

import json
from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)


# Cap on ``n`` in ``sample_table`` to keep the LLM context bounded.
_SAMPLE_TABLE_MAX_N = 100
_SAMPLE_TABLE_DEFAULT_N = 20


# =====================================================
# Tool implementations
# =====================================================


def tool_sample_table(
    ctx: ToolContext, *, full_name: str = "", n: int = _SAMPLE_TABLE_DEFAULT_N, **_kwargs
) -> str:
    """Return N random sample rows from ``full_name`` so the agent can see
    real values (not just column types). ``n`` is capped at 100.
    """
    logger.info("tool_sample_table: full_name=%s, n=%d", full_name, n)
    if not full_name:
        return json.dumps({"success": False, "error": "full_name is required"})

    try:
        capped_n = max(1, min(int(n), _SAMPLE_TABLE_MAX_N))
    except (TypeError, ValueError):
        capped_n = _SAMPLE_TABLE_DEFAULT_N

    sql = f"SELECT * FROM {full_name} ORDER BY RAND() LIMIT {capped_n}"
    logger.debug("tool_sample_table: SQL=%s", sql)

    try:
        rows = ctx.client.execute_query(sql)
        columns: List[str] = list(rows[0].keys()) if rows else []
        stringified_rows: List[List[Optional[str]]] = []
        for row in rows:
            stringified_rows.append(
                [str(row[c]) if row.get(c) is not None else None for c in columns]
            )
        logger.info(
            "tool_sample_table: %d row(s) × %d column(s)",
            len(stringified_rows),
            len(columns),
        )
        return json.dumps(
            {
                "success": True,
                "columns": columns,
                "rows": stringified_rows,
                "row_count": len(stringified_rows),
            }
        )
    except Exception as exc:
        logger.error("tool_sample_table: query failed: %s", exc, exc_info=True)
        return json.dumps({"success": False, "error": str(exc)})


def tool_column_value_overlap(
    ctx: ToolContext,
    *,
    from_table: str = "",
    from_column: str = "",
    to_table: str = "",
    to_column: str = "",
    **_kwargs,
) -> str:
    """Compute the one-sided overlap
    ``|distinct(from) ∩ distinct(to)| / |distinct(from)|``.

    The numerator dedupes ``from`` before intersecting. Returns 0.0 (and a
    note) when ``from_distinct_count`` is zero to avoid division by zero.
    """
    logger.info(
        "tool_column_value_overlap: %s.%s ↔ %s.%s",
        from_table,
        from_column,
        to_table,
        to_column,
    )
    if not (from_table and from_column and to_table and to_column):
        return json.dumps(
            {
                "success": False,
                "error": "from_table, from_column, to_table, to_column are all required",
            }
        )

    sql = (
        "WITH from_distinct AS ("
        f"  SELECT DISTINCT {from_column} AS v FROM {from_table} "
        f"  WHERE {from_column} IS NOT NULL"
        "),"
        " to_distinct AS ("
        f"  SELECT DISTINCT {to_column} AS v FROM {to_table} "
        f"  WHERE {to_column} IS NOT NULL"
        "),"
        " inter AS ("
        "  SELECT v FROM from_distinct INTERSECT SELECT v FROM to_distinct"
        ") "
        "SELECT (SELECT COUNT(*) FROM from_distinct) AS from_distinct_count, "
        "       (SELECT COUNT(*) FROM to_distinct)   AS to_distinct_count, "
        "       (SELECT COUNT(*) FROM inter)         AS intersection_count"
    )
    logger.debug("tool_column_value_overlap: SQL=%s", sql)

    try:
        rows = ctx.client.execute_query(sql)
        if not rows:
            return json.dumps(
                {"success": False, "error": "overlap query returned no rows"}
            )
        row = rows[0]
        from_distinct = int(row.get("from_distinct_count", 0) or 0)
        to_distinct = int(row.get("to_distinct_count", 0) or 0)
        intersection = int(row.get("intersection_count", 0) or 0)

        if from_distinct == 0:
            result: Dict[str, Any] = {
                "success": True,
                "overlap_pct": 0.0,
                "from_distinct_count": 0,
                "to_distinct_count": to_distinct,
                "intersection_count": 0,
                "note": (
                    f"{from_table}.{from_column} has zero distinct non-null values; "
                    "overlap_pct defaulted to 0.0 (no division by zero)."
                ),
            }
        else:
            result = {
                "success": True,
                "overlap_pct": intersection / from_distinct,
                "from_distinct_count": from_distinct,
                "to_distinct_count": to_distinct,
                "intersection_count": intersection,
            }
        logger.info(
            "tool_column_value_overlap: overlap_pct=%.4f (%d/%d)",
            result["overlap_pct"],
            intersection,
            from_distinct,
        )
        return json.dumps(result)
    except Exception as exc:
        logger.error("tool_column_value_overlap: query failed: %s", exc, exc_info=True)
        return json.dumps({"success": False, "error": str(exc)})


def tool_distinct_count(
    ctx: ToolContext, *, full_name: str = "", column: str = "", **_kwargs
) -> str:
    """Report row / distinct / null counts for ``full_name.column`` and
    derive ``is_unique`` and ``is_complete`` flags.

    * ``is_unique = distinct_count == row_count - null_count`` — i.e. the
      non-null subset has no duplicates.
    * ``is_complete = null_count == 0`` — no missing values.
    """
    logger.info("tool_distinct_count: %s.%s", full_name, column)
    if not (full_name and column):
        return json.dumps(
            {"success": False, "error": "full_name and column are required"}
        )

    sql = (
        f"SELECT COUNT(*) AS row_count, "
        f"       COUNT(DISTINCT {column}) AS distinct_count, "
        f"       COUNT(*) - COUNT({column}) AS null_count "
        f"FROM {full_name}"
    )
    logger.debug("tool_distinct_count: SQL=%s", sql)

    try:
        rows = ctx.client.execute_query(sql)
        if not rows:
            return json.dumps(
                {"success": False, "error": "distinct_count query returned no rows"}
            )
        row = rows[0]
        row_count = int(row.get("row_count", 0) or 0)
        distinct_count = int(row.get("distinct_count", 0) or 0)
        null_count = int(row.get("null_count", 0) or 0)
        non_null_rows = row_count - null_count

        result = {
            "success": True,
            "row_count": row_count,
            "distinct_count": distinct_count,
            "null_count": null_count,
            "is_unique": distinct_count == non_null_rows,
            "is_complete": null_count == 0,
        }
        logger.info(
            "tool_distinct_count: rows=%d, distinct=%d, nulls=%d, unique=%s, complete=%s",
            row_count,
            distinct_count,
            null_count,
            result["is_unique"],
            result["is_complete"],
        )
        return json.dumps(result)
    except Exception as exc:
        logger.error("tool_distinct_count: query failed: %s", exc, exc_info=True)
        return json.dumps({"success": False, "error": str(exc)})


def tool_submit_source_model(
    ctx: ToolContext, *, model: Optional[dict] = None, **_kwargs
) -> str:
    """Terminal Planner tool: validate ``model`` against
    :class:`SourceModel` and stash the dataclass on ``ctx.source_model``.

    Only structural validity is checked here (does ``SourceModel.from_dict``
    succeed?). Semantic checks — e.g. coverage against the live ontology —
    are the orchestrator's responsibility.
    """
    # Local import to keep ``agents.tools`` importable without
    # ``agents.agent_mapping_pge`` (avoids circular imports during pkg init).
    from agents.agent_mapping_pge.contracts import SourceModel

    logger.info("tool_submit_source_model: validating candidate model")
    if model is None or not isinstance(model, dict):
        return json.dumps(
            {"success": False, "error": "model must be a JSON object"}
        )

    try:
        source_model = SourceModel.from_dict(model)
    except (KeyError, TypeError, ValueError) as exc:
        # ``KeyError`` for missing required fields; ``TypeError`` / ``ValueError``
        # for bad coercions (e.g. confidence not float-parseable).
        logger.warning(
            "tool_submit_source_model: validation failed: %s: %s",
            type(exc).__name__,
            exc,
        )
        return json.dumps(
            {
                "success": False,
                "error": f"SourceModel validation failed: {type(exc).__name__}: {exc}",
            }
        )

    ctx.source_model = source_model
    summary = {
        "table_roles": len(source_model.table_roles),
        "canonical_ids": len(source_model.canonical_ids),
        "join_keys": len(source_model.join_keys),
        "entity_order_len": len(source_model.mapping_plan.entity_order),
        "relationship_order_len": len(source_model.mapping_plan.relationship_order),
    }
    logger.info("tool_submit_source_model: stored — %s", summary)
    return json.dumps({"success": True, "summary": summary})


# =====================================================
# OpenAI function-calling definitions
# =====================================================


SAMPLE_TABLE_DEF: dict = {
    "type": "function",
    "function": {
        "name": "sample_table",
        "description": (
            "Return up to N random sample rows from a table so you can see actual values "
            "(not just column types). n defaults to 20 and is capped at 100."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "full_name": {
                    "type": "string",
                    "description": "Fully-qualified table name (catalog.schema.table).",
                },
                "n": {
                    "type": "integer",
                    "description": "Sample size (default 20, max 100).",
                },
            },
            "required": ["full_name"],
        },
    },
}


COLUMN_VALUE_OVERLAP_DEF: dict = {
    "type": "function",
    "function": {
        "name": "column_value_overlap",
        "description": (
            "Compute the one-sided overlap |distinct(from) ∩ distinct(to)| / |distinct(from)|. "
            "Use this to validate a candidate join key before committing it to the SourceModel."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "from_table": {
                    "type": "string",
                    "description": "Fully-qualified source table.",
                },
                "from_column": {
                    "type": "string",
                    "description": "Column on the source side (numerator denominator).",
                },
                "to_table": {
                    "type": "string",
                    "description": "Fully-qualified target table.",
                },
                "to_column": {
                    "type": "string",
                    "description": "Column on the target side.",
                },
            },
            "required": ["from_table", "from_column", "to_table", "to_column"],
        },
    },
}


DISTINCT_COUNT_DEF: dict = {
    "type": "function",
    "function": {
        "name": "distinct_count",
        "description": (
            "Report row_count / distinct_count / null_count for a column, with is_unique "
            "and is_complete flags. Use this to vet a candidate canonical-ID column."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "full_name": {
                    "type": "string",
                    "description": "Fully-qualified table name (catalog.schema.table).",
                },
                "column": {
                    "type": "string",
                    "description": "Column to characterise.",
                },
            },
            "required": ["full_name", "column"],
        },
    },
}


SUBMIT_SOURCE_MODEL_DEF: dict = {
    "type": "function",
    "function": {
        "name": "submit_source_model",
        "description": (
            "Terminal Planner tool. Submit the final SourceModel JSON (matching "
            "SourceModel.to_dict() shape). Validates the structure and stores the "
            "dataclass on the ToolContext for the Generator stage to consume."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "model": {
                    "type": "object",
                    "description": (
                        "JSON-encoded SourceModel with table_roles, canonical_ids, "
                        "join_keys, and mapping_plan."
                    ),
                }
            },
            "required": ["model"],
        },
    },
}


# =====================================================
# Aggregate exports
# =====================================================


PLANNER_TOOL_DEFINITIONS: List[dict] = [
    SAMPLE_TABLE_DEF,
    COLUMN_VALUE_OVERLAP_DEF,
    DISTINCT_COUNT_DEF,
    SUBMIT_SOURCE_MODEL_DEF,
]


PLANNER_TOOL_HANDLERS: Dict[str, Callable] = {
    "sample_table": tool_sample_table,
    "column_value_overlap": tool_column_value_overlap,
    "distinct_count": tool_distinct_count,
    "submit_source_model": tool_submit_source_model,
}
