"""
Planner tools – used by the mapping-PGE Planner agent (Sprint 2+).

Exposes the OpenAI function-calling tools that let the Planner LLM probe
source tables and submit a validated ``SourceModel`` artefact:

* ``sample_table``         — N random rows from a table (n capped at 100).
* ``column_value_overlap`` — one-sided distinct-value overlap between two columns.
* ``normalized_value_overlap`` — same metric, but each side is a scalar SQL
  expression, so canonical-key normalizations can be proven before commit.
* ``distinct_count``       — uniqueness / completeness of a candidate canonical id.
* ``submit_source_model``  — terminal tool: validates the candidate SourceModel
  JSON against :class:`agents.agent_mapping_pge.contracts.SourceModel` and stores
  the dataclass instance on :attr:`ToolContext.source_model`.

All handlers return JSON strings (same convention as ``agents.tools.sql``)
and stringify scalar values for the LLM-facing surface.
"""

import json
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)


# Cap on ``n`` in ``sample_table`` to keep the LLM context bounded.
_SAMPLE_TABLE_MAX_N = 100
_SAMPLE_TABLE_DEFAULT_N = 20


# Permissive but injection-safe SQL identifier shape. We allow dots (for
# fully-qualified ``catalog.schema.table``) and backticks (for quoted
# identifiers), plus the usual alphanumerics + underscore. Anything else
# — semicolons, whitespace, quotes, comment markers — is rejected.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.`]+$")


# SQL keywords whose presence in a "normalization expression" indicates the
# string is no longer a scalar expression but a smuggled clause / subquery /
# DDL. A legitimate canonical-key expression (regexp_extract, regexp_replace,
# concat, substring, lower, upper, trim, coalesce, ||, string literals) needs
# none of these. Matched case-insensitively as whole words.
_EXPR_FORBIDDEN_WORDS = frozenset(
    {
        "select",
        "from",
        "where",
        "join",
        "union",
        "intersect",
        "except",
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "grant",
        "revoke",
        "table",
        "into",
        "exec",
        "execute",
        "call",
        "merge",
        "values",
        "having",
        "group",
        "order",
        "limit",
    }
)
_EXPR_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_safe_expression(expr: str, *, role: str) -> Optional[str]:
    """Return None if ``expr`` is a safe scalar SQL expression; else an error.

    Unlike :func:`_validate_identifier`, this permits the parentheses, commas,
    quotes and operators a canonical-key normalization needs (e.g.
    ``regexp_extract(EPISODE_ID, '([a-f0-9][a-f0-9-]+-preg-\\d+)', 1)`` or
    ``concat(regexp_extract(delivery_id, '...', 1), '-del')``). It still gets
    interpolated into SQL via an f-string, so it is gated against the obvious
    injection vectors: statement terminators, comment markers, and any SQL
    keyword that would turn the scalar into a clause/subquery/DDL.
    """
    if not isinstance(expr, str) or not expr.strip():
        return f"invalid {role}: must be a non-empty string"
    if ";" in expr or "--" in expr or "/*" in expr or "*/" in expr:
        return (
            f"invalid {role}: must not contain ';' or SQL comment markers "
            f"(got {expr!r})"
        )
    bad = sorted(
        {
            w.lower()
            for w in _EXPR_WORD_RE.findall(expr)
            if w.lower() in _EXPR_FORBIDDEN_WORDS
        }
    )
    if bad:
        return (
            f"invalid {role}: a canonical-key expression must be a single scalar "
            f"expression, not a clause/subquery. Forbidden keyword(s): "
            f"{', '.join(bad)} (got {expr!r})"
        )
    return None


def _validate_identifier(name: str, *, role: str) -> Optional[str]:
    """Return None if ``name`` is a valid SQL identifier; else an error message.

    Used to gate identifiers that get interpolated into SQL via f-strings.
    Even though today's callers are LLMs (not untrusted users), a hallucinated
    identifier like ``t; DROP TABLE x`` or ``nhs FROM secrets--`` would
    otherwise execute.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.fullmatch(name):
        return f"invalid {role}: {name!r}"
    return None


def _run_query(
    ctx: ToolContext,
    sql: str,
    *,
    tool_name: str,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Execute the SQL via the client. Returns ``(rows, None)`` on success,
    ``(None, error_str)`` on failure. On failure the SQL is logged at ERROR
    level alongside the exception (previously only at DEBUG).
    """
    try:
        result = ctx.client.execute_query(sql)
        return result, None
    except Exception as exc:
        logger.error(
            "%s: query failed: %s\nSQL: %s", tool_name, exc, sql, exc_info=True
        )
        return None, str(exc)


# =====================================================
# Tool implementations
# =====================================================


def tool_sample_table(
    ctx: ToolContext, *, full_name: str = "", n: Any = _SAMPLE_TABLE_DEFAULT_N, **_kwargs
) -> str:
    """Return N random sample rows from ``full_name`` so the agent can see
    real values (not just column types). ``n`` is capped at 100.
    """
    logger.info("tool_sample_table: full_name=%s, n=%s", full_name, n)
    if not full_name:
        return json.dumps({"success": False, "error": "full_name is required"})

    err = _validate_identifier(full_name, role="full_name")
    if err is not None:
        return json.dumps({"success": False, "error": err})

    # Strict ``n`` parsing: a malformed value is a tool-call error, not a
    # silent fallback. The default (when ``n`` is omitted) is already the int
    # ``_SAMPLE_TABLE_DEFAULT_N``, so ``int(n)`` is a no-op in that case.
    try:
        n_int = int(n)
    except (TypeError, ValueError):
        return json.dumps({"success": False, "error": f"invalid n: {n!r}"})
    capped_n = max(1, min(n_int, _SAMPLE_TABLE_MAX_N))

    sql = f"SELECT * FROM {full_name} ORDER BY RAND() LIMIT {capped_n}"
    logger.debug("tool_sample_table: SQL=%s", sql)

    rows, err = _run_query(ctx, sql, tool_name="tool_sample_table")
    if err is not None:
        return json.dumps({"success": False, "error": err})

    rows = rows or []
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

    for value, role in (
        (from_table, "from_table"),
        (from_column, "from_column"),
        (to_table, "to_table"),
        (to_column, "to_column"),
    ):
        err = _validate_identifier(value, role=role)
        if err is not None:
            return json.dumps({"success": False, "error": err})

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

    rows, err = _run_query(ctx, sql, tool_name="tool_column_value_overlap")
    if err is not None:
        return json.dumps({"success": False, "error": err})
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
            # Symmetric shape with the zero-denom branch: downstream consumers
            # can read ``note`` unconditionally.
            "note": "",
        }
    logger.info(
        "tool_column_value_overlap: overlap_pct=%.4f (%d/%d)",
        result["overlap_pct"],
        intersection,
        from_distinct,
    )
    return json.dumps(result)


def tool_normalized_value_overlap(
    ctx: ToolContext,
    *,
    from_table: str = "",
    from_expr: str = "",
    to_table: str = "",
    to_expr: str = "",
    **_kwargs,
) -> str:
    """Like :func:`tool_column_value_overlap`, but each side is an arbitrary
    scalar SQL *expression* rather than a bare column.

    This is the tool the Planner uses to PROVE a canonical-key normalization
    works before committing it. When two tables that map to the same ontology
    class have 0% raw-column overlap, the values are trust-local encodings of
    the same key. The Planner proposes a normalization expression per table
    (e.g. ``regexp_extract(EPISODE_ID, '([a-f0-9][a-f0-9-]+-preg-\\d+)', 1)``)
    and calls this tool to confirm the expressions land in a common value
    space (overlap_pct > 0). A still-zero overlap means the normalization is
    wrong — fix it before submitting.
    """
    logger.info(
        "tool_normalized_value_overlap: %s[%s] ↔ %s[%s]",
        from_table,
        from_expr,
        to_table,
        to_expr,
    )
    if not (from_table and from_expr and to_table and to_expr):
        return json.dumps(
            {
                "success": False,
                "error": "from_table, from_expr, to_table, to_expr are all required",
            }
        )

    for value, role in ((from_table, "from_table"), (to_table, "to_table")):
        err = _validate_identifier(value, role=role)
        if err is not None:
            return json.dumps({"success": False, "error": err})
    for value, role in ((from_expr, "from_expr"), (to_expr, "to_expr")):
        err = _validate_safe_expression(value, role=role)
        if err is not None:
            return json.dumps({"success": False, "error": err})

    sql = (
        "WITH from_distinct AS ("
        f"  SELECT DISTINCT {from_expr} AS v FROM {from_table} "
        f"  WHERE {from_expr} IS NOT NULL AND {from_expr} <> ''"
        "),"
        " to_distinct AS ("
        f"  SELECT DISTINCT {to_expr} AS v FROM {to_table} "
        f"  WHERE {to_expr} IS NOT NULL AND {to_expr} <> ''"
        "),"
        " inter AS ("
        "  SELECT v FROM from_distinct INTERSECT SELECT v FROM to_distinct"
        ") "
        "SELECT (SELECT COUNT(*) FROM from_distinct) AS from_distinct_count, "
        "       (SELECT COUNT(*) FROM to_distinct)   AS to_distinct_count, "
        "       (SELECT COUNT(*) FROM inter)         AS intersection_count"
    )
    logger.debug("tool_normalized_value_overlap: SQL=%s", sql)

    rows, err = _run_query(ctx, sql, tool_name="tool_normalized_value_overlap")
    if err is not None:
        return json.dumps({"success": False, "error": err})
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
                f"{from_expr} over {from_table} produced zero distinct non-empty "
                "values; the expression likely does not match the data — revise it."
            ),
        }
    else:
        result = {
            "success": True,
            "overlap_pct": intersection / from_distinct,
            "from_distinct_count": from_distinct,
            "to_distinct_count": to_distinct,
            "intersection_count": intersection,
            "note": "",
        }
    logger.info(
        "tool_normalized_value_overlap: overlap_pct=%.4f (%d/%d)",
        result["overlap_pct"],
        intersection,
        from_distinct,
    )
    return json.dumps(result)


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

    for value, role in ((full_name, "full_name"), (column, "column")):
        err = _validate_identifier(value, role=role)
        if err is not None:
            return json.dumps({"success": False, "error": err})

    sql = (
        f"SELECT COUNT(*) AS row_count, "
        f"       COUNT(DISTINCT {column}) AS distinct_count, "
        f"       COUNT(*) - COUNT({column}) AS null_count "
        f"FROM {full_name}"
    )
    logger.debug("tool_distinct_count: SQL=%s", sql)

    rows, err = _run_query(ctx, sql, tool_name="tool_distinct_count")
    if err is not None:
        return json.dumps({"success": False, "error": err})
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


NORMALIZED_VALUE_OVERLAP_DEF: dict = {
    "type": "function",
    "function": {
        "name": "normalized_value_overlap",
        "description": (
            "Same overlap metric as column_value_overlap, but each side is a "
            "scalar SQL EXPRESSION instead of a bare column. Use this to PROVE a "
            "canonical-key normalization before committing it: when two tables "
            "that map to the same ontology class have 0% raw-column overlap, "
            "propose a normalization expression per table (e.g. "
            "regexp_extract(EPISODE_ID, '([a-f0-9][a-f0-9-]+-preg-\\d+)', 1)) and "
            "call this to confirm overlap_pct > 0. A still-zero result means the "
            "expression is wrong — fix it before submit_source_model. Expressions "
            "must be a single scalar (functions/literals/operators only); "
            "subqueries and SQL keywords are rejected."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "from_table": {
                    "type": "string",
                    "description": "Fully-qualified source table.",
                },
                "from_expr": {
                    "type": "string",
                    "description": (
                        "Scalar SQL expression over the source table that "
                        "produces the canonical key (e.g. a regexp_extract / "
                        "concat). Bare column names are also accepted."
                    ),
                },
                "to_table": {
                    "type": "string",
                    "description": "Fully-qualified target table.",
                },
                "to_expr": {
                    "type": "string",
                    "description": "Scalar SQL expression over the target table.",
                },
            },
            "required": ["from_table", "from_expr", "to_table", "to_expr"],
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
    NORMALIZED_VALUE_OVERLAP_DEF,
    DISTINCT_COUNT_DEF,
    SUBMIT_SOURCE_MODEL_DEF,
]


PLANNER_TOOL_HANDLERS: Dict[str, Callable] = {
    "sample_table": tool_sample_table,
    "column_value_overlap": tool_column_value_overlap,
    "normalized_value_overlap": tool_normalized_value_overlap,
    "distinct_count": tool_distinct_count,
    "submit_source_model": tool_submit_source_model,
}
