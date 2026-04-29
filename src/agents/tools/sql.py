"""
SQL execution tool – used by the auto-mapping agent.

Provides a tool to execute SQL queries on Databricks via the DatabricksClient.
"""

import json
import re
from typing import Callable, Dict, List

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)

_SAMPLE_ROWS = 3


# =====================================================
# Tool implementation
# =====================================================


def tool_execute_sql(ctx: ToolContext, *, sql: str = "", **_kwargs) -> str:
    """Execute a SQL query on Databricks and return columns + sample rows."""
    logger.info("tool_execute_sql: executing query (%d chars)", len(sql))
    if not sql:
        logger.warning("tool_execute_sql: empty SQL")
        return json.dumps({"error": "sql parameter is required"})

    logger.debug("tool_execute_sql: SQL=%s", sql)

    stripped_sql = sql.strip().rstrip(";").strip()
    is_row_query = bool(re.match(r"^(SELECT|WITH)\b", stripped_sql, re.IGNORECASE))

    if is_row_query:
        test_sql = re.sub(
            r"\bLIMIT\s+\d+\b", "", stripped_sql, flags=re.IGNORECASE
        ).strip()
        test_sql_limited = f"{test_sql} LIMIT {_SAMPLE_ROWS}"
    else:
        test_sql_limited = stripped_sql
    logger.debug("tool_execute_sql: test_sql=%s", test_sql_limited)

    try:
        rows = ctx.client.execute_query(test_sql_limited)
        columns = list(rows[0].keys()) if rows else []
        sample = rows[:_SAMPLE_ROWS] if rows else []

        safe_sample = []
        for row in sample:
            safe_row = {}
            for k, v in row.items():
                safe_row[k] = str(v) if v is not None else None
            safe_sample.append(safe_row)

        logger.info(
            "tool_execute_sql: success — %d column(s): %s, %d sample row(s)",
            len(columns),
            columns,
            len(safe_sample),
        )
        return json.dumps(
            {
                "success": True,
                "columns": columns,
                "column_count": len(columns),
                "sample_rows": safe_sample,
                "row_count": len(safe_sample),
            }
        )
    except Exception as exc:
        logger.error("tool_execute_sql: query failed: %s", exc, exc_info=True)
        return json.dumps({"success": False, "error": str(exc)})


# =====================================================
# OpenAI function-calling definition
# =====================================================

SQL_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "execute_sql",
            "description": (
                "Execute a Databricks SQL query and return the column names plus a few sample rows. "
                "Use this to validate your SQL before submitting a mapping. "
                "The query is executed with a small LIMIT for testing; the stored mapping will have no LIMIT."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL SELECT query to execute (no LIMIT needed, one is added automatically for testing).",
                    }
                },
                "required": ["sql"],
            },
        },
    },
]

SQL_TOOL_HANDLERS: Dict[str, Callable] = {
    "execute_sql": tool_execute_sql,
}
