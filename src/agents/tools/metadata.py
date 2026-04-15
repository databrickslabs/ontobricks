"""
Metadata tools – shared across agents.

Provides tools to retrieve domain table metadata from the ToolContext.
All metadata is read from imported metadata for the current domain only — no direct
Unity Catalog queries. Metadata must be loaded in Domain settings first.

Limits are applied to avoid exceeding LLM context size when there are
many tables or columns. Use get_table_detail for full schema of a specific table.
"""

import json
from typing import Callable, Dict, List

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)

# Limits to avoid "too many" context overflow in auto-assign
_MAX_TABLES_IN_METADATA = 50
_MAX_COLUMNS_PER_TABLE = 80


# =====================================================
# Tool implementations
# =====================================================

def tool_get_metadata(ctx: ToolContext, **_kwargs) -> str:
    """Return domain table metadata from imported metadata only.
    Does NOT query Unity Catalog — uses pre-loaded metadata from Domain settings.
    When there are many tables/columns, results are limited to avoid context overflow.
    Use get_table_detail for full schema of a specific table."""
    logger.info("tool_get_metadata: retrieving domain metadata")
    tables = ctx.metadata.get("tables", [])
    if not tables:
        logger.info("tool_get_metadata: no tables in metadata")
        return json.dumps({"error": "No metadata loaded", "tables": []})

    total_tables = len(tables)
    tables = tables[:_MAX_TABLES_IN_METADATA]
    truncated_tables = total_tables > len(tables)

    logger.debug("tool_get_metadata: processing %d table(s)%s", len(tables),
                 f" (showing first {len(tables)} of {total_tables})" if truncated_tables else "")

    result: List[dict] = []
    total_cols = 0
    for t in tables:
        cols = [
            {"name": c.get("name"), "type": c.get("type"), "comment": (c.get("comment") or "")[:200]}
            for c in t.get("columns", [])[:_MAX_COLUMNS_PER_TABLE]
        ]
        original_col_count = len(t.get("columns", []))
        truncated_cols = original_col_count > len(cols)
        total_cols += len(cols)
        entry = {
            "name": t.get("name"),
            "full_name": t.get("full_name", t.get("name")),
            "comment": (t.get("comment") or "")[:500],
            "column_count": len(cols),
            "columns": cols,
        }
        if truncated_cols:
            entry["_note"] = f"Showing first {len(cols)} of {original_col_count} columns. Use get_table_detail('{entry['full_name']}') for full schema."
        result.append(entry)
        logger.debug(
            "tool_get_metadata: table '%s' — %d columns",
            entry["full_name"], len(cols),
        )

    out = {
        "tables": result,
        "table_count": len(result),
    }
    if truncated_tables:
        out["_truncated"] = True
        out["_message"] = f"Showing first {len(result)} of {total_tables} tables to avoid context overflow. Use get_table_detail(table_name) for any table's full schema."

    logger.info("tool_get_metadata: returning %d table(s), %d total column(s)%s",
                len(result), total_cols, " (truncated)" if truncated_tables else "")
    return json.dumps(out)


def tool_get_table_detail(ctx: ToolContext, *, table_name: str = "", **_kwargs) -> str:
    """Return detailed metadata for one specific table."""
    logger.info("tool_get_table_detail: looking up '%s'", table_name)
    if not table_name:
        logger.warning("tool_get_table_detail: called without table_name parameter")
        return json.dumps({"error": "table_name is required"})

    available_tables = ctx.metadata.get("tables", [])
    logger.debug(
        "tool_get_table_detail: searching '%s' in %d table(s): [%s]",
        table_name, len(available_tables),
        ", ".join(t.get("full_name", t.get("name", "?")) for t in available_tables),
    )
    for t in available_tables:
        name = t.get("full_name", t.get("name", ""))
        if name == table_name or t.get("name") == table_name:
            cols = [
                {"name": c.get("name"), "type": c.get("type"), "comment": c.get("comment", "")}
                for c in t.get("columns", [])
            ]
            logger.info("tool_get_table_detail: found '%s' — %d column(s)", name, len(cols))
            logger.debug(
                "tool_get_table_detail: '%s' columns: [%s]",
                name, ", ".join(f"{c['name']}:{c['type']}" for c in cols),
            )
            return json.dumps({
                "name": t.get("name"),
                "full_name": t.get("full_name"),
                "comment": t.get("comment", ""),
                "columns": cols,
                "column_count": len(cols),
            })
    logger.warning(
        "tool_get_table_detail: table '%s' not found in %d available table(s)",
        table_name, len(available_tables),
    )
    return json.dumps({"error": f"Table '{table_name}' not found"})


# =====================================================
# OpenAI function-calling definitions
# =====================================================

GET_METADATA_DEF: dict = {
    "type": "function",
    "function": {
        "name": "get_metadata",
        "description": (
            "Get the domain's database table metadata: table names (full catalog.schema.table names), "
            "column names, data types, and descriptions. Call this first to understand the available data."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

METADATA_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_metadata",
            "description": (
                "Get the domain's database table metadata: table names (full catalog.schema.table names), "
                "column names, data types, and descriptions. Call this first to understand the available data."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_detail",
            "description": "Get detailed metadata for a single table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Full (catalog.schema.table) or short table name",
                    }
                },
                "required": ["table_name"],
            },
        },
    },
]

METADATA_TOOL_HANDLERS: Dict[str, Callable] = {
    "get_metadata": tool_get_metadata,
    "get_table_detail": tool_get_table_detail,
}
