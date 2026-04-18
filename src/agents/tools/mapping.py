"""
Mapping submission tools – used by the auto-mapping agent.

Provides tools to record entity and relationship mappings into the ToolContext.
"""

import json
import re
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)


# =====================================================
# Tool implementations
# =====================================================


def tool_submit_entity_mapping(
    ctx: ToolContext,
    *,
    class_uri: str = "",
    class_name: str = "",
    sql_query: str = "",
    id_column: str = "",
    label_column: str = "",
    attribute_mappings: Optional[dict] = None,
    **_kwargs,
) -> str:
    """Record a completed entity mapping."""
    logger.info("tool_submit_entity_mapping: '%s' (uri=%s)", class_name, class_uri)
    if not class_uri or not sql_query:
        logger.warning("tool_submit_entity_mapping: missing required fields")
        return json.dumps({"error": "class_uri and sql_query are required"})

    clean_sql = (
        re.sub(r"\s+LIMIT\s+\d+\s*$", "", sql_query, flags=re.IGNORECASE)
        .strip()
        .rstrip(";")
    )

    mapping = {
        "ontology_class": class_uri,
        "class_name": class_name,
        "sql_query": clean_sql,
        "id_column": id_column,
        "label_column": label_column,
        "attribute_mappings": attribute_mappings or {},
    }

    logger.debug(
        "tool_submit_entity_mapping: '%s' — ID=%s, Label=%s, attrs=%d",
        class_name,
        id_column,
        label_column,
        len(mapping["attribute_mappings"]),
    )

    existing_idx = next(
        (
            i
            for i, m in enumerate(ctx.entity_mappings)
            if m.get("ontology_class") == class_uri
        ),
        -1,
    )
    if existing_idx >= 0:
        ctx.entity_mappings[existing_idx] = mapping
        logger.debug(
            "tool_submit_entity_mapping: updated existing mapping at index %d",
            existing_idx,
        )
    else:
        ctx.entity_mappings.append(mapping)
        logger.debug("tool_submit_entity_mapping: appended new mapping")

    mapped_attrs = len(mapping["attribute_mappings"])
    logger.info(
        "tool_submit_entity_mapping: '%s' recorded — ID=%s, Label=%s, %d attr(s) mapped",
        class_name,
        id_column,
        label_column,
        mapped_attrs,
    )
    return json.dumps(
        {
            "success": True,
            "entity": class_name,
            "id_column": id_column,
            "label_column": label_column,
            "attributes_mapped": mapped_attrs,
            "total_entity_mappings": len(ctx.entity_mappings),
        }
    )


def tool_submit_relationship_mapping(
    ctx: ToolContext,
    *,
    property_uri: str = "",
    property_name: str = "",
    sql_query: str = "",
    source_id_column: str = "",
    target_id_column: str = "",
    domain: str = "",
    range_class: str = "",
    direction: str = "forward",
    **_kwargs,
) -> str:
    """Record a completed relationship mapping."""
    logger.info(
        "tool_submit_relationship_mapping: '%s' (uri=%s)", property_name, property_uri
    )
    if not property_uri or not sql_query:
        logger.warning("tool_submit_relationship_mapping: missing required fields")
        return json.dumps({"error": "property_uri and sql_query are required"})

    clean_sql = (
        re.sub(r"\s+LIMIT\s+\d+\s*$", "", sql_query, flags=re.IGNORECASE)
        .strip()
        .rstrip(";")
    )

    def _extract_label(value: str) -> str:
        if not value:
            return ""
        if value.startswith("http://") or value.startswith("https://"):
            return (
                value.split("#")[-1]
                if "#" in value
                else value.rstrip("/").split("/")[-1]
            )
        return value

    if direction == "reverse":
        src_class, tgt_class = range_class, domain
    else:
        src_class, tgt_class = domain, range_class

    mapping = {
        "property": property_uri,
        "property_name": property_name,
        "property_label": property_name,
        "domain": domain,
        "range": range_class,
        "direction": direction,
        "sql_query": clean_sql,
        "source_id_column": source_id_column,
        "target_id_column": target_id_column,
        "source_class": src_class,
        "target_class": tgt_class,
        "source_class_label": _extract_label(src_class),
        "target_class_label": _extract_label(tgt_class),
    }

    logger.debug(
        "tool_submit_relationship_mapping: '%s' — src_col=%s, tgt_col=%s, direction=%s",
        property_name,
        source_id_column,
        target_id_column,
        direction,
    )

    existing_idx = next(
        (
            i
            for i, m in enumerate(ctx.relationships)
            if m.get("property") == property_uri
        ),
        -1,
    )
    if existing_idx >= 0:
        ctx.relationships[existing_idx] = mapping
        logger.debug(
            "tool_submit_relationship_mapping: updated existing at index %d",
            existing_idx,
        )
    else:
        ctx.relationships.append(mapping)
        logger.debug("tool_submit_relationship_mapping: appended new mapping")

    logger.info(
        "tool_submit_relationship_mapping: '%s' recorded — source=%s, target=%s",
        property_name,
        source_id_column,
        target_id_column,
    )
    return json.dumps(
        {
            "success": True,
            "relationship": property_name,
            "source_id_column": source_id_column,
            "target_id_column": target_id_column,
            "total_relationship_mappings": len(ctx.relationships),
        }
    )


# =====================================================
# OpenAI function-calling definitions
# =====================================================

MAPPING_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "submit_entity_mapping",
            "description": (
                "Submit a validated entity mapping. Call this AFTER execute_sql confirms the query works. "
                "Provide the class URI, SQL query, ID/Label columns, and attribute-to-column mappings."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "class_uri": {
                        "type": "string",
                        "description": "The ontology class URI (from get_ontology).",
                    },
                    "class_name": {
                        "type": "string",
                        "description": "Human-readable class name.",
                    },
                    "sql_query": {
                        "type": "string",
                        "description": "The validated SQL query (no LIMIT).",
                    },
                    "id_column": {
                        "type": "string",
                        "description": "Name of the column used as entity identifier (typically aliased AS ID).",
                    },
                    "label_column": {
                        "type": "string",
                        "description": "Name of the column used as entity label (typically aliased AS Label).",
                    },
                    "attribute_mappings": {
                        "type": "object",
                        "description": (
                            "Map of ontology attribute names to SQL column names. "
                            'Example: {"firstName": "first_name", "age": "customer_age"}'
                        ),
                        "additionalProperties": {"type": "string"},
                    },
                },
                "required": [
                    "class_uri",
                    "class_name",
                    "sql_query",
                    "id_column",
                    "label_column",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_relationship_mapping",
            "description": (
                "Submit a validated relationship mapping. Call this AFTER execute_sql confirms the query works. "
                "Provide the property URI, SQL query, and source/target ID columns."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "property_uri": {
                        "type": "string",
                        "description": "The ontology property URI (from get_ontology).",
                    },
                    "property_name": {
                        "type": "string",
                        "description": "Human-readable property name.",
                    },
                    "sql_query": {
                        "type": "string",
                        "description": "The validated SQL query returning source_id and target_id (no LIMIT).",
                    },
                    "source_id_column": {
                        "type": "string",
                        "description": "Column name for the source entity identifier.",
                    },
                    "target_id_column": {
                        "type": "string",
                        "description": "Column name for the target entity identifier.",
                    },
                    "domain": {
                        "type": "string",
                        "description": "Domain class URI of the relationship.",
                    },
                    "range_class": {
                        "type": "string",
                        "description": "Range class URI of the relationship.",
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["forward", "reverse"],
                        "description": "Relationship direction (default: forward).",
                    },
                },
                "required": [
                    "property_uri",
                    "property_name",
                    "sql_query",
                    "source_id_column",
                    "target_id_column",
                    "domain",
                    "range_class",
                ],
            },
        },
    },
]

MAPPING_TOOL_HANDLERS: Dict[str, Callable] = {
    "submit_entity_mapping": tool_submit_entity_mapping,
    "submit_relationship_mapping": tool_submit_relationship_mapping,
}
