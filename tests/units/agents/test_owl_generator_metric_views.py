"""Generate Stage 1 must see UC metric views as first-class sources."""

import json

from agents.agent_owl_generator import prompts
from agents.tools.context import ToolContext
from agents.tools.metadata import tool_get_metadata, tool_get_table_detail

_MV = {
    "name": "region_metrics",
    "full_name": "cat.sales.region_metrics",
    "object_kind": "metric_view",
    "comment": "Regional KPIs",
    "columns": [
        {"name": "region", "type": "string", "comment": "", "role": "dimension"},
        {"name": "revenue", "type": "double", "comment": "", "role": "measure"},
    ],
}


def test_get_metadata_exposes_object_kind_and_column_roles():
    ctx = ToolContext(host="h", token="t", metadata={"tables": [_MV]})
    listing = json.loads(tool_get_metadata(ctx))
    entry = listing["tables"][0]
    assert entry["object_kind"] == "metric_view"
    roles = {c["name"]: c["role"] for c in entry["columns"]}
    assert roles == {"region": "dimension", "revenue": "measure"}


def test_get_table_detail_exposes_object_kind_and_column_roles():
    ctx = ToolContext(host="h", token="t", metadata={"tables": [_MV]})
    detail = json.loads(
        tool_get_table_detail(ctx, table_name="cat.sales.region_metrics")
    )
    assert detail["object_kind"] == "metric_view"
    roles = {c["name"]: c["role"] for c in detail["columns"]}
    assert roles == {"region": "dimension", "revenue": "measure"}


def test_detection_prompt_treats_metric_view_dimensions_as_entities():
    text = prompts.build_detection_system_prompt(existing_anchors=())
    lowered = text.lower()
    assert "metric_view" in lowered or "metric view" in lowered
    assert "dimension" in lowered
    assert "measure" in lowered
    assert "never" in lowered and "measure" in lowered
