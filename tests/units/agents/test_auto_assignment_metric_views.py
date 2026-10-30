"""Auto-Map must treat Unity Catalog metric views as metric-aware sources."""

import json

import pytest

from agents.agent_auto_assignment.engine import SYSTEM_PROMPT
from agents.tools.context import ToolContext
from agents.tools.mapping import (
    tool_submit_entity_mapping,
    tool_submit_relationship_mapping,
)

pytestmark = pytest.mark.unit

_MV = {
    "name": "region_metrics",
    "full_name": "cat.sales.region_metrics",
    "object_kind": "metric_view",
    "columns": [
        {"name": "region", "type": "string", "role": "dimension"},
        {"name": "revenue", "type": "double", "role": "measure"},
    ],
}


def test_auto_map_prompt_has_metric_rule():
    assert "metric_view" in SYSTEM_PROMPT
    assert "MEASURE(" in SYSTEM_PROMPT
    assert "GROUP BY" in SYSTEM_PROMPT
    assert "object_kind" in SYSTEM_PROMPT


@pytest.fixture
def ctx():
    return ToolContext(
        host="https://test.databricks.com",
        token="fake-token",
        metadata={"tables": [_MV]},
    )


def test_submit_rejects_select_star_on_a_metric_view(ctx):
    result = json.loads(
        tool_submit_entity_mapping(
            ctx,
            class_uri="ex:RegionRevenue",
            class_name="RegionRevenue",
            sql_query="SELECT * FROM cat.sales.region_metrics",
            id_column="region",
            label_column="region",
        )
    )
    assert result["success"] is False
    assert "MEASURE" in result["error"] or "SELECT *" in result["error"]
    assert ctx.entity_mappings == []


def test_submit_accepts_measure_wrapped_metric_sql(ctx):
    sql = (
        "SELECT region AS ID, region AS Label, MEASURE(revenue) AS revenue "
        "FROM cat.sales.region_metrics GROUP BY region"
    )
    result = json.loads(
        tool_submit_entity_mapping(
            ctx,
            class_uri="ex:RegionRevenue",
            class_name="RegionRevenue",
            sql_query=sql,
            id_column="ID",
            label_column="Label",
        )
    )
    assert result["success"] is True
    assert len(ctx.entity_mappings) == 1


def test_relationship_submit_rejects_unwrapped_measure(ctx):
    result = json.loads(
        tool_submit_relationship_mapping(
            ctx,
            property_uri="ex:hasRevenue",
            property_name="hasRevenue",
            sql_query="SELECT region AS source_id, revenue AS target_id FROM cat.sales.region_metrics",
            source_id_column="source_id",
            target_id_column="target_id",
        )
    )
    assert result["success"] is False
    assert ctx.relationships == []
