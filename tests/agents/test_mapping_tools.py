"""Tests for ``agents.tools.mapping`` (the shared mapping-submission tools).

The historical mapping-submission tools were exercised only indirectly via
the auto-mapping agent's integration tests. With the Sprint 4 addition of
``unmapped_attributes`` to ``tool_submit_entity_mapping`` we need a direct
assertion that the new field round-trips through the mapping dict that
lands on ``ctx.entity_mappings``.
"""

import json

from agents.tools.context import ToolContext
from agents.tools.mapping import tool_submit_entity_mapping


def _ctx() -> ToolContext:
    return ToolContext(host="https://x", token="t")


class TestSubmitEntityMappingUnmappedAttributes:
    """The Sprint 4 NO-SILENT-DROPS invariant requires capturing attributes
    that the Generator intentionally did not map. The dict form
    ({"name", "reason"}) is the preferred shape; bare strings are accepted
    as a fallback."""

    def test_dict_form_round_trips(self):
        ctx = _ctx()
        payload = tool_submit_entity_mapping(
            ctx,
            class_uri="http://ex.org/maternity#Mother",
            class_name="Mother",
            sql_query="SELECT nhs_number AS ID, nhs_number AS Label FROM cat.sch.mothers",
            id_column="nhs_number",
            label_column="nhs_number",
            attribute_mappings={"nhsNumber": "nhs_number"},
            unmapped_attributes=[
                {"name": "ethnicity", "reason": "column absent from this table"}
            ],
        )
        body = json.loads(payload)
        assert body["success"] is True
        assert body["attributes_unmapped"] == 1
        # The mapping dict on the context carries the new field.
        assert len(ctx.entity_mappings) == 1
        mapping = ctx.entity_mappings[0]
        assert mapping["unmapped_attributes"] == [
            {"name": "ethnicity", "reason": "column absent from this table"}
        ]

    def test_string_form_round_trips(self):
        ctx = _ctx()
        tool_submit_entity_mapping(
            ctx,
            class_uri="http://ex.org/maternity#Mother",
            class_name="Mother",
            sql_query="SELECT nhs_number AS ID, nhs_number AS Label FROM cat.sch.mothers",
            id_column="nhs_number",
            label_column="nhs_number",
            attribute_mappings={"nhsNumber": "nhs_number"},
            unmapped_attributes=["ethnicity"],
        )
        assert ctx.entity_mappings[0]["unmapped_attributes"] == ["ethnicity"]

    def test_default_empty_list(self):
        ctx = _ctx()
        tool_submit_entity_mapping(
            ctx,
            class_uri="http://ex.org/maternity#Mother",
            class_name="Mother",
            sql_query="SELECT nhs_number AS ID, nhs_number AS Label FROM cat.sch.mothers",
            id_column="nhs_number",
            label_column="nhs_number",
            attribute_mappings={"nhsNumber": "nhs_number"},
        )
        assert ctx.entity_mappings[0]["unmapped_attributes"] == []
