"""Both PGE generators instruct metric-aware SQL for metric-view sources."""

from agents.agent_mapping_pge.generators import entity, relationship


def test_entity_prompt_has_metric_rule():
    p = entity.SYSTEM_PROMPT
    assert "metric_view" in p
    assert "MEASURE(" in p
    assert "GROUP BY" in p


def test_relationship_prompt_has_metric_rule():
    p = relationship.SYSTEM_PROMPT
    assert "metric_view" in p
    assert "MEASURE(" in p
