"""Response payload tests for Graph Chat assistant routes."""

from agents.agent_dtwin_chat.engine import AgentResult, SYSTEM_PROMPT
from api.routers.internal import dtwin


_PENDING_ACTION = {
    "token": "confirm-token",
    "entity_uri": "https://example.com/Customer/CUST1",
    "action": "main.ops.recompute_risk",
}


def test_system_prompt_documents_dataset_bridge_and_action_workflow():
    assert "CONTEXT (ontology design)" in SYSTEM_PROMPT
    assert "get_entity_context(entity_uri, fetch_dataset_rows?, follow_bridges?)" in SYSTEM_PROMPT
    assert "request_entity_action(entity_uri, action)" in SYSTEM_PROMPT
    assert "Never claim an Action ran" in SYSTEM_PROMPT


def test_chat_payload_includes_pending_action_for_json_and_sse_done():
    result = AgentResult(
        success=True,
        reply="Action is ready for confirmation.",
        pending_action=_PENDING_ACTION,
    )

    blocking_payload = dtwin._chat_response_payload(result)
    stream_payload = dtwin._chat_response_payload(result, event_type="done")

    assert blocking_payload["pending_action"] == _PENDING_ACTION
    assert "type" not in blocking_payload
    assert stream_payload["type"] == "done"
    assert stream_payload["pending_action"] == _PENDING_ACTION
