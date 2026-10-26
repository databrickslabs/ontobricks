"""Response payload tests for Graph Chat assistant routes."""

from types import SimpleNamespace
from unittest.mock import patch

from agents.agent_dtwin_chat.engine import SYSTEM_PROMPT, AgentResult
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


def test_chat_rejects_empty_domain_llm_before_running_agent(client):
    domain = SimpleNamespace(info={})

    with (
        patch.object(dtwin, "get_domain", return_value=domain),
        patch("agents.agent_dtwin_chat.run_agent") as run_chat_agent,
    ):
        response = client.post("/dtwin/assistant/chat", json={"message": "Hello"})

    assert response.status_code == 400
    assert response.json()["message"] == (
        "No LLM selected. Select one in Domain → General → AI."
    )
    run_chat_agent.assert_not_called()
