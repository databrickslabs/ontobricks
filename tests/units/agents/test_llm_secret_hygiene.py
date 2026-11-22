"""Credentials stay out of MLflow traces; LLM errors reach users without URL or body."""

from pathlib import Path

import pytest
import requests

from agents.llm_utils import llm_failure_message

pytestmark = pytest.mark.unit

SRC = Path(__file__).resolve().parents[3] / "src"


def _http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.exceptions.HTTPError(
        f"{status} Client Error for url: https://ws/serving-endpoints/x/invocations "
        "— body: {\"secret\": \"internal\"}",
        response=response,
    )


@pytest.mark.parametrize(
    "exc,expected",
    [
        (_http_error(404), "LLM request failed: endpoint not found (HTTP 404)."),
        (_http_error(500), "LLM request failed (HTTP 500)."),
        (requests.exceptions.ReadTimeout("https://ws/x"), "LLM request failed: the endpoint timed out."),
        (requests.exceptions.ConnectionError("https://ws/x"), "LLM request failed: could not reach the endpoint."),
        (RuntimeError("boom https://ws/x"), "LLM request failed."),
    ],
)
def test_llm_failure_message_drops_url_and_body(exc, expected):
    message = llm_failure_message(exc)
    assert message == expected
    assert "https://" not in message and "body" not in message


def test_no_agent_formats_the_raw_llm_exception():
    offenders = [
        str(p.relative_to(SRC))
        for p in (SRC / "agents").rglob("*.py")
        if 'f"LLM request failed: {' in p.read_text()
    ]
    assert offenders == []


mlflow = pytest.importorskip("mlflow")


def test_responses_agent_takes_credentials_from_constructor_not_custom_inputs(monkeypatch):
    from mlflow.types.responses import ResponsesAgentRequest

    from agents.agent_ontology_assistant import responses_agent as mod

    seen = {}

    def _fake_llm(host, token, endpoint_name, messages, **kwargs):
        seen.update(host=host, token=token, endpoint=endpoint_name)
        return {"choices": [{"message": {"content": "done"}}]}

    monkeypatch.setattr(mod, "call_serving_endpoint", _fake_llm)
    agent = mod.OntologyAssistantResponsesAgent(host="https://ws/", token="real-token")
    request = ResponsesAgentRequest(
        input=[{"role": "user", "content": "hi"}],
        custom_inputs={"endpoint_name": "ep", "host": "https://evil", "token": "evil"},
    )

    list(agent.predict_stream(request))

    assert seen == {"host": "https://ws", "token": "real-token", "endpoint": "ep"}


def test_responses_agent_reports_missing_credentials(monkeypatch):
    from mlflow.types.responses import ResponsesAgentRequest

    from agents.agent_ontology_assistant import responses_agent as mod

    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    request = ResponsesAgentRequest(
        input=[{"role": "user", "content": "hi"}],
        custom_inputs={"endpoint_name": "ep", "host": "https://ws", "token": "t"},
    )

    events = list(mod.OntologyAssistantResponsesAgent().predict_stream(request))

    assert "Missing Databricks host or token" in events[0].item["content"][0]["text"]


def test_responses_agent_helpers_are_not_raw_mlflow_traced():
    source = (SRC / "agents/agent_ontology_assistant/responses_agent.py").read_text()
    assert "@mlflow.trace" not in source


def test_assistant_route_does_not_put_credentials_in_custom_inputs():
    source = (SRC / "api/routers/internal/ontology.py").read_text()
    assert 'custom_inputs["token"]' not in source
    assert 'custom_inputs["host"]' not in source
    assert "OntologyAssistantResponsesAgent(host=host, token=token)" in source
