"""POST /ontology/assistant/invoke must not forward caller credentials in Apps mode.

Marketplace SOP (Phase 1): the app MUST NOT ask for or store user PATs. Since
0.10.0 the route always overwrites ``custom_inputs.host/token`` with the
server-resolved credentials (``require_domain_llm``), in every mode; these
tests guard that behaviour.
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import PermissionMiddleware, app

pytestmark = pytest.mark.unit

_route_module = importlib.import_module("api.routers.internal.ontology")
_agent_module = importlib.import_module("agents.agent_ontology_assistant")


def _forwarded_custom_inputs(app_mode: bool) -> dict:
    domain = MagicMock()
    domain.info = {"llm_endpoint": "ep"}
    domain.ontology = {}
    domain.get_classes.return_value = []
    domain.get_properties.return_value = []
    agent_cls = MagicMock()
    agent_cls.return_value.predict.return_value = MagicMock(
        custom_outputs={}, model_dump=lambda: {"ok": True}
    )
    with patch("back.core.databricks.is_databricks_app", return_value=app_mode), patch.object(
        PermissionMiddleware, "_resolve_roles", return_value=("admin", "admin")
    ), patch.object(_route_module, "get_domain", return_value=domain), patch.object(
        _route_module,
        "require_domain_llm",
        return_value=("https://server.example", "server-token", "ep", "chat"),
    ), patch.object(
        _agent_module, "OntologyAssistantResponsesAgent", agent_cls
    ), patch(
        "mlflow.types.responses.ResponsesAgentRequest"
    ) as request_cls:
        resp = TestClient(app).post(
            "/ontology/assistant/invoke",
            json={
                "input": [{"role": "user", "content": "hi"}],
                "custom_inputs": {"host": "https://caller.example", "token": "dapi-caller"},
            },
        )
    assert resp.status_code == 200, resp.text
    return request_cls.call_args.kwargs["custom_inputs"]


def test_caller_token_is_replaced_by_server_token_in_app_mode():
    assert _forwarded_custom_inputs(app_mode=True)["token"] == "server-token"


def test_caller_host_is_replaced_by_server_host_in_app_mode():
    assert _forwarded_custom_inputs(app_mode=True)["host"] == "https://server.example"


def test_caller_token_is_replaced_in_local_mode_too():
    assert _forwarded_custom_inputs(app_mode=False)["token"] == "server-token"
