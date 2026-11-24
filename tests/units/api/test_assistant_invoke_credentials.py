"""POST /ontology/assistant/invoke must not forward caller credentials in Apps mode.

Marketplace SOP (Phase 1): the app MUST NOT ask for or store user PATs. The 0.10.0
route drops caller ``custom_inputs.host/token`` and hands the server-resolved
credentials (``require_domain_llm``) to the agent constructor, in every mode;
these tests guard that behaviour.
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import PermissionMiddleware, app

pytestmark = pytest.mark.unit

_route_module = importlib.import_module("api.routers.internal.ontology")
_agent_module = importlib.import_module("agents.agent_ontology_assistant")


def _invoke(app_mode: bool):
    """Return (custom_inputs sent to the agent request, agent constructor kwargs)."""
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
    return request_cls.call_args.kwargs["custom_inputs"], agent_cls.call_args.kwargs


@pytest.mark.parametrize("app_mode", [True, False])
def test_caller_credentials_are_not_forwarded(app_mode):
    custom_inputs, _ = _invoke(app_mode)
    assert "token" not in custom_inputs and "host" not in custom_inputs


@pytest.mark.parametrize("app_mode", [True, False])
def test_agent_runs_with_server_credentials(app_mode):
    _, agent_kwargs = _invoke(app_mode)
    assert agent_kwargs == {"host": "https://server.example", "token": "server-token"}
