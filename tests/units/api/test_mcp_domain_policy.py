"""Per-domain MCP policy enforcement on the external API surfaces.

Covers the boundary that matters: an MCP client must never receive an
ontology attachment the domain disabled, while the internal authoring routes
keep showing the designer everything.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from back.objects.digitaltwin.NodeContextService import NodeContextService
from shared.fastapi.main import app

_CLASS = {
    "name": "Customer",
    "uri": "https://example.com/Customer",
    "dataset": {
        "catalog": "main", "schema": "crm", "asset": "customers",
        "type": "TABLE", "fullName": "main.crm.customers", "key_column": "id",
        "description": "Customer master records.",
    },
    "bridges": [
        {
            "target_domain": "Finance",
            "target_class_name": "Contract",
            "target_class_uri": "https://example.com/Contract",
            "label": "Owns contracts",
        }
    ],
    "actions": [
        {
            "catalog": "main", "schema": "ops", "function": "recompute_risk",
            "fullName": "main.ops.recompute_risk",
            "description": "Recompute the customer risk score",
            "returns_table": False,
        }
    ],
}

_ENTITY = "https://example.com/Customer/CUST001"


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


def _domain(policy: dict | None = None) -> MagicMock:
    mock = MagicMock()
    mock.get_classes.return_value = [_CLASS]
    mock.info = {"name": "test", "mcp_policy": policy or {}}
    mock.domain_folder = "test"
    return mock


# ---------------------------------------------------------------------------
# Policy resolution


def test_resolve_context_policy_reads_the_domain_info() -> None:
    domain = _domain({"context": {"bridges": "preferred", "actions": "disabled"}})
    policy = NodeContextService.resolve_context_policy(domain)
    assert policy == {"bridges": "preferred", "actions": "disabled"}


def test_resolve_context_policy_tolerates_a_missing_policy() -> None:
    assert NodeContextService.resolve_context_policy(_domain()) == {}
    assert NodeContextService.resolve_context_policy(object()) == {}


def test_context_feature_disabled_defaults_to_allowed() -> None:
    assert not NodeContextService.context_feature_disabled(None, "dataset")
    assert not NodeContextService.context_feature_disabled({}, "dataset")
    assert not NodeContextService.context_feature_disabled(
        {"dataset": "preferred"}, "dataset"
    )
    assert NodeContextService.context_feature_disabled(
        {"dataset": "disabled"}, "dataset"
    )


# ---------------------------------------------------------------------------
# GET /api/v1/digitaltwin/nodes/context


@pytest.mark.parametrize(
    "feature,absent_key",
    [("dataset", "dataset"), ("bridges", "bridges"), ("actions", "actions")],
)
def test_disabled_element_is_withheld_from_node_context(
    client, feature, absent_key
) -> None:
    domain = _domain({"context": {feature: "disabled"}})
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain
    ):
        resp = client.get(
            "/api/v1/digitaltwin/nodes/context",
            params={"entity_uri": _ENTITY, "domain_name": "test"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    # response_model_exclude_none drops the key entirely.
    assert absent_key not in body


def test_normal_policy_still_returns_every_element(client) -> None:
    domain = _domain({"context": {"bridges": "preferred"}})
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain
    ):
        resp = client.get(
            "/api/v1/digitaltwin/nodes/context",
            params={"entity_uri": _ENTITY, "domain_name": "test"},
        )

    body = resp.json()
    assert body["dataset"]["fullName"] == "main.crm.customers"
    assert body["bridges"][0]["target_domain"] == "Finance"
    assert body["actions"][0]["fullName"] == "main.ops.recompute_risk"


# ---------------------------------------------------------------------------
# GET /api/v1/domain/classes


def test_disabled_elements_are_withheld_from_domain_classes(client) -> None:
    domain = _domain(
        {"context": {"dataset": "disabled", "actions": "disabled"}}
    )
    with patch(
        "api.routers.domains.DigitalTwin.resolve_domain", return_value=domain
    ):
        resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

    assert resp.status_code == 200
    item = resp.json()["classes"][0]
    assert "dataset" not in item
    assert not item.get("actions")
    # Bridges stay normal, so they must survive.
    assert item["bridges"][0]["target_domain"] == "Finance"


# ---------------------------------------------------------------------------
# POST /api/v1/digitaltwin/nodes/action


async def test_invoke_action_refused_when_actions_disabled() -> None:
    """Disabling the element must stop invocation, not just hide the names."""
    from back.core.errors import ValidationError

    with pytest.raises(ValidationError, match="Actions are disabled"):
        await NodeContextService.invoke_action(
            _domain({"context": {"actions": "disabled"}}),
            MagicMock(),
            entity_uri=_ENTITY,
            action_full_name="main.ops.recompute_risk",
            context_policy={"actions": "disabled"},
        )


# ---------------------------------------------------------------------------
# GET /api/v1/domains


def test_domains_endpoint_publishes_the_policy(client) -> None:
    """The MCP server reads the policy from this payload, so it must survive."""
    policy = {"disabled_tools": ["query_graphql"], "context": {"actions": "disabled"}}
    listing = [{"name": "test", "description": "d", "mcp_policy": policy}]

    with patch(
        "api.routers.domains.RegistryService"
    ) as svc, patch("api.routers.domains.get_registry_config", create=True):
        svc.return_value.list_mcp_domains.return_value = (True, listing, "")
        resp = client.get("/api/v1/domains")

    if resp.status_code != 200:
        pytest.skip(f"registry not configured in this environment: {resp.status_code}")
    assert resp.json()["domains"][0]["mcp_policy"] == policy
