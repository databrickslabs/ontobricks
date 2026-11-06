"""Unit coverage for design-checks Auto-fix propose/apply routes."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

BASE = "http://test.org/ontology#"


class _Request:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


def _domain(*, llm_endpoint: str = ""):
    ontology = {
        "base_uri": BASE,
        "name": "test",
        "classes": [
            {
                "name": "Child",
                "uri": BASE + "Child",
                "parent": "Missing",
                "dataProperties": [],
            },
            {
                "name": "Payment",
                "uri": BASE + "Payment",
                "dataProperties": [],
            },
        ],
        "properties": [
            {
                "name": "paymentmethod",
                "uri": BASE + "paymentmethod",
                "type": "DatatypeProperty",
                "domain": "",
            }
        ],
        "groups": [],
    }
    data = {
        "ontology": ontology,
        "domain": {"info": {"llm_endpoint": llm_endpoint}},
        "assignment": {"entities": [], "relationships": []},
    }

    def get_classes():
        return data["ontology"]["classes"]

    def get_properties():
        return data["ontology"]["properties"]

    def get_entity_mappings():
        return data["assignment"]["entities"]

    def get_relationship_mappings():
        return data["assignment"]["relationships"]

    return SimpleNamespace(
        _data=data,
        ontology=ontology,
        info=data["domain"]["info"],
        groups=ontology["groups"],
        get_classes=get_classes,
        get_properties=get_properties,
        get_entity_mappings=get_entity_mappings,
        get_relationship_mappings=get_relationship_mappings,
        save=MagicMock(),
    )


@pytest.mark.asyncio
async def test_propose_e01(monkeypatch):
    from api.routers.internal import ontology as routes

    domain = _domain()
    monkeypatch.setattr(routes, "get_domain", lambda _manager: domain)

    result = await routes.propose_design_autofix(
        _Request(
            {
                "findings": [
                    {"rule": "DC-E01", "kind": "class", "name": "Child"}
                ]
            }
        ),
        session_mgr=object(),
        settings=SimpleNamespace(),
    )
    body = result.body
    import json

    payload = json.loads(body)
    assert result.status_code == 200
    assert payload["success"]
    assert payload["plan"]["operations"][0]["op"] == "clear_parent"


@pytest.mark.asyncio
async def test_propose_mixed_rules_400(monkeypatch):
    from api.routers.internal import ontology as routes

    domain = _domain()
    monkeypatch.setattr(routes, "get_domain", lambda _manager: domain)

    result = await routes.propose_design_autofix(
        _Request(
            {
                "findings": [
                    {"rule": "DC-E01", "kind": "class", "name": "Child"},
                    {"rule": "DC-W03", "kind": "attribute", "name": "paymentmethod"},
                ]
            }
        ),
        session_mgr=object(),
        settings=SimpleNamespace(),
    )
    import json

    payload = json.loads(result.body)
    assert result.status_code == 400
    assert payload["success"] is False


@pytest.mark.asyncio
async def test_apply_attach_attribute(monkeypatch):
    from api.routers.internal import ontology as routes

    domain = _domain()
    monkeypatch.setattr(routes, "get_domain", lambda _manager: domain)

    result = await routes.apply_design_autofix(
        _Request(
            {
                "operations": [
                    {
                        "op": "attach_attribute",
                        "target": {"kind": "attribute", "name": "paymentmethod"},
                        "params": {"entity": "Payment"},
                        "rationale": "test",
                        "source": "fallback",
                        "finding_id": "DC-W03:attribute:paymentmethod",
                    }
                ]
            }
        ),
        session_mgr=object(),
    )
    import json

    payload = json.loads(result.body)
    assert result.status_code == 200
    assert payload["success"]
    prop = next(p for p in domain.get_properties() if p["name"] == "paymentmethod")
    assert prop["domain"] == "Payment"
