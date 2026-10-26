"""Route wiring for Studio Import entity (handlers called directly)."""

import asyncio
from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError

pytestmark = pytest.mark.unit

SOURCE = {
    "base_uri": "http://src.org/onto#",
    "classes": [
        {"name": "Customer", "uri": "http://src.org/onto#Customer", "parent": ""},
        {"name": "Order", "uri": "http://src.org/onto#Order", "parent": ""},
    ],
    "properties": [
        {"name": "places", "uri": "http://src.org/onto#places",
         "domain": "Customer", "range": "Order"},
    ],
}


class _Request:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


@pytest.fixture
def ctx(monkeypatch, domain_session):
    from api.routers.internal import ontology as routes

    domain_session.ontology["base_uri"] = "http://tgt.org/onto#"
    monkeypatch.setattr(routes, "get_domain", lambda _mgr: domain_session)
    calls = []

    async def fake_load(_domain, _settings, name):
        calls.append(name)
        return SOURCE, "4"

    monkeypatch.setattr(routes, "_load_source_ontology", fake_load)
    return SimpleNamespace(routes=routes, calls=calls, domain=domain_session)


def test_catalog_returns_entities_and_relationships(ctx):
    body = asyncio.run(ctx.routes.entity_import_catalog("Sales", session_mgr=None, settings=None))
    assert body["success"] is True
    assert body["version"] == "4"
    assert [e["name"] for e in body["entities"]] == ["Customer", "Order"]
    assert body["relationships"][0]["name"] == "places"


def test_import_reloads_source_server_side(ctx):
    req = _Request({"domain": "Sales", "entities": ["Customer", "Order"], "renames": {}})
    body = asyncio.run(ctx.routes.entity_import(req, session_mgr=None, settings=None))
    assert ctx.calls == ["Sales"]
    assert set(body["imported"]) == {"Customer", "Order"}
    assert body["relationships"] == ["places"]
    assert {c["name"] for c in ctx.domain.ontology["classes"]} >= {"Customer", "Order"}


@pytest.mark.parametrize("payload", [
    {"domain": "", "entities": ["Customer"]},
    {"domain": "Sales", "entities": []},
])
def test_import_requires_domain_and_entities(ctx, payload):
    with pytest.raises(ValidationError):
        asyncio.run(ctx.routes.entity_import(_Request(payload), session_mgr=None, settings=None))


def test_load_source_rejects_current_domain():
    from api.routers.internal import ontology as routes

    current = SimpleNamespace(info={"name": "Sales"}, domain_folder="sales")
    with pytest.raises(ValidationError):
        asyncio.run(routes._load_source_ontology(current, None, "SALES"))
