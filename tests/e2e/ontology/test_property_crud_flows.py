"""
E2E — Ontology › Property (Relationship) CRUD.

Scenario: a user adds a property, reads it back, updates its description,
then deletes it and confirms it is gone.

Design note: like ``add_class``, ``add_property`` generates a URI from
the session's ``base_uri``.  In the test environment no ``base_uri`` is
pre-configured, so the generated URI is always the empty string "".
Multi-step tests that need a real URI therefore seed the session via a
single ``import-owl`` call which provides explicit property URIs.

Covered endpoints:
  POST /ontology/property/add
  POST /ontology/property/update
  POST /ontology/property/delete
  POST /ontology/import-owl        (setup for update/delete tests)
  GET  /ontology/load              (read-back)

DOM coverage:
  #relationships-section  visibility
  #ontology-relationships-container
  #propertiesList
"""

from __future__ import annotations

import json
import uuid

# Turtle with two data properties with explicit URIs.
_PROP_OWL = """
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@base <http://e2eprop.test/> .

<http://e2eprop.test/>       a owl:Ontology .
<http://e2eprop.test/Item>   a owl:Class ; rdfs:label "Item" .
<http://e2eprop.test/price>  a owl:DatatypeProperty ;
                               rdfs:domain <http://e2eprop.test/Item> ;
                               rdfs:label "price" .
<http://e2eprop.test/weight> a owl:DatatypeProperty ;
                               rdfs:domain <http://e2eprop.test/Item> ;
                               rdfs:label "weight" .
<http://e2eprop.test/sku>    a owl:DatatypeProperty ;
                               rdfs:domain <http://e2eprop.test/Item> ;
                               rdfs:label "sku" .
"""

_PRICE_URI  = "http://e2eprop.test/price"
_WEIGHT_URI = "http://e2eprop.test/weight"
_SKU_URI    = "http://e2eprop.test/sku"


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    token = cookies.get("csrf_token")
    if token:
        headers["X-CSRF-Token"] = token
    return headers


def _prime(page, live_server: str) -> dict:
    page.goto(live_server)
    page.wait_for_load_state("domcontentloaded")
    return _csrf_headers(page.context)


def _add_property(page, live_server: str, headers: dict, name: str) -> tuple[dict, int]:
    resp = page.context.request.post(
        f"{live_server}/ontology/property/add",
        headers=headers,
        data=json.dumps({"name": name, "description": f"E2E property {name}"}),
    )
    return json.loads(resp.body()), resp.status


def _delete_property(page, live_server: str, headers: dict, uri: str) -> tuple[dict, int]:
    resp = page.context.request.post(
        f"{live_server}/ontology/property/delete",
        headers=headers,
        data=json.dumps({"uri": uri}),
    )
    return json.loads(resp.body()), resp.status


def _load_properties(page, live_server: str) -> list:
    resp = page.request.get(f"{live_server}/ontology/load")
    return json.loads(resp.body())["config"]["properties"]


# ── DOM ───────────────────────────────────────────────────────────────────────

class TestOntologyRelationshipsSection:
    def test_section_visible_after_switch(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("relationships")')
        page.wait_for_timeout(400)
        assert page.locator("#relationships-section").is_visible()

    def test_relationships_container_present(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("relationships")')
        page.wait_for_timeout(400)
        assert page.locator("#ontology-relationships-container").count() == 1

    def test_properties_list_present(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("relationships")')
        page.wait_for_timeout(400)
        assert page.locator("#propertiesList").count() == 1


# ── API: add ──────────────────────────────────────────────────────────────────

class TestOntologyPropertyAdd:
    def test_add_property_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        _, status = _add_property(page, live_server, headers, "e2eAddProp")
        assert status == 200

    def test_add_property_returns_success_true(self, page, live_server):
        headers = _prime(page, live_server)
        payload, _ = _add_property(page, live_server, headers, "e2eSuccessProp")
        assert payload.get("success") is True

    def test_add_property_response_has_property_object(self, page, live_server):
        headers = _prime(page, live_server)
        payload, _ = _add_property(page, live_server, headers, "e2eHasProp")
        assert "property" in payload or "config" in payload, (
            f"Expected 'property' or 'config' in response, got: {list(payload.keys())}"
        )

    def test_add_property_appears_in_load(self, page, live_server):
        headers = _prime(page, live_server)
        unique = f"e2eProp_{uuid.uuid4().hex[:8]}"
        _add_property(page, live_server, headers, unique)
        names = [p.get("name", "") for p in _load_properties(page, live_server)]
        assert unique in names, f"Property '{unique}' not found in loaded properties: {names}"

    def test_multiple_properties_appear_after_import(self, page, live_server):
        """Import OWL with three properties at once; all must appear in the load."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _PROP_OWL}),
        )
        loaded_names = [p.get("name", "") for p in _load_properties(page, live_server)]
        for label in ("price", "weight", "sku"):
            assert label in loaded_names, (
                f"Property '{label}' not found after import. Got: {loaded_names}"
            )


# ── API: update ───────────────────────────────────────────────────────────────

class TestOntologyPropertyUpdate:
    def test_update_property_description(self, page, live_server):
        """Import OWL with a known property URI, then update its description."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _PROP_OWL}),
        )

        resp = page.context.request.post(
            f"{live_server}/ontology/property/update",
            headers=headers,
            data=json.dumps({"uri": _PRICE_URI, "description": "Updated by E2E"}),
        )
        assert resp.status == 200, resp.text()
        assert json.loads(resp.body()).get("success") is True

    def test_update_nonexistent_property_does_not_500(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/property/update",
            headers=headers,
            data=json.dumps({"uri": "http://ghost.test/noProp", "description": "x"}),
        )
        assert resp.status < 500


# ── API: delete ───────────────────────────────────────────────────────────────

class TestOntologyPropertyDelete:
    def test_delete_property_returns_success(self, page, live_server):
        """Import OWL with known property URIs, then delete one."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _PROP_OWL}),
        )

        payload, status = _delete_property(page, live_server, headers, _WEIGHT_URI)
        assert status == 200, payload
        assert payload.get("success") is True

    def test_deleted_property_absent_from_load(self, page, live_server):
        """After deleting 'sku' the load must no longer list it."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _PROP_OWL}),
        )

        _delete_property(page, live_server, headers, _SKU_URI)

        names = [p.get("name", "") for p in _load_properties(page, live_server)]
        assert "sku" not in names, f"Property 'sku' still present after delete. Got: {names}"

    def test_delete_nonexistent_property_does_not_500(self, page, live_server):
        headers = _prime(page, live_server)
        _, status = _delete_property(page, live_server, headers, "http://ghost.test/noProp")
        assert status < 500
