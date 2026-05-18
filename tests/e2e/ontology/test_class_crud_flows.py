"""
E2E — Ontology › Class CRUD.

Scenario: a user adds a class, verifies it appears in the session,
updates it, then deletes it and confirms it is gone.

Design note: ``add_class`` generates a URI from the session's ``base_uri``.
In the test environment no ``base_uri`` is pre-configured, so the generated
URI is the empty string "".  All multi-step tests that need a real URI
therefore use ``import-owl`` as their setup step — a single POST call that
stores classes with fully-qualified URIs.

Covered endpoints:
  POST /ontology/class/add
  POST /ontology/class/update
  POST /ontology/class/delete
  POST /ontology/import-owl      (setup for update/delete tests)
  GET  /ontology/load            (read-back)
  GET  /ontology/get-loaded-ontology

DOM coverage:
  #entities-section  visibility
  #ontology-entities-container  present
  #classHierarchyTree           present
"""

from __future__ import annotations

import json
import uuid

# Turtle snippet with three classes that have explicit URIs.
_SETUP_OWL = """
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@base <http://e2ecrud.test/> .

<http://e2ecrud.test/>           a owl:Ontology .
<http://e2ecrud.test/Alpha>      a owl:Class ; rdfs:label "Alpha" .
<http://e2ecrud.test/Beta>       a owl:Class ; rdfs:label "Beta" .
<http://e2ecrud.test/Gamma>      a owl:Class ; rdfs:label "Gamma" .
"""

_ALPHA_URI = "http://e2ecrud.test/Alpha"
_BETA_URI  = "http://e2ecrud.test/Beta"
_GAMMA_URI = "http://e2ecrud.test/Gamma"


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    token = cookies.get("csrf_token")
    if token:
        headers["X-CSRF-Token"] = token
    return headers


def _prime(page, live_server: str) -> dict:
    """Visit home to get a CSRF cookie; return ready headers."""
    page.goto(live_server)
    page.wait_for_load_state("domcontentloaded")
    return _csrf_headers(page.context)


def _add_class(page, live_server: str, headers: dict, name: str) -> dict:
    resp = page.context.request.post(
        f"{live_server}/ontology/class/add",
        headers=headers,
        data=json.dumps({"name": name, "description": f"E2E class {name}"}),
    )
    return json.loads(resp.body()), resp.status


def _delete_class(page, live_server: str, headers: dict, uri: str) -> dict:
    resp = page.context.request.post(
        f"{live_server}/ontology/class/delete",
        headers=headers,
        data=json.dumps({"uri": uri}),
    )
    return json.loads(resp.body()), resp.status


def _load_classes(page, live_server: str) -> list:
    resp = page.request.get(f"{live_server}/ontology/load")
    return json.loads(resp.body())["config"]["classes"]


# ── DOM ───────────────────────────────────────────────────────────────────────

class TestOntologyEntitiesSection:
    def test_section_visible_after_switch(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("entities")')
        page.wait_for_timeout(400)
        assert page.locator("#entities-section").is_visible()

    def test_entities_container_present(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("entities")')
        page.wait_for_timeout(400)
        assert page.locator("#ontology-entities-container").count() == 1

    def test_class_hierarchy_tree_present(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("entities")')
        page.wait_for_timeout(400)
        assert page.locator("#classHierarchyTree").count() == 1


# ── API: add ──────────────────────────────────────────────────────────────────

class TestOntologyClassAdd:
    def test_add_class_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        payload, status = _add_class(page, live_server, headers, "E2EAddTest")
        assert status == 200, payload

    def test_add_class_returns_success_true(self, page, live_server):
        headers = _prime(page, live_server)
        payload, _ = _add_class(page, live_server, headers, "E2ESuccessTest")
        assert payload.get("success") is True

    def test_add_class_response_has_class_object(self, page, live_server):
        headers = _prime(page, live_server)
        payload, _ = _add_class(page, live_server, headers, "E2EHasClass")
        assert "class" in payload or "config" in payload, (
            f"Expected 'class' or 'config' in response, got: {list(payload.keys())}"
        )

    def test_add_class_appears_in_load(self, page, live_server):
        headers = _prime(page, live_server)
        unique = f"E2EClass_{uuid.uuid4().hex[:8]}"
        _add_class(page, live_server, headers, unique)
        classes = _load_classes(page, live_server)
        names = [c.get("name", "") for c in classes]
        assert unique in names, f"Class '{unique}' not found in loaded classes: {names}"

    def test_multiple_classes_appear_after_import(self, page, live_server):
        """Import OWL with three classes at once; all must appear in the load."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _SETUP_OWL}),
        )
        loaded_names = [c.get("name", "") for c in _load_classes(page, live_server)]
        for label in ("Alpha", "Beta", "Gamma"):
            assert label in loaded_names, (
                f"Class '{label}' not found after import. Got: {loaded_names}"
            )


# ── API: update ───────────────────────────────────────────────────────────────

class TestOntologyClassUpdate:
    def test_update_class_description(self, page, live_server):
        """Import OWL to seed a class with a known URI, then update its description."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _SETUP_OWL}),
        )

        resp = page.context.request.post(
            f"{live_server}/ontology/class/update",
            headers=headers,
            data=json.dumps({"uri": _ALPHA_URI, "description": "Updated by E2E test"}),
        )
        assert resp.status == 200, resp.text()
        payload = json.loads(resp.body())
        assert payload.get("success") is True

    def test_update_nonexistent_class_returns_error(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/class/update",
            headers=headers,
            data=json.dumps({"uri": "http://does-not-exist.test/NoClass", "description": "x"}),
        )
        # Must not be a 5xx; 200 with success:false or 4xx both acceptable.
        assert resp.status < 500, f"Unexpected 5xx: {resp.text()}"
        payload = json.loads(resp.body())
        assert payload.get("success") is not True or "error" in payload or "message" in payload


# ── API: delete ───────────────────────────────────────────────────────────────

class TestOntologyClassDelete:
    def test_delete_class_returns_success(self, page, live_server):
        """Import OWL to seed a class with a known URI, then delete it."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _SETUP_OWL}),
        )

        payload, status = _delete_class(page, live_server, headers, _BETA_URI)
        assert status == 200, payload
        assert payload.get("success") is True

    def test_deleted_class_absent_from_load(self, page, live_server):
        """After deleting Gamma the load must no longer list it."""
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": _SETUP_OWL}),
        )

        _delete_class(page, live_server, headers, _GAMMA_URI)

        names = [c.get("name", "") for c in _load_classes(page, live_server)]
        assert "Gamma" not in names, f"Class 'Gamma' still present after delete. Got: {names}"

    def test_delete_nonexistent_class_does_not_500(self, page, live_server):
        headers = _prime(page, live_server)
        _, status = _delete_class(
            page, live_server, headers, "http://ghost.test/NoSuchClass"
        )
        assert status < 500, "Delete of non-existent class returned 5xx"


# ── API: get-loaded-ontology ──────────────────────────────────────────────────

class TestGetLoadedOntologyApi:
    def test_returns_200_or_404(self, page, live_server):
        """Fresh session may have no classes → 404 is documented behaviour."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/get-loaded-ontology")
        assert resp.status in (200, 404)

    def test_after_add_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        _add_class(page, live_server, headers, f"E2EGloaded_{uuid.uuid4().hex[:6]}")

        resp = page.request.get(f"{live_server}/ontology/get-loaded-ontology")
        assert resp.status == 200
        payload = json.loads(resp.body())
        assert payload.get("success") is True
        assert "ontology" in payload
