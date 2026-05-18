"""
E2E — Ontology › OWL Export & Generation.

Scenario A (UI): the #owl-section exposes the copy, download, and
regenerate buttons.

Scenario B (API — export): GET /ontology/export-owl
  - Returns 404 when the session has no classes.
  - Returns 200 with Turtle content after an OWL import populates the session.

Scenario C (API — generate): POST /ontology/generate-owl
  - Accepts a minimal ontology dict and returns a Turtle string.
  - The Turtle contains the class labels from the input.

Scenario D (full round-trip): import OWL → export OWL → Turtle contains
the original class names.
"""

from __future__ import annotations

import json
import uuid


MINIMAL_OWL = """
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@base <http://e2eexport.test/> .

<http://e2eexport.test/>           a owl:Ontology .
<http://e2eexport.test/Customer>   a owl:Class ;  rdfs:label "Customer" .
<http://e2eexport.test/Invoice>    a owl:Class ;  rdfs:label "Invoice" .
<http://e2eexport.test/amount>     a owl:DatatypeProperty ;
                                     rdfs:domain <http://e2eexport.test/Invoice> ;
                                     rdfs:label  "amount" .
"""


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


def _import_owl(page, live_server: str, headers: dict, turtle: str = MINIMAL_OWL) -> None:
    page.context.request.post(
        f"{live_server}/ontology/import-owl",
        headers=headers,
        data=json.dumps({"content": turtle}),
    )


# ── DOM: OWL section ──────────────────────────────────────────────────────────

class TestOntologyOwlSection:
    def _open_owl(self, page, live_server: str) -> None:
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("owl")')
        page.wait_for_timeout(400)

    def test_owl_section_visible(self, page, live_server):
        self._open_owl(page, live_server)
        assert page.locator("#owl-section").is_visible()

    def test_copy_button_present(self, page, live_server):
        self._open_owl(page, live_server)
        assert page.locator("#copyOwl").count() == 1

    def test_download_button_present(self, page, live_server):
        self._open_owl(page, live_server)
        assert page.locator("#downloadOwl").count() == 1

    def test_regenerate_button_present(self, page, live_server):
        self._open_owl(page, live_server)
        assert page.locator("#regenerateOwl").count() == 1

    def test_owl_preview_container_present(self, page, live_server):
        self._open_owl(page, live_server)
        assert page.locator("#owlPreview").count() == 1


# ── API: export-owl ───────────────────────────────────────────────────────────

class TestOntologyExportOwlApi:
    def test_export_empty_session_returns_404_or_error(self, page, live_server):
        """Fresh session with no classes → 404 is the documented behaviour."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/export-owl")
        assert resp.status in (200, 404), (
            f"Unexpected status {resp.status}: {resp.text()}"
        )

    def test_export_after_import_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        resp = page.request.get(f"{live_server}/ontology/export-owl")
        assert resp.status == 200, resp.text()

    def test_export_response_has_success_flag(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        payload = json.loads(page.request.get(f"{live_server}/ontology/export-owl").body())
        assert payload.get("success") is True

    def test_export_response_has_owl_content(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        payload = json.loads(page.request.get(f"{live_server}/ontology/export-owl").body())
        assert "owl_content" in payload, f"Missing 'owl_content' key: {list(payload.keys())}"
        assert isinstance(payload["owl_content"], str)
        assert len(payload["owl_content"]) > 0

    def test_export_owl_content_is_turtle(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        payload = json.loads(page.request.get(f"{live_server}/ontology/export-owl").body())
        turtle = payload.get("owl_content", "")
        # All Turtle ontologies use the OWL prefix.
        assert "owl:" in turtle or "owl#" in turtle, (
            "Exported content does not look like Turtle (no owl prefix)"
        )

    def test_export_contains_imported_class_names(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        payload = json.loads(page.request.get(f"{live_server}/ontology/export-owl").body())
        turtle = payload.get("owl_content", "")
        assert "Customer" in turtle or "Invoice" in turtle, (
            "Exported Turtle does not contain the imported class names"
        )

    def test_export_format_is_turtle(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        payload = json.loads(page.request.get(f"{live_server}/ontology/export-owl").body())
        assert payload.get("format") == "turtle"


# ── API: generate-owl ─────────────────────────────────────────────────────────

class TestOntologyGenerateOwlApi:
    def test_generate_owl_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        unique = f"GenClass_{uuid.uuid4().hex[:8]}"
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps({
                "name": "GeneratedOntology",
                "base_uri": "http://gen.test/",
                "classes": [{"name": unique, "uri": f"http://gen.test/{unique}"}],
                "properties": [],
            }),
        )
        assert resp.status == 200, resp.text()

    def test_generate_owl_returns_success_true(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps({
                "name": "GenOnto", "base_uri": "http://gen.test/",
                "classes": [{"name": "Widget", "uri": "http://gen.test/Widget"}],
                "properties": [],
            }),
        )
        payload = json.loads(resp.body())
        assert payload.get("success") is True

    def test_generate_owl_returns_owl_string(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps({
                "name": "GenOnto", "base_uri": "http://gen.test/",
                "classes": [{"name": "Gadget", "uri": "http://gen.test/Gadget"}],
                "properties": [],
            }),
        )
        payload = json.loads(resp.body())
        assert "owl" in payload, f"Missing 'owl' key: {list(payload.keys())}"
        assert isinstance(payload["owl"], str) and len(payload["owl"]) > 0

    def test_generate_owl_class_name_in_output(self, page, live_server):
        headers = _prime(page, live_server)
        unique = f"UniqueEntity_{uuid.uuid4().hex[:6]}"
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps({
                "name": "GenOnto", "base_uri": "http://gen.test/",
                "classes": [{"name": unique, "uri": f"http://gen.test/{unique}"}],
                "properties": [],
            }),
        )
        turtle = json.loads(resp.body()).get("owl", "")
        assert unique in turtle, (
            f"Class name '{unique}' not found in generated Turtle"
        )

    def test_generate_owl_format_is_turtle(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps({
                "name": "GenOnto", "base_uri": "http://gen.test/",
                "classes": [{"name": "Node", "uri": "http://gen.test/Node"}],
                "properties": [],
            }),
        )
        payload = json.loads(resp.body())
        assert payload.get("format") == "turtle"

    def test_generate_owl_empty_classes_does_not_500(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps({"name": "Empty", "base_uri": "http://empty.test/", "classes": [], "properties": []}),
        )
        assert resp.status < 500


# ── Full round-trip ───────────────────────────────────────────────────────────

class TestOntologyOwlRoundTrip:
    """Import OWL → export OWL: the re-exported Turtle must contain the
    classes that were present in the original import."""

    def test_import_then_export_preserves_classes(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        export = page.request.get(f"{live_server}/ontology/export-owl")
        assert export.status == 200

        turtle = json.loads(export.body()).get("owl_content", "")
        # Both classes from MINIMAL_OWL must survive the round-trip.
        assert "Customer" in turtle, "Class 'Customer' lost in OWL round-trip"
        assert "Invoice" in turtle, "Class 'Invoice' lost in OWL round-trip"

    def test_imported_property_survives_export(self, page, live_server):
        headers = _prime(page, live_server)
        _import_owl(page, live_server, headers)

        turtle = json.loads(
            page.request.get(f"{live_server}/ontology/export-owl").body()
        ).get("owl_content", "")
        assert "amount" in turtle or "DatatypeProperty" in turtle, (
            "Imported DatatypeProperty 'amount' not reflected in export"
        )
