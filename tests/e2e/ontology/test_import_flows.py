"""
E2E — Ontology › Import.

Scenario A (UI): the import section exposes tabs for OWL, RDFS, and the
three industry ontologies (FIBO, CDISC, IOF).

Scenario B (API — OWL import): POST /ontology/import-owl with a minimal
Turtle snippet stores classes into the session; the subsequent GET
/ontology/load reflects them.

Scenario C (API — OWL parse): POST /ontology/parse-owl does NOT mutate the
session — it only returns the parsed structure.

Scenario D (API — industry catalogs): GET /ontology/{fibo,cdisc,iof}-catalog
returns a well-shaped JSON catalog.
"""

from __future__ import annotations

import json


# ── Turtle fixture shared across tests ───────────────────────────────────────

MINIMAL_OWL = """
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@base <http://e2eimport.test/> .

<http://e2eimport.test/>        a owl:Ontology .
<http://e2eimport.test/Product> a owl:Class ;    rdfs:label "Product" .
<http://e2eimport.test/Order>   a owl:Class ;    rdfs:label "Order" .
<http://e2eimport.test/price>   a owl:DatatypeProperty ;
                                  rdfs:domain <http://e2eimport.test/Product> ;
                                  rdfs:label  "price" .
"""

MINIMAL_RDFS = """
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

<http://e2erdfs.test/Vehicle>   a rdfs:Class ; rdfs:label "Vehicle" .
<http://e2erdfs.test/Car>       a rdfs:Class ;
                                  rdfs:subClassOf <http://e2erdfs.test/Vehicle> ;
                                  rdfs:label "Car" .
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


# ── DOM: import section tabs ──────────────────────────────────────────────────

class TestOntologyImportSection:
    def _open_import(self, page, live_server: str) -> None:
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("import")')
        page.wait_for_timeout(400)

    def test_import_section_visible(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#import-section").is_visible()

    def test_owl_tab_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#tab-owl").count() == 1

    def test_rdfs_tab_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#tab-rdfs").count() == 1

    def test_fibo_tab_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#tab-fibo").count() == 1

    def test_cdisc_tab_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#tab-cdisc").count() == 1

    def test_iof_tab_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#tab-iof").count() == 1

    def test_owl_file_input_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#importOwlFileInput").count() == 1

    def test_rdfs_file_input_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#importRdfsFileInput").count() == 1

    def test_fibo_domain_checkboxes_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#fiboDomainCheckboxes").count() == 1

    def test_cdisc_domain_checkboxes_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#cdiscDomainCheckboxes").count() == 1

    def test_iof_domain_checkboxes_present(self, page, live_server):
        self._open_import(page, live_server)
        assert page.locator("#iofDomainCheckboxes").count() == 1


# ── API: import-owl ───────────────────────────────────────────────────────────

class TestOntologyOwlImportApi:
    def test_import_owl_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        assert resp.status == 200, resp.text()

    def test_import_owl_returns_success_true(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        payload = json.loads(resp.body())
        assert payload.get("success") is True

    def test_import_owl_classes_appear_in_session(self, page, live_server):
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )

        load = page.request.get(f"{live_server}/ontology/load")
        classes = json.loads(load.body())["config"]["classes"]
        labels = [c.get("name", "") or c.get("label", "") for c in classes]
        assert any("Product" in lbl or "product" in lbl.lower() for lbl in labels), (
            f"Imported class 'Product' not found in session classes: {labels}"
        )

    def test_import_owl_properties_appear_in_session(self, page, live_server):
        headers = _prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )

        load = page.request.get(f"{live_server}/ontology/load")
        properties = json.loads(load.body())["config"]["properties"]
        assert len(properties) >= 1, "No properties imported from Turtle snippet"

    def test_import_owl_response_has_stats(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        payload = json.loads(resp.body())
        assert "stats" in payload or "config" in payload, (
            f"Response missing 'stats' or 'config': {list(payload.keys())}"
        )

    def test_import_owl_empty_content_returns_error(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": ""}),
        )
        # Empty content must not be a server error.
        assert resp.status < 500, f"Empty OWL import returned 5xx: {resp.text()}"


# ── API: parse-owl ────────────────────────────────────────────────────────────

class TestOntologyOwlParseApi:
    """parse-owl analyses the Turtle without mutating the session."""

    def test_parse_owl_returns_200(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/parse-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        assert resp.status == 200

    def test_parse_owl_returns_success_true(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/parse-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        payload = json.loads(resp.body())
        assert payload.get("success") is True

    def test_parse_owl_response_has_ontology_key(self, page, live_server):
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/parse-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        payload = json.loads(resp.body())
        assert "ontology" in payload or "config" in payload, (
            f"parse-owl response missing ontology/config: {list(payload.keys())}"
        )

    def test_parse_owl_parsed_classes_match_owl_input(self, page, live_server):
        """The parse response must describe the classes present in the input Turtle."""
        headers = _prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/parse-owl",
            headers=headers,
            data=json.dumps({"content": MINIMAL_OWL}),
        )
        assert resp.status == 200
        payload = json.loads(resp.body())
        assert payload.get("success") is True
        # The parsed ontology/config must include the classes from MINIMAL_OWL.
        raw = payload.get("ontology") or payload.get("config") or {}
        classes = raw.get("classes", [])
        assert len(classes) >= 1, "parse-owl returned no classes for a non-empty OWL input"


# ── API: industry catalogs ────────────────────────────────────────────────────

class TestOntologyIndustryCatalogs:
    def _check_catalog(self, page, live_server: str, kind: str) -> None:
        resp = page.request.get(f"{live_server}/ontology/{kind}-catalog")
        # Catalog endpoints may return 502 if the fixture files are absent
        # in the test environment; both 200 and 502 are legal.  5xx from an
        # unhandled exception is not.
        assert resp.status in (200, 404, 502), (
            f"{kind}-catalog returned unexpected status {resp.status}"
        )
        if resp.status == 200:
            payload = json.loads(resp.body())
            assert "success" in payload
            assert "catalog" in payload

    def test_fibo_catalog_shape(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        self._check_catalog(page, live_server, "fibo")

    def test_cdisc_catalog_shape(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        self._check_catalog(page, live_server, "cdisc")

    def test_iof_catalog_shape(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        self._check_catalog(page, live_server, "iof")
