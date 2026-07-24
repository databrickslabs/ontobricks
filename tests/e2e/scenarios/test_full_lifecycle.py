"""
E2E — Full domain lifecycle journey.

Unlike the per-endpoint contract suites, this walks the *entire* authoring
flow as one continuous browser session (one cookie jar = one
:class:`DomainSession`), proving that state accumulates from step to step
exactly as it does for a real user:

    create domain → import an ontology (real file-input UI) → verify the
    entities render → create mappings → generate R2RML (real button) →
    export OWL → snapshot the whole domain.

Mutations alternate between genuine DOM interactions (file upload, button
clicks — proving the JS wiring) and ``page.context.request`` calls (the
same endpoints the page's JS hits — the house style for the contract
suites). Every step asserts the accumulated session state.

All steps are session-only and touch neither a SQL warehouse, a UC Volume,
nor an LLM endpoint, so the journey is deterministic under both the
real-int-creds and ``ONTOBRICKS_E2E_FAKE_CREDS=1`` modes. The
Databricks-backed tail of the lifecycle (UC save/load, KG build,
auto-assign, the LLM wizard) is intentionally out of scope here — those
belong in the live-gated suites.

Run:
    uv run pytest tests/e2e/scenarios/ -v
"""

from __future__ import annotations

import json


_DOMAIN_NAME = "SalesDomain"

# URIs come from the ``sample_owl_content`` fixture (base http://test.org/ontology#)
# and line up with ``sample_mapping_config`` so the mapping step references
# classes/properties that the import step actually created.
_CUSTOMER_URI = "http://test.org/ontology#Customer"
_ORDER_URI = "http://test.org/ontology#Order"


def _csrf_headers(context) -> dict:
    """JSON headers carrying the double-submit CSRF token from the cookie."""
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    if token := cookies.get("csrf_token"):
        headers["X-CSRF-Token"] = token
    return headers


def _json(resp) -> dict:
    return json.loads(resp.body())


class TestFullDomainLifecycle:
    """One session, the whole authoring journey, asserted at every hop."""

    def test_create_domain_to_r2rml_journey(
        self,
        page,
        live_server,
        tmp_path,
        sample_owl_content,
        sample_mapping_config,
    ):
        # ── 1. Prime the session (GET sets the csrf cookie) ──────────────────
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)

        # ── 2. Create a domain from scratch ──────────────────────────────────
        page.goto(f"{live_server}/domain")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(400)

        # Real DOM: the name field renders and is editable.
        name_field = page.locator("#domainName")
        assert name_field.is_visible()
        name_field.fill(_DOMAIN_NAME)

        # Persist via the same endpoint the page uses.
        resp = page.context.request.post(
            f"{live_server}/domain/info",
            headers=headers,
            data=json.dumps(
                {
                    "name": _DOMAIN_NAME,
                    "description": "E2E full-lifecycle journey",
                    "base_uri": "http://test.org/ontology#",
                }
            ),
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True

        info = _json(page.request.get(f"{live_server}/domain/info"))
        assert info.get("info", {}).get("name") == _DOMAIN_NAME

        # ── 3. Import an ontology through the real file-input UI ─────────────
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("import")')
        page.wait_for_timeout(300)

        ttl_file = tmp_path / "sales_ontology.ttl"
        ttl_file.write_text(sample_owl_content, encoding="utf-8")

        # Drives the #importOwlFileInput change handler → /ontology/parse-owl.
        page.set_input_files("#importOwlFileInput", str(ttl_file))

        # The JS surfaces a success banner in the persistent status panel.
        page.wait_for_selector("#importFinalResult .alert-success", timeout=15000)

        # API truth: the imported classes are now in the session ontology.
        onto = _json(page.request.get(f"{live_server}/ontology/load"))["config"]
        class_names = {c.get("name", "") for c in onto["classes"]}
        assert {"Customer", "Order", "Product"} <= class_names, class_names
        assert len(onto["properties"]) >= 1

        # ── 4. The imported classes render in the entities tree (JS wiring) ──
        # The import handler's session reload + tree render is fire-and-forget,
        # so let it settle before reading the rendered DOM.
        page.evaluate('SidebarNav.switchTo("entities")')
        page.wait_for_timeout(2000)
        tree_text = page.locator("#classHierarchyTree").text_content() or ""
        assert "Customer" in tree_text and "Order" in tree_text, (
            f"Imported classes not rendered in the hierarchy tree: {tree_text[:200]!r}"
        )

        # ── 5. Create entity + relationship mappings ─────────────────────────
        page.goto(f"{live_server}/mapping")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(400)

        resp = page.context.request.post(
            f"{live_server}/mapping/save",
            headers=headers,
            data=json.dumps({"config": sample_mapping_config}),
        )
        assert resp.status == 200, resp.text()
        assert _json(resp).get("success") is True

        mapping = _json(page.request.get(f"{live_server}/mapping/load"))["config"]
        mapped_classes = {m.get("ontology_class") for m in mapping["entities"]}
        assert {_CUSTOMER_URI, _ORDER_URI} <= mapped_classes, mapped_classes
        assert len(mapping["relationships"]) >= 1

        # ── 6. Generate R2RML through the real "Regenerate" button ───────────
        # Reload so mapping-init.js hydrates MappingState from /mapping/load.
        page.goto(f"{live_server}/mapping")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_function(
            "() => typeof MappingState !== 'undefined' && MappingState.config"
            " && (MappingState.config.entities || []).length > 0",
            timeout=10000,
        )
        page.evaluate('SidebarNav.switchTo("r2rml")')
        page.wait_for_timeout(300)
        page.click("#regenerateR2RMLBtn")

        page.wait_for_function(
            "() => {"
            "  const el = document.getElementById('r2rmlPreview');"
            "  return el && el.value && el.value.includes('rr:');"
            "}",
            timeout=10000,
        )
        r2rml_text = page.locator("#r2rmlPreview").input_value()
        assert "rr:TriplesMap" in r2rml_text or "rr:logicalTable" in r2rml_text

        # ── 7. Export the ontology back to OWL (round-trip) ──────────────────
        export = _json(page.request.get(f"{live_server}/ontology/export-owl"))
        assert export.get("success") is True
        assert "Customer" in export.get("owl_content", "")

        # ── 8. Snapshot the whole domain — everything survived the journey ───
        snapshot = _json(page.request.get(f"{live_server}/domain/export"))
        assert snapshot.get("success") is True
        blob = json.dumps(snapshot["domain"])
        assert "Customer" in blob and "Order" in blob
        # Mapping entities are part of the persisted domain snapshot too.
        assert _CUSTOMER_URI in blob
