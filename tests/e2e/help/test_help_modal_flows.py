"""
Layer 2 UI Tests -- Help Center modal (Playwright).

Verifies the shared Help Center modal that is injected into every page
via ``partials/layout/help_modal.html``:

- navbar toggle is present on standard pages,
- clicking it opens the modal,
- built-in sidebar sections are reachable,
- the dynamic ``/api/help/docs`` index loads without JS errors.
"""

import json


class TestHelpModalPresence:
    def test_help_toggle_visible_on_home(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#helpCenterToggle").is_visible()

    def test_help_modal_markup_injected(self, page, live_server):
        """``#helpModal`` must exist in the DOM of every main page."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#helpModal").count() == 1

    def test_help_toggle_opens_modal(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        page.click("#helpCenterToggle")
        # Bootstrap toggles .show on the modal element.
        page.wait_for_selector("#helpModal.show", timeout=3000)
        assert page.locator("#helpModal.show").is_visible()

    def test_help_modal_welcome_section_active_by_default(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        page.click("#helpCenterToggle")
        page.wait_for_selector("#helpModal.show", timeout=3000)
        active = page.locator(".help-section.active")
        assert active.count() == 1
        assert active.first.get_attribute("id") == "help-welcome"


class TestHelpDocsApi:
    """Integration smoke test for the in-modal docs index endpoint."""

    def test_help_docs_index_returns_json(self, page, live_server):
        """``GET /api/help/docs`` must return the catalogued categories."""
        response = page.request.get(f"{live_server}/api/help/docs")
        assert response.status == 200
        payload = json.loads(response.body())
        assert isinstance(payload, dict)
        assert "categories" in payload
        assert isinstance(payload["categories"], list)
        # At least one category with at least one doc.
        assert any(cat.get("docs") for cat in payload["categories"])

    def test_help_doc_fetch_round_trip(self, page, live_server):
        """Pick the first catalogued doc and fetch its markdown."""
        index = page.request.get(f"{live_server}/api/help/docs")
        assert index.status == 200
        cats = json.loads(index.body()).get("categories", [])
        slug = None
        for cat in cats:
            if cat.get("docs"):
                slug = cat["docs"][0]["slug"]
                break
        assert slug, "No catalogued docs found"

        doc = page.request.get(f"{live_server}/api/help/docs/{slug}")
        # 200 when the markdown file is present on disk; 404 otherwise.
        # Both are legal -- but 500 never is.
        assert doc.status in (200, 404)
        if doc.status == 200:
            body = json.loads(doc.body())
            assert body["slug"] == slug
            assert "markdown" in body

    def test_help_doc_unknown_slug_returns_404(self, page, live_server):
        response = page.request.get(f"{live_server}/api/help/docs/does-not-exist")
        assert response.status == 404

    def test_help_image_bad_name_is_rejected(self, page, live_server):
        """Path traversal / unsupported extensions must 404 without reading disk."""
        response = page.request.get(
            f"{live_server}/api/help/docs/images/..%2Fetc%2Fpasswd"
        )
        assert response.status == 404
