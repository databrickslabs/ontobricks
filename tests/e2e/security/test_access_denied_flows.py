"""
Layer 2 UI Tests -- ``/access-denied`` page (Playwright).

Covers the three wording variants (``reason=app``, ``domain``,
``bootstrap``) plus the default fallback when an unknown reason is
supplied.
"""

import pytest


class TestAccessDeniedPage:
    def test_default_reason_renders(self, page, live_server):
        response = page.goto(f"{live_server}/access-denied")
        page.wait_for_load_state("domcontentloaded")
        assert response is not None and response.status == 200
        body = (page.text_content("body") or "").lower()
        assert "access" in body or "denied" in body or "permission" in body

    @pytest.mark.parametrize("reason", ["app", "domain", "bootstrap"])
    def test_known_reasons_render(self, page, live_server, reason):
        response = page.goto(f"{live_server}/access-denied?reason={reason}")
        page.wait_for_load_state("domcontentloaded")
        assert response is not None and response.status == 200

    def test_unknown_reason_falls_back_gracefully(self, page, live_server):
        """An unrecognised ``reason`` parameter must not 500."""
        response = page.goto(f"{live_server}/access-denied?reason=wizardly")
        page.wait_for_load_state("domcontentloaded")
        assert response is not None and response.status == 200
