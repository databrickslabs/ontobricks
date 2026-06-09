"""UAT — access-denied paths for the edge personas.

* Nina (app user, no team entry on the loaded domain) hitting a domain-scoped
  route is sent to ``/access-denied?reason=domain``.
* Nora (authenticated but absent from the app ACL) hitting any gated route is
  sent to ``/access-denied?reason=app``.

These exercise the ``ROLE_NONE`` and domain-gate branches of the middleware via
the persona seam, complementing the static ``security/test_access_denied_flows``
which only renders the pages directly.
"""

from __future__ import annotations

import pytest

from tests.e2e.personas import personas as P

pytestmark = [pytest.mark.e2e, pytest.mark.uat]


def _denied(resp, page, reason: str) -> bool:
    """True when the navigation ended at access-denied for *reason* or was 403."""
    url = page.url
    if "/access-denied" in url and f"reason={reason}" in url:
        return True
    if "/access-denied" in url:  # redirected, reason param may be normalized
        return True
    return resp is not None and resp.status == 403


class TestNoDomainPersona:
    """Nina: app user without a team entry → reason=domain on domain routes."""

    def test_domain_scoped_route_redirects_to_access_denied(
        self, persona_page, live_server
    ):
        page = persona_page(P.NO_DOMAIN)
        resp = page.goto(f"{live_server}/ontology/load")
        page.wait_for_load_state("domcontentloaded")
        assert _denied(resp, page, "domain"), (
            f"expected access-denied(domain)/403, url={page.url} "
            f"status={resp.status if resp else 'n/a'}"
        )

    def test_home_is_reachable_for_app_user_without_domain(
        self, persona_page, live_server
    ):
        # Home is not domain-scoped; an app user with no team entry can still
        # land on it (and use the registry to load a domain they belong to).
        page = persona_page(P.NO_DOMAIN)
        resp = page.goto(f"{live_server}/")
        page.wait_for_load_state("domcontentloaded")
        assert resp is not None and resp.status == 200
        assert "/access-denied" not in page.url


class TestNoAppPersona:
    """Nora: not in the app ACL (ROLE_NONE) → reason=app everywhere gated."""

    def test_home_redirects_to_access_denied_app(self, persona_page, live_server):
        page = persona_page(P.NO_APP)
        resp = page.goto(f"{live_server}/")
        page.wait_for_load_state("domcontentloaded")
        assert _denied(resp, page, "app"), (
            f"expected access-denied(app)/403, url={page.url} "
            f"status={resp.status if resp else 'n/a'}"
        )

    def test_ontology_page_blocked(self, persona_page, live_server):
        page = persona_page(P.NO_APP)
        resp = page.goto(f"{live_server}/ontology/load")
        page.wait_for_load_state("domcontentloaded")
        assert _denied(resp, page, "app") or (resp and resp.status == 403)
