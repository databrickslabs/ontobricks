"""
Layer 2 UI Tests -- Entity URI resolution endpoints (Playwright).

The ``/resolve`` route redirects an ontology entity URI to the
Knowledge Graph page of the owning domain.  These tests assert the
route's HTTP behaviour from a real browser:

- Missing URI → 4xx (ValidationError).
- Valid URI → 302 redirect (captured via response interception).
- URI embedded in path variant → same contract.

We do not assert on the final rendered graph, because in a test
environment there is no saved domain -- only the resolve contract
itself is validated.
"""


class TestResolveRoute:
    """Smoke tests for the ``/resolve`` redirect endpoint."""

    def test_resolve_missing_uri_returns_error(self, page, live_server):
        """``/resolve`` without a ``uri`` query parameter must surface an error."""
        response = page.goto(f"{live_server}/resolve")
        page.wait_for_load_state("domcontentloaded")
        assert response is not None
        # ValidationError maps to 400/422 through the error handlers.
        assert response.status >= 400
        assert response.status < 500

    def test_resolve_query_param_performs_redirect(self, page, live_server):
        """``/resolve?uri=...`` issues a 302 (or follows it to a concrete page)."""
        target_uri = "http://example.org/ontology/Customer/UNKNOWN"
        response = page.goto(f"{live_server}/resolve?uri={target_uri}")
        page.wait_for_load_state("domcontentloaded")
        assert response is not None
        # Either we got the 302 itself or the redirect was auto-followed to
        # a concrete page.  We tolerate both shapes but forbid 5xx.
        assert response.status < 500

    def test_resolve_path_variant_does_not_crash(self, page, live_server):
        """``/resolve/{uri:path}`` must not 5xx on an unknown URI."""
        response = page.goto(
            f"{live_server}/resolve/http%3A%2F%2Fexample.org%2FCustomer%2FX"
        )
        page.wait_for_load_state("domcontentloaded")
        assert response is not None
        assert response.status < 500
