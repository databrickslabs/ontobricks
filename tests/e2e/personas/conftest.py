"""Fixtures for persona-based UAT flows.

Reuses the root ``tests/e2e/conftest.py`` session fixtures (``browser_instance``,
``live_server``, ``_live_bearer``) and adds:

* ``persona_context(persona)`` — a Playwright context whose ``extra_http_headers``
  carry the persona's seam headers, so every request (page nav, fetch, and
  ``context.request.*``) is authorized as that persona.
* ``persona_page(persona)`` — a page from that context, base-url pinned.

The persona seam is *offline only* (the deployed app does not enable
``ONTOBRICKS_TEST_AUTH``), so these fixtures skip in live mode. A session-scoped
probe (``_seam_active``) fails fast with a clear message if the local subprocess
was somehow started without the seam.
"""

from __future__ import annotations

import json
import os

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import csrf_headers

# Re-export the in-process MCP client fixtures so test_mcp_personas.py can use
# them (the MCP server is separate from the web app's permission middleware).
from tests.fixtures.mcp_client import mcp_app, mcp_client  # noqa: F401


def _is_live() -> bool:
    return bool(os.environ.get("ONTOBRICKS_LIVE_BASE"))


@pytest.fixture(scope="session")
def _seam_active(browser_instance, live_server, _live_bearer) -> bool:
    """Probe whether the test-auth seam is enforcing on the target server.

    Returns ``False`` in live mode (seam unavailable; persona tests skip).
    In local mode, drives a viewer write and confirms it is rejected (403);
    a non-403 means the subprocess lacks ``ONTOBRICKS_TEST_AUTH=1``.
    """
    if _is_live() or _live_bearer is not None:
        return False

    ctx = browser_instance.new_context(extra_http_headers=P.CONSUMER.headers)
    try:
        pg = ctx.new_page()
        pg.goto(live_server)
        pg.wait_for_load_state("domcontentloaded")
        resp = pg.context.request.post(
            f"{live_server}/ontology/class/add",
            headers=csrf_headers(pg.context),
            data=json.dumps({"name": "_seam_probe"}),
        )
        return resp.status == 403
    finally:
        ctx.close()


@pytest.fixture
def persona_context(browser_instance, live_server, _live_bearer, _seam_active):
    """Factory: ``persona_context(persona)`` → a seam-authorized context."""
    if _is_live() or _live_bearer is not None:
        pytest.skip(
            "persona seam is offline-only; not available against a live "
            "deployment (use the live_smoke suite for live acceptance)."
        )
    if not _seam_active:
        pytest.skip(
            "ONTOBRICKS_TEST_AUTH seam is not active on the test server — "
            "the e2e subprocess must be started with ONTOBRICKS_TEST_AUTH=1 "
            "(tests/e2e/conftest.py sets this automatically in local mode)."
        )

    created = []

    def _make(persona: P.Persona):
        ctx = browser_instance.new_context(extra_http_headers=persona.headers)
        created.append(ctx)
        return ctx

    yield _make

    for ctx in created:
        ctx.close()


@pytest.fixture
def persona_page(persona_context, live_server):
    """Factory: ``persona_page(persona)`` → a page in that persona's context."""

    def _make(persona: P.Persona):
        ctx = persona_context(persona)
        pg = ctx.new_page()
        pg.base_url = live_server
        return pg

    return _make
