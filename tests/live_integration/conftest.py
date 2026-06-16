"""Live-instance integration test fixtures.

These tests run against a **deployed** OntoBricks instance — not the in-proc
TestClient. They are gated behind the ``ONTOBRICKS_LIVE_BASE`` env var so they
do not run as part of the default CI matrix.

Usage:

    export ONTOBRICKS_LIVE_BASE=https://ontobricks-030-<workspace-id>.aws.databricksapps.com
    export ONTOBRICKS_LIVE_MCP_BASE=https://mcp-ontobricks-<workspace-id>.aws.databricksapps.com
    export DATABRICKS_CONFIG_PROFILE=fevm-ontobricks-int
    uv run pytest tests/live_integration/ -v -m live_integration --no-cov

The bearer token is minted from the active Databricks CLI profile via
``databricks auth token`` at fixture-setup time and refreshed once per session.
"""

from __future__ import annotations

import os
import time
from typing import Optional

import httpx
import pytest

from tests.fixtures.databricks_auth import DatabricksAuth


# ── Skip-the-whole-module gate ────────────────────────────────────────────────


def _live_base() -> Optional[str]:
    return os.environ.get("ONTOBRICKS_LIVE_BASE") or None


def _mcp_base() -> Optional[str]:
    return os.environ.get("ONTOBRICKS_LIVE_MCP_BASE") or None


def pytest_collection_modifyitems(config, items):
    """Skip the whole suite unless ``ONTOBRICKS_LIVE_BASE`` is set.

    A module-level ``pytestmark`` in a *conftest* is NOT honoured by pytest, so
    the skip must be applied via this collection hook. This is what actually
    keeps the suite out of normal (``pytest tests/``) runs and stops the
    fixture-less probes (e.g. ``test_root_redirects_when_unauthenticated``,
    which reads the env var directly) from erroring when the base is unset.
    """
    if _live_base():
        return
    skip = pytest.mark.skip(
        reason="ONTOBRICKS_LIVE_BASE not set; live integration suite skipped"
    )
    for item in items:
        if "tests/live_integration/" in str(item.path).replace("\\", "/"):
            item.add_marker(skip)


# ── Session-scoped fixtures ──────────────────────────────────────────────────


@pytest.fixture(scope="session")
def live_base() -> str:
    base = _live_base()
    if not base:
        pytest.skip("ONTOBRICKS_LIVE_BASE not set")
    return base.rstrip("/")


@pytest.fixture(scope="session")
def mcp_base() -> Optional[str]:
    return _mcp_base().rstrip("/") if _mcp_base() else None


@pytest.fixture(scope="session")
def bearer_token() -> str:
    """Mint a workspace OAuth token from the active Databricks CLI profile.

    The CLI's ``databricks auth token`` honours ``DATABRICKS_CONFIG_PROFILE``
    so each developer/CI run can target a different workspace by setting
    that one env var.
    """
    token = DatabricksAuth.mint_token(os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    if not token:
        pytest.skip(
            "databricks auth token failed or returned no access_token "
            "(check the CLI is installed and DATABRICKS_CONFIG_PROFILE is logged in)"
        )
    return token


@pytest.fixture(scope="session")
def auth_headers(bearer_token: str) -> dict:
    return {"Authorization": f"Bearer {bearer_token}"}


@pytest.fixture(scope="session")
def http(live_base: str, auth_headers: dict):
    """Session-scoped authenticated HTTP client pointing at the live app.

    Times out at 30s per request — Databricks Apps cold-start latency on a
    sleeping SQL warehouse can take ~15s; 30s is the conservative ceiling.
    """
    with httpx.Client(
        base_url=live_base,
        headers=auth_headers,
        timeout=30.0,
        follow_redirects=False,
    ) as client:
        yield client


@pytest.fixture(scope="session")
def mcp_http(mcp_base: Optional[str], auth_headers: dict):
    """Optional MCP-server client — skipped if ONTOBRICKS_LIVE_MCP_BASE unset."""
    if not mcp_base:
        pytest.skip("ONTOBRICKS_LIVE_MCP_BASE not set; MCP probes skipped")
    with httpx.Client(
        base_url=mcp_base,
        headers=auth_headers,
        timeout=30.0,
        follow_redirects=False,
    ) as client:
        yield client


# ── Helpers ──────────────────────────────────────────────────────────────────


def wait_for_warehouse(http: httpx.Client, max_wait_s: float = 60.0) -> None:
    """Best-effort warm-up call to wake a STOPPED SQL warehouse.

    Several live tests hit endpoints that fan out to a SQL warehouse. If the
    warehouse is asleep the first request can take 15-30s. Call this once at
    the start of any test that needs a hot warehouse so timing assertions
    don't fail spuriously.
    """
    start = time.time()
    deadline = start + max_wait_s
    while time.time() < deadline:
        resp = http.get("/healthz")
        if resp.status_code == 200:
            return
        time.sleep(2)
    raise RuntimeError(
        f"App /healthz did not return 200 within {max_wait_s}s"
    )
