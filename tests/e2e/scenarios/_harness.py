"""Shared helpers for the live scenario campaign suites.

The scenario suites (``test_scenario_*.py``) are long, live, order-dependent
journeys that all talk to a **running** OntoBricks app over HTTP via a
Playwright request context. They used to each carry a private copy of the same
URL / CSRF / JSON / task-poll helpers; those now live here so there is one
implementation to evolve.

Pytest **fixtures** (``scenario_base``, ``scenario_page``) live in the sibling
``conftest.py`` — this module holds only plain, importable functions so it can
be used from anywhere without fixture magic.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional
from urllib.parse import urlparse

# ``GET /health`` is a local liveness check, but the server can still be
# slow to answer right after a long KG build; a skip here aborts the rest
# of the campaign via pytest-dependency. Budget a full minute.
PROBE_TIMEOUT_S = 60.0
PROBE_PATHS = ("/health", "/healthz")

# Durable campaign domains. Prefix ``Zz`` (CamelCase stand-in for ``ZZ_`` —
# underscores are illegal in domain names) so they sort last in the registry.
CAMPAIGN_DOMAIN_NAME = "ZzTestScenario1"
CAMPAIGN_DOMAIN_FOLDER = CAMPAIGN_DOMAIN_NAME.lower()
METADATA_DOMAIN_NAME = "ZzTestMetadataIntegrity"
ZERO_COPY_DOMAIN_NAME = "ZzTestZeroCopy"
LIFECYCLE_DOMAIN_NAME = "ZzSalesDomain"
# Folders from before the Zz rename — still purged at campaign start.
_LEGACY_CAMPAIGN_FOLDERS = frozenset(
    {"testscenario1", "testzerocopy", "testmetadataintegrity", "salesdomain"}
)


def is_loopback_base(url: str) -> bool:
    """True when *url* points at this machine (local campaign, not Apps)."""
    host = (urlparse(url).hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def needs_apps_gateway_token(base: Optional[str]) -> bool:
    """Deployed Databricks Apps need a workspace bearer; localhost does not."""
    if not base:
        return False
    return not is_loopback_base(base)


def probe_app(
    base: str, *, timeout: float = PROBE_TIMEOUT_S, token: Optional[str] = None
) -> None:
    """Hit ``/health`` then ``/healthz``; raise the last error if neither is 200."""
    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    last_exc: Exception | None = None
    for probe in PROBE_PATHS:
        try:
            resp = httpx.get(f"{base}{probe}", timeout=timeout, headers=headers)
            if resp.status_code == 200:
                return
            last_exc = RuntimeError(f"{probe} → HTTP {resp.status_code}")
        except Exception as exc:  # noqa: BLE001 — surface as the skip reason
            last_exc = exc
    raise last_exc or RuntimeError("non-200 health")


def base_url() -> str:
    """Resolve the target app base URL (defaults to the local dev server)."""
    return (
        os.environ.get("ONTOBRICKS_LIVE_BASE")
        or os.environ.get("ONTOBRICKS_SCENARIO_BASE")
        or "http://localhost:8000"
    ).rstrip("/")


def is_campaign_leftover(name: str) -> bool:
    """True for ``Zz*`` / ``ZZ_*`` campaign domains (and the pre-rename folders)."""
    key = (name or "").strip().lower()
    if not key:
        return False
    if key.replace("_", "").startswith("zz"):
        return True
    return key in _LEGACY_CAMPAIGN_FOLDERS


def registry_domain_keys(page, base: str) -> list[str]:
    """Return registry domain folders/names from ``GET /domain/list-projects``."""
    try:
        data = json_body(page.request.get(f"{base}/domain/list-projects"))
    except Exception:  # noqa: BLE001 — empty registry / transient
        return []
    out: list[str] = []
    for item in data.get("domains", []) or []:
        name = item if isinstance(item, str) else (item.get("folder") or item.get("name") or "")
        if name:
            out.append(str(name))
    return out


def purge_campaign_domains(page, base: str, *, step=None) -> list[str]:
    """DELETE every leftover campaign domain. Returns the folders removed."""
    headers = csrf_headers(page.context)
    deleted: list[str] = []
    for folder in registry_domain_keys(page, base):
        if not is_campaign_leftover(folder):
            continue
        if step is not None:
            step(f"deleting leftover campaign domain '{folder}'")
        resp = page.context.request.delete(
            f"{base}/settings/registry/domains/{folder}",
            headers=headers,
            timeout=60_000,
        )
        assert resp.status in (200, 204), resp.text()
        deleted.append(folder)
    return deleted


def csrf_headers(context) -> dict:
    """JSON headers carrying the double-submit CSRF token from the cookie."""
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    if token := cookies.get("csrf_token"):
        headers["X-CSRF-Token"] = token
    return headers


def json_body(resp) -> dict:
    """Parse a Playwright response body as JSON; ``{}`` on any parse failure."""
    try:
        return json.loads(resp.body())
    except Exception:  # noqa: BLE001 — non-JSON / empty body
        return {}


def make_step(tag: str):
    """Return a ``_step(msg)`` logger that prefixes every line with ``[tag]``."""

    def _step(msg: str) -> None:
        print(f"\n[{tag}] {msg}", flush=True)

    return _step


def chain_marker(name: str, depends: tuple = ()):
    """Env-gated ``pytest-dependency`` marker for campaign chaining.

    Returns a list suitable for splatting into a module ``pytestmark``. The
    dependency marker is only emitted when ``ONTOBRICKS_SCENARIO_CHAIN=1`` (set
    by ``make scenario-campaign``); in that mode a failed/skipped upstream
    scenario cleanly skips the downstream ones instead of producing a confusing
    cascade of independent failures.

    When the env flag is absent (running a single scenario in isolation), it
    returns ``[]`` so the scenarios' own data-driven prerequisite skips remain
    the only gate — you can still re-run scenario 3 alone against an already
    set-up registry.
    """
    if os.environ.get("ONTOBRICKS_SCENARIO_CHAIN") != "1":
        return []
    import pytest

    return [pytest.mark.dependency(name=name, depends=list(depends), scope="session")]


def poll_task(page, base: str, task_id: str, timeout_s: int, label: str, step=None) -> dict:
    """Poll ``GET /tasks/<id>`` until a terminal state or *timeout_s* elapses.

    *step* is an optional ``_step``-style logger used for periodic progress
    lines; pass the caller's logger to keep the campaign output consistent.
    """
    deadline = time.monotonic() + timeout_s
    last_log = 0.0
    while time.monotonic() < deadline:
        page.wait_for_timeout(3000)
        try:
            data = json_body(page.request.get(f"{base}/tasks/{task_id}"))
        except Exception:  # noqa: BLE001 — transient while the task runs
            continue
        task = data.get("task") or {}
        status = task.get("status")
        if status in ("completed", "failed", "cancelled"):
            return task
        now = time.monotonic()
        if now - last_log > 20:
            last_log = now
            if step is not None:
                step(
                    f"  …{label}: {status or 'pending'} "
                    f"({task.get('progress', 0)}%, {int(deadline - now)}s left)"
                )
    raise AssertionError(f"{label} did not finish within {timeout_s}s")
