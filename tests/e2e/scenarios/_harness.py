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


def base_url() -> str:
    """Resolve the target app base URL (defaults to the local dev server)."""
    return (
        os.environ.get("ONTOBRICKS_LIVE_BASE")
        or os.environ.get("ONTOBRICKS_SCENARIO_BASE")
        or "http://localhost:8000"
    ).rstrip("/")


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
