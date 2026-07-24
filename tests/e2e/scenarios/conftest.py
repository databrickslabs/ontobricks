"""Auto-tag every test under ``tests/e2e/scenarios/`` with ``scenario``.

The scenario suites are long, full-journey walks (and, for the ``*_live``
ones, billable: warehouse + LLM + a durable registry write). They are
**excluded from routine and agent test runs** and only execute on explicit
request — see ``.cursor/08-testing-and-deployment.mdc``.

Run them with::

    uv run pytest -m scenario              # all scenarios
    uv run pytest tests/e2e/scenarios/ -m scenario

The live scenario additionally needs ``ONTOBRICKS_SCENARIO_LIVE=1``.
"""

from __future__ import annotations

import pathlib

import pytest

from tests.e2e.scenarios._harness import base_url

_SCENARIOS_DIR = pathlib.Path(__file__).parent.resolve()


def pytest_collection_modifyitems(items):
    """Mark tests collected from this directory as ``scenario``.

    A sub-directory ``conftest`` hook still receives the *whole* session's
    item list, so we must filter to items whose file lives under this
    directory before tagging them.
    """
    for item in items:
        item_path = pathlib.Path(str(getattr(item, "path", item.fspath))).resolve()
        if _SCENARIOS_DIR in item_path.parents or item_path == _SCENARIOS_DIR:
            item.add_marker(pytest.mark.scenario)


# ── Shared fixtures for the scenario campaign ────────────────────────────────
# Every scenario suite talks to a *running* app (the local dev server by
# default) through a plain Playwright request context — deliberately bypassing
# the parent ``page`` fixture, which injects an int-workspace bearer token that
# the scenarios must not carry.


@pytest.fixture(scope="module")
def scenario_base() -> str:
    """Resolve and smoke-check the target app before the browser spins up."""
    import httpx

    base = base_url()
    last_exc: Exception | None = None
    for probe in ("/health", "/healthz"):  # local serves /health; deployed /healthz
        try:
            resp = httpx.get(f"{base}{probe}", timeout=20.0)
            if resp.status_code == 200:
                return base
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    pytest.skip(
        f"No OntoBricks app reachable at {base} ({last_exc or 'non-200 health'}). "
        f"Start it with `scripts/start.sh` or set ONTOBRICKS_LIVE_BASE."
    )


@pytest.fixture
def scenario_page(browser_instance, scenario_base):
    """A fresh browser page on a clean context pointed at the running app."""
    ctx = browser_instance.new_context()
    pg = ctx.new_page()
    pg.base_url = scenario_base
    yield pg
    pg.close()
    ctx.close()
