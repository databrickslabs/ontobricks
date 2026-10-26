"""Auto-tag every test under ``tests/e2e/scenarios/`` with ``scenario``.

The scenario suites are long, full-journey walks (and, for the ``*_live``
ones, billable: warehouse + LLM + a durable registry write). They are
**excluded from routine and agent test runs** and only execute on explicit
request — see ``.cursor/08-testing-and-deployment.mdc``.

Run them with::

    uv run --frozen pytest -m scenario              # all scenarios
    uv run --frozen pytest tests/e2e/scenarios/ -m scenario

The live scenario additionally needs ``ONTOBRICKS_SCENARIO_LIVE=1``.
"""

from __future__ import annotations

import pathlib

import pytest

from tests.e2e.conftest import _install_redirect_fix, _resolve_live_bearer
from tests.e2e.scenarios._harness import base_url, probe_app

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
# default) through a Playwright request context. A local app gets a plain
# context; a deployed Databricks App sits behind the Apps gateway, which
# answers unauthenticated calls with a 200 HTML login page, so the context
# carries the workspace bearer (``_live_bearer``, ``None`` on localhost).


@pytest.fixture(scope="module")
def scenario_base(_live_bearer) -> str:
    """Resolve and smoke-check the target app before the browser spins up."""
    base = base_url()
    try:
        probe_app(base, token=_live_bearer)
    except Exception as exc:  # noqa: BLE001 — skip with the probe's reason
        pytest.skip(
            f"No OntoBricks app reachable at {base} ({exc}). "
            f"Start it with `scripts/start.sh` or set ONTOBRICKS_LIVE_BASE."
        )
    return base


@pytest.fixture
def scenario_page(browser_instance, scenario_base, _live_bearer):
    """A fresh browser page on a clean context pointed at the running app."""
    token = _resolve_live_bearer() if _live_bearer else None
    if token:
        ctx = browser_instance.new_context(
            extra_http_headers={"Authorization": f"Bearer {token}"}
        )
        _install_redirect_fix(ctx, scenario_base, token)
    else:
        ctx = browser_instance.new_context()
    pg = ctx.new_page()
    pg.base_url = scenario_base
    yield pg
    pg.close()
    ctx.close()
