"""
End-to-end test fixtures.

Starts a Uvicorn server on localhost:18765 backed by **real Databricks
integration-workspace credentials**, waits for it to be healthy, then
provides a Playwright browser and page to each test.

Default behaviour: the conftest mints a workspace OAuth token from the
Databricks CLI profile named by ``ONTOBRICKS_E2E_PROFILE`` (default
``fevm-ontobricks-int``) and exports it as ``DATABRICKS_TOKEN``/``HOST``
into the subprocess. This makes Databricks-dependent pages
(``/dtwin/``, ``/domain``, ``/resolve``) load instead of hanging.

Escape hatches (env vars):

* ``ONTOBRICKS_E2E_PROFILE`` — which CLI profile to mint a token from.
* ``ONTOBRICKS_E2E_WAREHOUSE_ID`` — override the int warehouse.
* ``ONTOBRICKS_E2E_FAKE_CREDS=1`` — disable the auto-mint and fall
  back to the original fake env (``test.databricks.com`` etc.). Useful
  for tests that should NOT touch a real workspace; in this mode the
  6 Databricks-dependent tests will time out.
* ``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN`` already set in the
  environment — respected (no auto-mint).

If the CLI cannot mint a token AND ``ONTOBRICKS_E2E_FAKE_CREDS`` is not
set, the whole suite is skipped with a clear message — better than
running with fake creds and producing 6 confusing timeouts.

Usage (local mode — in-process uvicorn on localhost:18765):
    uv run pytest tests/e2e/ -v

Live mode (run the SAME browser flows against a DEPLOYED Databricks App):

    export ONTOBRICKS_LIVE_BASE=https://ontobricks-030-<workspace-id>.aws.databricksapps.com
    export DATABRICKS_CONFIG_PROFILE=fevm-ontobricks-int
    uv run pytest tests/e2e/ -v --no-cov

When ``ONTOBRICKS_LIVE_BASE`` is set, no local server starts; the
Playwright browser context carries an ``Authorization: Bearer <token>``
header (minted from the CLI profile) so the Databricks Apps gateway
authenticates every request, and a route handler rewrites bare no-slash
section routes to their trailing-slash form (the deployed app emits
trailing-slash redirects to the wrong host). Environment-specific tests
(assume local admin / no-auth) and durable-mutating tests are auto-skipped;
opt in to the mutating ones with ``ONTOBRICKS_LIVE_ALLOW_MUTATING=1``
(CAUTION: the int workspace is shared).
"""

import atexit
import os
import pathlib
import socket
import subprocess
import sys
import time
from typing import Optional

import pytest

from tests.fixtures.databricks_auth import DatabricksAuth


E2E_PORT = 18765
E2E_BASE = f"http://localhost:{E2E_PORT}"

_E2E_DIR = pathlib.Path(__file__).parent.resolve()
_SCENARIOS_DIR = _E2E_DIR / "scenarios"


def _live_base() -> Optional[str]:
    """Deployed-app base URL for live mode, or None for local mode."""
    return os.environ.get("ONTOBRICKS_LIVE_BASE") or None


def _e2e_explicitly_selected(config) -> bool:
    """True when the caller clearly asked for the e2e browser suite.

    The e2e suite drives a **session-scoped** ``sync_playwright`` browser
    whose event loop stays alive in-process for the rest of the session; when
    it runs alongside the unit/mcp suites it breaks ``pytest-asyncio`` for
    every async test collected after it. It is a *nightly-only* suite, so the
    routine command (``uv run pytest -q -m "not scenario"``) must not run it —
    but a developer who explicitly targets it (``pytest tests/e2e``,
    ``-m e2e``, or live mode) must still get it.
    """
    if _live_base():
        return True
    markexpr = getattr(config.option, "markexpr", "") or ""
    if "e2e" in markexpr:
        return True
    try:
        args = list(config.invocation_params.args)
    except Exception:  # noqa: BLE001 — invocation params are best-effort
        args = []
    return any("e2e" in str(a).replace("\\", "/") for a in args)


def _install_redirect_fix(ctx, base: str, token: str) -> None:
    """Work around the deployed app's wrong-host ``redirect_slashes`` redirects.

    Behind the Databricks Apps gateway the app does not honour the forwarded
    host when building its trailing-slash redirects, so a navigation to e.g.
    ``/ontology`` answers 307 → ``https://localhost:8000/ontology/`` and the
    browser dies with ``ERR_CONNECTION_REFUSED``. The redirect runs in BOTH
    directions (some routes are canonical with a slash, some without), so we
    cannot blindly add or strip one.

    Instead, for each top-level document navigation we resolve the canonical
    URL server-side with httpx — following redirects but pinning every hop
    back onto ``base`` — then point the browser straight at the resolved
    same-origin URL (``continue_`` only allows same-origin rewrites). Sub-
    resource and XHR requests are passed through untouched (they already
    target ``base`` and carry the bearer header from the context).
    """
    import httpx

    base = base.rstrip("/")

    def _canonical(url: str) -> str:
        with httpx.Client(
            follow_redirects=False,
            timeout=30.0,
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            for _ in range(5):
                resp = client.get(url)
                if resp.status_code not in (301, 302, 303, 307, 308):
                    return url
                loc = resp.headers.get("location", "")
                if not loc:
                    return url
                # Pin the redirect target (possibly wrong-host) back onto base.
                if "://" in loc:
                    path = loc.split("://", 1)[1].split("/", 1)[-1]
                else:
                    path = loc.lstrip("/")
                url = f"{base}/{path}"
        return url

    def _route(route):
        req = route.request
        if req.resource_type != "document" or not req.url.startswith(base):
            route.continue_()
            return
        try:
            resolved = _canonical(req.url)
        except Exception:  # noqa: BLE001 — best-effort; fall back to the browser
            route.continue_()
            return
        if resolved != req.url:
            route.continue_(url=resolved)
        else:
            route.continue_()

    ctx.route("**/*", _route)


# Default integration workspace settings. Override via the
# ``ONTOBRICKS_E2E_*`` env vars documented at the top of this module.
DEFAULT_E2E_PROFILE = "fevm-ontobricks-int"
DEFAULT_E2E_HOST = "https://fevm-ontobricks-int.cloud.databricks.com"
DEFAULT_E2E_WAREHOUSE_ID = "fcdf5a06992ad225"

_server_proc = None


def _mint_workspace_token(profile: str) -> tuple[str, str] | None:
    """Mint a workspace OAuth token from the named Databricks CLI profile.

    Returns ``(host, access_token)`` on success, ``None`` if the CLI is
    not installed, the profile does not exist, or token minting fails.
    Thin adapter over the shared ``DatabricksAuth`` helper; the host falls
    back to the well-known int host so the local uvicorn always has one.
    """
    return DatabricksAuth.mint_host_and_token(profile, host_fallback=DEFAULT_E2E_HOST)


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0


def _wait_for_server(port: int, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) == 0:
                return
        time.sleep(0.25)
    raise RuntimeError(f"Server on port {port} did not start within {timeout}s")


def _kill_server():
    global _server_proc
    if _server_proc and _server_proc.poll() is None:
        _server_proc.terminate()
        try:
            _server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _server_proc.kill()
    _server_proc = None


@pytest.fixture(scope="session", autouse=True)
def _set_env():
    """Resolve Databricks env vars for the server subprocess.

    Default mode: mint a workspace OAuth token from the Databricks CLI
    profile named by ``ONTOBRICKS_E2E_PROFILE`` (default
    ``fevm-ontobricks-int``) and export it as ``DATABRICKS_TOKEN`` /
    ``DATABRICKS_HOST`` so the local uvicorn talks to the real int
    workspace.

    Respects any pre-set ``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` (so
    CI can pass workspace creds via env). Falls back to fake values
    only if ``ONTOBRICKS_E2E_FAKE_CREDS=1`` is set. Otherwise, if
    neither pre-set creds nor a working CLI profile exist, skips the
    whole suite with a clear message.
    """
    if _live_base():
        # Live mode: talk to a DEPLOYED app via the Databricks Apps gateway.
        # No local subprocess starts, so there is nothing to configure here
        # (no DATABRICKS_TOKEN/HOST/SECRET_KEY export, no skip-on-missing
        # creds). The bearer token is minted lazily by ``_live_bearer``.
        return

    fake_only = os.environ.get("ONTOBRICKS_E2E_FAKE_CREDS") == "1"

    # Always set SECRET_KEY.
    os.environ.setdefault("SECRET_KEY", "test-secret-key-e2e")

    if fake_only:
        os.environ.setdefault("DATABRICKS_HOST", "https://test.databricks.com")
        os.environ.setdefault("DATABRICKS_TOKEN", "test-token")
        os.environ.setdefault("DATABRICKS_SQL_WAREHOUSE_ID", "test-warehouse")
        return

    # If the caller already exported real creds, respect them.
    have_host = bool(os.environ.get("DATABRICKS_HOST"))
    have_token = bool(os.environ.get("DATABRICKS_TOKEN"))
    if have_host and have_token:
        os.environ.setdefault(
            "DATABRICKS_SQL_WAREHOUSE_ID",
            os.environ.get(
                "ONTOBRICKS_E2E_WAREHOUSE_ID", DEFAULT_E2E_WAREHOUSE_ID
            ),
        )
        return

    # Auto-mint from the configured profile.
    profile = os.environ.get("ONTOBRICKS_E2E_PROFILE", DEFAULT_E2E_PROFILE)
    minted = _mint_workspace_token(profile)
    if minted is None:
        pytest.skip(
            f"E2E suite needs real Databricks creds. Either:\n"
            f"  - run `databricks auth login --profile {profile} "
            f"--host {DEFAULT_E2E_HOST}` and retry, or\n"
            f"  - export DATABRICKS_HOST + DATABRICKS_TOKEN explicitly, "
            f"or\n"
            f"  - set ONTOBRICKS_E2E_FAKE_CREDS=1 to use fake values "
            f"(note: 6 Databricks-dependent tests will time out)."
        )

    host, token = minted
    os.environ["DATABRICKS_HOST"] = host
    os.environ["DATABRICKS_TOKEN"] = token
    os.environ.setdefault(
        "DATABRICKS_SQL_WAREHOUSE_ID",
        os.environ.get(
            "ONTOBRICKS_E2E_WAREHOUSE_ID", DEFAULT_E2E_WAREHOUSE_ID
        ),
    )


@pytest.fixture(scope="session")
def live_server(_set_env):
    """Base URL the browser points at.

    Live mode (``ONTOBRICKS_LIVE_BASE`` set): yield the deployed app URL and
    start no local server. Local mode: start OntoBricks in a subprocess to
    isolate it from test-process env changes.
    """
    global _server_proc

    live = _live_base()
    if live:
        yield live.rstrip("/")
        return

    if not _port_free(E2E_PORT):
        pytest.skip(f"Port {E2E_PORT} is already in use -- cannot start test server")

    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    src_dir = os.path.join(repo_root, "src")
    env = {**os.environ}
    env["PYTHONPATH"] = src_dir + os.pathsep + env.get("PYTHONPATH", "")
    # The unit-test harness (tests/conftest.py) forces CSRF_DISABLED=1 so
    # TestClient assertions don't need tokens; the E2E subprocess, however,
    # is meant to mirror real deployment where CSRF is live.  Strip the
    # override so tests/e2e/test_permissions_flows.py can actually exercise
    # the rejection branch.
    env.pop("CSRF_DISABLED", None)

    # Capture stdout/stderr into a session log so startup failures are
    # debuggable. Path is printed on failure.
    log_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "_e2e_server.log"
    )
    log_fh = open(log_path, "w")
    _server_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "shared.fastapi.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(E2E_PORT),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    atexit.register(_kill_server)

    try:
        _wait_for_server(E2E_PORT)
    except RuntimeError:
        _kill_server()
        log_fh.close()
        with open(log_path) as f:
            tail = f.read()[-4000:]
        pytest.fail(
            f"Failed to start test server. uvicorn log tail:\n{tail}"
        )

    yield E2E_BASE
    _kill_server()


@pytest.fixture(scope="session")
def browser_instance():
    """Launch a Playwright Chromium browser for the session."""
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    yield browser
    browser.close()
    pw.stop()


@pytest.fixture(scope="session")
def _live_bearer() -> Optional[str]:
    """Workspace bearer token for live mode; ``None`` in local mode.

    Profile precedence matches the live_integration suite —
    ``DATABRICKS_CONFIG_PROFILE`` first, then ``ONTOBRICKS_E2E_PROFILE``,
    then the int default — so a single env var configures both suites.
    """
    if not _live_base():
        return None
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE") or os.environ.get(
        "ONTOBRICKS_E2E_PROFILE", DEFAULT_E2E_PROFILE
    )
    token = DatabricksAuth.mint_token(profile)
    if not token:
        pytest.skip(
            "Live e2e needs a Databricks token. Run "
            f"`databricks auth login --profile {profile}` and retry."
        )
    return token


# Tests that assume the LOCAL admin/no-auth server (or a local host / locally
# synced files) and would misbehave against the deployed app + gateway —
# always skipped in live mode.
_LIVE_SKIP_ENV_SPECIFIC = (
    "settings/test_settings_flows.py::TestSettingsPage::test_host_display",
    "settings/test_write_flows.py::TestSettingsSaveSessionOnly",
    "security/test_permissions_flows.py::TestPermissionMiddlewareShape"
    "::test_settings_page_is_reachable_for_admin",
    # Help docs are served from the synced docs/ dir; the deployed bundle
    # excludes README.md, so the catalogued "Overview" (readme) slug 404s.
    "help/test_help_modal_flows.py::TestHelpDocsApi::test_help_doc_fetch_round_trip",
    "help/test_help_modal_flows.py::TestHelpDocsApi::test_all_catalogued_docs_return_200",
    # The Apps gateway rejects the path-traversal URL with 400 before the app's
    # 404 handler runs — still rejected, just a different status code.
    "help/test_help_modal_flows.py::TestHelpDocsApi::test_help_image_bad_name_is_rejected",
)

# Tests that perform DURABLE writes to the shared int registry — skipped in
# live mode unless ONTOBRICKS_LIVE_ALLOW_MUTATING=1.
_LIVE_SKIP_MUTATING = (
    "settings/test_write_flows.py::TestSettingsSaveRouteContracts",
    "domain/test_domain_api_flows.py::TestDomainWriteEndpoints"
    "::test_design_view_create_contract",
    "domain/test_domain_api_flows.py::TestDomainWriteEndpoints"
    "::test_design_view_save_current_contract",
)


def pytest_collection_modifyitems(config, items):
    """Tag / gate the e2e browser suite.

    Two responsibilities:

    1. **Nightly-only isolation** — tag every non-scenario e2e test with the
       ``e2e`` marker and, unless the suite is explicitly targeted, skip it.
       This keeps the session-scoped Playwright event loop out of routine
       runs so it cannot poison ``pytest-asyncio`` for the unit/mcp suites
       collected after it (see :func:`_e2e_explicitly_selected`).
    2. **Live-mode gating** — when ``ONTOBRICKS_LIVE_BASE`` is set, skip
       environment-specific flows always and durable-mutating flows unless the
       caller opts in with ``ONTOBRICKS_LIVE_ALLOW_MUTATING=1``.

    A sub-directory ``conftest`` hook still receives the *whole* session's
    item list, so we filter to items under this directory first.
    """
    targeted = _e2e_explicitly_selected(config)
    skip_nightly = pytest.mark.skip(
        reason="e2e browser suite is nightly-only; run it explicitly with "
        "`uv run pytest tests/e2e` (skipped in routine runs so the "
        "session-scoped Playwright loop can't break pytest-asyncio)."
    )
    for item in items:
        item_path = pathlib.Path(
            str(getattr(item, "path", item.fspath))
        ).resolve()
        under_e2e = _E2E_DIR in item_path.parents or item_path == _E2E_DIR
        under_scenarios = _SCENARIOS_DIR in item_path.parents
        if under_e2e and not under_scenarios:
            item.add_marker(pytest.mark.e2e)
            if not targeted:
                item.add_marker(skip_nightly)

    if not _live_base():
        return
    allow_mut = os.environ.get("ONTOBRICKS_LIVE_ALLOW_MUTATING") == "1"
    skip_env = pytest.mark.skip(
        reason="live mode: ENV-SPECIFIC (assumes local admin/no-auth host)"
    )
    skip_mut = pytest.mark.skip(
        reason="live mode: MUTATES shared int env; set "
        "ONTOBRICKS_LIVE_ALLOW_MUTATING=1 to run"
    )
    for item in items:
        nid = str(item.nodeid).replace("\\", "/")
        if any(s in nid for s in _LIVE_SKIP_ENV_SPECIFIC):
            item.add_marker(skip_env)
        elif not allow_mut and any(s in nid for s in _LIVE_SKIP_MUTATING):
            item.add_marker(skip_mut)


@pytest.fixture
def page(browser_instance, live_server, _live_bearer):
    """Provide a fresh browser page pointed at the server.

    In live mode the context carries an ``Authorization: Bearer`` header on
    every request (page nav, sub-resources, and ``page.context.request.*``
    API calls) and rewrites the deployed app's bad trailing-slash redirects.
    Local mode is unchanged.
    """
    if _live_bearer:
        ctx = browser_instance.new_context(
            extra_http_headers={"Authorization": f"Bearer {_live_bearer}"}
        )
        _install_redirect_fix(ctx, live_server, _live_bearer)
    else:
        ctx = browser_instance.new_context()
    pg = ctx.new_page()
    pg.base_url = live_server
    yield pg
    pg.close()
    ctx.close()
