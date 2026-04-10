"""
End-to-end test fixtures.

Starts a Uvicorn server on localhost:18765, waits for it to be healthy,
then provides a Playwright browser and page to each test.

NOTE: E2E tests should be run separately from unit tests:
    .venv/bin/python -m pytest tests/e2e/ -v
"""
import atexit
import os
import signal
import sys
import time
import socket
import subprocess

import pytest


E2E_PORT = 18765
E2E_BASE = f"http://localhost:{E2E_PORT}"

_server_proc = None


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
    """Ensure Databricks env vars are set for the server process."""
    os.environ.setdefault("DATABRICKS_HOST", "https://test.databricks.com")
    os.environ.setdefault("DATABRICKS_TOKEN", "test-token")
    os.environ.setdefault("DATABRICKS_SQL_WAREHOUSE_ID", "test-warehouse")
    os.environ.setdefault("SECRET_KEY", "test-secret-key-e2e")


@pytest.fixture(scope="session")
def live_server(_set_env):
    """Start OntoBricks in a subprocess to isolate from test process env changes."""
    global _server_proc

    if not _port_free(E2E_PORT):
        pytest.skip(f"Port {E2E_PORT} is already in use -- cannot start test server")

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    src_dir = os.path.join(repo_root, "src")
    env = {**os.environ}
    env["PYTHONPATH"] = src_dir + os.pathsep + env.get("PYTHONPATH", "")

    _server_proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "shared.fastapi.main:app",
            "--host", "127.0.0.1",
            "--port", str(E2E_PORT),
            "--log-level", "warning",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    atexit.register(_kill_server)

    try:
        _wait_for_server(E2E_PORT)
    except RuntimeError:
        _kill_server()
        pytest.fail("Failed to start test server")

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


@pytest.fixture
def page(browser_instance, live_server):
    """Provide a fresh browser page pointed at the live server."""
    ctx = browser_instance.new_context()
    pg = ctx.new_page()
    pg.base_url = live_server
    yield pg
    pg.close()
    ctx.close()
