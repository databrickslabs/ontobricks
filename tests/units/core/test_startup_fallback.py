"""The fallback app served when startup fails must not leak the error."""

import runpy
import sys
import types
from pathlib import Path

from fastapi.testclient import TestClient

RUN_PY = Path(__file__).resolve().parents[3] / "run.py"
SECRET = "dapi-secret-token-in-traceback"


def _fallback_app(monkeypatch):
    broken = types.ModuleType("shared.fastapi.main")

    def create_app():
        raise RuntimeError(f"cannot connect with {SECRET}")

    broken.create_app = create_app
    monkeypatch.setitem(sys.modules, "shared.fastapi.main", broken)
    return runpy.run_path(str(RUN_PY), run_name="run_fallback_test")["app"]


def test_fallback_routes_hide_the_startup_error(monkeypatch, capsys):
    client = TestClient(_fallback_app(monkeypatch))

    health = client.get("/health")
    page = client.get("/")

    assert health.json() == {"status": "error", "message": "App failed to start"}
    assert page.status_code == 200
    assert "Failed to Start" in page.text
    for body in (health.text, page.text):
        assert SECRET not in body
        assert "Traceback" not in body
        assert "RuntimeError" not in body
    assert SECRET in capsys.readouterr().out
