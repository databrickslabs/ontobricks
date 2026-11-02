"""The realtime stream job's import closure: no app layer, web stack, SPARQL parser or pydantic.

The job cluster imports the synced ``src`` tree (``PYTHONPATH``), not an app
wheel, so whatever ``back.core.streaming`` and ``jobs.triple_stream`` import
must already be on the Databricks Runtime (plus ``psycopg`` and
``databricks-sql-connector``, installed by the job). Each check runs in a fresh
interpreter so the test session's own imports do not hide a regression.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_SRC = Path(__file__).resolve().parents[3] / "src"
_FORBIDDEN = (
    "back.objects",
    "api",
    "front",
    "agents",
    "fastapi",
    "starlette",
    "neo4j",
    "rdflib",
    "pydantic",
    "pydantic_settings",
)


def _loaded(*modules: str) -> set[str]:
    """The forbidden packages a fresh interpreter has loaded after importing *modules*."""
    code = (
        "import importlib, json, sys\n"
        f"for name in {list(modules)!r}:\n"
        "    importlib.import_module(name)\n"
        f"forbidden = {list(_FORBIDDEN)!r}\n"
        "print(json.dumps(sorted(f for f in forbidden"
        " if any(m == f or m.startswith(f + '.') for m in sys.modules))))\n"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        env={"PYTHONPATH": str(_SRC), "PATH": "/usr/bin:/bin"},
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return set(json.loads(result.stdout.strip().splitlines()[-1]))


class TestTripleStreamClosure:
    def test_the_streaming_package_imports_none_of_the_app_layers(self):
        assert _loaded("back.core.streaming") == set()

    def test_the_stream_job_imports_none_of_the_app_layers(self):
        assert _loaded("jobs.triple_stream.main", "jobs.triple_stream.processor") == set()
