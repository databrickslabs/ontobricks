"""Local-dev preflight must not treat Apple python3 3.9 as a blocker when uv is present.

macOS still ships /usr/bin/python3 as 3.9. Local start (`scripts/start.sh` →
`scripts/setup.sh`) ran the --local preflight against PATH python3 and aborted
before `uv venv` could provision a CPython matching requires-python (>=3.11).
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PREFLIGHT = ROOT / "scripts/_internal/_deploy-preflight.sh"

_STUB_PY39 = """#!/bin/bash
if [[ "${1:-}" == "-c" ]]; then
    case "${2:-}" in
        *print*) echo "3.9"; exit 0 ;;
        *) exit 1 ;;
    esac
fi
exit 0
"""

_STUB_PY311 = """#!/bin/bash
if [[ "${1:-}" == "-c" ]]; then
    case "${2:-}" in
        *print*) echo "3.11"; exit 0 ;;
        *) exit 0 ;;
    esac
fi
exit 0
"""

_STUB_UV = """#!/bin/bash
if [[ "${1:-}" == "--version" ]]; then
    echo "uv 0.0.0 (test stub)"
    exit 0
fi
exit 0
"""


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _run_local_dev(tmp_path: Path, *, python_stub: str, with_uv: bool) -> subprocess.CompletedProcess[str]:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    _write_exec(bindir / "python3", python_stub)
    if with_uv:
        _write_exec(bindir / "uv", _STUB_UV)
    env = os.environ.copy()
    env["PATH"] = f"{bindir}{os.pathsep}/usr/bin{os.pathsep}/bin"
    script = f"source '{PREFLIGHT}'\n_preflight_check_local_dev\n_preflight_summary\n"
    return subprocess.run(
        ["bash", "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )


def test_old_path_python_passes_when_uv_is_present(tmp_path: Path) -> None:
    result = _run_local_dev(tmp_path, python_stub=_STUB_PY39, with_uv=True)
    combined = result.stdout + result.stderr
    assert result.returncode == 0, combined
    assert "too old" not in combined


def test_old_path_python_fails_without_uv(tmp_path: Path) -> None:
    result = _run_local_dev(tmp_path, python_stub=_STUB_PY39, with_uv=False)
    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert "too old" in combined
    assert "3.9" in combined


def test_path_python_311_passes_without_uv(tmp_path: Path) -> None:
    result = _run_local_dev(tmp_path, python_stub=_STUB_PY311, with_uv=False)
    combined = result.stdout + result.stderr
    assert result.returncode == 0, combined
    assert "Python 3.11" in combined
