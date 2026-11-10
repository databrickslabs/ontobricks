"""Regression: back.core.w3c must import without cycling through back.objects.

The cycle was: SPARQLRuleEngine → back.core.w3c (barrel) → OntologyGenerator
→ ClassModel → back.objects → Owl → back.core.w3c (still loading).
Collecting test_owl_parser / test_business_rules alone used to fail with
ImportError. A clean subprocess import is the only reliable guard.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_ROOT = Path(__file__).resolve().parents[3]
_SRC = str(_ROOT / "src")


def test_reasoning_and_w3c_import_in_a_clean_process():
    env = {**os.environ, "PYTHONPATH": _SRC}
    # Import order mirrors the failing path (reasoning → w3c → objects).
    code = (
        "import back.core.reasoning; "
        "import back.core.w3c; "
        "from back.objects.ontology.Owl import Owl; "
        "assert back.core.w3c.OntologyGenerator is not None; "
        "assert Owl is not None"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        cwd=str(_ROOT),
        check=False,
    )
    assert result.returncode == 0, result.stderr
