"""The scorer must be usecase-agnostic: no maternity/domain identifiers.

Guards the D1/D2 contract — a domain-specific token leaking into the scorer
would bias the harness toward one usecase and reward overfitting.
"""

import re
from pathlib import Path

import pytest

# Directory of the pge_eval package + the CLI entry point = "the scorer".
_SCORER_PKG = Path(__file__).resolve().parents[3] / "src" / "agents" / "pge_eval"
_CLI = Path(__file__).resolve().parents[3] / "scripts" / "goals_eval.py"

# Domain tokens that must never appear in scorer code.
_FORBIDDEN = [
    r"trust_a",
    r"trust_b",
    r"trust_c",
    r"\bpreg",
    r"maternity",
    r"\bnhs\b",
]


def _scorer_files():
    files = list(_SCORER_PKG.glob("*.py"))
    files.append(_CLI)
    return files


@pytest.mark.parametrize("path", _scorer_files(), ids=lambda p: p.name)
def test_no_domain_token_in_scorer_file(path):
    text = path.read_text(encoding="utf-8").lower()
    for pattern in _FORBIDDEN:
        assert re.search(pattern, text) is None, (
            f"domain token /{pattern}/ found in {path.name}"
        )
