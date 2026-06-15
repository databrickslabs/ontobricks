"""The scorer must be usecase-agnostic: no maternity/domain identifiers.

Guards the D1/D2 contract — a domain-specific token leaking into the scorer
would bias the harness toward one usecase and reward overfitting.
"""

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[3]
# Directory of the pge_eval package + the CLI entry point = "the scorer".
_SCORER_PKG = _ROOT / "src" / "agents" / "pge_eval"
_CLI = _ROOT / "scripts" / "goals_eval.py"

# The PGE generator/evaluator PROMPTS that run for EVERY domain. The scorer is
# the headline agnostic surface, but these prompts bias generation for all
# domains, so they must stay domain-neutral too (illustrative examples only).
_PROMPT_FILES = [
    _ROOT / "src" / "agents" / "agent_owl_generator" / "engine.py",
    _ROOT / "src" / "agents" / "agent_mapping_pge" / "generators" / "entity.py",
    _ROOT / "src" / "agents" / "agent_mapping_pge" / "planner.py",
]

# Domain tokens that must never appear. Broadened well past the original
# (trust_a/b/c, preg, maternity, nhs) to catch disguised NHS/CDM/SPR coupling.
# Generic English words (patient/delivery/order) are deliberately excluded —
# they collide with legitimate prose (e.g. "OWL delivery").
_FORBIDDEN = [
    r"trust_a",
    r"trust_b",
    r"trust_c",
    r"\bpreg\b",
    r"-preg-",
    r"pregnancy",
    r"maternity",
    r"\bnhs\b",
    r"\bspr\b",
    r"caesar",
    r"gestation",
    r"antenatal",
    r"postnatal",
    r"apgar",
    r"\bmother\b",
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
            f"domain token /{pattern}/ found in scorer file {path.name}"
        )


@pytest.mark.parametrize("path", _PROMPT_FILES, ids=lambda p: p.name)
def test_no_domain_token_in_generator_prompt(path):
    """PGE generation/evaluation prompts must stay usecase-agnostic — concrete
    NHS/CDM/maternity vocabulary in a system prompt biases EVERY domain's run."""
    text = path.read_text(encoding="utf-8").lower()
    for pattern in _FORBIDDEN:
        assert re.search(pattern, text) is None, (
            f"domain token /{pattern}/ found in generator prompt {path.name} — "
            "use a domain-neutral example instead"
        )
