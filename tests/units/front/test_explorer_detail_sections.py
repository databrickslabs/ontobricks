"""Contracts for the KG Explorer Details tab: every section starts collapsed."""

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def _sec_helper(js: str) -> str:
    start = js.index("function _sec(")
    return js[start : js.index("\n        }", start)]


def test_section_helper_always_renders_collapsed() -> None:
    helper = _sec_helper(SIGMA_JS.read_text(encoding="utf-8"))
    assert "function _sec(icon, title, body)" in helper
    assert "entity-detail-section collapsed" in helper
    assert "startOpen" not in helper


def test_no_section_call_asks_to_start_open() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    calls = re.findall(r"_sec\((?:[^;]|\n)*?\);", js[js.index("function _sec(") :])
    assert len(calls) >= 10
    assert not [c for c in calls if re.search(r",\s*(true|false)\);$", c)]
