"""Contracts for Explorer Search match-type defaults."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
REASONING_JS = REPO_ROOT / "src/front/static/query/js/query-reasoning.js"


def test_html_defaults_match_to_starts_with() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    assert 'id="sgFilterMatchType"' in html
    assert '<option value="starts" selected>Starts with</option>' in html
    assert '<option value="contains">Contains</option>' in html


def test_search_fallback_is_starts() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "?.value || 'starts'" in js
    assert "?.value || 'contains'" not in js


def test_programmatic_local_name_search_keeps_contains() -> None:
    sigma = SIGMA_JS.read_text(encoding="utf-8")
    assert "match_type: 'contains'" in sigma
    reasoning = REASONING_JS.read_text(encoding="utf-8")
    assert "matchSel.value = 'contains'" in reasoning
