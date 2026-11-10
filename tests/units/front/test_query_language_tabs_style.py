"""Static contract: Graph → Query GraphQL / SPARQL tabs match Data Quality page tabs."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
SHELL = ROOT / "src/front/templates/partials/dtwin/_query_query.html"
DQ = ROOT / "src/front/templates/partials/dtwin/_query_dataquality.html"
SPARQL_CSS = ROOT / "src/front/static/query/css/query-sparql.css"


def _tab_list(shell: str) -> str:
    match = re.search(r'<ul[^>]*id="queryLanguageTabs"[^>]*>.*?</ul>', shell, re.DOTALL)
    assert match, "queryLanguageTabs <ul> not found"
    return match.group(0)


def _tab_button(shell: str, button_id: str) -> str:
    match = re.search(
        rf'<button[^>]*id="{button_id}"[^>]*>(.*?)</button>', shell, re.DOTALL
    )
    assert match, f"{button_id} button not found"
    return match.group(0)


def test_query_section_header_matches_data_quality_spacing():
    shell = SHELL.read_text(encoding="utf-8")
    dq = DQ.read_text(encoding="utf-8")

    assert 'class="section-header d-flex justify-content-between align-items-center mb-4"' in shell
    assert 'class="section-header d-flex justify-content-between align-items-center mb-4"' in dq


def test_query_language_tabs_use_card_integrated_data_quality_pattern():
    shell = SHELL.read_text(encoding="utf-8")
    tabs = _tab_list(shell)
    classes = re.search(r'class="([^"]+)"', tabs).group(1).split()

    assert 'class="card query-card-fill"' in shell
    assert 'class="card-body d-flex flex-column p-0"' in shell
    assert "nav" in classes
    assert "nav-tabs" in classes
    assert "ob-tabs" in classes
    assert "nav-fill" in classes
    assert "flex-shrink-0" in classes
    assert "query-language-tabs" in classes
    assert "ob-tabs--compact" not in classes
    assert 'class="tab-content flex-grow-1 overflow-auto query-language-content"' in shell


def test_query_card_fill_css_mirrors_data_quality_stretch():
    css = SPARQL_CSS.read_text(encoding="utf-8")
    assert "#graphql-section .query-card-fill" in css
    assert "#graphql-section .query-card-fill > .card-body" in css
    assert "#graphql-section > .section-header" in css


@pytest.mark.parametrize(
    "button_id, icon",
    [
        ("queryGraphqlTab", "bi-diagram-3"),
        ("querySparqlTab", "bi-braces"),
    ],
)
def test_query_language_tab_buttons_start_with_an_icon(button_id, icon):
    shell = SHELL.read_text(encoding="utf-8")
    button = _tab_button(shell, button_id)

    assert f'<i class="bi {icon} me-1"></i>' in button
    label = re.sub(r"<[^>]+>", "", button).strip()
    assert label in {"GraphQL", "SPARQL"}


def test_query_language_tabs_keep_their_bootstrap_wiring():
    shell = SHELL.read_text(encoding="utf-8")

    assert 'data-bs-target="#queryGraphqlPane"' in shell
    assert 'data-bs-target="#querySparqlPane"' in shell
    assert 'aria-controls="queryGraphqlPane"' in shell
    assert 'aria-controls="querySparqlPane"' in shell
