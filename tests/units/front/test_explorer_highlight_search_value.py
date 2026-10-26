"""Contracts for the Explorer Filter-tab highlight control next to Search Value."""

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def test_search_value_has_icon_highlight_button() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    value_idx = html.index('id="sgFilterValue"')
    snippet = html[value_idx : value_idx + 800]
    assert 'data-sg-action="highlightFilterValue"' in snippet
    match = re.search(
        r'data-sg-action="highlightFilterValue"[^>]*>(.*?)</button>',
        snippet,
        re.S,
    )
    assert match is not None
    inner = match.group(1)
    assert "bi-geo-alt" in inner
    assert "bi-search" not in inner
    assert "bi-arrow-repeat" not in inner
    assert "bi-highlighter" not in inner
    assert "Highlight" not in inner


def test_find_in_graph_context_menu_uses_location_icon() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    ctx_start = html.index('data-sg-ctx="openFindPopup"')
    ctx = html[ctx_start : ctx_start + 200]
    assert "bi-geo-alt" in ctx
    assert "bi-search" not in ctx
    popup_header = html[html.index('id="sgFindPopup"') : html.index('id="sgFindPopup"') + 500]
    assert '<i class="bi bi-geo-alt"></i> Find in graph' in popup_header
    assert "bi-highlighter" not in popup_header
    apply_btn = html[html.index('data-sg-action="applySearch"') : html.index('data-sg-action="applySearch"') + 180]
    assert "bi-geo-alt" in apply_btn
    assert "bi-highlighter" not in apply_btn


def test_highlight_filter_value_reads_search_value_input() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "highlightFilterValue:" in js
    start = js.index("highlightFilterValue:")
    body = js[start : start + 400]
    assert "sgFilterValue" in body
    assert "sgSearchValue" not in body
    assert "_applyHighlightQuery" in body


def test_explorer_right_panel_tab_is_named_search() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    tab = html[html.index('id="sgTabFilter"') : html.index('id="sgTabFilterPane"')]
    assert "bi-search" in tab
    assert "</i> Search" in tab
    assert "</i> Filter" not in tab
    pane_head = html[html.index('id="sgTabFilterPane"') : html.index('id="sgFilterEntityType"')]
    assert "Filter Graph" not in pane_head

