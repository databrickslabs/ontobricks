"""Contract tests for Knowledge Graph readiness indicator UI."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PARTIAL = REPO_ROOT / "src/front/templates/partials/dtwin/_kg_status_indicator.html"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
SYNC_CSS = REPO_ROOT / "src/front/static/query/css/query-sync.css"

# Sub-pages that must include the readiness indicator next to their title.
_KG_PAGES = [
    "src/front/templates/partials/dtwin/_query_sigmagraph.html",
    "src/front/templates/partials/dtwin/_query_dataquality.html",
    "src/front/templates/partials/dtwin/_query_chat.html",
    "src/front/templates/partials/dtwin/_query_insights.html",
    "src/front/templates/partials/dtwin/_query_reasoning.html",
    "src/front/templates/partials/dtwin/_query_cohorts.html",
    "src/front/templates/partials/dtwin/_query_graphql.html",
    "src/front/templates/partials/dtwin/_query_analytics.html",
]


def test_kg_status_partial_exposes_data_attribute():
    html = PARTIAL.read_text(encoding="utf-8")
    assert 'data-kg-status' in html
    assert "kg-status-indicator" in html


def test_kg_subpages_include_status_partial():
    for rel in _KG_PAGES:
        html = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert '_kg_status_indicator.html' in html, f"missing indicator include in {rel}"


def test_update_kg_ready_indicators_covers_three_states():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "function updateKgReadyIndicators()" in js
    assert "Building…" in js
    assert "Graph ready" in js
    assert "Graph not built" in js
    assert "kg-go-build-btn" in js


def test_backend_label_covers_lakehouse_neo4j_lakebase():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "function _kgBackendLabel()" in js
    assert "return 'Lakehouse'" in js
    assert "return 'Neo4j'" in js
    assert "return 'Lakebase'" in js


def test_go_to_build_delegated_click_handler_present():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "kg-go-build-btn" in js
    assert 'data-section="sync"' in js


def test_kg_status_css_present():
    css = SYNC_CSS.read_text(encoding="utf-8")
    assert ".kg-status-indicator" in css
    assert ".kg-go-build-btn" in css
