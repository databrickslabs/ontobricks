"""UI contracts for the shared materialized-inference purge action."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]


def _read(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


def test_shared_purge_button_is_in_reasoning_and_cohorts():
    include = '{% include "partials/dtwin/_purge_inferences_button.html" %}'
    paths = [
        "src/front/templates/partials/dtwin/_query_reasoning.html",
        "src/front/templates/partials/ontology/_ontology_cohorts.html",
    ]
    for path in paths:
        assert include in _read(path)


def test_build_purge_lives_in_header_kebab():
    build = _read("src/front/templates/partials/dtwin/_query_build.html")
    more = _read("src/front/templates/partials/dtwin/_build_more_actions.html")
    header = build.split('id="buildHeaderActions"', 1)[1].split('id="buildPageTabs"', 1)[0]
    assert '_build_more_actions.html' in header
    assert 'js-purge-inferences-btn' in more


def test_build_more_actions_offers_purge_and_refresh_cache():
    more = _read("src/front/templates/partials/dtwin/_build_more_actions.html")
    assert 'bi-three-dots-vertical' in more
    assert 'ob-dbx-menu-toggle' in more
    assert 'ob-dbx-menu' in more
    assert 'border-0' in more
    assert 'btn-outline-secondary' not in more
    assert 'text-danger' not in more
    assert 'ob-dbx-menu-label">Purge inferences' in more
    assert 'ob-dbx-menu-label">Refresh cache' in more
    assert 'data-sync-action="refresh-adjacency-only"' in more
    assert 'js-dbx-adjacency-refresh' in more


def test_ob_dbx_menu_lives_in_shared_components_css():
    css = _read("src/front/static/global/css/components.css")
    assert '.ob-dbx-menu' in css
    assert '.ob-dbx-menu-icon' in css
    assert '.ob-dbx-menu-label' in css
    assert '.ob-dbx-menu-toggle' in css
    assert 'font-size: 0.8125rem' in css
    # Must not be re-declared in the Build-only stylesheet.
    sync_css = _read("src/front/static/query/css/query-sync.css")
    assert '.ob-dbx-menu' not in sync_css


def test_purge_label_is_plain_purge_and_refreshes_inferred_breakdown():
    js = _read("src/front/static/query/js/query-purge-inferences.js")
    assert "InferredBreakdownModule.load()" in js


def test_shared_button_uses_destructive_style_without_inline_handler():
    html = _read(
        "src/front/templates/partials/dtwin/_purge_inferences_button.html"
    )
    assert "</i>Purge" in html
    assert "Purge Inferences" not in html
    assert "btn-outline-danger" in html
    assert "js-purge-inferences-btn" in html
    assert "onclick=" not in html


def test_shared_action_confirms_checks_permission_and_calls_endpoint():
    js = _read("src/front/static/query/js/query-purge-inferences.js")
    assert "showConfirmDialog" in js
    assert "canRefreshGraph" in js
    assert "'/dtwin/reasoning/inferred'" in js
    assert "method: 'DELETE'" in js
    assert "showNotification" in js
    assert "checkTripleStoreStatus(true)" in js


def test_confirmation_loads_and_names_combined_materialized_count():
    js = _read("src/front/static/query/js/query-purge-inferences.js")
    assert "method: 'GET'" in js
    assert "materialized_inference_count" in js
    assert "reasoning and cohorts" in js


def test_dtwin_loads_shared_purge_script_once():
    html = _read("src/front/templates/dtwin.html")
    assert html.count("query/js/query-purge-inferences.js") == 1
