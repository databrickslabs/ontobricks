"""UI contracts for the shared materialized-inference purge action."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]


def _read(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


def test_shared_purge_button_is_in_all_kg_build_and_inference_sections():
    include = '{% include "partials/dtwin/_purge_inferences_button.html" %}'
    paths = [
        "src/front/templates/partials/dtwin/_query_sync.html",
        "src/front/templates/partials/dtwin/_query_databricks_build.html",
        "src/front/templates/partials/dtwin/_query_reasoning.html",
        "src/front/templates/partials/dtwin/_query_cohorts.html",
    ]
    for path in paths:
        assert include in _read(path)


def test_shared_button_uses_destructive_style_without_inline_handler():
    html = _read(
        "src/front/templates/partials/dtwin/_purge_inferences_button.html"
    )
    assert "Purge Inferences" in html
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
