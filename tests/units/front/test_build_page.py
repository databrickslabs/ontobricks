"""Graph → Management → Build: one page for the Original and the Certified Graph."""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
PAGE = FRONT / "templates/partials/dtwin/_query_build.html"
DTWIN = FRONT / "templates/dtwin.html"
JS = FRONT / "static/query/js/query-certified.js"


def _html() -> str:
    return PAGE.read_text(encoding="utf-8")


def _management_items():
    menus = json.loads((FRONT / "config/menu_config.json").read_text(encoding="utf-8"))
    digitaltwin = next(m for m in menus["menus"] if m["id"] == "digitaltwin")
    group = next(g for g in digitaltwin["groups"] if g["id"] == "digitaltwin-management")
    return group["items"]


def test_one_build_entry_replaces_the_two_graph_pages():
    items = _management_items()
    build = items[0]
    assert (build["id"], build["label"], build["icon"]) == ("sync", "Build", "bi-hammer")
    assert "certified" not in [i["id"] for i in items]


def test_the_page_is_the_only_build_section():
    html = DTWIN.read_text(encoding="utf-8")
    assert "certified-section" not in html
    assert "_query_certified.html" not in html
    section = html.split('id="sync-section"', 1)[1].split("</div>", 1)[0]
    assert "partials/dtwin/_query_build.html" in section
    assert html.index("query/js/query-sync.js") < html.index("query/js/query-certified.js")


def test_a_summary_diagram_sits_above_the_tabs():
    html = _html()
    flow = html.split('id="buildFlow"', 1)[1].split('id="buildPageTabs"', 1)[0]
    assert flow.index('id="buildFlowOriginal"') < flow.index("dt-arch-connector")
    assert flow.index("dt-arch-connector") < flow.index('id="buildFlowCertified"')
    for element in ("dbxBuildStatusText", "certBackendIcon", "certStateBadge", "certFlowTarget"):
        assert f'id="{element}"' in flow
    assert html.index('class="card') < html.index('id="buildFlow"')


def test_the_tabs_are_build_runs_cache_objects():
    tabs = _html().split('id="buildPageTabs"', 1)[1].split("</ul>", 1)[0]
    order = [tabs.index(f'id="{t}"') for t in
             ("buildTabBuildBtn", "buildTabRunsBtn", "buildTabCacheBtn", "buildTabObjectsBtn")]
    assert order == sorted(order)
    assert tabs.count('class="nav-item"') == 4


def test_the_build_tab_has_an_original_then_a_certified_section():
    pane = _html().split('id="buildPaneBuild"', 1)[1].split('id="buildPaneRuns"', 1)[0]
    original, certified = pane.split('id="buildCertifiedHeading"', 1)
    assert "_query_databricks_build.html" in original
    assert 'id="sync-lakebase-panel"' in certified
    assert "_query_sync.html" in certified
    assert 'id="certServedLine"' in certified


def test_the_header_has_one_build_and_no_backend_logo():
    header = _html().split("section-header", 1)[1].split('class="card', 1)[0]
    assert header.count('id="dbxBuildStartBtn"') == 1
    assert "build-backend-logo" not in header
    assert "Build" in header


def test_the_page_has_no_inline_style_or_script():
    html = _html()
    assert "style=" not in html
    assert "onclick=" not in html
    assert "<script" not in html


def test_the_script_fills_the_certified_part_of_the_build_page():
    js = JS.read_text(encoding="utf-8")
    assert "/dtwin/certified/status" in js
    assert "section === 'sync'" in js
    assert "_setBackendBrandIcon" in js
    assert "innerHTML" not in js
    assert "/dtwin/certified-build/start" not in js


def test_a_finished_build_refreshes_the_certified_state():
    js = (FRONT / "static/query/js/query-databricks-build.js").read_text(encoding="utf-8")
    body = js.split("function _finishDbxBuild", 1)[1].split("\n}\n", 1)[0]
    assert "window.loadCertifiedStatus()" in body


def test_the_build_is_disabled_on_a_read_only_version():
    html = (FRONT / "templates/partials/dtwin/_query_databricks_build.html").read_text(encoding="utf-8")
    js = (FRONT / "static/query/js/query-databricks-build.js").read_text(encoding="utf-8")
    assert 'id="dbxBuildReadOnlyNotice"' in html
    assert "dbxBuildEditable = data.editable !== false" in js
    start = js.split("async function startDatabricksBuild", 1)[1][:700]
    assert "!dbxBuildEditable" in start


def test_no_inferred_tab():
    assert "buildPaneInferred" not in _html()
