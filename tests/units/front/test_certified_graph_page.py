"""Graph → Management → Certified Graph: sibling of the Original Graph page."""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
PARTIAL = FRONT / "templates/partials/dtwin/_query_certified.html"
DTWIN = FRONT / "templates/dtwin.html"
JS = FRONT / "static/query/js/query-certified.js"


def _management_items():
    menus = json.loads((FRONT / "config/menu_config.json").read_text(encoding="utf-8"))
    digitaltwin = next(m for m in menus["menus"] if m["id"] == "digitaltwin")
    group = next(g for g in digitaltwin["groups"] if g["id"] == "digitaltwin-management")
    return group["items"]


def test_the_menu_lists_certified_right_after_original():
    items = _management_items()
    ids = [i["id"] for i in items]
    assert ids[:2] == ["sync", "certified"]
    certified = items[1]
    assert certified["label"] == "Certified Graph"
    assert certified["icon"] == "bi-patch-check"


def test_the_page_hosts_the_certified_section():
    html = DTWIN.read_text(encoding="utf-8")
    section = html.split('id="certified-section"', 1)[1].split("</div>", 1)[0]
    assert 'class="sidebar-section"' in html.split('id="certified-section"', 1)[0][-80:] + section
    assert "partials/dtwin/_query_certified.html" in section
    assert html.index("query/js/query-sync.js") < html.index("query/js/query-certified.js")


def test_the_partial_follows_the_section_skeleton():
    html = PARTIAL.read_text(encoding="utf-8")
    assert html.index('class="content-section"') < html.index("section-header")
    header = html.split("section-header", 1)[1].split('class="card', 1)[0]
    assert "Certified Graph" in header
    assert 'id="certBuildStartBtn"' in header
    assert 'id="certStateBadge"' in header
    body = html.split('class="card', 1)[1]
    assert "ob-tabs-wrap" in body and "nav nav-tabs ob-tabs" in body
    for pane in ("certPaneBuild", "certPaneRuns"):
        assert f'id="{pane}"' in body
    for element in ("certReadOnlyNotice", "certProgressArea", "certRunsBody"):
        assert f'id="{element}"' in body
    for removed in ("certTarget", "certServedRelation", "certVersionStatus"):
        assert removed not in html
    assert "style=" not in html
    assert "onclick=" not in html
    assert "<script" not in html


def test_the_script_drives_status_and_build():
    js = JS.read_text(encoding="utf-8")
    assert "/dtwin/certified/status" in js
    assert "/dtwin/certified-build/start" in js
    assert "'/tasks/'" in js
    assert "section === 'certified'" in js
    assert "_setBackendBrandIcon" in js
    assert "innerHTML = " not in js.split("function _renderRuns", 1)[0]


def test_the_backend_panel_moved_from_original_to_certified():
    """Readiness, architecture and build log of a Lakebase / Neo4j backend live on the Certified page."""
    original = (FRONT / "templates/partials/dtwin/_query_build.html").read_text(encoding="utf-8")
    certified = PARTIAL.read_text(encoding="utf-8")
    assert "_query_sync.html" not in original
    assert 'id="sync-lakebase-actions"' not in original
    panel = certified.split('id="sync-lakebase-panel"', 1)[1]
    assert "partials/dtwin/_query_sync.html" in panel.split("</div>", 1)[0]
    assert 'id="sync-lakebase-actions"' in certified


def test_a_graph_engine_target_builds_through_the_backend_panel():
    js = JS.read_text(encoding="utf-8")
    assert "PANEL_TARGETS = ['lakebase', 'neo4j']" in js
    assert "startCertifiedGraphBuild()" in js


def test_the_backend_logo_is_sized_like_the_original_page():
    css = (FRONT / "static/query/css/query-sync.css").read_text(encoding="utf-8")
    assert "#certified-section .build-backend-logo .ob-brand-icon" in css
    assert "#certified-section .sync-content-section" in css


def test_the_original_page_has_only_build_and_runs_tabs():
    """Inferences live in the Certified Graph; Cache and Objects describe its backend."""
    original = (FRONT / "templates/partials/dtwin/_query_build.html").read_text(encoding="utf-8")
    tabs = original.split('id="buildPageTabs"', 1)[1].split("</ul>", 1)[0]
    assert tabs.count('class="nav-item"') == 2
    for gone in ("buildPaneInferred", "buildPaneCache", "buildPaneObjects"):
        assert gone not in original


def test_the_certified_page_hosts_cache_and_objects_tabs():
    html = PARTIAL.read_text(encoding="utf-8")
    tabs = html.split('id="certPageTabs"', 1)[1].split("</ul>", 1)[0]
    order = [tabs.index(f'id="{t}"') for t in
             ("certTabBuildBtn", "certTabRunsBtn", "certTabCacheBtn", "certTabObjectsBtn")]
    assert order == sorted(order)
    assert "partials/dtwin/_build_objects.html" in html.split('id="certPaneObjects"', 1)[1]
