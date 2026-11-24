"""Contract tests for the Build → Objects tab markup and loader wiring."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
BUILD_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_build.html").read_text(encoding="utf-8")
OBJECTS_HTML = (ROOT / "src/front/templates/partials/dtwin/_build_objects.html").read_text(encoding="utf-8")
DTWIN_HTML = (ROOT / "src/front/templates/dtwin.html").read_text(encoding="utf-8")
OBJECTS_JS = (ROOT / "src/front/static/query/js/query-build-objects.js").read_text(encoding="utf-8")
RUNS_JS = (ROOT / "src/front/static/domain/js/domain-runs.js").read_text(encoding="utf-8")


def test_objects_tab_and_pane_follow_cache():
    assert BUILD_HTML.index('id="buildTabObjectsBtn"') > BUILD_HTML.index('id="buildTabCacheBtn"')
    assert BUILD_HTML.index('id="buildPaneObjects"') > BUILD_HTML.index('id="buildPaneCache"')
    assert "partials/dtwin/_build_objects.html" in BUILD_HTML


@pytest.mark.parametrize(
    "element_id",
    [
        "buildObjectsReloadBtn",
        "buildObjectsDeleteAllBtn",
        "buildObjectsLoading",
        "buildObjectsNotice",
        "buildObjectsError",
        "buildObjectsContent",
    ],
)
def test_objects_pane_exposes_hooks(element_id):
    assert f'id="{element_id}"' in OBJECTS_HTML


def test_delete_all_is_hidden_until_the_server_allows_drops():
    assert 'id="buildObjectsDeleteAllBtn"' in OBJECTS_HTML
    button = OBJECTS_HTML.split('id="buildObjectsDeleteAllBtn"', 1)[0].rsplit("<button", 1)[1]
    assert "d-none" in button


def test_script_is_loaded_and_fetches_objects():
    assert "query/js/query-build-objects.js" in DTWIN_HTML
    assert "/dtwin/build/objects" in OBJECTS_JS
    assert "shown.bs.tab" in OBJECTS_JS


def test_drop_buttons_are_gated_on_can_drop():
    assert "can_drop" in OBJECTS_JS
    assert "if (canDrop) actionCell.appendChild(dropButton(item))" in OBJECTS_JS


def test_drops_go_through_the_admin_settings_endpoints():
    for endpoint in (
        "/settings/graph-engine/drop-uc-object",
        "/settings/graph-engine/lakebase-drop-object",
        "/settings/graph-engine/neo4j-drop-label",
    ):
        assert endpoint in OBJECTS_JS


def test_group_by_toggle_defaults_to_usage():
    assert 'name="buildObjectsGroupBy" id="buildObjectsGroupByUsage"' in OBJECTS_HTML
    assert 'value="usage" autocomplete="off" checked' in OBJECTS_HTML
    assert 'id="buildObjectsGroupBySource"' in OBJECTS_HTML
    assert "groupBy = 'usage'" in OBJECTS_JS


def test_usage_view_is_fed_by_server_usages():
    assert "usages: data.usages" in OBJECTS_JS or "data.usages || []" in OBJECTS_JS
    assert "buckets.get(item.usage)" in OBJECTS_JS


def test_domain_runs_wires_objects_tab():
    assert "buildTabObjectsBtn" in RUNS_JS
    assert "'objects'" in RUNS_JS


def test_objects_list_uses_the_work_list_template():
    # Same layout as Build → Runs: filter bar with search, row count, one
    # .ob-runs-table per group, sentence-case header row, no table-light band.
    assert 'class="ob-filter-search"' in OBJECTS_HTML
    assert 'id="buildObjectsFilter"' in OBJECTS_HTML
    assert 'class="ob-runs-count" id="buildObjectsCount"' in OBJECTS_HTML
    assert "table table-hover align-middle ob-runs-table mb-0" in OBJECTS_JS
    assert "table-light" not in OBJECTS_JS
    assert "ob-runs-table" in OBJECTS_JS


def test_objects_list_has_header_row_and_no_filled_badges():
    assert "function tableHead(columns)" in OBJECTS_JS
    assert "tableHead(['Kind', 'Name', 'State', ''])" in OBJECTS_JS
    assert "tableHead(['Kind', 'Name', 'Source', 'State', ''])" in OBJECTS_JS
    assert "KIND_BADGES" not in OBJECTS_JS
    assert "style.width" not in OBJECTS_JS


def test_objects_filter_is_bound_to_content_and_reapplied_on_render():
    assert "ObListFilter.bind(" in OBJECTS_JS
    assert "el('buildObjectsContent'), el('buildObjectsCount'), 'object'" in OBJECTS_JS
    assert OBJECTS_JS.count("applyFilter();") >= 2  # render + clearContent


def test_list_filter_counts_body_rows_only():
    list_filter = (ROOT / "src/front/static/global/js/list-filter.js").read_text(encoding="utf-8")
    assert "querySelectorAll('tbody > tr')" in list_filter
