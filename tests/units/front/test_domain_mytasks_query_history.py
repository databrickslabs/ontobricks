"""UI contracts for the shared work-list template (Databricks Query History style).

A shared filter bar (``.ob-filter-bar``: search + filter dropdown buttons +
count) above a single flat, compact, hoverable list (``.ob-runs-table``).
The look lives in ``global/css/components.css``; the free-text filter is the
shared ``global/js/list-filter.js``.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
STATIC = REPO_ROOT / "src/front/static"
TEMPLATES = REPO_ROOT / "src/front/templates"
TASKS_HTML = TEMPLATES / "partials/domain/_domain_mytasks.html"
COLLAB_JS = STATIC / "domain/js/domain-collaboration.js"
COLLAB_CSS = STATIC / "domain/css/domain-collaboration.css"
COMPONENTS_CSS = STATIC / "global/css/components.css"
LIST_FILTER_JS = STATIC / "global/js/list-filter.js"
DOMAIN_RUNS_JS = STATIC / "domain/js/domain-runs.js"
BUILD_RUNS_HTML = TEMPLATES / "partials/domain/_domain_build_runs.html"
ANALYTICS_RUNS_HTML = TEMPLATES / "partials/domain/_domain_analytics_runs.html"
HOME_TASKS_JS = STATIC / "home/js/home-tasks.js"
HOME_TASKS_CSS = STATIC / "registry/css/registry-my-tasks.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _css_block(css: str, selector: str) -> str:
    """Body of the first rule whose selector line starts with *selector*."""
    marker = selector + " {"
    assert marker in css, selector
    return css.split(marker, 1)[1].split("}", 1)[0]


def test_filter_bar_has_search_type_status_and_count():
    html = _read(TASKS_HTML)
    assert 'class="ob-filter-bar"' in html
    assert 'class="ob-filter-search"' in html
    assert 'id="domainTasksSearch"' in html
    assert 'type="search"' in html
    assert 'id="domainTasksTypeFilter"' in html
    assert 'id="domainTasksStatusFilter"' in html
    assert 'id="domainTasksCount"' in html


def test_list_body_stays_inside_one_card():
    html = _read(TASKS_HTML)
    assert html.index('class="card"') < html.index('class="ob-filter-bar"')
    assert html.index('class="ob-filter-bar"') < html.index('id="domainMyTasksContainer"')


def test_single_flat_hoverable_table():
    js = _read(COLLAB_JS)
    assert "table table-hover align-middle ob-runs-table ob-tasks-table" in js
    assert "Assigned to me</div>" not in js
    assert "Review worklist</div>" not in js


def test_status_is_icon_plus_word_not_filled_pill():
    js = _read(COLLAB_JS)
    assert "ob-run-status" in js
    assert "bg-secondary-subtle" not in js
    assert "bg-info-subtle text-dark border-info" not in js


def test_filters_are_applied_client_side():
    js = _read(COLLAB_JS)
    for needle in ("function applyTaskFilters", "domainTasksSearch",
                   "domainTasksTypeFilter", "domainTasksStatusFilter"):
        assert needle in js, needle


def test_filter_bar_styles_are_shared_in_components_css():
    css = _read(COMPONENTS_CSS)
    for selector in (".ob-filter-bar", ".ob-filter-search", ".ob-filter-btn"):
        assert selector + " {" in css or selector + ":" in css, selector
    # Task-only styles must not re-declare the shared filter bar.
    assert ".ob-filter-bar" not in _read(COLLAB_CSS)


def test_compact_rows_are_shared_in_components_css():
    css = _read(COMPONENTS_CSS)
    body = _css_block(css, ".ob-runs-table > tbody > tr > td")
    assert "padding: 0.3rem 0.75rem" in body
    assert ".ob-runs-table .btn-sm {" in css
    # The old, taller padding must be gone.
    assert "padding: 0.55rem 0.75rem" not in css


def test_list_filter_helper_is_global_and_documented():
    js = _read(LIST_FILTER_JS)
    assert "window.ObListFilter" in js
    assert "function bind(" in js
    assert "indexOf(q)" in js


@pytest.mark.parametrize("html_path", [BUILD_RUNS_HTML, ANALYTICS_RUNS_HTML])
def test_run_lists_use_shared_filter_bar(html_path):
    html = _read(html_path)
    assert 'class="ob-filter-bar"' in html
    assert 'class="ob-filter-search"' in html
    assert 'type="search"' in html
    assert "ob-runs-toolbar" not in html


def test_run_list_filters_are_bound_and_reapplied():
    js = _read(DOMAIN_RUNS_JS)
    assert "ObListFilter.bind(" in js
    assert "'runsFilter'" in js
    assert "'analyticsRunsFilter'" in js
    # Re-applied after every render so the count reflects the active filter.
    assert js.count("f.apply()") == 2


def test_list_filter_loaded_before_domain_runs():
    for page in ("domain.html", "intelligence.html", "dtwin.html"):
        html = _read(TEMPLATES / page)
        assert html.index("global/js/list-filter.js") < html.index(
            "domain/js/domain-runs.js"), page


def test_home_tasks_use_shared_table_style():
    js = _read(HOME_TASKS_JS)
    assert "ob-runs-table" in js
    assert "my-tasks-table" not in js
    # The old uppercase header overrides are gone; only the helpers remain.
    css = _read(HOME_TASKS_CSS)
    assert ".my-tasks-table" not in css
    assert ".my-tasks-approvals" in css
