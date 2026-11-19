"""Run-history lists follow the Databricks Jobs → Runs list.

Graph → Build → Runs, Intelligence → Analytics → Runs and Settings →
Automation → Runs share one flat list style (``.ob-runs-table``): transparent
header, hairline rows, start time first as a link, an icon + word status
(no filled pills) and a ghost details control.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[3]
BUILD_RUNS = REPO / "src/front/templates/partials/domain/_domain_build_runs.html"
ANALYTICS_RUNS = REPO / "src/front/templates/partials/domain/_domain_analytics_runs.html"
SETTINGS_RUNS = REPO / "src/front/templates/partials/settings/_settings_runs.html"
RUNS_RENDER = REPO / "src/front/static/global/js/runs-render.js"
DOMAIN_RUNS_JS = REPO / "src/front/static/domain/js/domain-runs.js"
COMPONENTS_CSS = REPO / "src/front/static/global/css/components.css"
MAIN_CSS = REPO / "src/front/static/global/css/main.css"
SETTINGS_LOCKS_JS = REPO / "src/front/static/config/js/settings-locks.js"

ROW_BUILDERS = (
    "buildRunCells",
    "analyticsRunCells",
    "communityRunCells",
    "patternRunCells",
    "simulationRunCells",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _rule(css: str, selector: str) -> str:
    match = re.search(rf"{re.escape(selector)}\s*\{{([^}}]*)\}}", css)
    assert match is not None, f"Missing CSS rule for {selector}"
    return match.group(1)


def _function_body(src: str, name: str) -> str:
    match = re.search(
        rf"function {name}\(run, idx, opts\) \{{(.*?)\n    \}}", src, re.DOTALL
    )
    assert match is not None, f"{name} not found"
    return match.group(1)


def _tables(html: str) -> list[str]:
    return re.findall(r"<table\b[^>]*>.*?</table>", html, re.DOTALL)


@pytest.mark.parametrize(
    "path,count",
    [(BUILD_RUNS, 1), (ANALYTICS_RUNS, 1), (SETTINGS_RUNS, 5)],
)
def test_every_runs_table_uses_the_shared_list_style(path, count):
    tables = _tables(_read(path))
    assert len(tables) == count
    for table in tables:
        opening = table[: table.index(">") + 1]
        assert "ob-runs-table" in opening
        assert "style=" not in opening, "No inline font-size on run tables"
        assert "table-light" not in table, "Header is transparent, not a grey band"
        first_th = re.search(r"<th\b[^>]*>(.*?)</th>", table, re.DOTALL)
        assert first_th and first_th.group(1).strip() == "Start time"


@pytest.mark.parametrize("path", [BUILD_RUNS, SETTINGS_RUNS])
def test_build_runs_list_shows_duration(path):
    html = _read(path)
    build = _tables(html)[0]
    assert ">Duration<" in build
    assert ">Run ID<" in build


@pytest.mark.parametrize("path,count_id", [(BUILD_RUNS, "runsCount"), (ANALYTICS_RUNS, "analyticsRunsCount")])
def test_per_domain_runs_have_a_toolbar_with_a_run_count(path, count_id):
    html = _read(path)
    toolbar = html[html.index('class="ob-filter-bar"') :]
    assert f'id="{count_id}"' in toolbar[: toolbar.index("Refresh")]
    js = _read(DOMAIN_RUNS_JS)
    assert f"document.getElementById('{count_id}')" in js


def test_rows_use_icon_and_word_status_not_filled_badges():
    src = _read(RUNS_RENDER)
    assert "function runStatus(" in src
    assert "ob-run-status" in src
    for label in ("'Succeeded'", "'Failed'", "'Canceled'", "'Running'"):
        assert label in src
    for name in ROW_BUILDERS:
        body = _function_body(src, name)
        assert "runStatus(run.status)" in body, name
        assert "StatusBadge(" not in body, f"{name} still renders a filled badge"
        assert "versionBadge(" not in body, f"{name} still renders a version pill"
        assert "timeLink(" in body, f"{name} must lead with a start-time link"


def test_details_control_is_a_ghost_button():
    src = _read(RUNS_RENDER)
    detail = re.search(r"function detailsCell\([^)]*\) \{(.*?)\n    \}", src, re.DOTALL)
    assert detail is not None
    assert "ob-run-details-btn" in detail.group(1)
    assert "btn-outline-primary" not in detail.group(1)


def test_runs_list_css_matches_databricks_list():
    css = _read(COMPONENTS_CSS)
    head = _rule(css, ".ob-runs-table > thead > tr > th")
    assert "background: transparent" in head
    assert "font-size: 0.75rem" in head
    assert "color: var(--db-text-secondary)" in head
    cell = _rule(css, ".ob-runs-table > tbody > tr > td")
    assert "border-bottom: 1px solid var(--db-border-light)" in cell
    assert "font-size" not in cell or "0.8125rem" in cell
    assert "color: var(--db-status-success)" in _rule(css, ".ob-run-status.is-success > .bi")
    assert "color: var(--db-status-danger)" in _rule(css, ".ob-run-status.is-failed > .bi")
    assert "color: var(--db-link)" in _rule(css, ".ob-run-link")
    ghost = _rule(css, ".ob-run-details-btn")
    assert "background: transparent" in ghost
    assert "border: 0" in ghost
    assert re.search(r"--db-link:\s*#2272B4", _read(MAIN_CSS))


def test_settings_locks_list_uses_the_shared_list_style():
    """Settings → Locks renders its table with the same flat list style."""
    js = _read(SETTINGS_LOCKS_JS)
    match = re.search(r"'<table class=\"([^\"]*)\">'", js)
    assert match is not None, "Locks table markup not found"
    classes = match.group(1).split()
    assert "ob-runs-table" in classes
    assert "table-light" not in js, "Locks header is transparent, not a grey band"
    assert "table-sm" not in classes
