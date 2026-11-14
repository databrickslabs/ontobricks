"""Static contract for Intelligence → Configuration → Cohorts (Design / Runs tabs)."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
PAGE = FRONT / "templates/intelligence.html"
SHELL = FRONT / "templates/partials/ontology/_ontology_cohorts.html"
RUNS_PANE = FRONT / "templates/partials/dtwin/_query_cohorts.html"
COHORT_JS = FRONT / "static/query/js/query-cohorts.js"
DTWIN_ROUTE = FRONT / "routes/dtwin.py"
INTEL_ROUTE = FRONT / "routes/intelligence.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_runs_is_no_longer_a_sidebar_section():
    html = _read(PAGE)
    assert 'id="cohort-run-section"' not in html
    assert 'id="cohorts-section"' in html
    assert "partials/dtwin/_query_cohorts.html" in _read(SHELL)
    assert html.count('include "partials/ontology/_ontology_cohorts.html"') == 1


def test_shell_has_design_and_runs_tabs():
    shell = _read(SHELL)
    assert 'id="cohortTabs"' in shell
    for btn, pane in (
        ("cohortTabDesignBtn", "cohortPaneDesign"),
        ("cohortTabRunsBtn", "cohortPaneRuns"),
    ):
        assert f'id="{btn}"' in shell
        assert f'id="{pane}"' in shell
        assert f'data-bs-target="#{pane}"' in shell
        assert f'aria-labelledby="{btn}"' in shell


def test_one_saved_rules_rail_is_shared_by_both_tabs():
    shell = _read(SHELL)
    runs = _read(RUNS_PANE)
    assert shell.count('id="cohortRulesList"') == 1
    assert shell.count('class="cohort-rail"') == 1
    for stale in ("cohortRunRulesList", "cohortRunRulesEmpty", "cohortRunRulesCount"):
        assert stale not in shell and stale not in runs
    # The Runs pane body does not own a second rail or page header.
    assert 'class="cohort-rail"' not in runs
    assert "section-header" not in runs


def test_header_actions_swap_with_the_active_tab():
    shell = _read(SHELL)
    # Design actions are visible by default; Runs actions start hidden.
    assert 'class="d-flex align-items-center gap-2" data-cohort-actions="design"' in shell
    assert 'class="d-flex align-items-center gap-2 d-none" data-cohort-actions="runs"' in shell
    design = shell.split('data-cohort-actions="design"', 1)[1].split(
        'data-cohort-actions="runs"', 1
    )[0]
    runs = shell.split('data-cohort-actions="runs"', 1)[1].split(
        'id="cohortTabs"', 1
    )[0]
    assert 'id="cohortNewBtn"' in design and 'id="cohortSaveBtn"' in design
    assert 'id="cohortPreviewBtn"' in runs and 'id="cohortMaterializeBtn"' in runs
    assert 'id="cohortOutputsBtn"' in runs
    assert "_purge_inferences_button.html" in runs


def test_runs_pane_keeps_preview_and_diagnostics_ids():
    runs = _read(RUNS_PANE)
    for element in (
        "cohortRuleSummary",
        "cohortPreviewBody",
        "cohortTraceResult",
        "cohortExplainTarget",
        "cohortExplainResult",
    ):
        assert f'id="{element}"' in runs, element


def test_legacy_cohort_run_links_redirect_to_runs_tab():
    dtwin = _read(DTWIN_ROUTE)
    intel = _read(INTEL_ROUTE)
    assert 'dest = "/intelligence/?section=cohorts&tab=runs"' in dtwin
    assert 'request.query_params.get("section") == "cohort-run"' in intel
    assert 'dest = "/intelligence/?section=cohorts&tab=runs"' in intel


def test_js_drives_tabs_from_the_tab_param_and_keeps_rule_selection_on_tab():
    js = _read(COHORT_JS)
    assert "'cohort-run-section'" not in js
    assert "cohortRunRules" not in js
    assert "cohortTabBuildBtn" not in js
    assert "_showTab(params.get('tab')" in js
    assert "_syncTabChrome(tab)" in js
    assert "replaceState" in js
    # Loading a rule must not force the Design tab.
    load_rule = js.split("loadRule(rule_id) {", 1)[1].split("newRule(reset", 1)[0]
    assert "_showTab" not in load_rule
    assert "_showBuildTab" not in load_rule
