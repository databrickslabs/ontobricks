"""Static contract for the Simulation page (menu 5)."""

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
PAGE = FRONT / "templates/simulation.html"
MENU_CONFIG = FRONT / "config/menu_config.json"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _menu() -> dict:
    menus = json.loads(_read(MENU_CONFIG))["menus"]
    return next(m for m in menus if m["id"] == "simulation")


def test_cohorts_group_is_first():
    assert [g["id"] for g in _menu()["groups"]] == [
        "simulation-cohorts",
        "simulation-scenarios",
    ]
    items = [i["id"] for g in _menu()["groups"] for i in g["items"]]
    assert "results" not in items
    assert "runs" in items


def test_menu_follows_knowledge_graph():
    ids = [m["id"] for m in json.loads(_read(MENU_CONFIG))["menus"]]
    assert ids.index("simulation") == ids.index("digitaltwin") + 1
    assert _menu()["route"] == "/simulation/"


def test_workbench_is_not_a_sidebar_item():
    items = [i["id"] for g in _menu()["groups"] for i in g["items"]]
    assert "workbench" not in items
    html = _read(PAGE)
    assert 'id="workbench-section"' not in html
    assert 'id="simScenariosList"' in html
    assert 'id="simScenariosEditor"' in html
    wb = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    js = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert 'id="simWbPurgeBtn"' in wb
    assert 'id="simWbClearBtn"' not in wb
    assert "</i>Purge" in wb
    assert "simWbPurgeBtn" in js
    assert "purgeScenario" in js
    assert 'id="results-section"' not in html
    nav = _read(FRONT / "static/global/js/sidebar-nav.js")
    assert "_showSection: function" in nav
    assert "getElementById(target + '-section')" in nav


def test_every_menu_item_has_a_section_and_one_is_active():
    html = _read(PAGE)
    items = [i for g in _menu()["groups"] for i in g["items"]]
    for item in items:
        assert f'id="{item["id"]}-section"' in html, item["id"]
    assert len(re.findall(r'class="sidebar-section active"', html)) == 1
    default = next(i for i in items if i.get("default"))
    assert f'id="{default["id"]}-section" class="sidebar-section active"' in html


def test_partials_and_assets_exist():
    html = _read(PAGE)
    for partial in re.findall(r'include "(partials/simulation/[^"]+)"', html):
        assert (FRONT / "templates" / partial).is_file(), partial
    for asset in re.findall(r"filename='(simulation/[^']+)'", html):
        assert (FRONT / "static" / asset).is_file(), asset


def test_no_inline_scripts_or_styles():
    html = _read(PAGE)
    for partial in (FRONT / "templates/partials/simulation").glob("*.html"):
        html += _read(partial)
    scripts = re.findall(r"<script\b([^>]*)>", html)
    assert all("src=" in attrs or 'type="application/json"' in attrs for attrs in scripts)
    assert "style=" not in html
    assert "onclick=" not in html


def test_js_uses_shared_notifications_not_alert():
    for js in (FRONT / "static/simulation/js").glob("*.js"):
        src = _read(js)
        assert "alert(" not in src and "confirm(" not in src.replace("showDeleteConfirm(", "")


def test_run_registers_with_background_tasks():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    assert "refreshTasks()" in js
    assert "applyScenarioView" in js
    assert "syncScenarioUrl" in js
    assert "ontobricks_simulation_open" in js
    assert "switchTo('workbench')" not in js


def test_run_progress_matches_build_bar_without_results_cta():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert 'id="simWbViewResultsBtn"' not in html
    assert "ob-progress-md" in html
    assert "progress-bar-striped" in html
    assert 'id="simWbProgressMsg"' in html
    assert "#simWbProgress .progress.ob-progress-md" in css
    assert "this.showResults(simId)" not in js
    assert "tick()" in js


def test_entity_panel_links_to_workbench():
    js = _read(FRONT / "static/query/js/query-entity-details.js")
    assert "/simulation/?section=scenarios" in js
    assert "entity=" in js
    assert "&label=" in js
    assert "Simulate from here" in js
    assert "getDisplayLabel(entity)" in js


def test_hypothetical_facts_show_id_type_and_predicate_kind():
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    page = _read(FRONT / "static/simulation/js/simulation.js")
    wizard = _read(FRONT / "static/simulation/js/simulation-fact-wizard.js")
    css = _read(FRONT / "static/simulation/css/simulation.css")
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    assert "_termCell" in wb
    assert "sim-term-caption" in wb
    assert "(${esc(caption)})" in wb
    assert "sim-term-kind" in css
    assert "display: flex" in css.split(".sim-term {", 1)[1].split("}", 1)[0]
    assert "justify-content: flex-start" in css.split(".sim-term {", 1)[1].split("}", 1)[0]
    assert "margin-bottom: 0" in css.split(".sim-term-kind {", 1)[1].split("}", 1)[0]
    assert "flex: 0 1 auto" in css.split(".sim-term-kind {", 1)[1].split("}", 1)[0]
    assert "flex: 1" not in css.split(".sim-term-kind {", 1)[1].split("}", 1)[0]
    assert "shortUri(uri)" in wb
    assert "Relationship" in wb
    assert "Attribute" in wb
    assert "predicate_kind" in wb
    assert "sim-term-emoji" in wb
    assert "subject_type" in wb
    assert 'id="simWbPatchesBody"' in html
    assert "sim-patch-card" in wb
    assert "col-lg-4" in wb
    assert "sim-patch-fields" in wb
    assert "align-items-center" in wb
    assert "align-items-start" not in wb.split("sim-patch-card", 1)[1].split("_termCell", 1)[0]
    assert "sim-term-field-label" not in wb
    assert "sim-patches-list" in html
    assert "gap-1" in html
    assert "sim-patches-table" not in html
    assert "table-sm" not in html.split('id="simWb-pane-facts"', 1)[1].split('id="simWb-pane-reasoning"', 1)[0]
    assert "padding: 0.35rem 0.5rem" in css.split(".sim-patch-card .card-body", 1)[1].split("}", 1)[0]
    assert "resolveEntitySeed" in page
    assert "_lookupInput" not in wb
    assert "_onPatchEdit" not in wb
    assert 'data-field="op"' not in wb
    assert "form-select" not in wb.split("renderPatches", 1)[1].split("_opIcon", 1)[0]
    card = wb.split("sim-patch-card", 1)[1].split(").join('')", 1)[0]
    assert 'data-edit="' in card
    assert "data-remove=" in card
    assert card.index("data-edit=") < card.index("data-remove=")
    assert "bi-plus-circle" in wb
    assert "bi-arrow-repeat" in wb
    assert "bi-dash-circle" in wb
    assert "bi-pencil" in wb
    assert 'class="form-control sim-term-id"' not in wb
    assert "on('simWbAddPatchBtn', 'click', () => this.openFactWizard())" in wb
    assert wizard.count('text-muted">${SimulationPage.esc(s.uri)}</div>') == 0
    assert "onclick=" not in html
    assert "style=" not in html


def test_hypothetical_facts_prompt_uses_agent():
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    pane = html.split('id="simWb-pane-facts"', 1)[1].split('id="simWb-pane-reasoning"', 1)[0]
    assert "simWbFactWizardBtn" not in pane
    assert pane.index('id="simWbAddPatchBtn"') < pane.index('id="simWbFactPrompt"')
    assert pane.index('id="simWbFactPrompt"') < pane.index('id="simWbFactPromptClearBtn"')
    assert pane.index('id="simWbFactPromptClearBtn"') < pane.index('id="simWbFactPromptBtn"')
    assert pane.index('id="simWbFactPromptBtn"') < pane.index('id="simWbPatchesBody"')
    assert 'id="simWbPatchSelectAll"' in pane
    assert 'id="simWbPatchRemoveSelectedBtn"' in pane
    assert pane.index('id="simWbPatchSelectAll"') < pane.index('id="simWbAddPatchBtn"')
    assert "data-select=" in wb
    assert "_removeSelectedPatches" in wb
    assert "simWbFactPromptClearBtn" in wb
    assert "flex-sm-row" in pane
    assert "justify-content-end mt-2" not in pane
    assert "proposeFacts" in wb
    propose = wb.split("proposeFacts", 1)[1].split("_persistLastFactPrompt", 1)[0]
    assert "this.patches.push" not in propose
    assert "_samePatchList" in wb
    assert "this.patches = merged.map" in propose
    assert "/simulation/facts/agent" in wb
    assert "/simulation/facts/agent/stream" in wb
    assert "simWbFactProgress" in pane
    assert "simWbFactProgressSteps" in pane
    assert "simWbFactProgressHide" in pane
    assert pane.index("simWbFactProgressHide") < pane.index("simWbFactProgressSteps")
    assert "_appendFactProgress" in wb
    assert "_hideFactProgress" in wb
    assert "simWbFactProgressHide" in wb
    assert "_factReplyHtml" in wb
    assert "renderMarkdown(" in wb
    assert "class=\"oc-md\"" in wb or "class='oc-md'" in wb
    pages_css = _read(FRONT / "static/global/css/pages.css")
    assert ".success-toast-popup:has(.oc-md)" in pages_css
    assert ".success-toast-popup .oc-md blockquote" in pages_css
    sim_css = _read(FRONT / "static/simulation/css/simulation.css")
    steps_css = sim_css.split(".sim-fact-progress-steps {", 1)[1].split("}", 1)[0]
    assert "overflow-y: auto" in steps_css
    assert "max-height:" in steps_css
    head_css = sim_css.split(".sim-fact-progress-head {", 1)[1].split("}", 1)[0]
    assert "justify-content: flex-end" in head_css
    utils_js = _read(FRONT / "static/global/js/utils.js")
    assert "fade show d-flex align-items-start" in utils_js
    assert "description: this._val('simWbDescription')" not in wb.split("proposeFacts", 1)[1].split("_persistLastFactPrompt", 1)[0]
    assert "this._set('simWbDescription', data.description)" not in wb
    assert "fact_prompt: this._val('simWbFactPrompt')" in wb
    assert "this._set('simWbFactPrompt', spec.fact_prompt || '')" in wb
    assert "_persistLastFactPrompt" in wb
    assert "onclick=" not in html
    assert "style=" not in html


def test_workbench_prefers_entity_label_over_local_id():
    js = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert "seedFromEntity" in js
    assert "What if: ${SimulationPage.shortUri(entity)}" not in js
    assert "_applyFocusLabel" in js
    page = _read(FRONT / "static/simulation/js/simulation.js")
    assert "resolveEntityLabel" in page
    assert "uriCell(s.focus, s.focus_label)" in page


def test_scenarios_list_shows_spinner_while_loading():
    html = _read(FRONT / "templates/partials/simulation/_sim_scenarios.html")
    js = _read(FRONT / "static/simulation/js/simulation.js")
    assert "spinner-border spinner-border-sm" in html
    assert "Loading scenarios" in html
    assert "_loadingBlock" in js
    assert "Loading scenarios…" in js


def test_scenarios_list_uses_data_quality_card_style():
    html = _read(FRONT / "templates/partials/simulation/_sim_scenarios.html")
    js = _read(FRONT / "static/simulation/js/simulation.js")
    css = _read(FRONT / "static/global/css/main.css")
    assert 'id="simScenariosBody"' in html
    assert 'id="simScenariosTable"' not in html
    assert "sim-scenarios-list" in html
    assert "_renderScenarioCard" in js
    assert "ob-rule-card" in js
    assert "sim-scenario-card" in js
    assert "small fw-medium" in js
    assert ".ob-rule-card" in css
    assert "<table" not in html
    assert "<tr>" not in js.split("renderScenarios()", 1)[1].split("openScenario(", 1)[0]


def test_scenario_card_edit_action_uses_pencil_icon():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    card = js.split("_renderScenarioCard(s) {", 1)[1].split("openScenario(", 1)[0]
    edit = card.split('data-action="open"', 1)[1].split("</button>", 1)[0]
    assert 'title="Edit"' in edit
    assert "bi-pencil" in edit


def test_workbench_picks_individual_inference_rules():
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    page = _read(PAGE)
    js = _read(FRONT / "static/simulation/js/simulation-rules.js")
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert 'tree_id="simWbPhases"' in html
    assert "partials/layout/_inference_phase_tree.html" in html
    assert "query-reasoning.css" in page
    assert page.index("global/js/phase-tree.js") < page.index("simulation/js/simulation-rules.js")
    assert "new PhaseTree(" in js
    assert "/ontology/if-then/list" in js
    assert "rule_names" in wb
    assert "collectRuleNames" in js
    assert "onclick=" not in html


def test_scenario_panes_use_direct_content_section_stretch():
    page = _read(PAGE)
    list_html = _read(FRONT / "templates/partials/simulation/_sim_scenarios.html")
    wb = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    section = page.split('id="scenarios-section"', 1)[1]
    assert re.search(
        r'id="simScenariosList"\s+class="[^"]*\bcontent-section\b',
        section,
    )
    assert re.search(
        r'id="simScenariosEditor"\s+class="[^"]*\bcontent-section\b',
        section,
    )
    assert not re.search(
        r'id="simScenariosList"[^>]*>\s*<div class="content-section"',
        section,
    )
    assert '<div class="content-section">' not in list_html
    assert '<div class="content-section">' not in wb
    assert 'class="card h-100"' in list_html
    assert 'class="card h-100"' in wb
    assert "ob-tabs-wrap" in wb
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert ".sidebar-section > .content-section > #simWbProgress" in css
    assert "flex: 0 0 auto" in css.split(
        ".sidebar-section > .content-section > #simWbProgress {", 1
    )[1].split("}", 1)[0]


def test_workbench_uses_four_ob_tabs():
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    results = _read(FRONT / "templates/partials/simulation/_sim_results.html")
    js = _read(FRONT / "static/simulation/js/simulation.js")
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    res = _read(FRONT / "static/simulation/js/simulation-results.js")
    assert 'class="nav nav-tabs ob-tabs nav-fill"' in html
    assert 'id="simWbTabs"' in html
    assert 'id="simWb-pane-general"' in html
    assert 'id="simWb-pane-facts"' in html
    assert 'id="simWb-pane-reasoning"' in html
    assert 'id="simWb-pane-results"' in html
    assert "Hypothetical facts" in html
    assert "ob-tabs-wrap" in html
    assert "simResultsPicker" not in results
    assert "switchTo('results')" not in js
    assert "showTab" in wb
    assert "tab: 'results'" in js
    assert 'id="simWbBackBtn"' in html
    assert "Workbench.currentId" in res or "SimulationWorkbench.currentId" in res
    assert "onclick=" not in html


def test_workbench_tabs_show_loading_spinner():
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    rules = _read(FRONT / "static/simulation/js/simulation-rules.js")
    res = _read(FRONT / "static/simulation/js/simulation-results.js")
    css = _read(FRONT / "static/simulation/css/simulation.css")
    overlay = html.split('id="simWbTabLoading"', 1)[1]
    assert 'class="sim-tab-loading d-none"' in html
    assert "spinner-border" in overlay
    assert 'id="simWbTabLoadingLabel"' in overlay
    assert html.index('id="simWb-pane-general"') < html.index('id="simWbTabLoading"')
    loading_css = css.split(".sim-tab-loading", 1)[1].split("}", 1)[0]
    assert "z-index: 10" in loading_css
    assert "setTabBusy" in wb
    assert "setTabBusy('general'" in wb
    assert "setTabBusy('facts'" in wb
    assert "setTabBusy('reasoning'" in rules
    assert "setTabBusy('results'" in res
    assert ".sim-tab-loading" in css
    assert "onclick=" not in html
    assert "style=" not in html


def test_workbench_general_tab_stacks_sections():
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    js = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    page = _read(FRONT / "static/simulation/js/simulation.js")
    assert 'id="simWbGeneralStack"' in html
    assert "col-xl-4" not in html
    assert html.index("Scenario") < html.index("Scope") < html.index("Sandbox objects")
    assert "align-items-end" not in html
    assert "align-items-start" in html
    scope = html.split("Scope", 1)[1].split("Sandbox objects", 1)[0]
    assert scope.index('id="simWbFocusGroup"') < scope.index('id="simWbFocusCaption"')
    assert 'id="simWbArtifacts"' in html
    assert "sim-artifact-table" in js
    assert "paintArtifacts" in js
    assert "refreshArtifacts" in js
    assert "SimulationWorkbench.refreshArtifacts" in page
    assert "sim-artifact-hint" in js
    assert "sim-col-role" in js
    assert "Live inferred graph UNION the sandbox" in js
    assert "explorer_asserted" not in js
    assert "Private copy of the in-scope triples" in js
    assert "before_picture" in js
    assert "Before picture" in js
    assert "Private Neo4j graph of the in-scope triples" in js
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert ".sim-artifact-table.table-sm td" in css
    assert "white-space: nowrap" in css.split(".sim-col-role", 1)[1].split("}", 1)[0]
    assert "white-space: normal" in css.split(".sim-artifact-hint", 1)[1]
    assert "sim-artifact-name-cell" in js


def test_results_empty_rows_mention_rerun_when_listing_unavailable():
    res = _read(FRONT / "static/simulation/js/simulation-results.js")
    assert "listing_unavailable" in res
    assert "Re-run the scenario to list added and removed triples from the sandbox." in res


def test_workbench_save_aborts_stale_run():
    js = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert "/abort" in js
    assert "/is running/i.test" in js
    assert "_stopPolling" in js


def test_scenarios_list_has_purge_action():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    assert 'data-action="purge"' in js
    assert "purgeScenario" in js
    assert "/purge" in js
    assert "bi-eraser" in js
    assert "SimulationWorkbench.load" in js.split("async purgeScenario", 1)[1].split("async deleteScenario", 1)[0]


def test_open_scenario_does_not_bounce_to_list():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    assert "e.preventDefault()" in js
    assert 'href="#"' not in js
    assert "section=scenarios&simulation=" in js
    assert "if (section === 'scenarios' && !this._openingEditor) this.showList" not in js
    assert 'data-section="scenarios"' in js


def test_compare_section_is_a_scenarios_submenu_with_overlay_graph():
    scenarios = next(g for g in _menu()["groups"] if g["id"] == "simulation-scenarios")
    ids = [i["id"] for i in scenarios["items"]]
    assert ids.index("scenarios") < ids.index("compare") < ids.index("runs")
    html = _read(PAGE)
    partial = _read(FRONT / "templates/partials/simulation/_sim_compare.html")
    js = _read(FRONT / "static/simulation/js/simulation-compare.js")
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert 'id="compare-section" class="sidebar-section"' in html
    assert "partials/simulation/_sim_compare.html" in html
    assert "simulation/js/simulation-compare.js" in html
    for element in (
        "simCompareScenario",
        "simCompareSource",
        "simCompareRunBtn",
        "simCompareCanvas",
        "simCompareDetails",
    ):
        assert f'id="{element}"' in partial
    assert 'value="inferred"' in partial and 'value="asserted"' in partial
    assert "/simulation/${encodeURIComponent(id)}/compare" in js
    assert "include_inferred" in js
    assert "sidebarSectionChanged" in js
    assert "--db-status-success" in js and "--db-status-danger" in js
    assert "query-sigmagraph" not in js
    assert ".sim-compare-canvas" in css
    assert "var(--db-status-success)" in css


def test_compare_has_graphs_and_evaluate_ob_tabs():
    partial = _read(FRONT / "templates/partials/simulation/_sim_compare.html")
    assert 'class="nav nav-tabs ob-tabs nav-fill"' in partial
    assert 'class="card h-100"' in partial
    assert "card-body p-0 ob-tabs-wrap" in partial
    assert 'id="simCompareTabs"' in partial
    for tab, pane in (
        ("simCompare-tab-graphs", "simCompare-pane-graphs"),
        ("simCompare-tab-evaluate", "simCompare-pane-evaluate"),
    ):
        assert f'id="{tab}"' in partial
        assert f'id="{pane}"' in partial
        assert f'data-bs-target="#{pane}"' in partial
        assert f'aria-controls="{pane}"' in partial
    assert 'class="bi bi-columns-gap me-1"' in partial
    assert 'class="bi bi-chat-square-text me-1"' in partial
    assert "ob-tabs-wrap" in partial


def test_compare_evaluate_pane_has_prompt_and_two_side_by_side_outputs():
    partial = _read(FRONT / "templates/partials/simulation/_sim_compare.html")
    for element in (
        "simEvalPrompt",
        "simEvalRunBtn",
        "simEvalLiveOutput",
        "simEvalSandboxOutput",
        "simEvalLiveError",
        "simEvalSandboxError",
        "simEvalLiveSteps",
        "simEvalSandboxSteps",
        "simEvalLiveStatus",
        "simEvalSandboxStatus",
    ):
        assert f'id="{element}"' in partial, element
    assert partial.count('class="col-12 col-lg-6') == 2
    # Answers are rendered Markdown (shared .oc-md), not raw text areas.
    assert partial.count('class="oc-md sim-eval-answer"') == 3  # Live, Sandbox, Insights
    assert 'id="simEvalLiveOutput"' in partial and "<textarea" not in partial.split('id="simEvalHint"')[1]
    assert partial.count("Thinking process") == 3


def test_compare_evaluate_has_business_insights_pane_below_both_answers():
    partial = _read(FRONT / "templates/partials/simulation/_sim_compare.html")
    for element in (
        "simEvalInsightsOutput",
        "simEvalInsightsError",
        "simEvalInsightsSteps",
        "simEvalInsightsStatus",
    ):
        assert f'id="{element}"' in partial, element
    # Full-width block after the Live | Sandbox row, not inside it.
    assert partial.index('id="simEvalSandboxOutput"') < partial.index('id="simEvalInsights"')
    assert "bi-lightbulb" in partial
    assert partial.count('class="col-12 col-lg-6') == 2


def test_compare_graph_sources_sit_above_the_tabs():
    partial = _read(FRONT / "templates/partials/simulation/_sim_compare.html")
    above, tabs = partial.split('id="simCompareTabs"', 1)
    for element in ("simCompareScenario", "simCompareSource"):
        assert f'id="{element}"' in above, element
        assert f'id="{element}"' not in tabs, element
    assert 'class="sim-compare-sources' in above
    # Live graph comes first, Scenario second.
    assert above.index('id="simCompareSource"') < above.index('id="simCompareScenario"')
    graphs = tabs.split('id="simCompare-pane-evaluate"')[0]
    assert 'id="simCompareCanvas"' in graphs
    assert 'id="simCompareRunBtn"' in graphs
    assert "Comparison Setup" not in partial
    # Compare action and details live in the right-hand pane beside the canvas.
    assert "sim-compare-right-pane" in graphs
    assert "sim-compare-bottom-pane" not in partial
    assert graphs.index('id="simCompareCanvas"') < graphs.index('id="simCompareRunBtn"')


def test_evaluate_asks_both_graphs_statelessly_via_graph_chat():
    js = _read(FRONT / "static/simulation/js/simulation-evaluate.js")
    html = _read(PAGE)
    assert "simulation/js/simulation-evaluate.js" in html
    assert "'/dtwin/assistant/chat/stream'" in js
    assert "persist: false" in js
    assert "history: []" in js
    assert "include_inferred: includeInferred, simulation_id: ''" in js
    assert "include_inferred: true, simulation_id: scenario.id" in js
    assert "SimulationCompare._includeInferred()" in js
    # Live agent steps are streamed; replies go through the sanitising renderer.
    assert "event.type === 'step'" in js
    assert "step_type === 'tool_call'" in js and "step_type === 'tool_result'" in js
    assert "renderMarkdown(text)" in js
    assert "innerHTML = text" not in js
    # Titles name the live source and the scenario, not "Sandbox".
    assert "_liveLabel()" in js and "scenario.label" in js
    # Enter submits the prompt; Shift+Enter keeps a newline.
    assert "e.key === 'Enter' && !e.shiftKey" in js
    assert "SimulationEvaluate.refresh()" in _read(FRONT / "static/simulation/js/simulation-compare.js")


def test_evaluate_runs_insights_only_when_both_answers_succeed():
    js = _read(FRONT / "static/simulation/js/simulation-evaluate.js")
    assert "'/dtwin/assistant/insights/stream'" in js
    assert "live.ok && sandbox.ok" in js
    assert "Insights need both answers." in js
    # Insights reuse the SSE client and the shared step/finish renderers.
    assert "this._sse(" in js
    assert "this._step('insights', event)" in js
    assert "this._finish('insights'" in js
    # Ask both stays busy until the insights run settles.
    assert "if (seq === this._seq) this._setBusy(false);" in js
    assert "innerHTML = text" not in js


def test_evaluate_report_modal_is_included_on_the_simulation_page():
    html = _read(PAGE)
    modal = _read(FRONT / "templates/partials/simulation/_sim_insights_report_modal.html")
    assert 'partials/simulation/_sim_insights_report_modal.html' in html
    assert 'id="simEvalReportModal"' in modal
    assert 'id="simEvalReportBody"' in modal
    assert 'id="simEvalReportPrintBtn"' in modal
    assert 'data-bs-dismiss="modal"' in modal
    assert 'modal-xl' in modal
    assert 'bi-file-earmark-pdf' in modal


def test_evaluate_opens_a_printable_report_after_insights_succeed():
    partial = _read(FRONT / "templates/partials/simulation/_sim_compare.html")
    js = _read(FRONT / "static/simulation/js/simulation-evaluate.js")
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert 'id="simEvalReportBtn"' in partial
    assert "disabled" in partial.split('id="simEvalReportBtn"', 1)[1][:80]
    assert "bootstrap.Modal.getOrCreateInstance" in js
    assert "window.print()" in js
    assert "/simulation/${encodeURIComponent(" in js
    assert "SimulationCompare.data" in js
    assert "sim-eval-report-printing" in css
    assert "@media print" in css
    assert "Hypothetical facts" in js
    assert "Graph comparison" in js
    assert "Analysis" in js


def test_evaluate_report_uses_compare_graph_snapshot_cached_after_refresh():
    """Sigma WebGL canvases clear after composite (preserveDrawingBuffer:false).

    The report opens from the Evaluate tab while the Graphs pane is
    ``display:none``, so a live ``drawImage`` of those canvases yields an
    empty square. Capture a PNG in the same turn as ``renderer.refresh()``
    while the Graphs pane is visible, then reuse that snapshot in the report.
    """
    compare = _read(FRONT / "static/simulation/js/simulation-compare.js")
    evaluate = _read(FRONT / "static/simulation/js/simulation-evaluate.js")

    assert "lastGraphPng:" in compare or "lastGraphPng =" in compare
    assert "_snapshotGraphPng" in compare
    assert "this.renderer.refresh()" in compare
    assert "this._snapshotGraphPng()" in compare
    assert "this.lastGraphPng = ''" in compare or 'this.lastGraphPng = ""' in compare

    snapshot = compare.split("_snapshotGraphPng() {", 1)[1].split("\n    },", 1)[0]
    # Hidden pane renders at 1x1 — must not overwrite the good snapshot.
    assert "container.offsetWidth < 2" in snapshot
    assert "container.offsetHeight < 2" in snapshot
    assert "return this.lastGraphPng;" in snapshot
    # Synchronous refresh in the same turn as the WebGL read.
    assert snapshot.index("this.renderer.refresh()") < snapshot.index("drawImage")
    assert "c.width > 1 && c.height > 1" in snapshot

    assert "SimulationCompare.lastGraphPng" in evaluate
    # Must not re-read hidden WebGL canvases as the primary source.
    graph_png = evaluate.split("_graphPng()", 1)[1].split("_mdColumn", 1)[0]
    assert "querySelectorAll('canvas')" not in graph_png
    assert "lastGraphPng" in graph_png


def test_evaluate_css_fills_the_tab_height():
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert "#simCompareTabContent > .tab-pane.active" in css
    assert ".sim-eval-output" in css
