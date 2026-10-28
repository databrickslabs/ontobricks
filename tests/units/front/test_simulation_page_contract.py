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
    assert 'id="simWbClearBtn"' in wb
    assert "</i>Clear" in wb
    assert "simWbClearBtn" in js
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


def test_completed_run_keeps_view_results_cta():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    assert 'id="simWbViewResultsBtn"' in html
    assert "keepProgress: true" in js
    assert "this.showResults(simId)" in js
    assert "tick()" in js


def test_entity_panel_links_to_workbench():
    js = _read(FRONT / "static/query/js/query-entity-details.js")
    assert "/simulation/?section=scenarios" in js
    assert "entity=" in js
    assert "&label=" in js
    assert "Simulate from here" in js
    assert "getDisplayLabel(entity)" in js


def test_hypothetical_facts_show_labels_not_full_uris():
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    page = _read(FRONT / "static/simulation/js/simulation.js")
    wizard = _read(FRONT / "static/simulation/js/simulation-fact-wizard.js")
    assert "termLabel" in page
    assert "SimulationPage.termLabel" in wb
    assert "_lookupInput('subject', i, p.subject" not in wb
    assert "subject_label" in wb
    assert wizard.count('text-muted">${SimulationPage.esc(s.uri)}</div>') == 0


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
    assert "_loadingRow" in js
    assert "Loading scenarios…" in js


def test_workbench_picks_individual_inference_rules():
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    page = _read(PAGE)
    js = _read(FRONT / "static/simulation/js/simulation-rules.js")
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert 'data-sim-rulelist="swrl"' in html
    assert 'data-sim-expand="swrl"' in html
    assert "query-reasoning.css" in page
    assert "simulation/js/simulation-rules.js" in page
    assert "/ontology/swrl/list" in js
    assert "rule_names" in wb
    assert "collectRuleNames" in js
    assert "onclick=" not in html


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
    assert "Live graph with inferred triples" in js
    assert "Live asserted graph only" in js
    assert "Private copy of the in-scope triples" in js
    assert "Private Neo4j graph of the in-scope triples" in js
    css = _read(FRONT / "static/simulation/css/simulation.css")
    assert ".sim-artifact-table.table-sm td" in css
    assert "white-space: nowrap" in css.split(".sim-col-role", 1)[1].split("}", 1)[0]
    assert "white-space: normal" in css.split(".sim-artifact-hint", 1)[1]
    assert "sim-artifact-name-cell" in js


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


def test_open_scenario_does_not_bounce_to_list():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    assert "e.preventDefault()" in js
    assert 'href="#"' not in js
    assert "section=scenarios&simulation=" in js
    assert "if (section === 'scenarios' && !this._openingEditor) this.showList" not in js
    assert 'data-section="scenarios"' in js
