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
        "simulation-results",
    ]


def test_menu_follows_knowledge_graph():
    ids = [m["id"] for m in json.loads(_read(MENU_CONFIG))["menus"]]
    assert ids.index("simulation") == ids.index("digitaltwin") + 1
    assert _menu()["route"] == "/simulation/"


def test_workbench_is_not_a_sidebar_item():
    items = [i["id"] for g in _menu()["groups"] for i in g["items"]]
    assert "workbench" not in items
    html = _read(PAGE)
    assert 'id="workbench-section"' in html
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
    wb = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert "params.get('simulation')" in wb


def test_completed_run_keeps_view_results_cta():
    js = _read(FRONT / "static/simulation/js/simulation.js")
    html = _read(FRONT / "templates/partials/simulation/_sim_workbench.html")
    assert 'id="simWbViewResultsBtn"' in html
    assert "keepProgress: true" in js
    assert "this.showResults(simId)" in js
    assert "tick()" in js


def test_entity_panel_links_to_workbench():
    js = _read(FRONT / "static/query/js/query-entity-details.js")
    assert "/simulation/?section=workbench" in js
    assert "entity=" in js
    assert "&label=" in js
    assert "Simulate from here" in js
    assert "getDisplayLabel(entity)" in js


def test_workbench_prefers_entity_label_over_local_id():
    js = _read(FRONT / "static/simulation/js/simulation-workbench.js")
    assert "params.get('label')" in js
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
