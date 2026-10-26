"""Explorer can switch the graph source to a ready simulation sandbox."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
RESULTS_HTML = REPO_ROOT / "src/front/templates/partials/simulation/_sim_results.html"
RESULTS_JS = REPO_ROOT / "src/front/static/simulation/js/simulation-results.js"


def test_explorer_header_has_simulation_select() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    header = html.split('<div class="section-header', 1)[1].split("visualization-layout", 1)[0]
    assert 'id="sgSimulationSelect"' in header
    assert "Live graph" in header
    assert 'data-sg-change="onSimulationChange"' in header
    assert 'id="sgSimulationBanner"' in html
    assert "unioned with" in html


def test_filter_and_neighbors_pass_simulation_id() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "function _withGraphSource(" in js
    assert "phase: 'sandbox'" not in js
    assert "inferred.disabled" not in js
    assert "simulation_id=" in js
    assert "onSimulationChange:" in js
    assert "loadSimulations: function" in js
    assert "_loadSimulationSelect({ autoLoad: false })" not in js
    sync = SYNC_JS.read_text(encoding="utf-8")
    assert "simulation_id: simulationId" in sync


def test_simulation_select_uses_public_api_from_dom_ready() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    marker = "Replaces inline onclick/onchange/oninput handlers"
    after = js.split(marker, 1)[1]
    assert "_loadSimulationSelect(" not in after
    assert "SigmaGraph.loadSimulations()" in after


def test_results_open_in_explorer() -> None:
    html = RESULTS_HTML.read_text(encoding="utf-8")
    assert 'id="simResultsExplorerBtn"' in html
    js = RESULTS_JS.read_text(encoding="utf-8")
    assert "section=sigmagraph&simulation=" in js
