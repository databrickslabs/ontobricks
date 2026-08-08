"""Contract tests for Knowledge Graph readiness indicator UI."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PARTIAL = REPO_ROOT / "src/front/templates/partials/dtwin/_kg_status_indicator.html"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
SYNC_CSS = REPO_ROOT / "src/front/static/query/css/query-sync.css"

# Sub-pages that must include the readiness indicator next to their title.
_KG_PAGES = [
    "src/front/templates/partials/dtwin/_query_sigmagraph.html",
    "src/front/templates/partials/dtwin/_query_dataquality.html",
    "src/front/templates/partials/dtwin/_query_chat.html",
    "src/front/templates/partials/dtwin/_query_insights.html",
    "src/front/templates/partials/dtwin/_query_reasoning.html",
    "src/front/templates/partials/dtwin/_query_cohorts.html",
    "src/front/templates/partials/dtwin/_query_graphql.html",
    "src/front/templates/partials/dtwin/_query_analytics.html",
]


def test_kg_status_partial_exposes_data_attribute():
    html = PARTIAL.read_text(encoding="utf-8")
    assert 'data-kg-status' in html
    assert "kg-status-indicator" in html


def test_kg_subpages_include_status_partial():
    for rel in _KG_PAGES:
        html = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert '_kg_status_indicator.html' in html, f"missing indicator include in {rel}"


def test_update_kg_ready_indicators_covers_three_states():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "function updateKgReadyIndicators()" in js
    assert "Building…" in js
    assert "Graph ready" in js
    assert "Graph not built" in js
    assert "kg-go-build-btn" in js


def test_backend_label_covers_lakehouse_neo4j_lakebase():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "function _kgBackendLabel()" in js
    assert "return 'Lakehouse'" in js
    assert "return 'Neo4j'" in js
    assert "return 'Lakebase'" in js


def test_backend_badges_include_matching_brand_icons():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "function _backendBrandIconClass(" in js
    assert "ob-icon-postgresql" in js
    assert "ob-icon-databricks" in js
    assert "ob-icon-neo4j" in js
    assert "function _kgBackendIconMarkup()" in js
    ready = js[js.index("Graph ready") : js.index("} else if (tripleStoreStatusUnknown)")]
    assert "_kgBackendIconMarkup()" in ready
    unknown = js[js.index("Status unavailable") : js.index("} else {", js.index("Status unavailable"))]
    assert "_kgBackendIconMarkup()" in unknown


def test_go_to_build_delegated_click_handler_present():
    js = SYNC_JS.read_text(encoding="utf-8")
    assert "kg-go-build-btn" in js
    assert 'data-section="sync"' in js


def test_kg_status_css_present():
    css = SYNC_CSS.read_text(encoding="utf-8")
    assert ".kg-status-indicator" in css
    assert ".kg-go-build-btn" in css


class TestAnUnreachableEngineIsNotAMissingGraph:
    """`has_data` is tri-state; null means "we could not check".

    Reporting that as "Graph not built" is what showed the badge on Analytics
    while the Build page listed the graph as present.
    """

    def test_the_indicator_has_a_distinct_unknown_state(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        assert "tripleStoreStatusUnknown" in js
        assert "Status unavailable" in js

    def test_the_unknown_state_is_reached_before_the_not_built_state(self):
        """Otherwise the `else` branch claims a missing graph first.

        Pins the whole condition, so a branch left in place but neutered (an
        added `false &&`, say) is still caught.
        """
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("function updateKgReadyIndicators()"):]
        body = body[: body.index("\n}")]
        branch = "} else if (tripleStoreStatusUnknown) {"
        assert branch in body
        assert body.index(branch) < body.index("Graph not built")

    def test_the_unknown_state_does_not_offer_a_rebuild(self):
        """A connection blip must not invite a needless rebuild."""
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("function updateKgReadyIndicators()"):]
        body = body[: body.index("\n}")]
        unknown_branch = body[body.index("tripleStoreStatusUnknown"):]
        unknown_branch = unknown_branch[: unknown_branch.index("} else {")]
        assert "kg-go-build-btn" not in unknown_branch

    def test_a_null_has_data_sets_unknown_rather_than_false(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        assert "has_data === null" in js

    def test_a_failed_status_request_is_also_unknown(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("async function checkTripleStoreStatus("):]
        body = body[: body.index("\n}")]
        catch_block = body[body.index("} catch (e) {"):]
        assert "tripleStoreStatusUnknown = true" in catch_block

    def test_the_status_area_does_not_call_an_unknown_graph_empty(self):
        """It used to fall through to "Graph is empty. Run Synchronize"."""
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("function renderTripleStoreStatus(data)"):]
        body = body[: body.index("\n}\n")]
        assert "data.has_data === null" in body
        assert body.index("data.has_data === null") < body.index("Graph is empty")

    def test_the_databricks_build_status_text_handles_unknown(self):
        js = (
            REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"
        ).read_text(encoding="utf-8")
        assert "ts.has_data === null" in js


class TestTheLiveProbeCorrectsTheCachedBadge:
    """`sync_info` serves the status from a 5-minute cache.

    `/dtwin/sync/dt-existence` is force-refreshed, so when it finds graph data the
    badge has to follow — previously the live answer only ever reached the Build
    page's artefact cards and every KG sub-page kept the stale one.
    """

    def test_the_live_probe_result_is_reconciled(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        assert "function _reconcileReadinessWithLiveProbe(" in js

    def test_it_runs_after_the_force_refreshed_fetch(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("async function _loadDtExistence()"):]
        body = body[: body.index("\n}")]
        assert "_reconcileReadinessWithLiveProbe(data)" in body

    def test_it_repaints_the_indicators(self):
        """Promoting the flag alone would leave the stale badge on screen."""
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("function _reconcileReadinessWithLiveProbe("):]
        body = body[: body.index("\n}\n")]
        assert "updateDataMenus()" in body

    def test_only_an_affirmative_live_result_promotes_readiness(self):
        """An unknown live probe must not manufacture a ready graph."""
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("function _reconcileReadinessWithLiveProbe("):]
        body = body[: body.index("\n}\n")]
        assert "graph_has_data !== true" in body

    def test_a_pending_skeleton_is_ignored(self):
        js = SYNC_JS.read_text(encoding="utf-8")
        body = js[js.index("function _reconcileReadinessWithLiveProbe("):]
        body = body[: body.index("\n}\n")]
        assert "pending" in body

    def test_only_a_definite_false_may_erase_the_existence_signal(self):
        """The old override fired on any falsy status, including a failed probe."""
        js = SYNC_JS.read_text(encoding="utf-8")
        assert "if (tsStatus.has_data === false && payload.dt_existence)" in js
        assert "if (!tripleStoreHasData && payload.dt_existence)" not in js
