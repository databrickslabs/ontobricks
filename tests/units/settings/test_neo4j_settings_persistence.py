"""Contract tests: Neo4j Settings form persists via registry graph_engine_config."""

from __future__ import annotations

from pathlib import Path

import pytest

SETTINGS_JS = "/static/config/js/settings.js"
SETTINGS_TEMPLATE = (
    Path(__file__).resolve().parents[3] / "src/front/templates/settings.html"
)


def _static(client, path: str) -> str:
    resp = client.get(path)
    assert resp.status_code == 200, f"GET {path} returned {resp.status_code}"
    return resp.text


@pytest.fixture
def settings_js(client) -> str:
    return _static(client, SETTINGS_JS)


@pytest.fixture
def settings_template() -> str:
    return SETTINGS_TEMPLATE.read_text(encoding="utf-8")


class TestNeo4jSettingsPersistenceWiring:
    def test_defines_apply_neo4j_form_from_config(self, settings_js):
        assert "function applyNeo4jFormFromConfigTextarea" in settings_js

    def test_load_graph_engine_config_hydrates_neo4j_form(self, settings_js):
        idx = settings_js.find("async function loadGraphEngineConfig")
        assert idx >= 0
        body = settings_js[idx : idx + 900]
        assert "applyNeo4jFormFromConfigTextarea" in body

    def test_save_always_merges_neo4j_panel(self, settings_js):
        idx = settings_js.find("async function saveGraphDbSettings")
        assert idx >= 0
        body = settings_js[idx : idx + 1200]
        assert "mergeNeo4jPanelIntoConfigTextarea()" in body
        merge_idx = body.find("mergeNeo4jPanelIntoConfigTextarea()")
        gated = body.rfind("if (graphDbHeavyLoaded)", 0, merge_idx)
        assert gated < 0, "Neo4j merge must run even when graphDbHeavyLoaded is false"

    def test_neo4j_section_triggers_config_load(self, settings_js):
        assert "s === 'neo4j'" in settings_js or "s !== 'neo4j'" in settings_js
        idx = settings_js.find("sidebarSectionChanged")
        assert idx >= 0
        window = settings_js[idx : idx + 1800]
        assert "neo4j" in window
        assert "loadGraphEngineConfig" in window

    def test_hydrate_loads_connections_and_never_touches_password(self, settings_js):
        idx = settings_js.find("function applyNeo4jFormFromConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 2000]
        assert "connections" in body
        assert "fillNeo4jDetailForm" in body or "selectNeo4jConnection" in body
        assert "neo4jPassword" not in body
        assert "function fillNeo4jDetailForm" in settings_js
        assert "neo4jUsername" in settings_js

    def test_merge_writes_connections_array(self, settings_js):
        idx = settings_js.find("function mergeNeo4jPanelIntoConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 2000]
        assert "auth_method: 'databricks_secret'" in body or "auth_method: \"databricks_secret\"" in body
        assert "connections" in body
        assert "root.neo4j" in body
        assert "writeEngineConfigRoot" in body

    def test_secret_scope_and_key_dropdowns_are_populated_live(self, settings_js):
        assert "async function loadNeo4jSecretScopes" in settings_js
        assert "async function loadNeo4jSecretKeys" in settings_js
        assert "/settings/graph-engine/neo4j-secret-scopes" in settings_js
        assert "/settings/graph-engine/neo4j-secret-keys" in settings_js

    def test_scope_dropdown_reselects_per_connection(self, settings_js):
        """Caching the scope list must not skip re-selecting the profile's scope."""
        idx = settings_js.find("async function loadNeo4jSecretScopes")
        assert idx >= 0
        body = settings_js[idx : idx + 1200]
        assert "neo4jSecretScopesLoaded" not in body
        assert "_populateSelectOptions" in body
        assert "getSelectedNeo4jConnection" in body

    def test_key_dropdown_reselects_per_connection(self, settings_js):
        idx = settings_js.find("async function loadNeo4jSecretKeys")
        assert idx >= 0
        body = settings_js[idx : idx + 1400]
        assert "getSelectedNeo4jConnection" in body
        assert "_populateSelectOptions" in body

    def test_form_read_keeps_stored_secret_while_dropdowns_hydrate(self, settings_js):
        """The selects read empty mid-hydration; reads must not wipe scope/key."""
        idx = settings_js.find("function readNeo4jDetailForm")
        assert idx >= 0
        body = settings_js[idx : idx + 1200]
        assert "_neo4jSecretHydrating" in body
        assert "stored.secret_scope" in body
        assert "stored.secret_key" in body

    def test_master_detail_helpers_exist(self, settings_js):
        assert "function renderNeo4jConnectionList" in settings_js
        assert "function addNeo4jConnection" in settings_js
        assert "function deleteSelectedNeo4jConnection" in settings_js
        assert "btnAddNeo4jConnection" in settings_js

    def test_test_connection_is_grouped_with_selected_connection_actions(
        self, settings_template
    ):
        detail_idx = settings_template.find('id="neo4jDetailTitle"')
        test_idx = settings_template.find('id="btnTestNeo4jConnection"')
        delete_idx = settings_template.find('id="btnDeleteNeo4jConnection"')
        assert detail_idx >= 0
        assert detail_idx < test_idx < delete_idx
        assert test_idx - detail_idx < 800

    def test_test_connection_disabled_without_selection(self, settings_js):
        idx = settings_js.find("function updateNeo4jSelectionHints")
        assert idx >= 0
        body = settings_js[idx : idx + 900]
        assert "btnTestNeo4jConnection" in body
        assert "test.disabled = _neo4jSelectedIdx < 0" in body

    def test_objects_tab_has_connection_dropdown(self, settings_template):
        assert 'id="neo4jObjectsConnection"' in settings_template
        assert 'id="n4tab-health"' not in settings_template
        assert 'id="n4pane-health"' not in settings_template

    def test_objects_uses_dropdown_connection_name(self, settings_js):
        assert "function syncNeo4jObjectsConnectionSelect" in settings_js
        assert "function objectsNeo4jConnectionName" in settings_js
        idx = settings_js.find("async function loadNeo4jLabels")
        assert idx >= 0
        body = settings_js[idx : idx + 1200]
        assert "objectsNeo4jConnectionName()" in body
        assert "function loadNeo4jHealth" not in settings_js
        assert "n4tab-health" not in settings_js
