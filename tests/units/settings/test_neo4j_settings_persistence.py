"""Contract tests: Neo4j Settings form persists via registry graph_engine_config."""

from __future__ import annotations

import pytest

SETTINGS_JS = "/static/config/js/settings.js"


def _static(client, path: str) -> str:
    resp = client.get(path)
    assert resp.status_code == 200, f"GET {path} returned {resp.status_code}"
    return resp.text


@pytest.fixture
def settings_js(client) -> str:
    return _static(client, SETTINGS_JS)


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
        # Must not gate Neo4j merge solely on the Lakebase/Delta heavy-load flag.
        merge_idx = body.find("mergeNeo4jPanelIntoConfigTextarea()")
        gated = body.rfind("if (graphDbHeavyLoaded)", 0, merge_idx)
        assert gated < 0, "Neo4j merge must run even when graphDbHeavyLoaded is false"

    def test_neo4j_section_triggers_config_load(self, settings_js):
        assert "s === 'neo4j'" in settings_js or "s !== 'neo4j'" in settings_js
        # Opening the Neo4j sidebar section must load/hydrate engine config.
        idx = settings_js.find("sidebarSectionChanged")
        assert idx >= 0
        window = settings_js[idx : idx + 1800]
        assert "neo4j" in window
        assert "loadGraphEngineConfig" in window

    def test_hydrate_loads_secret_scopes_and_never_touches_password(self, settings_js):
        idx = settings_js.find("function applyNeo4jFormFromConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 1200]
        assert "neo4jUsername" in body
        assert "loadNeo4jSecretScopes" in body
        assert "neo4jPassword" not in body

    def test_merge_always_forces_databricks_secret_and_drops_password(self, settings_js):
        idx = settings_js.find("function mergeNeo4jPanelIntoConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 1500]
        assert "o.auth_method = 'databricks_secret'" in body
        assert "delete o.password" in body
        assert "neo4jSecretScope" in body and "neo4jSecretKey" in body

    def test_secret_scope_and_key_dropdowns_are_populated_live(self, settings_js):
        assert "async function loadNeo4jSecretScopes" in settings_js
        assert "async function loadNeo4jSecretKeys" in settings_js
        assert "/settings/graph-engine/neo4j-secret-scopes" in settings_js
        assert "/settings/graph-engine/neo4j-secret-keys" in settings_js

    def test_merge_writes_neo4j_bucket(self, settings_js):
        idx = settings_js.find("function mergeNeo4jPanelIntoConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 2800]
        assert "root.neo4j" in body
        assert "o.database = database" in body or "o.database = database || 'neo4j'" in body
        assert "writeEngineConfigRoot" in body
        assert "normalizeEngineConfigRoot" in settings_js
