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

    def test_hydrate_restores_password_when_input_enabled(self, settings_js):
        idx = settings_js.find("function applyNeo4jFormFromConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 2000]
        assert "neo4jPassword" in body
        assert "o.password" in body
        assert "!pwdEl.disabled" in body

    def test_merge_keeps_password_when_field_blank(self, settings_js):
        idx = settings_js.find("function mergeNeo4jPanelIntoConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 2800]
        assert "previously persisted password" in body
        assert "if (pwd)" in body and "o.password = pwd" in body
        assert "delete o.password" in body

    def test_merge_writes_neo4j_bucket(self, settings_js):
        idx = settings_js.find("function mergeNeo4jPanelIntoConfigTextarea")
        assert idx >= 0
        body = settings_js[idx : idx + 2800]
        assert "root.neo4j" in body
        assert "o.database = database" in body or "o.database = database || 'neo4j'" in body
        assert "writeEngineConfigRoot" in body
        assert "normalizeEngineConfigRoot" in settings_js
