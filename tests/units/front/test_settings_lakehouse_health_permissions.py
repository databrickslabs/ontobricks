"""UI contracts for Settings Lakehouse permission-health and tab rails."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_HTML = REPO_ROOT / "src/front/templates/settings.html"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, signature: str, span: int = 5200) -> str:
    start = source.index(signature)
    return source[start : start + span]


def _health_tab_block() -> str:
    html = _read(SETTINGS_HTML)
    anchor = html.index('id="dtpane-health" role="tabpanel"')
    return html[anchor - 500 : anchor + 1200]


class TestLakehouseHealthCopyAndRenderer:
    def test_health_copy_describes_registry_permission_probe_only(self):
        block = _health_tab_block()
        normalized = block.lower()
        assert "effective" in normalized
        assert "permissions" in normalized
        assert "<code>catalog.schema</code>" in block
        for forbidden in (
            "active domain",
            "r2rml view",
            "_data",
            "assets",
            "row count",
            "row counts",
            "triples:",
        ):
            assert forbidden not in normalized

    def test_renderer_reads_permission_shape_and_not_legacy_asset_fields(self):
        body = _function_body(
            _read(SETTINGS_JS), "async function loadDeltaTripleStoreHealth(options)"
        )
        for expected in (
            "permissions",
            "principal",
            "operational",
            "inherited_from",
            "registry_catalog",
            "registry_schema",
            "error",
        ):
            assert expected in body
        for obsolete in (
            "active_domain",
            "view_fqn",
            "data_table_fqn",
            "inferred_table_fqn",
            "materialization",
            "warehouse_id",
        ):
            assert obsolete not in body

    def test_renderer_announces_status_with_icon_and_text(self):
        body = _function_body(
            _read(SETTINGS_JS), "async function loadDeltaTripleStoreHealth(options)"
        )
        assert "bi-check-circle" in body
        assert "bi-x-circle" in body
        assert "Operational" in body
        assert "Missing permissions" in body

    def test_health_response_checks_http_status_before_json_parsing(self):
        body = _function_body(
            _read(SETTINGS_JS), "async function loadDeltaTripleStoreHealth(options)"
        )
        assert "if (!resp.ok)" in body
        assert "const data = await resp.json();" in body
        assert "throw new Error(" in body

    def test_health_result_container_is_live_status_region(self):
        block = _health_tab_block()
        assert 'id="deltaHealthResult"' in block
        assert 'role="status"' in block
        assert 'aria-live="polite"' in block


class TestSettingsCanonicalTabRails:
    @pytest.mark.parametrize(
        ("tabs_id", "content_id"),
        [
            ("deltaTabs", "deltaTabContent"),
            ("lakebaseTabs", "lakebaseTabContent"),
            ("neo4jTabs", "neo4jTabContent"),
        ],
    )
    def test_tab_group_uses_card_integrated_canonical_hierarchy(self, tabs_id, content_id):
        html = _read(SETTINGS_HTML)
        pattern = re.compile(
            rf'<div class="card h-100">\s*'
            rf'<div class="card-body p-0 ob-tabs-wrap">[\s\S]*?'
            rf'<ul class="nav nav-tabs ob-tabs nav-fill" id="{tabs_id}" role="tablist">',
            re.DOTALL,
        )
        assert pattern.search(html), tabs_id

        content_pattern = re.compile(
            rf'<div class="tab-content p-3" id="{content_id}">',
            re.DOTALL,
        )
        assert content_pattern.search(html), content_id

    @pytest.mark.parametrize("content_id", ["deltaTabContent", "lakebaseTabContent", "neo4jTabContent"])
    def test_tab_content_surfaces_do_not_use_ob_tab_content_class(self, content_id):
        html = _read(SETTINGS_HTML)
        match = re.search(rf'<div class="([^"]+)" id="{content_id}">', html)
        assert match, content_id
        assert "ob-tab-content" not in match.group(1)


class TestSettingsTabRailScrollBehavior:
    def test_each_settings_tab_rail_registers_bootstrap_tab_activation_listener(self):
        js = _read(SETTINGS_JS)
        assert "['deltaTabs', 'lakebaseTabs', 'neo4jTabs']" in js
        assert "const rail = document.getElementById(tabsId);" in js
        assert "rail.addEventListener('shown.bs.tab'" in js

    def test_selected_tab_is_scrolled_with_nearest_inline_behavior(self):
        js = _read(SETTINGS_JS)
        assert "target.scrollIntoView({" in js
        assert "inline: 'nearest'" in js
        assert "block: 'nearest'" in js

    def test_focusing_a_tab_scrolls_it_into_the_rail_viewport(self):
        js = _read(SETTINGS_JS)
        assert "rail.addEventListener('focusin'" in js
        assert "sourceTarget.closest('[role=\"tab\"]')" in js
