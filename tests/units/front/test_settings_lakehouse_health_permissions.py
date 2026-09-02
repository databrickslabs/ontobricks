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
