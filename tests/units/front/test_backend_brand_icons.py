"""Backend selectors use local, color brand icons at Bootstrap-icon size."""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_MENU = Path("src/front/config/menu_config.json")
_SETTINGS = Path("src/front/templates/settings.html")
_CSS = Path("src/front/static/global/css/components.css")
_IMG = Path("src/front/static/global/img")
_BUILD = Path("src/front/templates/partials/dtwin/_query_sync.html")
_BUILD_JS = Path("src/front/static/query/js/query-sync.js")
_VALIDATION = Path("src/front/templates/partials/domain/_domain_validation.html")
_VALIDATION_JS = Path("src/front/static/domain/js/domain-validation.js")
_REGISTRY_CONFIG = Path("src/front/templates/partials/registry/_registry_configuration.html")

_EXPECTED = {
    "lakebase": ("ob-icon-postgresql", "lakebase-icon.svg"),
    "delta": ("ob-icon-databricks", "databricks-icon.svg"),
    "neo4j": ("ob-icon-neo4j", "neo4j-icon.svg"),
}


def _backend_items() -> dict[str, dict]:
    menus = json.loads(_MENU.read_text(encoding="utf-8"))["menus"]
    settings = next(menu for menu in menus if menu["id"] == "settings")
    backend = next(group for group in settings["groups"] if group["id"] == "settings-triplestore")
    return {item["id"]: item for item in backend["items"]}


def test_backend_menu_uses_brand_icon_classes():
    items = _backend_items()
    for item_id, (modifier, _) in _EXPECTED.items():
        assert items[item_id]["icon"] == f"ob-brand-icon {modifier}"


def test_backend_headers_match_menu_brand_icons():
    template = _SETTINGS.read_text(encoding="utf-8")
    expected_headers = {
        "Lakebase": "ob-icon-postgresql",
        "Lakehouse": "ob-icon-databricks",
        "Neo4j": "ob-icon-neo4j",
    }
    for label, modifier in expected_headers.items():
        assert f'<i class="ob-brand-icon {modifier} me-2"></i>{label}</h4>' in template


def test_brand_icons_are_local_color_svgs():
    for _, filename in _EXPECTED.values():
        svg = (_IMG / filename).read_text(encoding="utf-8")
        assert "<svg" in svg
        assert "#" in svg, f"{filename} must define a brand color"


def test_brand_icon_box_matches_text_icon_size():
    css = _CSS.read_text(encoding="utf-8")
    brand_rule = css[css.index(".ob-brand-icon {") : css.index("}", css.index(".ob-brand-icon {"))]
    assert "width: 1em;" in brand_rule
    assert "height: 1em;" in brand_rule
    assert "background-size: contain;" in brand_rule


def test_graph_backend_cards_are_brand_icon_aware():
    build = _BUILD.read_text(encoding="utf-8")
    validation = _VALIDATION.read_text(encoding="utf-8")
    assert (
        'id="dtGraphBackendIcon" '
        'class="ob-brand-icon ob-icon-postgresql dt-arch-icon-lakebase-img flex-shrink-0"'
        in build
    )
    assert (
        'id="psDtGraphBackendIcon" '
        'class="ob-brand-icon ob-icon-postgresql dt-arch-icon-lakebase-img flex-shrink-0"'
        in validation
    )

    build_js = _BUILD_JS.read_text(encoding="utf-8")
    validation_js = _VALIDATION_JS.read_text(encoding="utf-8")
    assert "dtGraphBackendIcon" in build_js
    assert "_setBackendBrandIcon(" in build_js
    assert "psDtGraphBackendIcon" in validation_js
    assert "_psSetBackendBrandIcon(" in validation_js


def test_registry_lakebase_card_uses_postgresql_icon():
    html = _REGISTRY_CONFIG.read_text(encoding="utf-8")
    assert (
        '<i class="ob-brand-icon ob-icon-postgresql me-1"></i> Lakebase Connection'
        in html
    )
