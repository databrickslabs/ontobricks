"""Help Center workflow copy must stay in sync with menu_config.json.

Regression guard for drift found during the 2026-08-05 Help Center review:
the Step-by-Step Workflow accordion referenced menu items ("Graph Viewer",
"GraphQL", Registry's "Schedule"/"API") that had been renamed or moved
(Explorer/Query, Settings) without the Help Center copy being updated, and
missed newer sections entirely (Chat, Cohorts, Analytics, Build/Runs,
Pitfalls). These tests lock in the current, correct labels.
"""

import html
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_HELP = Path("src/front/templates/partials/layout/help_modal.html")
_MENU = Path("src/front/config/menu_config.json")
_EXPLORER = Path("src/front/templates/partials/dtwin/_query_sigmagraph.html")
_QUERY_SHELL = Path("src/front/templates/partials/dtwin/_query_query.html")
_GRAPHQL = Path("src/front/templates/partials/dtwin/_query_graphql.html")
_SIGMA_JS = Path("src/front/static/query/js/query-sigmagraph.js")
_BUILD_LAKEBASE = Path("src/front/templates/partials/dtwin/_query_sync.html")
_BUILD_LAKEHOUSE = Path("src/front/templates/partials/dtwin/_query_databricks_build.html")
_SYNC_JS = Path("src/front/static/query/js/query-sync.js")
_ONTOLOGY_DESIGNER = Path("src/front/templates/partials/ontology/_ontology_map.html")
_MAPPING_DESIGNER = Path("src/front/templates/partials/mapping/_mapping_design.html")
_BUSINESS_VIEWS = Path("src/front/templates/partials/ontology/_ontology_design.html")
_ONTOLOGY_ENTITIES = Path("src/front/templates/partials/ontology/_ontology_entities.html")


def _help_html() -> str:
    return _HELP.read_text(encoding="utf-8")


def _menus():
    return {m["id"]: m for m in json.loads(_MENU.read_text(encoding="utf-8"))["menus"]}


def test_registry_workflow_step_matches_menu_icon():
    html = _help_html()
    icon = _menus()["registry"]["icon"]
    assert f'<i class="bi {icon} me-2"></i><strong>1. Registry</strong>' in html
    # Schedule/API/Registry Location live under Settings, not the Registry modal.
    assert "<strong>Schedule</strong>" not in html
    assert "<strong>Registry Location</strong>" not in html


def test_knowledge_graph_workflow_uses_current_item_labels():
    """digitaltwin-data items were renamed from Graph Viewer/GraphQL to Explorer/Query."""
    html = _help_html()
    digitaltwin = _menus()["digitaltwin"]
    all_items = {
        item["id"]: item["label"]
        for group in digitaltwin["groups"]
        for item in group["items"]
    }
    assert all_items["sigmagraph"] == "Explorer"
    assert all_items["graphql"] == "Query"

    hw5_start = html.index('id="hw5"')
    hw5_end = html.index("</div>\n                                </div>", hw5_start)
    hw5_body = html[hw5_start:hw5_end]

    assert "<strong>Explorer</strong>" in hw5_body
    assert "<strong>Query</strong>" in hw5_body
    # Every real digitaltwin item must be mentioned so the guide can't silently drift again.
    for label in all_items.values():
        assert label in hw5_body, f"Knowledge Graph workflow doc is missing '{label}'"


def test_explorer_uses_share_icon_consistently():
    digitaltwin = _menus()["digitaltwin"]
    navigation = next(group for group in digitaltwin["groups"] if group["id"] == "digitaltwin-data")
    explorer = next(item for item in navigation["items"] if item["id"] == "sigmagraph")

    assert explorer["icon"] == "bi-share"
    assert '<i class="bi bi-share me-2"></i>Graph Explorer' in _EXPLORER.read_text(
        encoding="utf-8"
    )
    assert '<div class="help-feature-icon"><i class="bi bi-share"></i></div>' in _help_html()


def test_kg_viewer_buttons_use_menu_icons():
    """Explorer canvas and Query-shell Ontology button reuse menu icons."""
    explorer_icon = "bi-share"
    ontology_icon = _menus()["ontology"]["icon"]

    sigma_js = _SIGMA_JS.read_text(encoding="utf-8")
    assert f'<i class="bi {explorer_icon}' in sigma_js
    assert "Graph Viewer" in sigma_js

    explorer_html = _EXPLORER.read_text(encoding="utf-8")
    assert (
        'data-sg-action="openOntologyViewer"' in explorer_html
        and f'<i class="bi {ontology_icon}"></i> Ontology' in explorer_html
    )

    query_shell_html = _QUERY_SHELL.read_text(encoding="utf-8")
    assert (
        'data-query-action="ontology"' in query_shell_html
        and f'<i class="bi {ontology_icon}"></i> Ontology' in query_shell_html
    )

    graphql_html = _GRAPHQL.read_text(encoding="utf-8")
    assert 'id="graphqlOpenNewTab"' in graphql_html
    assert "data-query-action=\"ontology\"" not in graphql_html


def test_build_uses_fast_forward_icon_consistently():
    digitaltwin = _menus()["digitaltwin"]
    management = next(
        group for group in digitaltwin["groups"] if group["id"] == "digitaltwin-management"
    )
    build = next(item for item in management["items"] if item["id"] == "sync")

    assert build["icon"] == "bi-fast-forward"
    for template_path in (_BUILD_LAKEBASE, _BUILD_LAKEHOUSE):
        template = template_path.read_text(encoding="utf-8")
        assert '<i class="bi bi-fast-forward me-2"></i>' in template
        assert '<i class="bi bi-fast-forward me-1"></i>Build' in template
    assert '<i class="bi bi-fast-forward me-1"></i>Go to Build' in _SYNC_JS.read_text(
        encoding="utf-8"
    )


def test_ontology_and_mapping_designers_use_pencil_icon():
    menus = _menus()
    ontology_designer = next(
        item
        for group in menus["ontology"]["groups"]
        for item in group["items"]
        if item["id"] == "map"
    )
    mapping_designer = next(
        item
        for group in menus["assignment"]["groups"]
        for item in group["items"]
        if item["id"] == "design"
    )

    assert ontology_designer["label"] == "Studio"
    assert mapping_designer["label"] == "Studio"
    assert ontology_designer["icon"] == "bi-pencil"
    assert mapping_designer["icon"] == "bi-pencil"
    assert '<i class="bi bi-pencil me-2"></i>Ontology Studio' in _ONTOLOGY_DESIGNER.read_text(
        encoding="utf-8"
    )
    assert (
        '<i class="bi bi-pencil me-2"></i>Mapping Studio'
        in _MAPPING_DESIGNER.read_text(encoding="utf-8")
    )


def test_business_views_uses_yelp_icon():
    business_views = next(
        item
        for group in _menus()["ontology"]["groups"]
        for item in group["items"]
        if item["id"] == "design"
    )
    assert business_views["label"] == "Business Views"
    assert business_views["icon"] == "bi-yelp"
    assert (
        '<i class="bi bi-yelp me-2"></i>Visual Ontology Designer - Business Views'
        in _BUSINESS_VIEWS.read_text(encoding="utf-8")
    )


def test_ontology_entities_uses_list_nested_icon():
    entities = next(
        item
        for group in _menus()["ontology"]["groups"]
        for item in group["items"]
        if item["id"] == "entities"
    )
    assert entities["label"] == "Entities"
    assert entities["icon"] == "bi-list-nested"
    assert (
        '<i class="bi bi-list-nested me-2"></i>Entities (Hierarchy)'
        in _ONTOLOGY_ENTITIES.read_text(encoding="utf-8")
    )


def test_ontology_advanced_list_matches_menu_items():
    """ontology-advanced group items must all be named under 'Advanced features'."""
    page = _help_html()
    ontology = _menus()["ontology"]
    advanced_group = next(g for g in ontology["groups"] if g["id"] == "ontology-advanced")
    advanced_labels = {item["label"] for item in advanced_group["items"]}

    hw3_start = page.index('id="hw3"')
    hw3_end = page.index("</div>\n                                </div>", hw3_start)
    hw3_body = page[hw3_start:hw3_end]
    advanced_start = hw3_body.index("Advanced features")
    advanced_body = hw3_body[advanced_start:]

    for label in advanced_labels:
        escaped = html.escape(label)
        assert escaped in advanced_body, f"Ontology Advanced doc is missing '{label}'"


def _menu_item_ids(menu_id: str) -> set[str]:
    return {
        item["id"]
        for group in _menus()[menu_id]["groups"]
        for item in group["items"]
    }


def test_owl_and_r2rml_are_designer_exports_not_sidebar_items():
    """OWL and Mapping R2RML live on Studio toolbars, not as sidebar sections."""
    ontology_ids = _menu_item_ids("ontology")
    mapping_ids = _menu_item_ids("assignment")
    ontology_group_ids = {g["id"] for g in _menus()["ontology"]["groups"]}

    assert "owl" not in ontology_ids
    assert "ontology-w3c" not in ontology_group_ids
    assert "r2rml" not in mapping_ids
    assert "sparksql" in mapping_ids

    ontology_designer = _ONTOLOGY_DESIGNER.read_text(encoding="utf-8")
    mapping_designer = _MAPPING_DESIGNER.read_text(encoding="utf-8")
    assert 'id="mapDownloadOwl"' in ontology_designer
    assert "aria-label=\"Export OWL\"" in ontology_designer
    assert 'id="mappingExportR2RMLBtn"' in mapping_designer
    assert "aria-label=\"Export R2RML\"" in mapping_designer

    help_html = _help_html()
    assert "W3C Standards &rarr; OWL" not in help_html
    assert "Download OWL" not in help_html
    assert "Export</strong>" in help_html
