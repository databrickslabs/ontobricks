"""Contracts for incremental refreshes in the Ontology Designer map."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"


def test_entity_save_describes_whether_the_map_topology_changed():
    js = PANELS_JS.read_text(encoding="utf-8")

    assert "requiresMapRebuild: isNew || isRename || parentChanged" in js
    assert "entityName: name" in js


def test_metadata_only_entity_save_refreshes_the_existing_node():
    js = PANELS_JS.read_text(encoding="utf-8")

    assert "function refreshMapAfterEntitySave(changeInfo)" in js
    assert "if (changeInfo.requiresMapRebuild)" in js
    assert "refreshMapNodeFromConfig(changeInfo.entityName)" in js


def test_incremental_node_refresh_updates_visible_metadata_and_badge():
    js = MAP_JS.read_text(encoding="utf-8")

    assert "function refreshMapNodeFromConfig(name)" in js
    assert "nodeSelection.select('.map-node-icon').text(node.icon)" in js
    assert "nodeSelection.select('.map-node-label').text(node.label || node.name)" in js
    assert "nodeSelection.select('title').text(node.label || node.name)" in js
    assert "nodeSelection.selectAll('.map-node-external-badge-bg').remove()" in js
    assert "window.refreshMapNodeFromConfig = refreshMapNodeFromConfig" in js


def test_relationship_save_only_rebuilds_for_structural_changes():
    js = PANELS_JS.read_text(encoding="utf-8")

    assert "requiresMapRebuild: isNew || isRename || endpointsChanged" in js
    assert "function refreshMapAfterRelationshipSave(changeInfo)" in js
    assert "refreshMapRelationshipFromConfig(changeInfo.relationshipName)" in js


def test_incremental_relationship_refresh_updates_direction_and_label():
    js = MAP_JS.read_text(encoding="utf-8")

    assert "function refreshMapRelationshipFromConfig(name)" in js
    assert "link.label = property.label || property.name" in js
    assert "refreshMapLinkDirection(name, property.direction)" in js
    assert "window.refreshMapRelationshipFromConfig = refreshMapRelationshipFromConfig" in js

    direction_start = js.index("function refreshMapLinkDirection(name, direction)")
    direction_end = js.index("window.refreshMapLinkDirection = refreshMapLinkDirection")
    direction_block = js[direction_start:direction_end]
    assert "ontologyMapSvg.selectAll('.map-link')" in direction_block
    assert "d3.selectAll('.map-link')" not in direction_block

    rel_start = js.index("function refreshMapRelationshipFromConfig(name)")
    rel_end = js.index("window.refreshMapRelationshipFromConfig = refreshMapRelationshipFromConfig")
    rel_block = js[rel_start:rel_end]
    assert "ontologyMapSvg.selectAll('.map-link-label')" in rel_block
    assert "d3.selectAll('.map-link-label')" not in rel_block
    assert "if (labelSelection.empty()) return false" in rel_block
