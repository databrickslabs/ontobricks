"""Contracts for Ontology Studio multi-select gestures on the D3 map."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"


def _map_init_block():
    js = MAP_JS.read_text(encoding="utf-8")
    start = js.index("async function initOntologyMap()")
    end = js.index("function initMapSearch(nodes)", start)
    return js[start:end]


def test_modifier_click_toggles_without_opening_panel():
    block = _map_init_block()
    assert "event.ctrlKey || event.metaKey" in block
    assert "_toggleMapEntitySelection(d.name)" in block


def test_modifier_marquee_replaces_selection():
    block = _map_init_block()
    assert "function startMapMarquee(event)" in block
    assert "_setMapSelection(namesInside)" in block
    assert "ontologyMapZoom.filter" in block


def test_escape_clears_multi_selection():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "function handleMapSelectionKeyDown(event)" in js
    assert "if (event.key !== 'Escape'" in js


def _marquee_block():
    block = _map_init_block()
    start = block.index("function startMapMarquee(event)")
    end = block.index("svg.on('pointerdown'", start)
    return block[start:end]


def test_marquee_starts_only_on_empty_canvas():
    marquee = _marquee_block()
    assert "event.target.tagName !== 'svg'" in marquee
    # Anything but the bare <svg> was already rejected above; the old
    # ``closest('.map-node, .map-link, ...')`` guard was unreachable.
    assert "closest(" not in marquee


def _relationship_hitarea_block():
    block = _map_init_block()
    start = block.index(".attr('class', 'map-link-hitarea')")
    end = block.index(".attr('class', 'map-link-label')", start)
    return block[start:end]


def test_relationship_selection_clears_entity_set():
    hit = _relationship_hitarea_block()
    assert hit.count("_clearMapSelection({ closePanel: false })") == 2
    assert "d3.selectAll('.map-node').classed('selected', false)" not in hit
    assert "guardedCloseSharedPanel" not in hit
    assert "await guardedCloseSharedPanel" not in hit
    assert "editPropertyByName(d.name)" in hit


def test_selection_helpers_support_close_panel_option():
    js = MAP_JS.read_text(encoding="utf-8")
    start = js.index("function _setMapSelection(names, options)")
    end = js.index("function handleMapSelectionKeyDown(event)", start)
    helpers = js[start:end]
    assert "options.closePanel === false" in helpers
    assert "function _clearMapSelection(options)" in helpers


def test_canvas_click_clears_selection_then_closes_panel_once():
    block = _map_init_block()
    start = block.index("svg.on('click', function()")
    end = block.index("function startMapMarquee(event)", start)
    canvas = block[start:end]
    assert "_clearMapSelection({ closePanel: false })" in canvas
    assert canvas.count("guardedCloseSharedPanel()") == 1
    assert "await guardedCloseSharedPanel" not in canvas


def _function_block(js, signature):
    start = js.index(signature)
    next_positions = []
    for needle in ("\nfunction ", "\nasync function "):
        pos = js.find(needle, start + 1)
        if pos != -1:
            next_positions.append(pos)
    end = min(next_positions) if next_positions else len(js)
    return js[start:end]


def test_selected_drag_translates_whole_group():
    block = _map_init_block()
    assert "mapDragStartPositions = new Map" in block
    assert "node.fx = start.x + deltaX" in block


def test_multi_context_menu_limits_actions():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "function showMapMultiSelectContextMenu(")
    assert "Create Business View" in block
    assert "Delete ${names.length} entities" in block
    assert "create-relationship" not in block
    assert "createBusinessViewFromSelection(live)" in block
    assert "deleteEntitiesFromMap(live)" in block


def test_multi_delete_mutates_and_saves_once():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "async function deleteEntitiesFromMap(")
    assert "const namesSet = new Set(names)" in block
    assert "await saveConfigToSession()" in block
    assert block.count("initOntologyMap()") == 1
    assert "title: 'Delete entities'" in block
    assert "typeof showConfirmDialog !== 'function'" in block
    assert block.count("showConfirmDialog(") == 1
    # Notify the number actually removed, not the number requested.
    assert "const removedCount =" in block
    assert "${removedCount}" in block
    assert "${names.length} entities deleted" not in block
    assert "async function createBusinessViewFromSelection(names)" in js


def test_multi_business_view_keeps_only_internal_links():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "async function createBusinessViewFromSelection(")
    assert "const visibleNames = new Set(names)" in block
    assert "visibleNames.has(source) && visibleNames.has(target)" in block
    assert "baseName: 'Auto_Selection'" in block
    assert "_createMapBusinessView(" in block
    assert "neighbourNames" not in block
    helper = _function_block(js, "async function _createMapBusinessView(")
    assert "const cx = 450, cy = 280, radius = 230" in helper
    assert "hiddenEntities" in helper
    assert "hiddenRelationships" in helper
    assert "hiddenInheritances" in helper
    assert "/domain/design-views/create" in helper
    assert "/domain/design-views/switch" in helper
    assert "/domain/design-views/save-current" in helper
    assert "SidebarNav.switchTo('design')" in helper


def test_single_entity_business_view_stays_one_hop():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "async function createBusinessViewFromEntity(")
    assert "baseName: `Auto_${selectedName}`" in block
    assert "const neighbourNames = new Set()" in block
    assert "[selectedName, ...Array.from(neighbourNames)]" in block
    # Single-entity layout: selected entity at the centre, neighbours on a circle.
    assert "i === 0" in block
    assert "neighbourNames.size" in block
    assert "_createMapBusinessView(" in block


def _node_contextmenu_block():
    block = _map_init_block()
    start = block.index("nodeElements.on('contextmenu'")
    end = block.index("svg.on('click'", start)
    return block[start:end]


def _svg_contextmenu_block():
    block = _map_init_block()
    start = block.index("svg.on('contextmenu'")
    end = block.index("ontologyMapSimulation.on('tick'", start)
    return block[start:end]


def test_modifier_contextmenu_on_node_toggles_without_menu():
    """macOS Control-click fires contextmenu, not click; toggle before any menu."""
    block = _node_contextmenu_block()
    assert "event.ctrlKey || event.metaKey" in block
    assert "event.preventDefault()" in block
    assert "event.stopPropagation()" in block
    toggle_at = block.index("_toggleMapEntitySelection(d.name)")
    assert block.index("event.preventDefault()") < toggle_at
    assert block.index("event.stopPropagation()") < toggle_at
    assert "return" in block[toggle_at:toggle_at + 80]
    assert toggle_at < block.index("_setMapSelection([d.name]")
    assert toggle_at < block.index("showMapMultiSelectContextMenu")
    assert toggle_at < block.index("showMapContextMenu")


def test_modifier_contextmenu_on_svg_preserves_marquee():
    """Control-drag starts marquee on pointerdown; contextmenu must not open Canvas menu."""
    block = _svg_contextmenu_block()
    modifier_at = block.index("event.ctrlKey || event.metaKey")
    show_at = block.index("showMapCanvasContextMenu")
    assert modifier_at < show_at
    assert "event.preventDefault()" in block[:show_at]
    assert "return" in block[modifier_at:show_at]


# ── Final-review fixes ────────────────────────────────────────────────────────


def test_canvas_click_honours_marquee_suppression():
    """A browser click follows the marquee pointerup; it must not clear."""
    block = _map_init_block()
    start = block.index("svg.on('click', function()")
    end = block.index("function startMapMarquee(event)", start)
    canvas = block[start:end]
    assert "_consumeMapCanvasClickSuppression()" in canvas
    first_statement = canvas.index("_consumeMapCanvasClickSuppression()")
    assert first_statement < canvas.index("_clearMapSelection(")
    assert first_statement < canvas.index("guardedCloseSharedPanel()")


def test_marquee_pointerup_arms_click_suppression_and_checks_modifier():
    marquee = _marquee_block()
    up = marquee[marquee.index("function onUp(evt)"):]
    assert "_suppressNextMapCanvasClick()" in up
    assert "_isMapSelectGesture(evt)" in up
    # Selection changes only after the gesture check passed.
    assert up.index("_isMapSelectGesture(evt)") < up.index("_setMapSelection(namesInside")


def test_marquee_cleans_up_on_pointercancel_and_blur():
    marquee = _marquee_block()
    assert "window.addEventListener('pointercancel'" in marquee
    assert "window.removeEventListener('pointercancel'" in marquee
    assert "window.addEventListener('blur'" in marquee
    assert "window.removeEventListener('blur'" in marquee
    cleanup = marquee[marquee.index("function cleanup()"):marquee.index("function onCancel()")]
    assert "_setMapSelection" not in cleanup
    assert "_clearMapSelection" not in cleanup


def test_suppression_helpers_are_time_boxed():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "function _suppressNextMapCanvasClick()" in js
    block = _function_block(js, "function _consumeMapCanvasClickSuppression()")
    # One-shot: consumed on first click, and expires if no click follows.
    assert "_mapCanvasClickSuppressUntil = 0" in block
    assert "performance.now()" in block


def test_selection_change_closes_context_menu():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "function _setMapSelection(")
    assert "hideMapContextMenu()" in block


def test_escape_closes_menu_and_respects_modal_and_inactive_studio():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "function handleMapSelectionKeyDown(")
    assert "hideMapContextMenu()" in block
    assert ".modal.show" in block
    assert "#map-section.active" in block
    assert block.index(".modal.show") < block.index("_clearMapSelection()")
    assert block.index("#map-section.active") < block.index("_clearMapSelection()")


def test_multi_menu_actions_reread_live_selection():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "function showMapMultiSelectContextMenu(")
    assert "_liveMapSelectionNames()" in block
    assert "deleteEntitiesFromMap(live)" in block
    assert "createBusinessViewFromSelection(live)" in block
    assert "deleteEntitiesFromMap(names)" not in block
    helper = _function_block(js, "function _liveMapSelectionNames()")
    assert "mapSelectedEntityNames" in helper


def test_map_rebuild_does_not_close_the_panel():
    block = _map_init_block()
    start = block.index("const thisGeneration = ++_mapInitGeneration;")
    head = block[start:start + 300]
    assert "_clearMapSelection({ closePanel: false })" in head
    assert "_clearMapSelection();" not in head


def test_business_view_builders_share_one_helper():
    js = MAP_JS.read_text(encoding="utf-8")
    assert js.count("/domain/design-views/create") == 1
    assert js.count("/domain/design-views/switch") == 1
    assert js.count("/domain/design-views/save-current") == 1
    for signature in (
        "async function createBusinessViewFromEntity(",
        "async function createBusinessViewFromSelection(",
    ):
        block = _function_block(js, signature)
        assert "_createMapBusinessView(" in block
        assert len(block.splitlines()) < 60, signature


def test_new_pointerdown_resets_leftover_click_suppression():
    block = _map_init_block()
    start = block.index("svg.on('pointerdown'")
    handler = block[start:block.index("svg.on('contextmenu'", start)]
    assert "_mapCanvasClickSuppressUntil = 0" in handler
    assert handler.index("_mapCanvasClickSuppressUntil = 0") < handler.index("startMapMarquee(event)")


def test_escape_ignores_events_from_modal_even_after_show_class_removed():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "function handleMapSelectionKeyDown(")
    assert "event.target.closest('.modal')" in block
    assert "document.body.classList.contains('modal-open')" in block
    assert block.index("event.target.closest('.modal')") < block.index("_clearMapSelection()")
    assert block.index("modal-open") < block.index("_clearMapSelection()")


def test_select_mode_helper_treats_toolbar_and_modifiers_as_equivalent():
    js = MAP_JS.read_text(encoding="utf-8")
    block = _function_block(js, "function _isMapSelectGesture(")
    assert "mapSelectMode" in block
    assert "event.ctrlKey || event.metaKey" in block


def test_select_mode_toggle_is_bound_on_map_init():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "function initMapSelectToggle()" in js
    assert "getElementById('mapToggleSelect')" in js
    init = _map_init_block()
    assert "initMapSelectToggle()" in init
    assert init.index("initMapSelectToggle()") < init.index("showOntologyMapLoading(true)")


def test_select_mode_click_and_marquee_reuse_the_shared_gesture_helper():
    block = _map_init_block()
    click = block[block.index("nodeElements.on('click'"):block.index("nodeElements.on('contextmenu'")]
    assert "_isMapSelectGesture(event)" in click
    marquee = _marquee_block()
    assert "_isMapSelectGesture(event)" in marquee
    zoom = block[block.index("ontologyMapZoom.filter"):block.index("svg.call(ontologyMapZoom)")]
    assert "_isMapSelectGesture(event)" in zoom


def test_business_view_position_callback_documents_all_arguments():
    js = MAP_JS.read_text(encoding="utf-8")
    assert (
        "@param {function(number, number, number, number): {x: number, y: number}} spec.positionFor"
        in js
    )
    assert "(index, cx, cy, radius)" in js
