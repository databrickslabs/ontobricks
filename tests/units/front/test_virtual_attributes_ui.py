"""Contracts for the virtual attributes UI (authoring + Graph Explorer)."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
PANELS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-shared-panels.css"
LOADERS_JS = REPO_ROOT / "src/front/static/query/js/query-loaders.js"
DETAILS_JS = REPO_ROOT / "src/front/static/query/js/query-entity-details.js"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
VA_JS = REPO_ROOT / "src/front/static/query/js/query-virtual-attributes.js"
DESIGN_JS = REPO_ROOT / "src/front/static/global/js/ontology-design.js"
INFO_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-information.js"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"


# ---------------------------------------------------------------------------
# Ontology designer
# ---------------------------------------------------------------------------


def test_attributes_tab_has_virtual_attributes_box_with_add_button():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert 'id="sharedEntityVirtualAttributes"' in js
    assert "openVirtualAttributeSelectorModal()" in js
    assert "renderSharedEntityVirtualAttributes(viewOnly)" in js


def test_virtual_attributes_box_states_the_two_constraints():
    """The user has to know the values are not queryable, and that the
    function is called with the entity ID alone."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "exactly one parameter" in js
    assert "not queryable" in js.lower()


def test_virtual_attribute_picker_reuses_the_shared_uc_cascade():
    """The catalog → schema → function cascade is shared with the Actions
    picker; a second copy would drift."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "_ucFillCatalogSelect" in js
    assert "_ucFillSchemaSelect" in js
    assert "_ucFetchFunctions" in js
    assert "_vaOnCatalogChange" in js
    assert "_vaOnSchemaChange" in js


def test_virtual_attribute_picker_enforces_the_single_parameter_contract():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "Number(fn.param_count) === 1" in js


def test_one_attribute_is_derived_per_returned_column():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "_vaDeriveAttributes" in js
    assert "return_columns" in js


def test_picker_states_how_many_values_each_function_returns():
    """The output count is what the user is choosing: it is the number of
    virtual attributes the function will create."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "_ucOutputCount" in js
    assert "_ucOutputBadge" in js
    assert "creates ${count} attribute" in js
    # Both pickers share the modal title, so both must state the count.
    assert js.count("_ucOutputBadge(fn)") >= 2


def test_unknown_output_count_is_stated_rather_than_guessed():
    """A table function whose result columns the metastore did not report must
    not be shown as returning nothing."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "outputs unknown" in js


def test_derived_names_are_deduplicated_against_the_class():
    """Mapped and virtual attributes render in one list downstream, so a
    collision has to be settled at authoring time."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "_vaTakenNames" in js
    assert "dataProperties" in js


def test_virtual_attributes_state_is_saved_and_hydrated():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "let sharedPanelVirtualAttributes = []" in js
    assert (
        "sharedPanelVirtualAttributes = cls.virtualAttributes "
        "? JSON.parse(JSON.stringify(cls.virtualAttributes)) : []"
    ) in js
    assert (
        "virtualAttributes: sharedPanelVirtualAttributes.length > 0 "
        "? sharedPanelVirtualAttributes : undefined"
    ) in js
    assert "onVirtualAttributeDescriptionChange" in js
    assert "onVirtualAttributeLabelChange" in js
    assert "removeSharedEntityVirtualAttribute" in js


def test_removal_is_confirmed_and_drops_the_whole_function_group():
    """The attributes are the function's signature: dropping one alone would
    leave the column-to-attribute mapping ambiguous."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "showConfirmDialog" in js
    assert "sharedPanelVirtualAttributes.splice" in js


def test_designer_panel_has_virtual_attribute_styles():
    css = PANELS_CSS.read_text(encoding="utf-8")
    assert ".va-group" in css
    assert ".va-function-list" in css


# ---------------------------------------------------------------------------
# Round-trips that must not drop the declarations
# ---------------------------------------------------------------------------


def test_loaders_retain_class_virtual_attributes():
    js = LOADERS_JS.read_text(encoding="utf-8")
    assert "virtualAttributes: cls.virtualAttributes || []" in js


def test_designer_sync_preserves_virtual_attributes():
    """Designer → ontology sync rebuilds each class from the canvas; anything
    it forgets to carry over is silently lost on the next save."""
    js = DESIGN_JS.read_text(encoding="utf-8")
    assert "virtualAttributes: existing.virtualAttributes || []" in js


def test_owl_and_rdfs_import_preserve_virtual_attributes():
    js = INFO_JS.read_text(encoding="utf-8")
    assert js.count("virtualAttributes: cls.virtualAttributes || []") >= 2


# ---------------------------------------------------------------------------
# Graph Explorer
# ---------------------------------------------------------------------------


def test_virtual_attributes_module_is_loaded_by_the_explorer():
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert "query/js/query-virtual-attributes.js" in html


def test_module_exports_its_entry_points():
    js = VA_JS.read_text(encoding="utf-8")
    assert "window.renderVirtualAttributeSection = renderVirtualAttributeSection" in js
    assert "window.computeVirtualAttributes = computeVirtualAttributes" in js
    assert (
        "window.computeVirtualAttributesForNode = computeVirtualAttributesForNode" in js
    )


def test_section_renders_declarations_before_any_computation():
    """Declarations arrive with the node context, so the section must render
    with empty values rather than waiting on a warehouse round-trip."""
    js = VA_JS.read_text(encoding="utf-8")
    assert "not computed" in js
    assert "Compute" in js


def test_values_are_cached_per_entity_and_function():
    js = VA_JS.read_text(encoding="utf-8")
    assert "_vaCacheKey" in js
    assert "entityUri + '|' + fullName" in js
    assert "Recompute" in js


def test_compute_calls_the_internal_endpoint_with_the_function_filter():
    js = VA_JS.read_text(encoding="utf-8")
    assert "/dtwin/nodes/virtual-attributes?entity_uri=" in js
    assert "'&function=' + encodeURIComponent(fullName)" in js


def test_results_are_matched_by_function_name_not_by_position():
    """A single-group computation returns one entry, which would otherwise
    land on the first group's rows."""
    js = VA_JS.read_text(encoding="utf-8")
    assert 'data-va-function="' in js
    assert "_vaGroupFullName(group)" in js


def test_errors_and_messages_are_surfaced_per_group():
    js = VA_JS.read_text(encoding="utf-8")
    assert "_vaStatusHtml" in js
    assert "entry.error" in js
    assert "entry.message" in js


def test_rendered_values_are_escaped():
    js = VA_JS.read_text(encoding="utf-8")
    assert "escapeHtml" in js


def test_context_menu_entry_opens_the_panel_before_computing():
    """Right-clicking does not open the detail panel, so the values would
    otherwise have nowhere to land."""
    js = VA_JS.read_text(encoding="utf-8")
    assert "SigmaGraph.selectEntity" in js
    assert "_vaWaitForSection" in js


def test_sigmagraph_details_renders_the_virtual_attributes_section():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "entityMapping && entityMapping.virtualAttributes" in js
    assert "renderVirtualAttributeSection(entity.id, virtualGroups)" in js
    assert "'Virtual Attributes ('" in js


def test_entity_details_renders_the_virtual_attributes_section():
    js = DETAILS_JS.read_text(encoding="utf-8")
    assert "entityMapping?.virtualAttributes || classInfo?.virtualAttributes" in js
    assert "renderVirtualAttributeSection(entity.id, virtualGroups)" in js


def test_sigmagraph_context_menu_has_a_compute_entry():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "computeVirtualAttributesForNode" in js
