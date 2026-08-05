"""Contracts for the class Actions UI (ontology authoring + Graph Explorer)."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
LOADERS_JS = REPO_ROOT / "src/front/static/query/js/query-loaders.js"
DETAILS_JS = REPO_ROOT / "src/front/static/query/js/query-entity-details.js"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
DASHBOARD_JS = REPO_ROOT / "src/front/static/query/js/query-dashboard.js"
DESIGN_JS = REPO_ROOT / "src/front/static/global/js/ontology-design.js"
INFO_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-information.js"


def test_external_tab_has_actions_box_with_add_button():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert 'id="sharedEntityActions"' in js
    assert 'id="sharedEntityActionsContent"' in js
    assert "openActionSelectorModal()" in js
    assert "renderSharedEntityActions(viewOnly)" in js


def test_actions_box_documents_the_single_parameter_contract():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "exactly one parameter" in js


def test_action_picker_cascades_catalog_schema_and_lists_uc_functions():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "/settings/catalogs" in js
    assert "/settings/schemas/" in js
    assert "/settings/uc-functions?catalog=" in js
    assert "_actionOnCatalogChange" in js
    assert "_actionOnSchemaChange" in js


def test_action_picker_disables_functions_without_exactly_one_param():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "Number(fn.param_count) === 1" in js
    assert "Needs exactly 1 parameter" in js


def test_actions_state_is_saved_and_hydrated():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "let sharedPanelActions = []" in js
    assert "sharedPanelActions = cls.actions ? JSON.parse(JSON.stringify(cls.actions)) : []" in js
    assert "actions: sharedPanelActions.length > 0 ? sharedPanelActions : undefined" in js
    assert "onActionDescriptionChange" in js
    assert "removeSharedEntityAction" in js


def test_loaders_retain_class_actions():
    js = LOADERS_JS.read_text(encoding="utf-8")
    assert "actions: cls.actions || []" in js
    assert "actions: classInfo?.actions || []" in js


def test_designer_sync_preserves_dataset_actions_and_bridges():
    """Designer → ontology sync must not wipe References-tab metadata."""
    js = DESIGN_JS.read_text(encoding="utf-8")
    assert "bridges: existing.bridges || []" in js
    assert "dataset: existing.dataset || null" in js
    assert "actions: existing.actions || []" in js


def test_owl_and_rdfs_import_preserve_dataset_and_actions():
    """Import remappers must keep OntoBricks External fields for registry save."""
    js = INFO_JS.read_text(encoding="utf-8")
    assert js.count("dataset: cls.dataset || null") >= 2
    assert js.count("actions: cls.actions || []") >= 2
    assert js.count("bridges: cls.bridges || []") >= 2


def test_entity_details_renders_actions_section():
    js = DETAILS_JS.read_text(encoding="utf-8")
    assert "entityMapping?.actions || classInfo?.actions" in js
    assert "openEntityActionModal(" in js
    assert "action.description" in js
    assert "bi bi-lightning-charge" in js


def test_sigmagraph_details_renders_actions_section():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "entityMapping && entityMapping.actions" in js
    assert "openEntityActionModal(" in js
    assert "'Actions ('" in js


def test_sigmagraph_context_menu_has_action_items_and_dispatch():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert 'data-sg-node-action="action-invoke"' in js
    assert "data-action=" in js
    assert "data-description=" in js
    assert "action === 'action-invoke'" in js
    assert "openEntityActionModal(actUri, actName, actLbl, actDesc)" in js


def test_action_modal_posts_to_the_invoke_endpoint():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert "function openEntityActionModal" in js
    assert "async function _runEntityAction" in js
    assert "/api/v1/digitaltwin/nodes/action" in js
    assert "method: 'POST'" in js
    assert "action_full_name" in js


def test_action_modal_shows_function_description():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert "description" in js
    assert "safeDescription" in js
    assert "descriptionBlock" in js
    assert 'class="text-muted small mb-3"' in js


def test_action_modal_has_loading_scalar_table_and_error_states():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert 'id="entityActionResultModal"' in js
    assert "Running action" in js
    assert "Action completed." in js
    assert "No result returned." in js
    assert "Failed to run the action." in js
    assert "escapeHtml(c)" in js
    assert "escapeHtml(text)" in js
