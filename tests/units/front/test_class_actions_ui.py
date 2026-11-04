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


def _behaviour_pane(js):
    return js[
        js.index('data-form-tab-content="behaviour"') : js.index(
            'data-form-tab-content="links"'
        )
    ]


def test_behaviour_tab_section_heading_is_functions():
    pane = _behaviour_pane(PANELS_JS.read_text(encoding="utf-8"))
    assert "icon: 'bi-lightning-charge', title: 'Functions'" in pane
    assert "title: 'Actions'" not in pane


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
    assert "const splitActions = _splitClassActions(cls.actions);" in js
    assert "sharedPanelActions = splitActions.uc;" in js
    assert "sharedPanelActions.concat(sharedPanelJobs)" in js
    assert "onActionDescriptionChange" in js
    assert "removeSharedEntityAction" in js


def test_behaviour_tab_has_jobs_box_and_picker():
    js = PANELS_JS.read_text(encoding="utf-8")
    pane = _behaviour_pane(js)
    assert 'id="sharedEntityJobs"' in pane
    assert "openJobSelectorModal()" in pane
    assert "/settings/jobs?q=" in js
    assert "let sharedPanelJobs = []" in js
    assert "renderSharedEntityJobs(viewOnly)" in js


def test_loaders_retain_class_actions():
    js = LOADERS_JS.read_text(encoding="utf-8")
    assert "actions: cls.actions || []" in js
    assert "actions: classInfo?.actions || []" in js


def test_designer_sync_preserves_dataset_actions_and_bridges():
    """Designer → ontology sync must not wipe Links/Model/Behaviour metadata."""
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
    assert "bi bi-play-circle" in js
    assert "_entityActionIsJob" in js


def test_sigmagraph_details_renders_actions_section():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "entityMapping && entityMapping.actions" in js
    assert "openEntityActionModal(" in js
    assert "'Behaviour ('" in js


def test_sigmagraph_context_menu_has_action_items_and_dispatch():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert 'data-sg-node-action="action-invoke"' in js
    assert "data-action=" in js
    assert "data-description=" in js
    assert "action === 'action-invoke'" in js
    assert "openEntityActionModal(actUri, actName, actLbl, actDesc)" in js
    assert "bi bi-play-circle" in js
    assert "_entityActionIsJob" in js


def test_action_modal_posts_to_the_invoke_endpoint():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert "function openEntityActionModal" in js
    assert "async function _runEntityAction" in js
    assert "/api/v1/digitaltwin/nodes/action" in js
    assert "method: 'POST'" in js
    assert "action_full_name" in js
    assert "function _entityActionIsJob" in js
    assert "refreshTasks()" in js


def test_job_action_opens_a_popup_and_follows_the_background_task():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    opener = js.split("function openEntityActionModal", 1)[1].split("\nfunction ", 1)[0]
    assert "_runEntityAction(entityUri, actionFullName).then(function () {});" not in opener
    assert "_openJobActionModal(" in opener
    assert "Starting job" in js
    assert "bootstrap.Modal.getOrCreateInstance" in js
    assert "/tasks/' + encodeURIComponent(" in js
    assert "run_page_url" in js
    assert "toggleTaskDropdown" in js
    assert "Tracked in Background Tasks" in js
    assert "showNotification(" in js.split("function _openJobActionModal", 1)[1]
    assert 'style="' not in js.split("function _openJobActionModal", 1)[1].split("\nfunction findAttributeValue", 1)[0]


def _job_status(task_status, life_cycle):
    import json
    import shutil
    import subprocess

    import pytest

    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    fn = "function _jobRunStatus" + js.split("function _jobRunStatus", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
    script = fn + f"process.stdout.write(JSON.stringify(_jobRunStatus({json.dumps(task_status)}, {json.dumps(life_cycle)})));"
    out = subprocess.run([node, "-e", script], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def test_job_status_mapping_has_an_icon_and_a_label():
    running = _job_status("running", "RUNNING")
    queued = _job_status("running", "PENDING")
    done = _job_status("completed", "TERMINATED")
    failed = _job_status("failed", "INTERNAL_ERROR")
    assert running["label"] == "Running" and running["icon"]
    assert queued["label"] == "Queued" and queued["icon"]
    assert done["label"] == "Succeeded" and done["terminal"] is True
    assert failed["label"] == "Failed" and failed["terminal"] is True
    assert running["terminal"] is False


def test_background_task_links_to_the_databricks_run():
    tracker = (REPO_ROOT / "src/front/static/global/js/task-tracker.js").read_text(encoding="utf-8")
    item = tracker.split("function renderTaskItem", 1)[1].split("\nfunction ", 1)[0]
    assert "run_page_url" in item
    assert "escapeAttr(" in item
    assert 'rel="noopener"' in item


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
