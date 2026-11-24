"""Contract tests for the single "Add Data Assets" dialog (Domain > Unity Catalog).

The catalog/schema pickers, the table list and the Import action live in one
modal. These tests pin the template and JS contract so the old chained popups
do not come back.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = REPO_ROOT / "src/front/templates/partials/domain/_domain_metadata.html"
JS = REPO_ROOT / "src/front/static/domain/js/domain-metadata.js"
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, name: str) -> str:
    start = source.index(f"function {name}(")
    end = source.index("\n}\n", start)
    return source[start:end]


def test_legacy_chained_modals_are_removed():
    html = _read(TEMPLATE)
    assert 'id="loadMetadataLocationModal"' not in html
    assert 'id="tableSelectionModal"' not in html
    assert 'id="loadMetadataLocationWidget"' not in html
    assert 'id="loadMetadataConfirmBtn"' not in html


def test_single_dialog_holds_pickers_table_list_and_import():
    html = _read(TEMPLATE)
    dialog = html[html.index('id="addDataAssetsModal"') :]
    dialog = dialog[: dialog.index("<!-- Table Details Modal -->")]
    for hook in (
        'id="addDataAssetsCatalog"',
        'id="addDataAssetsSchema"',
        'id="addDataAssetsRefreshBtn"',
        'id="addDataAssetsList"',
        'id="tableSelectionBody"',
        'id="tableSearchFilter"',
        'id="selectedCountInfo"',
        'id="importSelectedBtn"',
        'data-meta-action="import-selected"',
        'data-meta-action="import-select-all"',
        'data-meta-action="import-new-only"',
        'data-meta-action="add-assets-refresh"',
    ):
        assert hook in dialog, hook
    assert 'id="addDataAssetsUseContextBtn"' not in dialog
    assert "add-assets-use-context" not in dialog


def test_entry_button_still_opens_the_dialog():
    html = _read(TEMPLATE)
    assert 'id="loadMetadataBtn"' in html
    assert 'data-meta-action="show-load-modal"' in html


def test_import_button_is_labelled_add_assets():
    html = _read(TEMPLATE)
    button = html[html.index('id="importSelectedBtn"') :]
    button = button[: button.index("</button>")]
    assert "Add 0 assets" in button


def test_js_opens_the_single_dialog_without_nested_widget():
    js = _read(JS)
    body = _function_body(js, "showLoadMetadataModal")
    assert "addDataAssetsModal" in body
    assert "_populateAddDataAssetsCatalogs()" in body
    assert "UCLocationWidget.init" not in body
    assert "loadDomainLocation" not in body
    assert "addDataAssetsUseContextBtn" not in body
    populate = _function_body(js, "_populateAddDataAssetsCatalogs")
    assert "UCLocationWidget.loadCatalogs" in populate


def test_js_has_no_chained_modal_state():
    js = _read(JS)
    for stale in (
        "pendingLoadCatalog",
        "pendingLoadSchema",
        "loadMetadataModal",
        "loadMetadataWidgetInitialized",
        "loadMetadataFromUC",
        "showTableSelectionModal",
        "tableSelectionModal",
        "loadMetadataLocationWidget",
        "addDataAssetsContext",
        "useContextLocationForAssets",
        "add-assets-use-context",
    ):
        assert stale not in js, stale


def test_js_refresh_clears_uc_cache_and_relists():
    js = _read(JS)
    body = _function_body(js, "refreshAddDataAssets")
    assert "UCLocationWidget.clearCache" in body
    assert "_populateAddDataAssetsCatalogs" in body
    assert "loadTablesForSelection" in body


def test_schema_pick_lists_tables_inline():
    js = _read(JS)
    body = _function_body(js, "onAddDataAssetsSchemaChange")
    assert "loadTablesForSelection(catalog, schema)" in body
    listing = _function_body(js, "loadTablesForSelection")
    assert "/domain/metadata/list-tables" in listing
    assert "renderImportTable(catalog, schema, perms)" in listing
    assert "new bootstrap.Modal" not in listing


def test_import_reads_location_from_the_dialog():
    js = _read(JS)
    body = _function_body(js, "importSelectedTables")
    assert "getAddDataAssetsLocation()" in body
    assert "addDataAssetsModal" in body
    assert "tableSelectionModal" not in body


def test_import_limit_path_does_not_reassign_const():
    js = _read(JS)
    body = _function_body(js, "importSelectedTables")
    assert not re.search(r"const selectedTables\b", body)


def test_catalog_and_schema_changes_are_delegated_from_the_template_root():
    js = _read(JS)
    assert "el.id === 'addDataAssetsCatalog'" in js
    assert "el.id === 'addDataAssetsSchema'" in js
    assert "add-assets-refresh" in js


def test_already_loaded_rows_have_no_checkbox():
    js = _read(JS)
    body = _function_body(js, "renderImportTable")
    assert "importRowCheckboxHtml" in body or (
        "table.already_loaded" in body and "<td></td>" in body
    )
    assert "import-table-checkbox" in body


def test_read_only_gate_no_longer_targets_removed_button():
    css = _read(PERMISSIONS_CSS)
    assert "#loadMetadataConfirmBtn" not in css
    assert "#importSelectedBtn" in css
    assert '[data-meta-action="show-load-modal"]' in css


needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node required")

_HARNESS = """
const fs = require('fs');
global.window = {};
global.document = { addEventListener: () => {}, getElementById: () => null };
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const result = (() => { __BODY__ })();
process.stdout.write(JSON.stringify(result));
"""


def _eval(body: str):
    script = _HARNESS.replace("__SOURCE__", json.dumps(str(JS))).replace("__BODY__", body)
    done = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr + done.stdout
    return json.loads(done.stdout)


@needs_node
def test_select_all_skips_already_loaded_assets():
    """Already-loaded assets are not selectable; Select All only checks new ones."""
    result = _eval(
        """
        const tables = [
            {name: 'a', already_loaded: true},
            {name: 'b', already_loaded: false},
            {name: 'c', already_loaded: false},
        ];
        return computeSelectAllImportState(tables, ['a', 'b', 'c'], true, 40);
        """
    )
    assert result["selections"] == {"a": False, "b": True, "c": True}
    assert result["skippedNew"] == 0


@needs_node
def test_select_all_still_caps_new_assets_at_the_remaining_slots():
    result = _eval(
        """
        const tables = [
            {name: 'old', already_loaded: true},
            {name: 'n1', already_loaded: false},
            {name: 'n2', already_loaded: false},
            {name: 'n3', already_loaded: false},
        ];
        return computeSelectAllImportState(tables, ['old', 'n1', 'n2', 'n3'], true, 2);
        """
    )
    assert result["selections"] == {"old": False, "n1": True, "n2": True, "n3": False}
    assert result["skippedNew"] == 1


@needs_node
def test_deselect_all_clears_new_rows_and_leaves_loaded_false():
    result = _eval(
        """
        const tables = [
            {name: 'a', already_loaded: true},
            {name: 'b', already_loaded: false},
        ];
        return computeSelectAllImportState(tables, ['a', 'b'], false, 40);
        """
    )
    assert result["selections"] == {"a": False, "b": False}
