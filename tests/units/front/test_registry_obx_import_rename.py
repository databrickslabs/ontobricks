"""Structural contracts for Registry OBX Import-as naming."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = (ROOT / "src/front/static/registry/js/registry.js").read_text(
    encoding="utf-8"
)
TEMPLATE = (
    ROOT / "src/front/templates/partials/registry/_import_obx_modal.html"
).read_text(encoding="utf-8")
STYLES = (ROOT / "src/front/static/registry/css/registry.css").read_text(
    encoding="utf-8"
)


def test_import_preview_has_import_as_contract():
    assert ">Import as</th>" in TEMPLATE
    assert 'class="form-control form-control-sm import-obx-name"' in SCRIPT
    assert 'pattern="[A-Z][A-Za-z0-9]*"' in SCRIPT
    assert "actionRadio(idx, 'rename'" not in SCRIPT


def test_changed_name_builds_rename_decision():
    assert "sanitizeImportFolder(importName)" in SCRIPT
    assert "sourceFolder !== targetFolder" in SCRIPT
    assert "action: 'rename'" in SCRIPT
    assert "new_name: importName" in SCRIPT


def test_import_name_validation_rejects_duplicates():
    assert "IMPORT_NAME_PATTERN" in SCRIPT
    assert "targetFolders.has(targetFolder)" in SCRIPT
    assert "showNotification(" in SCRIPT


def test_import_action_controls_follow_target_name():
    assert "function syncImportActionControls(row)" in SCRIPT
    assert "import-obx-conflict-actions" in SCRIPT
    assert "import-obx-create-action" in SCRIPT
    assert "syncImportActionControls(row)" in SCRIPT


def test_import_name_column_keeps_a_readable_width():
    assert 'class="registry-th-import-obx-name"' in TEMPLATE
    assert ".registry-th-import-obx-name" in STYLES
    assert "min-width: 14rem;" in STYLES
    assert ".registry-th-import-obx-action {\n    width: 12rem;" in STYLES


def test_import_preview_has_domain_checkboxes():
    assert 'id="importObxSelectAll"' in TEMPLATE
    assert 'class="form-check-input import-obx-pick"' in SCRIPT
    assert "function syncImportPickState()" in SCRIPT
    assert "action: 'skip'" in SCRIPT
    assert "Pick at least one domain to import" in SCRIPT
    assert "registry-th-import-obx-check" in TEMPLATE
    assert ".registry-th-import-obx-check" in STYLES


def test_import_preview_shows_spinner_while_reading_file():
    assert 'id="importObxPreviewBusy"' in TEMPLATE
    assert "spinner-border spinner-border-sm" in TEMPLATE
    assert "importObxPreviewBusy" in SCRIPT


def test_load_from_github_shows_busy_then_fetches_obx():
    assert 'id="btnImportFromGithub"' in TEMPLATE
    github_click = SCRIPT.split("btnImportFromGithub")[1].split(
        "async function previewImportFile"
    )[0]
    assert "busyEl?.classList.remove('d-none')" in github_click
    assert "/settings/github/obx" in github_click
    assert "AbortController" in github_click
    assert "renderImportObxPreview(data)" in github_click
    assert "pendingImportFromGithub = true" in github_click
    assert "showNotification" in github_click
    assert "from_github" in SCRIPT
