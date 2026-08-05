"""Contracts for centered Registry child modals with stacked blur behavior."""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"
REGISTRY_LAYOUT = (
    REPO_ROOT / "src/front/templates/partials/layout/registry_modal.html"
)
REGISTRY_DOMAINS = (
    REPO_ROOT / "src/front/templates/partials/registry/_registry_domains.html"
)
EXPORT_MODAL = (
    REPO_ROOT / "src/front/templates/partials/registry/_export_obx_modal.html"
)
IMPORT_MODAL = (
    REPO_ROOT / "src/front/templates/partials/registry/_import_obx_modal.html"
)


@pytest.mark.parametrize("template", [EXPORT_MODAL, IMPORT_MODAL])
def test_registry_child_modal_is_centered(template: Path):
    markup = template.read_text(encoding="utf-8")
    assert "modal-dialog-centered" in markup


def test_registry_child_modals_are_siblings_not_nested():
    layout = REGISTRY_LAYOUT.read_text(encoding="utf-8")
    domains = REGISTRY_DOMAINS.read_text(encoding="utf-8")
    registry_close = layout.rfind("</div>")
    for modal_name in ("_export_obx_modal.html", "_import_obx_modal.html"):
        assert modal_name not in domains
        assert layout.index(modal_name) > registry_close


def test_export_uses_shared_stacked_modal_helper():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    start = source.index("async function openExportObxModal")
    end = source.index("btnExportObxConfirm", start)
    assert "showStackedModal(modalEl)" in source[start:end]


def test_import_uses_shared_stacked_modal_helper():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    start = source.index("function openImportObxModal")
    end = source.index(
        "document.getElementById('importObxFile')?.addEventListener",
        start,
    )
    assert "showStackedModal(modalEl)" in source[start:end]
