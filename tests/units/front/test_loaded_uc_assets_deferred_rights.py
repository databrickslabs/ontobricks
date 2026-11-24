"""Loaded Unity Catalog Assets: two-phase paint + reload spinner contracts."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = REPO_ROOT / "src/front/templates/partials/domain/_domain_metadata.html"
JS = REPO_ROOT / "src/front/static/domain/js/domain-metadata.js"
ROUTER = REPO_ROOT / "src/api/routers/internal/domain.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_heading_is_loaded_unity_catalog_assets():
    html = _read(TEMPLATE)
    assert "Loaded Unity Catalog Assets" in html
    assert ">Loaded Tables<" not in html


def test_list_reload_overlay_exists():
    html = _read(TEMPLATE)
    assert 'id="metadataListOverlay"' in html
    assert "Reloading assets…" in html


def test_js_reloads_with_overlay_after_async_task():
    js = _read(JS)
    assert "listOverlay: true" in js
    assert "refreshSelectPermissionsInBackground" in js
    assert "/domain/metadata/check-select" in js
    assert "setMetadataListOverlay" in js


def test_backend_exposes_check_select_endpoint():
    router = _read(ROUTER)
    assert '"/metadata/check-select"' in router
    assert "check_select_permissions" in router
    select_mod = REPO_ROOT / "src/back/objects/domain/_metadata_select.py"
    assert "async def check_select_permissions" in _read(select_mod)
    assert "probe_select_permissions" in _read(
        REPO_ROOT / "src/back/core/databricks/uc/MetadataService.py"
    )
