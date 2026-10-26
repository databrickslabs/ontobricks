"""Settings → Global data-assets import limit (default 40)."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_HTML = REPO_ROOT / "src/front/templates/settings.html"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"
METADATA_JS = REPO_ROOT / "src/front/static/domain/js/domain-metadata.js"


def test_global_section_has_import_limit_input() -> None:
    html = SETTINGS_HTML.read_text(encoding="utf-8")
    section = html[html.index('id="global-section"') : html.index('id="ui-section"')]
    assert 'id="dataAssetsImportLimit"' in section
    snippet = section[section.index('id="dataAssetsImportLimit"') :]
    assert 'value="40"' in snippet.split(">", 1)[0]
    assert "Admin only" in section[section.index("Data assets import limit") :]


def test_settings_js_loads_and_saves_import_limit() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    assert "loadDataAssetsImportLimit" in js
    assert "/settings/data-assets-import-limit" in js
    assert "dataAssetsImportLimit" in js
    assert "data_assets_import_limit" in js
    assert "/settings/save-data-assets-import-limit" in js


def test_metadata_import_ui_respects_import_limit() -> None:
    js = METADATA_JS.read_text(encoding="utf-8")
    assert "import_limit" in js
    assert "_remainingImportSlots" in js
    show = js[js.index("function showTableSelectionModal") : js.index("function toggleImportTableSelection")]
    assert "_remainingImportSlots" in show
    select_new = js[js.index("function selectNewTablesOnly") : js.index("function filterImportTables")]
    assert "_remainingImportSlots" in select_new
    import_fn = js[js.index("async function importSelectedTables") : js.index("async function importSelectedTables") + 1800]
    assert "_remainingImportSlots" in import_fn
