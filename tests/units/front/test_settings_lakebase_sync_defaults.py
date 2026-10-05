"""Settings → Lakebase Bulk loading: Managed sync default, Snapshot-only schedule."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"
SETTINGS_TEMPLATE = REPO_ROOT / "src/front/templates/settings.html"


def _mode_select_html() -> str:
    html = SETTINGS_TEMPLATE.read_text(encoding="utf-8")
    start = html.index('id="lakebaseSyncMode"')
    return html[start : html.index("</select>", start)]


def _schedule_select_html() -> str:
    html = SETTINGS_TEMPLATE.read_text(encoding="utf-8")
    start = html.index('id="lakebaseSyncTableMode"')
    return html[start : html.index("</select>", start)]


def test_managed_sync_is_first_and_selected() -> None:
    block = _mode_select_html()
    managed = block.index('value="managed_synced"')
    app = block.index('value="app_managed"')
    assert managed < app
    assert "selected" in block[managed:app]


def test_bulk_loading_copy_recommends_managed_sync() -> None:
    html = SETTINGS_TEMPLATE.read_text(encoding="utf-8")
    pane = html.split('id="lkpane-bulk"', 1)[1].split('id="lkpane-', 1)[0]
    assert "Managed sync</strong> is the recommended path" in pane
    assert "Most workspaces start with" not in pane
    assert "(default)" not in pane


def test_schedule_is_snapshot_only() -> None:
    block = _schedule_select_html()
    assert 'value="snapshot"' in block
    assert 'value="triggered"' not in block
    assert 'value="continuous"' not in block
    html = SETTINGS_TEMPLATE.read_text(encoding="utf-8")
    assert "Change Data Feed" in html
    assert "R2RML view" in html


def test_apply_from_config_proposes_managed_sync_when_unset() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    assert (
        "syncModeEl.value = (o.sync_mode === 'app_managed') "
        "? 'app_managed' : 'managed_synced'"
    ) in js
    assert (
        "(o.sync_mode === 'managed_synced') ? 'managed_synced' : 'app_managed'"
        not in js
    )
    assert "if (stEl) stEl.value = 'snapshot';" in js
    assert "o.sync_table_mode = 'snapshot';" in js
