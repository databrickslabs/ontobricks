"""Settings → Lakebase Bulk loading: Managed sync default; Snapshot / Triggered / Continuous cap."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


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


def test_schedule_offers_snapshot_triggered_and_continuous_in_that_order() -> None:
    block = _schedule_select_html()
    snapshot = block.index('value="snapshot"')
    triggered = block.index('value="triggered"')
    continuous = block.index('value="continuous"')
    assert snapshot < triggered < continuous
    assert "selected" in block[snapshot:triggered]


def test_continuous_describes_the_continuous_pipeline_and_the_view_refresh() -> None:
    """Rows reach Lakebase only once the ``_data`` view refreshes, not as they land."""
    block = _schedule_select_html()
    start = block.index('value="continuous"')
    option = block[start : block.index("</option>", start)]
    assert "continuous slot pipeline" in option
    assert "refreshes" in option
    assert "every 15 minutes" in option  # more than ten sources: the scheduled refresh
    assert "as they land" not in option


def test_schedule_help_explains_the_cap_and_the_change_feed() -> None:
    html = SETTINGS_TEMPLATE.read_text(encoding="utf-8")
    start = html.index('id="lakebaseSyncTableModeHelp"')
    help_text = html[start : html.index("</div>", start)]
    assert "Change Data Feed" in help_text
    assert "<code>_data</code>" in help_text
    assert "snapshot" in help_text.lower()
    assert "R2RML view" not in help_text


def test_schedule_select_points_at_its_help() -> None:
    html = SETTINGS_TEMPLATE.read_text(encoding="utf-8")
    start = html.index('<select class="form-select form-select-sm" id="lakebaseSyncTableMode"')
    tag = html[start : html.index(">", start)]
    assert 'aria-describedby="lakebaseSyncTableModeHelp"' in tag


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
    assert (
        "if (stEl) stEl.value = ['triggered', 'continuous'].includes(o.sync_table_mode) "
        "? o.sync_table_mode : 'snapshot';"
    ) in js
    assert "if (stEl) o.sync_table_mode = stEl.value || 'snapshot';" in js
    assert "o.sync_table_mode = 'snapshot';" not in js
