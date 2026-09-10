"""Contracts for Build and Query warehouse coordination in Settings."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"


def test_lakehouse_controls_sync_query_state_with_rt_toggle() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    assert "function syncLakehouseWarehouseControls()" in js
    assert "querySelect.disabled = !useRt;" in js
    assert "deltaBuildWarehouseSelect" in js
    assert "deltaUseSea')?.addEventListener('change'" in js


def test_non_rt_save_clears_query_override() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    assert "const warehouseId = useRt ? querySelect.value : '';" in js
    assert "Select a Query SQL Warehouse for Lakehouse//RT" in js
    assert "Select a Lakehouse//RT Query SQL Warehouse." in js
    assert "must be different from the Build SQL Warehouse" in js


def test_lakehouse_build_selector_is_editable_and_persisted() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    assert "function saveBuildWarehouseSelection(errors)" in js
    assert "'/settings/select-build-warehouse'" in js
    assert "deltaBuildWarehouseSelect')?.addEventListener('change'" in js
    assert "(wh.warehouse_type || '').toUpperCase() !== 'REYDEN'" in js


def test_query_selector_uses_all_non_build_warehouses() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    sync_block = js.split("function syncLakehouseWarehouseControls()", 1)[1].split(
        "function setDeltaWarehouseStatus()", 1
    )[0]
    assert ".filter((wh) => wh.id !== buildId)" in sync_block
    assert "=== 'REYDEN'" not in sync_block


def test_warehouse_tab_uses_lakehouse_configuration_spinner() -> None:
    js = SETTINGS_JS.read_text(encoding="utf-8")
    assert "dttab-warehouse')?.addEventListener('shown.bs.tab'" in js
    assert "setGraphDbHeavyLoading(true);" in js
    assert "await loadDeltaWarehouseSelect(" in js
    assert "setGraphDbHeavyLoading(false);" in js
