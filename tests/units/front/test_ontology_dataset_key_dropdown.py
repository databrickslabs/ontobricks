"""Contracts for ontology external-dataset key-column selection."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_PANELS_JS = (
    REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
)


def _source() -> str:
    return SHARED_PANELS_JS.read_text(encoding="utf-8")


def test_dataset_key_column_uses_select_not_text_input():
    source = _source()
    assert 'id="datasetKeyColumnSelect"' in source
    assert 'id="datasetKeyColumnInput"' not in source
    assert 'onchange="onDatasetKeyColumnChange(this.value)"' in source


def test_dataset_columns_use_existing_mapping_endpoint_and_memory_cache():
    source = _source()
    assert "const _datasetColumnCache = new Map();" in source
    assert "fetch('/mapping/table-columns'" in source
    assert "catalog: dataset.catalog" in source
    assert "schema: dataset.schema" in source
    assert "table: dataset.asset" in source


def test_dataset_column_states_and_missing_saved_key_are_rendered():
    source = _source()
    for label in (
        "Select a key column…",
        "Loading columns…",
        "No columns found",
        "Failed to load columns",
        "(missing)",
    ):
        assert label in source


def test_dataset_column_fetch_has_retry_and_stale_response_guard():
    source = _source()
    assert "function retryDatasetKeyColumns()" in source
    assert "_loadDatasetKeyColumns(sharedPanelDataset, true)" in source
    assert "_isCurrentDataset(datasetKey)" in source
    assert "_datasetColumnCache.set(datasetKey, columns)" in source


def test_key_column_change_keeps_existing_dirty_state_contract():
    source = _source()
    start = source.index("function onDatasetKeyColumnChange")
    body = source[start : start + 250]
    assert "sharedPanelDataset.key_column = value || null;" in body
    assert "markPanelDirty();" in body


def test_empty_column_list_is_not_cached_and_offers_retry():
    """HTTP 200 + columns=[] must not poison the cache; empty state is retryable."""
    source = _source()
    assert "_setDatasetKeyColumnState('No columns found', true)" in source

    load_start = source.index("async function _loadDatasetKeyColumns")
    load_body = source[load_start : load_start + 1600]
    set_idx = load_body.index("_datasetColumnCache.set(datasetKey, columns)")
    guard_window = load_body[max(0, set_idx - 160) : set_idx]
    assert "columns.length" in guard_window


def test_dataset_key_column_options_built_with_dom_apis():
    """Column names may contain quotes; build options via DOM, not HTML interpolation."""
    source = _source()
    start = source.index("function _populateDatasetKeyColumnSelect")
    end = source.index("async function _loadDatasetKeyColumns")
    body = source[start:end]
    assert "new Option(" in body
    assert "replaceChildren" in body
    assert '`<option value="' not in body
    assert "Select a key column…" in body
    assert "(missing)" in body
