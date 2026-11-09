"""Contracts for ontology external-dataset description field."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_PANELS_JS = (
    REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
)


def _source() -> str:
    return SHARED_PANELS_JS.read_text(encoding="utf-8")


def test_dataset_description_textarea_and_handler_exist():
    source = _source()
    assert "id: `datasetDescriptionInput-${idx}`" in source
    assert "function onDatasetDescriptionChange" in source
    assert "ds.description" in source
    assert "Purpose of this dataset" in source or "purpose of this dataset" in source


def test_multi_dataset_list_and_metric_view_picker():
    source = _source()
    assert "let sharedPanelDatasets = []" in source
    assert "sharedPanelDatasets.push" in source
    assert "METRIC_VIEW" in source
    assert "table, view, or metric view" in source
    assert "datasets: sharedPanelDatasets.length > 0 ? sharedPanelDatasets : undefined" in source


def test_first_selection_defaults_from_asset_comment():
    source = _source()
    start = source.index("function _datasetSelectAsset")
    body = source[start : start + 900]
    assert "asset.comment" in body
    assert "key_column: null" in body
    assert "description:" in body
    assert "Dataset already linked" in body


def test_description_change_marks_panel_dirty():
    source = _source()
    start = source.index("function onDatasetDescriptionChange")
    body = source[start : start + 280]
    assert "ds.description = value.trim() || null;" in body
    assert "markPanelDirty();" in body


def test_description_has_right_aligned_uc_metadata_button():
    source = _source()
    assert 'id="datasetDescriptionFromUcBtn-${idx}"' in source
    assert "function loadDatasetDescriptionFromDataSource" in source
    assert "fetch('/domain/metadata'" in source
    start = source.index("function loadDatasetDescriptionFromDataSource")
    body = source[start : start + 2200]
    assert "match.comment || match.description" in body
    assert "onDatasetDescriptionChange" in body or "ds.description" in body
    assert "Data Assets" in body
