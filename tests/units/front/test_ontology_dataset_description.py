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
    assert 'id="datasetDescriptionInput"' in source
    assert "function onDatasetDescriptionChange" in source
    assert "sharedPanelDataset.description" in source
    assert "Purpose of this dataset" in source or "purpose of this dataset" in source


def test_first_selection_defaults_from_asset_comment_and_switch_preserves():
    source = _source()
    start = source.index("function _datasetSelectAsset")
    body = source[start : start + 900]
    assert "previousDescription" in body
    assert "asset.comment" in body
    assert "key_column: null" in body
    assert "description:" in body


def test_description_change_marks_panel_dirty():
    source = _source()
    start = source.index("function onDatasetDescriptionChange")
    body = source[start : start + 280]
    assert "sharedPanelDataset.description = value.trim() || null;" in body
    assert "markPanelDirty();" in body


def test_description_has_right_aligned_uc_metadata_button():
    source = _source()
    assert 'id="datasetDescriptionFromUcBtn"' in source
    assert "function loadDatasetDescriptionFromDataSource" in source
    assert "justify-content-between" in source
    assert "fetch('/domain/metadata'" in source
    start = source.index("function loadDatasetDescriptionFromDataSource")
    body = source[start : start + 2200]
    assert "match.comment || match.description" in body
    assert "onDatasetDescriptionChange" in body or "sharedPanelDataset.description" in body
    assert "Data Sources" in body
