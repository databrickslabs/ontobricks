"""Contracts for Graph Explorer external dataset display + preview."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
LOADERS_JS = REPO_ROOT / "src/front/static/query/js/query-loaders.js"
DETAILS_JS = REPO_ROOT / "src/front/static/query/js/query-entity-details.js"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
DASHBOARD_JS = REPO_ROOT / "src/front/static/query/js/query-dashboard.js"


def test_loaders_retain_class_dataset():
    js = LOADERS_JS.read_text(encoding="utf-8")
    assert "dataset: cls.dataset || null" in js
    assert "dataset: classInfo?.dataset || null" in js


def test_entity_details_renders_dataset_section_with_preview():
    js = DETAILS_JS.read_text(encoding="utf-8")
    assert "Dataset" in js
    assert "entityMapping?.dataset || classInfo?.dataset" in js
    assert "dataset.key_column" in js
    assert "dataset.description" in js
    assert "Description" in js
    assert "Preview rows" in js
    assert "openDatasetPreviewModal" in js


def test_sigmagraph_details_renders_dataset_section_with_preview():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "bi bi-table" in js
    assert "Preview rows" in js
    assert "openDatasetPreviewModal" in js
    assert "entityMapping.dataset" in js or "entityMapping && entityMapping.dataset" in js


def test_sigmagraph_context_menu_has_dataset_preview():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert 'data-sg-node-action="dataset-preview"' in js
    assert "Dataset preview" in js
    assert "openDatasetPreviewModal" in js


def test_dataset_preview_modal_fetches_ten_rows():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert "function openDatasetPreviewModal" in js
    assert "/api/v1/digitaltwin/nodes/context" in js
    assert "fetch_dataset_rows" in js
    assert "dataset_row_limit" in js
    assert "'10'" in js or '"10"' in js
    assert "key_column_missing" in js
    assert "Failed to load rows" in js


def test_dataset_preview_modal_has_loading_empty_and_error_states():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert "Loading dataset rows" in js
    assert "No linked dataset for this entity type." in js
    assert "No matching rows for this entity." in js
    assert "Could not resolve the ontology class" in js
    assert "Key column is not configured" in js
    assert "Failed to load dataset preview" in js


def test_dataset_preview_renders_union_of_row_columns_safely():
    js = DASHBOARD_JS.read_text(encoding="utf-8")
    assert "const columns = []" in js
    assert "const seen = new Set()" in js
    assert "Object.keys(row || {})" in js
    assert "escapeHtml(c)" in js
    assert "escapeHtml(text)" in js


def test_context_menu_forwards_entity_uri_class_and_id_to_preview():
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert 'data-sg-node-action="dataset-preview"' in js
    assert 'data-uri="' in js
    assert "meta.entity.id" in js
    assert "data-class" in js
    assert "data-id" in js
    assert "openDatasetPreviewModal(dsUri, dsCls, dsId)" in js
