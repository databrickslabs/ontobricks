"""Contracts for Graph Explorer external dataset description display."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
LOADERS_JS = REPO_ROOT / "src/front/static/query/js/query-loaders.js"
DETAILS_JS = REPO_ROOT / "src/front/static/query/js/query-entity-details.js"


def test_loaders_retain_class_dataset():
    js = LOADERS_JS.read_text(encoding="utf-8")
    assert "dataset: cls.dataset || null" in js
    assert "dataset: classInfo?.dataset || null" in js


def test_entity_details_renders_dataset_section():
    js = DETAILS_JS.read_text(encoding="utf-8")
    assert "Dataset" in js
    assert "entityMapping?.dataset || classInfo?.dataset" in js
    assert "dataset.key_column" in js
    assert "dataset.description" in js
    assert "Purpose" in js
