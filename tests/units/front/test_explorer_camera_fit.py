"""Contracts for KG Explorer camera fit: selected node + neighbors, not a tight zoom."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def _camera_helpers(js: str) -> str:
    return js[js.index("// Camera helpers") : js.index("// Node / Edge reducers")]


def test_focus_camera_does_not_use_tight_single_node_ratio() -> None:
    helpers = _camera_helpers(SIGMA_JS.read_text(encoding="utf-8"))
    assert "ratio: 0.08" not in helpers
    assert "CAMERA_MIN_RATIO" in helpers
    assert "CAMERA_FIT_PADDING" in helpers
    assert "Math.max(CAMERA_MIN_RATIO" in helpers


def test_focus_expands_selection_with_neighbors() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    helpers = _camera_helpers(js)
    assert "function _nodeSetWithNeighbors" in helpers
    assert "forEachNeighbor" in helpers[helpers.index("function _nodeSetWithNeighbors") :]
    click_node = js[js.index("_renderer.on('clickNode'") : js.index("_renderer.on('doubleClickNode'")]
    select_entity = js[js.index("selectEntity: function (entityId)") : js.index("discussNode:")]
    assert "_nodeSetWithNeighbors" in click_node
    assert "_focusCameraOnNodes" in click_node
    assert "_nodeSetWithNeighbors" in select_entity
    assert "_focusCameraOnNodes(_nodeSetWithNeighbors(" in select_entity
