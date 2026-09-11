"""Regression contracts for manual mapping column assignments.

The shared panel keeps live assignments in ``EntityPanelState`` and
``RelPanelState``. Manual Apply and the Status tab must consume that live state
instead of removed summary elements or the snapshot captured when the panel
opened (GitHub issue #158).
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
MANUAL_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-manual.js"


def _brace_block_after(source: str, start: int) -> str:
    index = source.index("{", start) + 1
    depth = 1
    body_start = index
    while index < len(source) and depth > 0:
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
        index += 1
    return source[body_start : index - 1] if depth == 0 else ""


def _function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?:async\s+)?function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{", source
    )
    assert match, f"{name}() must exist"
    return _brace_block_after(source, match.end() - 1)


def _method_body(source: str, name: str) -> str:
    match = re.search(
        rf"{re.escape(name)}\s*:\s*(?:async\s+)?function\s*\([^)]*\)\s*\{{",
        source,
    )
    assert match, f"{name}() must exist"
    return _brace_block_after(source, match.end() - 1)


def test_manual_apply_reads_live_entity_and_relationship_state():
    body = _method_body(MANUAL_JS.read_text(encoding="utf-8"), "saveMapping")

    assert "EntityPanelState.idColumn" in body
    assert "EntityPanelState.labelColumn" in body
    assert "RelPanelState.sourceIdColumn" in body
    assert "RelPanelState.targetIdColumn" in body
    assert "epSummaryId" not in body
    assert "epSummaryLabel" not in body
    assert "rpSummarySource" not in body
    assert "rpSummaryTarget" not in body


def test_entity_status_refreshes_after_the_mapping_grid_changes():
    source = DESIGN_JS.read_text(encoding="utf-8")
    body = _function_body(source, "renderEntityPanelGrid")
    refresh_body = _function_body(source, "refreshEntityPanelStatus")

    assert 'id="epStatusIdIcon"' in source
    assert 'id="epStatusIdDetail"' in source
    assert 'id="epStatusLabelIcon"' in source
    assert 'id="epStatusLabelDetail"' in source
    assert "EntityPanelState.idColumn" in refresh_body
    assert "EntityPanelState.labelColumn" in refresh_body
    assert "refreshEntityPanelStatus()" in body


def test_relationship_status_refreshes_after_the_mapping_grid_changes():
    source = DESIGN_JS.read_text(encoding="utf-8")
    body = _function_body(source, "renderRelPanelGrid")
    refresh_body = _function_body(source, "refreshRelationshipPanelStatus")

    assert 'id="rpStatusSourceIcon"' in source
    assert 'id="rpStatusSourceDetail"' in source
    assert 'id="rpStatusTargetIcon"' in source
    assert 'id="rpStatusTargetDetail"' in source
    assert "RelPanelState.sourceIdColumn" in refresh_body
    assert "RelPanelState.targetIdColumn" in refresh_body
    assert "refreshRelationshipPanelStatus()" in body
