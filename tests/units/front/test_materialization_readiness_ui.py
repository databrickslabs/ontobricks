"""Domain information panel: the readiness line under the materialization mode."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"
SCRIPT = REPO_ROOT / "src/front/static/domain/js/domain-information.js"


def _readiness_block() -> str:
    html = TEMPLATE.read_text(encoding="utf-8")
    start = html.index('id="materializationReadiness"')
    return html[start : html.index("</div>\n                        </div>", start)]


def test_the_readiness_line_sits_in_the_materialization_section() -> None:
    html = TEMPLATE.read_text(encoding="utf-8")
    section = html.index('id="materializationModeSection"')
    assert section < html.index('id="materializationReadiness"') < html.index('id="searchCacheSection"')


def test_the_enable_button_is_hidden_and_edit_gated_until_needed() -> None:
    block = _readiness_block()
    button = block[block.index('id="enableRowTrackingBtn"') - 120 : block.index("Enable row tracking")]
    assert "domain-editable" in button and "d-none" in button
    assert "<script" not in block and "style=" not in block


def test_the_script_calls_both_endpoints() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    assert "fetch('/dtwin/incremental/readiness'" in js
    assert "fetch('/dtwin/incremental/enable-row-tracking'" in js
    assert "addEventListener('click', enableRowTracking)" in js


def test_table_names_are_rendered_as_text() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    start = js.index("function renderMaterializationReadiness")
    body = js[start : js.index("async function loadMaterializationReadiness")]
    assert "innerHTML" not in body
    assert "textContent" in body


def test_the_enable_action_checks_the_response_status() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    start = js.index("async function enableRowTracking")
    body = js[start : js.index("function enableVersionSelector")]
    assert "resp.ok" in body


def test_the_compile_error_and_the_merge_fallback_are_shown() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    start = js.index("function renderMaterializationReadiness")
    body = js[start : js.index("async function loadMaterializationReadiness")]
    assert "compile_error" in body and "merge_fallback" in body


def test_only_a_probed_untracked_source_asks_for_row_tracking() -> None:
    """A topic-fed realtime pipeline lists its sources unprobed (``row_tracking: null``)."""
    js = SCRIPT.read_text(encoding="utf-8")
    start = js.index("function _readinessUntracked")
    body = js[start : js.index("function renderMaterializationReadiness")]
    assert "s.row_tracking === false" in body
    assert "row_tracking_required === false" in js


@pytest.mark.parametrize(
    "element", ["materializationReadinessMappings", "materializationReadinessLakehouseRT"]
)
def test_the_mapping_and_lakehouse_rt_lines_start_hidden(element) -> None:
    block = _readiness_block()
    line = block[block.index(f'id="{element}"') - 60 : block.index(f'id="{element}"')]
    assert "d-none" in line


def test_the_mapping_classes_are_counted_with_the_batch_ones_as_a_tooltip() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    start = js.index("function _renderReadinessMappings")
    body = js[start : js.index("function _renderReadinessLakehouseRT")]
    assert "row-local (microbatch-eligible)" in body and "batch-incremental" in body
    assert "m.kind === 'batch'" in body and "line.title" in body
    assert "innerHTML" not in body


def test_the_lakehouse_rt_line_shows_the_warehouse_and_the_toggle() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    start = js.index("function _renderReadinessLakehouseRT")
    body = js[start : js.index("function renderMaterializationReadiness")]
    assert "query_warehouse_configured" in body and "use_sea" in body
    assert "innerHTML" not in body
