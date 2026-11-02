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
