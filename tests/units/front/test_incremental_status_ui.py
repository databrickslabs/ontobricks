"""Digital twin build panels: the incremental status card."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATES = REPO_ROOT / "src/front/templates"
CARD = TEMPLATES / "partials/dtwin/_incremental_status_card.html"
SCRIPT = REPO_ROOT / "src/front/static/query/js/incremental-status.js"
INCLUDE = '{% include "partials/dtwin/_incremental_status_card.html" %}'


@pytest.mark.parametrize("panel", ["_query_sync.html", "_query_databricks_build.html"])
def test_both_build_panels_include_the_card(panel):
    assert INCLUDE in (TEMPLATES / "partials/dtwin" / panel).read_text(encoding="utf-8")


def test_the_card_is_hidden_until_filled_and_has_no_inline_code():
    html = CARD.read_text(encoding="utf-8")
    assert "data-incremental-status" in html and "d-none" in html
    assert "<script" not in html and "style=" not in html


def test_the_page_loads_the_script():
    page = (TEMPLATES / "dtwin.html").read_text(encoding="utf-8")
    assert "query/js/incremental-status.js" in page


def test_the_script_reads_the_status_endpoint_and_renders_text():
    js = SCRIPT.read_text(encoding="utf-8")
    assert "fetch('/dtwin/incremental/status'" in js
    assert "innerHTML" not in js and "textContent" in js
    assert "mode_requested === 'full'" in js


@pytest.mark.parametrize(
    "script", ["query/js/query-sync.js", "query/js/query-databricks-build.js"]
)
def test_both_panels_load_the_status(script):
    js = (REPO_ROOT / "src/front/static" / script).read_text(encoding="utf-8")
    assert "loadIncrementalStatus()" in js


def test_a_realtime_stream_line_is_rendered():
    js = SCRIPT.read_text(encoding="utf-8")
    assert "status.realtime" in js and "'Realtime stream'" in js


def test_the_last_build_line_shows_the_slots_changed() -> None:
    js = SCRIPT.read_text(encoding="utf-8")
    assert "facts.slots_changed" in js and "slotsChangedText" in js


def test_the_run_details_show_the_slots_changed_next_to_the_refresh_technique() -> None:
    js = (REPO_ROOT / "src/front/static/global/js/runs-render.js").read_text(encoding="utf-8")
    technique = js.index("kv('Refresh Technique'")
    assert js.index("kv('Slots Changed'", technique) - technique < 120
