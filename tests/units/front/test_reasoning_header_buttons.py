"""Contract: Digital Twin Reasoning header button ordering and primary style.

The Run button is a primary CTA (btn-primary) placed immediately to the left
of the discussion button, and the Purge overrides button precedes it.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
REASONING_HTML = (
    ROOT / "src/front/templates/partials/dtwin/_query_reasoning.html"
).read_text(encoding="utf-8")


def _header_actions() -> str:
    start = REASONING_HTML.index('class="section-header')
    end = REASONING_HTML.index("</div>\n    </div>\n    <div class=\"card\">", start)
    return REASONING_HTML[start:end]


def test_run_button_is_primary_and_labelled_run():
    assert 'id="runReasoningBtn"' in REASONING_HTML
    btn_start = REASONING_HTML.index('id="runReasoningBtn"')
    tag_start = REASONING_HTML.rindex("<button", 0, btn_start)
    tag = REASONING_HTML[tag_start:REASONING_HTML.index(">", btn_start)]
    assert "btn-primary" in tag
    assert "btn-outline-primary" not in tag
    assert '<i class="bi bi-play-fill me-1"></i> Run' in REASONING_HTML


def test_run_button_is_left_of_discussion_button_and_after_purge():
    actions = _header_actions()
    purge_pos = actions.index("_purge_inferences_button.html")
    run_pos = actions.index('id="runReasoningBtn"')
    discuss_pos = actions.index("onto-discuss-btn")
    assert purge_pos < run_pos < discuss_pos
