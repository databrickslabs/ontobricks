"""Contract: primary "Run" action buttons on Data Quality, Graph Analytics and Communities.

- Digital Twin → Data Quality: the Run button is the filled primary CTA.
- Intelligence → Analytics: the header action is labelled "Run", is primary,
  and sits immediately left of the discussion button.
- Intelligence → Communities: the former "Detect" button is labelled "Run".
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
DQ_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_dataquality.html").read_text(encoding="utf-8")
ANALYTICS_HTML = (ROOT / "src/front/templates/partials/intelligence/_intel_analytics.html").read_text(encoding="utf-8")
COMMUNITIES_HTML = (ROOT / "src/front/templates/partials/intelligence/_intel_communities.html").read_text(encoding="utf-8")


def _button_tag(html: str, marker: str) -> str:
    pos = html.index(marker)
    start = html.rindex("<button", 0, pos)
    return html[start:html.index(">", pos)]


def test_data_quality_run_button_is_primary():
    tag = _button_tag(DQ_HTML, 'id="runDQChecksBtn"')
    assert "btn-primary" in tag
    assert "btn-outline-primary" not in tag


def test_analytics_run_button_is_primary_and_labelled_run():
    tag = _button_tag(ANALYTICS_HTML, 'id="analyticsComputeBtn"')
    assert "btn-primary" in tag
    assert "btn-outline-primary" not in tag
    compute_pos = ANALYTICS_HTML.index('id="analyticsComputeBtn"')
    label = ANALYTICS_HTML[compute_pos:ANALYTICS_HTML.index("</button>", compute_pos)]
    assert label.rstrip().endswith("Run") and "Run Analysis" not in label


def test_analytics_run_button_sits_left_of_discussion_button():
    compute_pos = ANALYTICS_HTML.index('id="analyticsComputeBtn"')
    discuss_pos = ANALYTICS_HTML.index("onto-discuss-btn")
    assert compute_pos < discuss_pos
    # Nothing but the Run button's closing tag lies between it and the discussion button.
    discuss_open = ANALYTICS_HTML.rindex("<button", 0, discuss_pos)
    between = ANALYTICS_HTML[ANALYTICS_HTML.index("</button>", compute_pos) + len("</button>"):discuss_open]
    assert between.strip() == ""


def test_communities_detect_button_is_renamed_run():
    tag_pos = COMMUNITIES_HTML.index('id="commDetectBtn"')
    button = COMMUNITIES_HTML[tag_pos:COMMUNITIES_HTML.index("</button>", tag_pos)]
    label = button[button.index(">") + 1:]
    assert label.rstrip().endswith("Run")
    assert "Detect" not in label
    assert "btn-primary" in _button_tag(COMMUNITIES_HTML, 'id="commDetectBtn"')
