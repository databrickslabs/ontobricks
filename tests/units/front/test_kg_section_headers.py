"""KG section headers omit domain/graph-DB chrome already shown in the navbar.

The L2 header already shows the current domain, version, and status. Repeating
"Domain: …", "Graph DB: …", or a Switch domain control under the section
subtitle was noise.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
_TEMPLATES = (
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_query.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_chat.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_dataquality.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_reasoning.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_cohorts.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_analytics.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_insights.html",
)


@pytest.mark.parametrize(
    "template",
    _TEMPLATES,
    ids=(
        "explorer",
        "query",
        "chat",
        "dataquality",
        "reasoning",
        "cohorts",
        "analytics",
        "insights",
    ),
)
def test_kg_section_header_has_no_domain_or_graph_db_line(template: Path):
    html = template.read_text(encoding="utf-8")
    header = html.split('<div class="section-header', 1)[1]
    header = header.split("</div>", 1)[0]
    assert "Domain:" not in header
    assert "Graph DB:" not in header
    assert "Switch domain" not in header
    assert "js-version-status-badge" not in header
