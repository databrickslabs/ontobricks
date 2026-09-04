"""KG Explorer / Query / Chat section headers omit the redundant Domain line.

The first header (navbar) already shows the current domain, version, and
status. Repeating "Domain: … vN Draft" in the page subtitle was noise.
Graph DB name + Switch domain stay.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
_TEMPLATES = (
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_graphql.html",
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_chat.html",
)


@pytest.mark.parametrize("template", _TEMPLATES, ids=("explorer", "query", "chat"))
def test_kg_data_tab_header_has_no_domain_line(template: Path):
    html = template.read_text(encoding="utf-8")
    assert "Domain:" not in html
    assert "js-version-status-badge" not in html
    assert "Graph DB:" in html
    assert "Switch domain" in html
