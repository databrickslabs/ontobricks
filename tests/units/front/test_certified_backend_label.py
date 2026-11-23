"""Context → Configuration names the backend after the graph it serves."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
INFO = REPO_ROOT / "src/front/templates/partials/domain/_domain_information.html"


def test_the_backend_selector_is_the_certified_graph_backend() -> None:
    html = INFO.read_text(encoding="utf-8")
    fieldset = html.split('id="domainGraphBackendCards"', 1)[1].split("</fieldset>", 1)[0]
    assert "Certified Graph backend" in fieldset
    assert 'id="domainGraphBackendHelp"' in fieldset
    assert "Original Graph" in fieldset
    assert "> Graph Backend <" not in fieldset
