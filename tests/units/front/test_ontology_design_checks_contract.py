"""Contract: Ontology → Sanity Checks sidebar section wiring."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
ONTOLOGY_HTML = REPO_ROOT / "src/front/templates/ontology.html"
PARTIAL = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_design_checks.html"
INIT_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-init.js"
MODULE_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-design-checks.js"
MENU = REPO_ROOT / "src/front/config/menu_config.json"
REVIEW_JS = REPO_ROOT / "src/front/static/domain/js/domain-review.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_menu_has_sanity_checks_item():
    text = _read(MENU)
    assert '"id": "designchecks"' in text
    assert '"label": "Sanity Checks"' in text
    assert '"icon": "bi-clipboard2-check"' in text


def test_page_includes_section_assets_and_partial():
    html = _read(ONTOLOGY_HTML)
    assert 'id="designchecks-section"' in html
    assert 'partials/ontology/_ontology_design_checks.html' in html
    assert "ontology-design-checks.js" in html
    assert "ontology-design-checks.css" in html


def test_partial_has_required_ids():
    html = _read(PARTIAL)
    for token in (
        'id="designChecksRefreshBtn"',
        'id="dcBadgeError"',
        'id="dcBadgeWarning"',
        'id="dcBadgeInfo"',
        'id="dc-tab-error"',
        'id="dc-pane-error"',
        'aria-controls="dc-pane-error"',
        'class="nav nav-tabs ob-tabs nav-fill"',
    ):
        assert token in html, token


def test_init_hook_calls_module():
    src = _read(INIT_JS)
    assert "section === 'designchecks'" in src
    assert "DesignChecksModule.init()" in src


def test_module_fetches_endpoint():
    src = _read(MODULE_JS)
    assert "/ontology/design-checks" in src
    assert "window.DesignChecksModule" in src


def test_review_consistency_links_to_section():
    src = _read(REVIEW_JS)
    assert "/ontology/?section=designchecks" in src
    assert "Sanity Checks (advisory)" in src
