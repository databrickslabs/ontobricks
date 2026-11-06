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
    # Count badges live in the tab headers (same pattern as Data Quality).
    assert 'id="designChecksSummary"' not in html
    assert html.index('id="dc-tab-error"') < html.index('id="dcBadgeError"') < html.index('id="dc-pane-error"')
    assert html.index('id="dc-tab-warning"') < html.index('id="dcBadgeWarning"') < html.index('id="dc-pane-warning"')
    assert html.index('id="dc-tab-info"') < html.index('id="dcBadgeInfo"') < html.index('id="dc-pane-info"')


def test_init_hook_calls_module():
    src = _read(INIT_JS)
    assert "section === 'designchecks'" in src
    assert "DesignChecksModule.init()" in src


def test_module_fetches_endpoint():
    src = _read(MODULE_JS)
    assert "/ontology/design-checks" in src
    assert "window.DesignChecksModule" in src


def test_attribute_findings_do_not_open_relationships():
    # DatatypeProperties are attributes; Relationships only lists ObjectProperties.
    src = _read(MODULE_JS)
    assert "kind === 'attribute'" in src
    assert "section: 'entities'" in src or "section=entities" in src
    assert "data-dc-domain" in src


def test_autofix_wiring_present():
    src = _read(MODULE_JS)
    assert "/ontology/design-checks/autofix/propose" in src
    assert "/ontology/design-checks/autofix/apply" in src
    assert "dcAutofixModal" in src
    assert "_openAutofixModal" in src
    assert "data-dc-fix-all" in src


def test_orphan_modal_removed():
    src = _read(MODULE_JS)
    assert "_resolveOrphanAttribute" not in src
    assert "_attachOrphanAttribute" not in src
    assert "_removeOrphanAttribute" not in src
    assert "dcOrphanAttrModal" not in src


def test_review_consistency_links_to_section():
    src = _read(REVIEW_JS)
    assert "/ontology/?section=designchecks" in src
    assert "Sanity Checks (advisory)" in src
