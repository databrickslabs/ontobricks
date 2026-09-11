"""Responsive contract for the level-2 contextual navigation."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
EDIT_LOCK_JS = REPO_ROOT / "src/front/static/global/js/edit-lock.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _mobile_block(css: str) -> str:
    match = re.search(
        r"@media\s*\(max-width:\s*767\.98px\)\s*\{(.*?)\n\}",
        css,
        flags=re.DOTALL,
    )
    assert match, "No mobile breakpoint in main.css"
    return match.group(1)


def test_mobile_subnav_hides_redundant_breadcrumb_and_spacer():
    css = _mobile_block(_read(MAIN_CSS))

    assert re.search(
        r"\.ob-subnav-breadcrumb-wrap\s*,\s*"
        r"\.ob-subnav-flex-spacer\s*\{[^}]*display:\s*none\s*!important",
        css,
        flags=re.DOTALL,
    )


def test_mobile_subnav_keeps_controls_but_hides_their_visual_labels():
    css = _mobile_block(_read(MAIN_CSS))
    html = _read(BASE_HTML)

    assert re.search(
        r"\.ob-subnav-label\s*\{[^}]*display:\s*none",
        css,
        flags=re.DOTALL,
    )
    # Four workspace links plus Save, Versions and Close keep their accessible
    # text in the DOM while CSS hides only the visual label on mobile.
    assert html.count('class="ob-subnav-label"') == 7


def test_mobile_subnav_compacts_padding_without_horizontal_scrolling():
    css = _mobile_block(_read(MAIN_CSS))

    assert re.search(
        r"#obSubnav\s+\.container-fluid\s*\{[^}]*padding-inline:",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.ob-subnav-link\s*,[^}]*"
        r"\.ob-subnav-close-btn\s*\{[^}]*padding-inline:",
        css,
        flags=re.DOTALL,
    )
    assert "overflow-x: auto" not in css


def test_icon_only_mobile_subnav_controls_have_tooltips():
    html = _read(BASE_HTML)

    for element_id in (
        "subnavDomainDropdown",
        "subnavOntologyDropdown",
        "subnavMappingDropdown",
        "subnavKgDropdown",
    ):
        tag = re.search(
            rf'<a\b[^>]*id="{element_id}"[^>]*>',
            html,
            flags=re.DOTALL,
        )
        assert tag, element_id
        assert 'title="' in tag.group(0), f"{element_id} lacks an icon tooltip"


def test_expired_lease_resume_action_uses_the_responsive_label():
    js = _read(EDIT_LOCK_JS)

    assert '<span class="ob-subnav-label">Resume editing</span>' in js
