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


def test_mobile_subnav_hides_flex_spacer():
    css = _mobile_block(_read(MAIN_CSS))

    assert re.search(
        r"\.ob-subnav-flex-spacer\s*\{[^}]*display:\s*none\s*!important",
        css,
        flags=re.DOTALL,
    )


def test_mobile_subnav_keeps_controls_but_hides_their_visual_labels():
    css = _mobile_block(_read(MAIN_CSS))
    html = _read(BASE_HTML)

    assert re.search(
        r"\.ob-subnav-action:not\(\.ob-subnav-resume-btn\)\s+\.ob-subnav-label\s*\{[^}]*display:\s*none",
        css,
        flags=re.DOTALL,
    )
    # Save/Close are icon-only in markup; Resume injects its label from JS.
    assert html.count('class="ob-subnav-label"') == 0


def test_mobile_subnav_compacts_padding_without_horizontal_scrolling():
    css = _mobile_block(_read(MAIN_CSS))

    assert re.search(
        r"#obSubnav\s+\.container-fluid\s*\{[^}]*padding-inline:",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.ob-subnav-resume-btn\s*\{[^}]*padding-inline:",
        css,
        flags=re.DOTALL,
    )
    assert "overflow-x: auto" not in css


def test_icon_only_mobile_subnav_controls_have_tooltips():
    html = _read(BASE_HTML)

    # Workspace rail dropdowns are gone; app switcher + Close/Save keep titles.
    switcher = re.search(
        r'<button\b[^>]*id="obAppSwitcher"[^>]*>',
        html,
        flags=re.DOTALL,
    )
    assert switcher and 'title="' in switcher.group(0)
    assert re.search(r'class="[^"]*\bob-subnav-close-btn\b[^"]*"[^>]*title="', html)
    assert re.search(r'class="[^"]*\bob-subnav-save-btn\b[^"]*"[^>]*title="', html)


def test_expired_lease_resume_action_uses_the_responsive_label():
    js = _read(EDIT_LOCK_JS)

    assert '<span class="ob-subnav-label">Resume</span>' in js
