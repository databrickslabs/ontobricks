"""Settings/Home domain-chrome behavior for the return control.

`#obSubnav` is the always-visible unified app bar. Domain chrome inside it is
mutually exclusive:
- Settings or Home + loaded domain: show "Back to context" (hide workspace rail)
- Settings or Home + no domain: hide return + workspace rail
- Other pages + loaded domain: show workspace rail (Context / Close / Save / switcher)
- Other pages + no domain: hide workspace rail
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
SIDEBAR_LAYOUT_CSS = REPO_ROOT / "src/front/static/global/css/sidebar-layout.css"
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
HOME_HTML = REPO_ROOT / "src/front/templates/home.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _rule(css: str, selector: str) -> str:
    match = re.search(rf"{re.escape(selector)}\s*\{{([^}}]*)\}}", css, re.DOTALL)
    assert match is not None, f"Missing CSS rule for {selector}"
    return match.group(1)


def _mobile_block(css: str) -> str:
    match = re.search(
        r"@media\s*\(max-width:\s*767\.98px\)\s*\{(.*?)\n\}",
        css,
        flags=re.DOTALL,
    )
    assert match is not None, "No mobile breakpoint in main.css"
    return match.group(1)


def _update_domain_menu_visibility_body() -> str:
    js = _read(NAVBAR_JS)
    match = re.search(
        r"function updateDomainMenuVisibility\(hasDomain\) \{(.*?)\n\}",
        js,
        re.DOTALL,
    )
    assert match is not None, "updateDomainMenuVisibility not found"
    return match.group(1)


def test_reads_context_return_pages_from_body_dataset():
    body = _update_domain_menu_visibility_body()
    assert "page === 'settings'" in body
    assert "page === 'home'" in body


def test_home_declares_page_id():
    assert '{% block page_id %}home{% endblock %}' in _read(HOME_HTML)


def test_toggles_settings_return_link_for_settings_with_domain():
    body = _update_domain_menu_visibility_body()
    assert "settingsDomainReturnNav" in body
    assert "settingsDomainReturnNav.classList.toggle('d-none', !showSettingsReturn);" in body


def test_toggles_domain_chrome_for_non_settings_with_domain():
    body = _update_domain_menu_visibility_body()
    # Workspace dropdown rail is gone; domain chrome (context + app switcher +
    # Close/Save) toggles via data-subnav-domain-chrome.
    assert "domainWorkspaceSubnavNav" not in body
    assert "document.querySelectorAll('[data-subnav-domain-chrome]').forEach((el) => {" in body
    assert "el.classList.toggle('d-none', !showWorkspaceRail);" in body


def test_subnav_visibility_is_driven_by_the_four_state_combinations():
    body = _update_domain_menu_visibility_body()
    assert "const showSettingsReturn = isContextReturnPage && hasDomain;" in body
    assert "const showWorkspaceRail = !isContextReturnPage && hasDomain;" in body
    # Unified bar stays visible; only domain chrome / return toggle.
    assert "subnav.classList.toggle('d-none'" not in body
    assert "document.querySelectorAll('[data-subnav-domain-chrome]').forEach((el) => {" in body
    assert "el.classList.toggle('d-none', !showWorkspaceRail);" in body


def test_is_context_return_page_flag_is_computed_once_at_function_top():
    body = _update_domain_menu_visibility_body()
    assert "const isContextReturnPage = page === 'settings' || page === 'home';" in body


def test_settings_return_link_sits_right_of_sidebar_column_on_desktop():
    css = _read(MAIN_CSS)
    html = _read(BASE_HTML)
    rule = _rule(css, ".ob-subnav-settings-return-link")
    sidebar_css = _read(SIDEBAR_LAYOUT_CSS)

    assert 'id="settingsDomainReturnLink"' in html
    assert 'class="ob-subnav-settings-return-link ob-settings-domain-return-link"' in html
    # Natural width: the "Back to {name} V{n}" label is never shortened.
    assert "width: auto" in rule
    assert "max-width: none" in rule
    assert "white-space: nowrap" in rule
    assert "width: 200px" not in rule
    assert "justify-content: flex-start" in rule
    assert "text-align: left" in rule
    assert "background: transparent" in rule
    assert "border: 0" in rule
    # Anchored to the content column (right of the sidebar), only where a
    # sidebar exists; the width variable follows the collapse state.
    anchored = re.search(
        r"html:has\(\.sidebar-layout\)\s+#settingsDomainReturnNav\s*\{([^}]*)\}",
        css,
    )
    assert anchored, "Return link must anchor to the sidebar column on desktop"
    assert "position: absolute" in anchored.group(1)
    assert "left: calc(var(--ob-sidebar-width, 200px) + 0.5rem)" in anchored.group(1)
    assert "--ob-sidebar-width: 200px" in sidebar_css
    assert "html.ob-sidebar-collapsed:has(.sidebar-layout)" in sidebar_css
    assert "--ob-sidebar-width: 48px" in sidebar_css


def test_settings_return_link_resets_fixed_width_on_mobile():
    mobile = _mobile_block(_read(MAIN_CSS))
    assert re.search(
        r"\.ob-subnav-settings-return-link\s*\{[^}]*width\s*:\s*auto",
        mobile,
    )
    assert re.search(
        r"\.ob-subnav-settings-return-link\s*\{[^}]*min-width\s*:\s*0",
        mobile,
    )
    assert re.search(
        r"\.ob-subnav-settings-return-link\s*\{[^}]*max-width\s*:\s*none",
        mobile,
    )


def test_settings_return_label_shows_context_name_and_version():
    """The visible return link reads "Back to {name} V{version}" when a context is open."""
    js = _read(NAVBAR_JS)
    html = _read(BASE_HTML)
    css = _read(MAIN_CSS)

    assert 'id="settingsDomainReturnLabel"' in html
    assert "document.getElementById('settingsDomainReturnLabel')" in js
    assert "`Back to ${domainName} V${version}`" in js
    assert "'Back to context'" in js
    # No truncation: the label keeps its full text on one line.
    label = _rule(css, ".ob-subnav-settings-return-link > span")
    assert "white-space: nowrap" in label
    assert "text-overflow" not in label
    assert "ellipsis" not in label
