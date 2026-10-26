"""Settings-specific L2 behavior for the domain return control.

The L2 (`#obSubnav`) remains a single shared slot with mutually exclusive
presentations:
- Settings + loaded domain: show only "Back to domain"
- Settings + no domain: hide all L2
- Non-settings + loaded domain: show normal workspace rail
- Non-settings + no domain: hide all L2
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
BREADCRUMB_JS = REPO_ROOT / "src/front/static/global/js/breadcrumb.js"
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"


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


def test_reads_settings_page_from_body_dataset():
    body = _update_domain_menu_visibility_body()
    assert "document.body.dataset.page === 'settings'" in body


def test_toggles_settings_return_link_for_settings_with_domain():
    body = _update_domain_menu_visibility_body()
    assert "settingsDomainReturnNav" in body
    assert "settingsDomainReturnNav.classList.toggle('d-none', !showSettingsReturn);" in body


def test_toggles_workspace_rail_for_non_settings_with_domain():
    body = _update_domain_menu_visibility_body()
    assert "domainWorkspaceSubnavNav" in body
    assert "domainWorkspaceSubnavNav.classList.toggle('d-none', !showWorkspaceRail);" in body


def test_subnav_visibility_is_driven_by_the_four_state_combinations():
    body = _update_domain_menu_visibility_body()
    assert "const showSettingsReturn = isSettingsPage && hasDomain;" in body
    assert "const showWorkspaceRail = !isSettingsPage && hasDomain;" in body
    assert "subnav.classList.toggle('d-none', !showSettingsReturn && !showWorkspaceRail);" in body
    assert "document.querySelectorAll('[data-subnav-domain-chrome]').forEach((el) => {" in body
    assert "el.classList.toggle('d-none', !showWorkspaceRail);" in body


def test_is_settings_page_flag_is_computed_once_at_function_top():
    body = _update_domain_menu_visibility_body()
    assert "const isSettingsPage = document.body.dataset.page === 'settings';" in body


def test_breadcrumb_never_reveals_on_settings_pages():
    js = _read(BREADCRUMB_JS)
    init = re.search(r"init\(\) \{(.*?)\n    \},", js, re.DOTALL)
    assert init is not None, "Breadcrumb.init not found"
    body = init.group(1)

    settings_guard = "if (document.body.dataset.page === 'settings') return;"
    assert settings_guard in body
    assert body.index(settings_guard) < body.index("wrap.classList.remove('d-none');")


def test_settings_return_link_matches_sidebar_width_on_desktop():
    css = _read(MAIN_CSS)
    html = _read(BASE_HTML)
    rule = _rule(css, ".ob-subnav-settings-return-link")

    assert 'id="settingsDomainReturnLink"' in html
    assert 'class="ob-subnav-settings-return-link ob-settings-domain-return-link"' in html
    assert "width: 200px" in rule
    assert "min-width: 200px" in rule
    assert "max-width: 200px" in rule
    assert "justify-content: center" in rule


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
