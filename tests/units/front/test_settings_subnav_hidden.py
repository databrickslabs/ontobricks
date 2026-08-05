"""The L2 subnav (Domain/Ontology/Mapping/Knowledge Graph tabs) must stay
hidden on Settings pages, even when a domain is open in session.

Settings is a cross-domain area with its own left sidebar navigation
(``get_menu('settings')``); the domain-contextual tabs above it are noise
there and were leaking through whenever ``hasDomain`` was true.

``<body data-page="settings">`` is already asserted server-side in
``test_ui_rendering.py::test_body_has_page_id_settings`` (issue #78). These
tests pin the client-side half: that ``updateDomainMenuVisibility`` reads
that same attribute and forces the subnav closed on Settings regardless of
``hasDomain``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


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
    """Must key off the same attribute the server renders for Settings."""
    body = _update_domain_menu_visibility_body()
    assert "document.body.dataset.page === 'settings'" in body


def test_subnav_dnone_toggle_is_forced_on_settings_regardless_of_domain():
    """The `d-none` toggle on #obSubnav must OR in the settings-page check,
    so a truthy hasDomain cannot re-show the subnav on /settings."""
    body = _update_domain_menu_visibility_body()
    toggle = re.search(
        r"subnav\.classList\.toggle\(['\"]d-none['\"],\s*([^)]+)\)",
        body,
    )
    assert toggle is not None, "subnav d-none toggle not found"
    condition = toggle.group(1)
    assert "isSettingsPage" in condition
    assert "!hasDomain" in condition


def test_is_settings_page_flag_is_computed_once_at_function_top():
    body = _update_domain_menu_visibility_body()
    assert "const isSettingsPage = document.body.dataset.page === 'settings';" in body


def test_l1_domain_nav_item_untouched_by_settings_check():
    """Only the L2 subnav is settings-gated — the L1 Domain nav item (path
    breadcrumb near the logo) keeps following hasDomain alone, since it is
    not the "second level navbar" this guard targets."""
    body = _update_domain_menu_visibility_body()
    domain_nav_toggle = re.search(
        r"domainNav\.classList\.toggle\(['\"]d-none['\"],\s*([^)]+)\)",
        body,
    )
    assert domain_nav_toggle is not None
    assert "isSettingsPage" not in domain_nav_toggle.group(1)
