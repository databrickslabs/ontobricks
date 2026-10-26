"""Regression contracts for retaining the loaded-domain navbar badge."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"


def _navbar_js() -> str:
    return NAVBAR_JS.read_text(encoding="utf-8")


def test_transient_navbar_failure_restores_last_confirmed_domain():
    js = _navbar_js()

    assert "restoreLastConfirmedDomainInfo()" in js
    assert "updateDomainMenuVisibility(false);" not in js


def test_successful_domain_state_is_remembered_until_closed():
    js = _navbar_js()

    assert "rememberDomainInfo(data);" in js
    assert "clearRememberedDomainInfo();" in js
    assert "sessionStorage.setItem(DOMAIN_INFO_STORAGE_KEY" in js
    assert "sessionStorage.removeItem(DOMAIN_INFO_STORAGE_KEY)" in js


def test_remembered_domain_is_restored_before_async_navbar_refresh():
    js = _navbar_js()
    match = re.search(r"function initNavbar\(\) \{(.*?)\n\}", js, re.DOTALL)

    assert match is not None
    body = match.group(1)
    assert body.index("restoreLastConfirmedDomainInfo();") < body.index(
        "loadNavbarState();"
    )


def test_domain_load_refreshes_badge_before_reload():
    """After loading a new domain the navbar badge must be refreshed from the
    server session *before* navigation, so the badge never keeps showing
    the previous domain during the delay."""
    js = _navbar_js()
    match = re.search(r"async function doDomainLoad\([^)]*\) \{(.*?)\n\}", js, re.DOTALL)

    assert match is not None
    body = match.group(1)
    # Stale remembered info is dropped, then the fresh state is fetched and
    # applied to the badge before Domain → General navigation runs.
    assert "clearRememberedDomainInfo();" in body
    assert "await loadNavbarState();" in body
    assert "navigateToDomainGeneral()" in body
    assert body.index("await loadNavbarState();") < body.index(
        "navigateToDomainGeneral()"
    )
    assert body.index("clearRememberedDomainInfo();") < body.index(
        "await loadNavbarState();"
    )
    assert "location.reload()" not in body


def test_domain_load_navigates_to_general():
    """Loading a domain must open Domain → General, not reload the current page."""
    js = _navbar_js()
    match = re.search(
        r"function navigateToDomainGeneral\(\) \{(.*?)\n\}", js, re.DOTALL
    )

    assert match is not None
    body = match.group(1)
    assert "DOMAIN_GENERAL_HREF" in js
    assert "/domain/?section=information" in js
    assert "window.location.href = DOMAIN_GENERAL_HREF;" in body
    assert "path === '/domain'" in body
    assert "handleImportResponse" in js
    import_match = re.search(
        r"function handleImportResponse\(data\) \{(.*?)\n\}", js, re.DOTALL
    )
    assert import_match is not None
    assert "navigateToDomainGeneral()" in import_match.group(1)
