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
