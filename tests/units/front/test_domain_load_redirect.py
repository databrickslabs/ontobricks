"""Loading a domain always lands on Context → Home."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src" / "front" / "static"


def test_home_has_no_second_open_path():
    """Home is the Registry now: Browse opens contexts via registry.js, so the
    removed All Contexts gateway must not keep its own open path."""
    js = (FRONT / "home" / "js" / "home.js").read_text(encoding="utf-8")
    assert "async function openDomain(" not in js
    assert "loadDomainGateway" not in js


def test_registry_load_goes_to_context_home():
    js = (FRONT / "registry" / "js" / "registry.js").read_text(encoding="utf-8")
    start = js.index("async function loadRegistryDomainVersion(")
    end = js.index("async function openVersionInValidation(")
    body = js[start:end]
    assert "navigateToDomainGeneral()" in body or "navigateToDomainHome()" in body
    assert "window.location.reload()" not in body
    assert "/domain/?section=home" in body
    assert "/domain/?section=information" not in body
    assert "/domain/?section=review" not in body


def test_version_list_load_goes_to_context_home():
    js = (FRONT / "domain" / "js" / "domain-versions.js").read_text(encoding="utf-8")
    assert js.count("navigateToDomainGeneral()") + js.count("navigateToDomainHome()") >= 2
    assert "/domain/?section=home" in js
    assert "window.location.reload()" not in js


def test_navbar_navigate_helper_targets_home():
    js = (FRONT / "global" / "js" / "navbar.js").read_text(encoding="utf-8")
    assert "DOMAIN_HOME_HREF = '/domain/?section=home'" in js
    assert "function navigateToDomainHome()" in js
    home_fn = js.split("function navigateToDomainHome()", 1)[1].split(
        "\nfunction ", 1
    )[0]
    assert "DOMAIN_HOME_HREF" in home_fn
    assert "section === 'home'" in home_fn
