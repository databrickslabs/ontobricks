"""Loading a domain always lands on Domain → General."""

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


def test_registry_load_goes_to_general():
    js = (FRONT / "registry" / "js" / "registry.js").read_text(encoding="utf-8")
    start = js.index("async function loadRegistryDomainVersion(")
    end = js.index("async function openVersionInValidation(")
    body = js[start:end]
    assert "navigateToDomainGeneral()" in body
    assert "window.location.reload()" not in body
    assert "/domain/?section=information" in body
    assert "/domain/?section=review" not in body


def test_version_list_load_goes_to_general():
    js = (FRONT / "domain" / "js" / "domain-versions.js").read_text(encoding="utf-8")
    assert js.count("navigateToDomainGeneral()") >= 2
    assert "window.location.reload()" not in js
