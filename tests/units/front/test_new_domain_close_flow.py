"""Contracts for closing the current domain before creating another."""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"

pytestmark = pytest.mark.unit


def _source() -> str:
    return NAVBAR_JS.read_text(encoding="utf-8")


def _function(name: str, next_name: str) -> str:
    source = _source()
    return source.split(f"async function {name}", maxsplit=1)[1].split(
        f"async function {next_name}", maxsplit=1
    )[0]


def test_new_domain_closes_loaded_domain_before_opening_dialog():
    body = _function("domainNew()", "domainSave()")
    close_pos = body.index("await closeCurrentDomain({ navigate: false })")
    dialog_pos = body.index("await showNewDomainDialog()")
    assert close_pos < dialog_pos


def test_new_domain_aborts_when_close_does_not_complete():
    body = _function("domainNew()", "domainSave()")
    assert "if (!closed) return;" in body


def test_close_helper_saves_only_when_allowed_and_requested():
    body = _function("closeCurrentDomain(options = {})", "domainClose()")
    assert "showCloseDomainDialog({ allowSave })" in body
    assert "choice === 'save' && allowSave" in body
    assert "return closeDomainSession({ navigate });" in body


def test_regular_close_keeps_home_navigation():
    body = _function("domainClose()", "closeDomainSession(options = {})")
    assert "closeCurrentDomain({ navigate: true })" in body


def test_in_place_close_clears_the_loaded_domain_from_navigation():
    body = _function("closeDomainSession(options = {})", "showCloseDomainDialog")
    assert "applyDomainInfo({});" in body
    assert body.index("applyDomainInfo({});") < body.index("hideDomainLoading();")


def test_close_dialog_omits_save_action_when_saving_is_unavailable():
    source = _source()
    body = source.split("function showCloseDomainDialog(options = {})", 1)[1]
    body = body.split("// ==========================================", 1)[0]
    assert "allowSave ?" in body
    assert "const saveBtn = document.getElementById('closeSaveBtn')" in body
    assert "if (saveBtn)" in body
