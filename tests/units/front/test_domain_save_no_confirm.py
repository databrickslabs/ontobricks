"""Contract: domain Save persists immediately without a confirmation modal."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"


def test_domain_save_skips_confirmation_modal():
    js = NAVBAR_JS.read_text(encoding="utf-8")
    assert "async function domainSave()" in js
    assert "await doDomainSave()" in js
    assert "id=\"domainSaveModal\"" not in js
    assert "btnConfirmSave" not in js


def test_show_domain_save_dialog_delegates_to_direct_save():
    js = NAVBAR_JS.read_text(encoding="utf-8")
    assert "async function showDomainSaveDialog" in js
    assert "return doDomainSave({ afterSave: opts.afterSave })" in js
