"""Contract: New Domain dialog rejects spaces / special characters."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"


def _source() -> str:
    return UTILS_JS.read_text(encoding="utf-8")


def test_new_domain_dialog_enforces_alphanumeric_camelcase():
    source = _source()
    assert "function isValidDomainName" in source
    assert "function enforceDomainNameCamelCase" in source
    assert "/^[A-Z][a-zA-Z0-9]*$/" in source
    assert "no spaces or special characters" in source
    start = source.index("function showNewDomainDialog")
    end = source.index("window.showNewDomainDialog", start)
    body = source[start:end]
    assert "isValidDomainName(name)" in body
    assert "enforceDomainNameCamelCase" in body
