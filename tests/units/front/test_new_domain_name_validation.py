"""Contract: New Domain dialog rejects spaces / special characters."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"


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


def test_new_domain_fields_remain_editable_while_loaded_domain_is_read_only():
    """Creating another domain must not inherit the loaded domain's write lock."""
    dialog = _source().split("function showNewDomainDialog", maxsplit=1)[1]
    dialog = dialog.split("window.showNewDomainDialog", maxsplit=1)[0]
    assert dialog.count("new-domain-field") == 3

    css = PERMISSIONS_CSS.read_text(encoding="utf-8")
    rule_start = css.index(
        "body:is(.read-only-version, .role-viewer, .read-only-locked)"
        ':not([data-page="digitaltwin"]):not([data-page="settings"]) input'
    )
    generic_field_rule = css[rule_start : css.index("}", rule_start)]
    assert generic_field_rule.count(":not(.new-domain-field)") == 3
