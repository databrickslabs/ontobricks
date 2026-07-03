"""Switch-version modal guards for read-only (PUBLISHED / IN-REVIEW) domains.

Regression for https://github.com/databrickslabs/ontobricks/issues/97 —
the L2 Switch popup must stay navigable on read-only versions while the
save-before-switch option is disabled.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_permissions_css_exempts_switch_version_select_from_read_only_gate():
    """#switchVersionSelect must stay interactive when body.read-only-version is set."""
    css = _read(PERMISSIONS_CSS)
    assert "#switchVersionSelect" in css
    read_only_select_rule = re.search(
        r"body:is\(\.read-only-version[^\n]+select:not\([^;]+\)",
        css,
    )
    assert read_only_select_rule, "read-only select disable rule not found"
    rule = read_only_select_rule.group(0)
    assert "#switchVersionSelect" in rule, (
        "permissions.css must exempt #switchVersionSelect from the read-only "
        "select disable rule (same pattern as #domainVersionSelect)"
    )


def test_navbar_js_disables_save_before_switch_on_read_only_versions():
    """Switch modal must gate save-first on lifecycle editability."""
    js = _read(NAVBAR_JS)
    assert "function isSwitchSaveAllowed()" in js
    assert "function configureSwitchSaveOption()" in js
    assert "configureSwitchSaveOption();" in js
    assert "isSwitchSaveAllowed()" in js
    assert "switchSaveFirstHint" in js
    assert "saveCheckbox.disabled" in js


def test_navbar_js_confirm_skips_save_when_checkbox_disabled():
    """Confirm handler must not call save when the checkbox is disabled."""
    js = _read(NAVBAR_JS)
    assert "!saveCheckbox.disabled" in js
    assert "&& isSwitchSaveAllowed()" in js
