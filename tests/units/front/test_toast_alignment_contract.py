"""Bottom-right toasts: icon, message and close button share one row line."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"


def _show_toast_body() -> str:
    js = UTILS_JS.read_text(encoding="utf-8")
    start = js.index("function _showToast(")
    return js[start : js.index("\n}\n", start)]


def test_toast_icon_is_not_nudged_below_the_text_line():
    """`mt-1` pushed the icon 4px under the first text line."""
    icon = re.search(r"'<i class=\"bi ' \+ iconClass \+ '([^\"]*)\"></i>'", _show_toast_body())
    assert icon, "toast icon markup not found"
    assert "mt-" not in icon.group(1)


def test_toast_row_aligns_items_to_the_first_line():
    assert "d-flex align-items-start" in _show_toast_body()


def test_btn_close_keeps_bootstrap_1em_box():
    """`height: auto` collapsed the empty button to its 7px padding."""
    css = COMPONENTS_CSS.read_text(encoding="utf-8")
    rule = re.search(r"\n\.btn-close\s*\{([^}]*)\}", css).group(1)
    assert "height: 1em" in rule
    assert "height: auto" not in rule
