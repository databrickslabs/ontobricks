"""Sidebar must paint collapsed by default — no expand-then-collapse flash."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
SIDEBAR_JS = REPO_ROOT / "src/front/static/global/js/sidebar-nav.js"
SIDEBAR_INIT = REPO_ROOT / "src/front/static/global/js/sidebar-collapse-init.js"
SIDEBAR_CSS = REPO_ROOT / "src/front/static/global/css/sidebar-layout.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_base_loads_sidebar_collapse_init_before_paint():
    """Pre-paint script is synchronous (no defer) so preference applies before first paint."""
    html = _read(BASE_HTML)
    assert "sidebar-collapse-init.js" in html
    # Isolate the script tag itself (comments must not pollute the check).
    tag_start = html.rfind("<script", 0, html.index("sidebar-collapse-init.js"))
    tag_end = html.index("</script>", html.index("sidebar-collapse-init.js")) + len("</script>")
    tag = html[tag_start:tag_end]
    assert "defer" not in tag
    assert "sidebar-collapse-init.js" in html.split("sidebar-nav.js")[0]


def test_sidebar_collapse_init_defaults_to_collapsed_html_class():
    js = _read(SIDEBAR_INIT)
    assert "ontobricks-sidebar-collapsed" in js
    assert "ob-sidebar-collapsed" in js
    assert "!== '0'" in js or '!== "0"' in js


def test_sidebar_css_honours_prepaint_html_collapsed_class():
    css = _read(SIDEBAR_CSS)
    assert "html.ob-sidebar-collapsed .sidebar-nav" in css
    assert "html:not(.ob-sidebar-ready) .sidebar-nav" in css


def test_sidebar_nav_syncs_html_collapsed_class_on_toggle():
    js = _read(SIDEBAR_JS)
    assert "ob-sidebar-collapsed" in js
    assert "ob-sidebar-ready" in js
    assert "_applyCollapsed:" in js
    toggle = js[js.index("toggleCollapse: function") : js.index("_updateTooltips: function")]
    assert "_applyCollapsed" in toggle
