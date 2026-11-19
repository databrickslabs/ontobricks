"""Sidebar must paint collapsed by default — no expand-then-collapse flash."""

import re
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


def test_sidebar_toggle_lives_fixed_left_of_context_name_in_l2():
    """Collapse control is a fixed L2 control left of the Context crumb, not inside the rail."""
    html = _read(BASE_HTML)
    js = _read(SIDEBAR_JS)
    main_css = (REPO_ROOT / "src/front/static/global/css/main.css").read_text(encoding="utf-8")

    assert 'id="obSidebarToggle"' in html
    assert html.index("obSidebarToggle") < html.index("domainContextTrigger")
    assert "ob-subnav-sidebar-toggle" in html
    # Stay visible on Settings too (not tied to domain-only chrome).
    toggle_li = re.search(
        r'<li\b[^>]*id="obSidebarToggleItem"[^>]*>',
        html,
    )
    assert toggle_li, "Missing #obSidebarToggleItem"
    assert "data-subnav-domain-chrome" not in toggle_li.group(0)

    setup = js[js.index("_setupCollapse: function") :]
    assert "obSidebarToggle" in setup
    assert "insertBefore(btn, nav.firstChild)" not in setup
    assert "nav.querySelector('.sidebar-toggle')" not in setup

    assert ".ob-subnav-sidebar-toggle" in main_css
    assert not re.search(r"\.ob-subnav-sidebar-toggle:hover\s*\{", main_css)


def test_sidebar_toggle_icon_is_centred_on_sidebar_icon_column():
    """The L2 toggle icon uses the same size and centre line as the sidebar nav icons.

    Sidebar icon centre (from the shell edge): shell padding 0.25rem + nav-link
    margin 0.25rem + border-left 3px + padding-left 0.5rem + half of the 20px
    icon column (10px) = ~29px. The L2 button is 2.25rem wide and starts after
    the 0.25rem container inset, so its margin-left must be ~0.44rem.
    """
    main_css = (REPO_ROOT / "src/front/static/global/css/main.css").read_text(encoding="utf-8")
    sidebar_css = _read(SIDEBAR_CSS)

    nav_icon = re.search(r"\.sidebar-nav \.nav-link i\s*\{([^}]*)\}", sidebar_css)
    assert nav_icon and "font-size: 0.95rem" in nav_icon.group(1)

    toggle_icon = re.search(r"\.ob-subnav-sidebar-toggle i\s*\{([^}]*)\}", main_css)
    assert toggle_icon and "font-size: 0.95rem" in toggle_icon.group(1)

    toggle_item = re.search(r"\.ob-subnav-sidebar-toggle-item\s*\{([^}]*)\}", main_css)
    assert toggle_item and re.search(r"margin-left\s*:\s*0\.4\d*rem", toggle_item.group(1))
