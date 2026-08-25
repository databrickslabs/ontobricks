"""Contract: the ontology split panel mirrors the KG Explorer canvas layout.

Ontology Designer / Entities / Relationships share one right-hand detail panel.
It must read as the same control as the Knowledge Graph Explorer panel: a
permanently visible card next to the canvas, separated by a 0.5rem gutter and a
slim drag handle, resizable between 200px and 600px.
"""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-shared-panels.css"
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
SIGMAGRAPH_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"
MAP_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-map.css"
ASSISTANT_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-assistant.js"
FRONTEND_RULE = REPO_ROOT / ".cursor/11-frontend-design.mdc"

# Geometry shared with the Explorer, kept in one place so drift is one failure.
PANEL_MIN_WIDTH = "200px"
RESIZE_MIN = "200"
RESIZE_MAX = "900"
LAYOUT_GAP = "0.5rem"

# The ontology panel hosts edit forms, so it defaults wider than the Explorer's
# read-only details panel — just enough for the 4-tab strip to stay on one line.
PANEL_WIDTH = "420px"
EXPLORER_PANEL_WIDTH = "320px"
FORM_TAB_STRIP_WIDTH = 374


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _block(css: str, selector: str) -> str:
    """Return the declaration block of the rule listing `selector`.

    Handles comma-separated selector lists, where the wanted selector is often
    not the one adjacent to the brace.
    """
    stripped = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    for selectors, declarations in re.findall(
        r"([^{}]+)\{([^{}]*)\}", stripped, flags=re.DOTALL
    ):
        if any(part.strip() == selector for part in selectors.split(",")):
            return declarations
    raise AssertionError(f"Missing rule for {selector}")


def _declaration(block: str, prop: str) -> str | None:
    match = re.search(rf"(?<![a-z-]){re.escape(prop)}\s*:\s*([^;]+);", block)
    return match.group(1).strip() if match else None


def test_detail_panel_is_permanently_visible():
    """The panel is part of the layout, not a drawer that collapses to zero."""
    css = _read(PANELS_CSS)
    block = _block(css, ".shared-detail-panel")

    assert _declaration(block, "width") == PANEL_WIDTH
    assert _declaration(block, "min-width") == PANEL_MIN_WIDTH
    assert _declaration(block, "max-width") == "50%"


def test_default_width_fits_the_form_tab_strip_on_one_line():
    """The tab strip has `flex-wrap: nowrap`, so a narrower default clips it."""
    css = _read(PANELS_CSS)
    panel = _block(css, ".shared-detail-panel")
    body = _block(css, ".shared-detail-panel .panel-body")

    padding = _declaration(body, "padding")
    assert padding == "0.75rem", "width budget below assumes 12px padding a side"

    width = int(_declaration(panel, "width").removesuffix("px"))
    # 2px borders + 24px padding, and the strip must not be the thing that
    # decides the width — leave a little slack for font rendering.
    assert width >= FORM_TAB_STRIP_WIDTH + 26
    assert width <= FORM_TAB_STRIP_WIDTH + 26 + 60, "wider than the tabs need"


def test_no_panel_open_width_override_remains():
    """`width: 0` + `.panel-open` was the old drawer mechanic — it must be gone."""
    css = _read(PANELS_CSS)

    panel_open_width = re.search(
        r"\.panel-open\s+\.shared-detail-panel[^{]*\{[^}]*width\s*:",
        css,
        flags=re.DOTALL,
    )
    assert panel_open_width is None, "panel-open must no longer drive panel width"


def test_split_layout_uses_explorer_gutter():
    """Canvas and panel are separated by the same 0.5rem gutter as the Explorer."""
    css = _read(PANELS_CSS)
    block = _block(css, "#ontology-map-wrapper.has-detail-panel")

    assert _declaration(block, "gap") == LAYOUT_GAP


def test_panes_are_framed_and_the_shell_is_not():
    """Two side-by-side cards, like the Explorer — not one card around both."""
    css = _read(PANELS_CSS)

    shell = _block(css, ".ob-split-shell")
    assert _declaration(shell, "background") == "transparent"
    assert _declaration(shell, "box-shadow") == "none"

    pane = _block(css, ".ob-split-pane")
    panel = _block(css, ".shared-detail-panel")
    for prop in ("border", "border-radius"):
        assert _declaration(pane, prop) == _declaration(panel, prop), prop


def test_panel_is_hosted_on_the_wrapper_not_the_canvas():
    """As a sibling of the canvas pane, so the pane itself can be framed."""
    js = _read(PANELS_JS)

    assert re.search(r"'map-section':\s*'ontology-map-wrapper'", js)
    assert "'map-section': 'ontology-map-container'" not in js


def test_resize_handle_matches_explorer_and_stays_visible():
    """8px hit area, 3px x 40px grip, always displayed — same as #sgResizeHandle."""
    css = _read(PANELS_CSS)
    handle = _block(css, ".detail-panel-resize-handle")

    assert _declaration(handle, "width") == "8px"
    assert _declaration(handle, "display") == "flex"
    assert _declaration(handle, "cursor") == "col-resize"

    bar = _block(css, ".detail-panel-resize-handle .resize-handle-bar")
    assert _declaration(bar, "width") == "3px"
    assert _declaration(bar, "height") == "40px"


def test_split_panel_css_uses_clarity_tokens_not_bootstrap_blue():
    """The handle used --bs-primary (#0d6efd), which is outside the palette."""
    css = _read(PANELS_CSS)

    assert "--bs-primary" not in css
    assert "0d6efd" not in css.lower()


def test_resize_clamp_leaves_room_for_the_default_width():
    """A clamp below the default width would snap the panel on first drag."""
    js = _read(PANELS_JS)

    assert re.search(
        rf"Math\.max\(\s*{RESIZE_MIN}\s*,\s*Math\.min\(\s*{RESIZE_MAX}\s*,",
        js,
    ), "panel resize must clamp to the documented bounds"
    assert int(RESIZE_MAX) >= int(PANEL_WIDTH.removesuffix("px"))
    assert int(RESIZE_MIN) <= int(PANEL_MIN_WIDTH.removesuffix("px"))


def test_panels_are_created_eagerly_so_the_canvas_is_sized_once():
    """D3 reads container.clientWidth once, so the panel must exist beforehand."""
    js = _read(PANELS_JS)

    assert "function ensureDetailPanels(" in js
    assert re.search(r"DOMContentLoaded[\s\S]{0,200}ensureDetailPanels\(\)", js)


def test_closing_the_panel_restores_the_placeholder():
    """Closing returns to the 'pick something' state instead of collapsing."""
    js = _read(PANELS_JS)

    assert "function renderPanelPlaceholder(" in js
    assert re.search(
        r"function closeSharedPanel\([^)]*\)\s*\{[\s\S]*?renderPanelPlaceholder\(",
        js,
    )


def test_panel_has_no_close_button():
    """A permanent panel can't be closed, so the cross would be a dead control."""
    js = _read(PANELS_JS)
    css = _read(PANELS_CSS)

    assert "sharedClosePanelBtn" not in js
    assert "panel-close-btn" not in js
    assert "panel-close-btn" not in css


def test_canvas_background_click_clears_the_panel():
    """Clicking empty canvas is the only way out, so it must flush a pending edit."""
    js = _read(MAP_JS)

    background_handler = re.search(
        r"svg\.on\('click',\s*function\s*\([^)]*\)\s*\{(.*?)\n    \}\);",
        js,
        flags=re.DOTALL,
    )
    assert background_handler, "canvas background click handler not found"
    assert "guardedCloseSharedPanel" in background_handler.group(1)


def test_panel_body_is_the_only_scroller():
    """An inner scrolling box put a second scrollbar on the Attributes tab."""
    css = _read(PANELS_CSS)
    js = _read(PANELS_JS)

    assert "panel-box-scroll" not in css
    assert "panel-box-scroll" not in js

    body = _block(css, ".shared-detail-panel .panel-body")
    assert _declaration(body, "overflow-y") == "auto"

    # The form grows past the body rather than shrinking to it, so the overflow
    # lands on the body instead of on a box inside the form.
    form = _block(css, ".shared-detail-panel .panel-body #sharedEntityForm")
    assert _declaration(form, "flex") == "1 0 auto"


def test_designer_insets_match_the_explorer():
    """Both canvases must sit the same distance from the sidebar."""
    designer = _read(MAP_CSS)
    explorer = _read(SIGMAGRAPH_CSS)

    designer_wrapper = _block(designer, ".sidebar-content:has(#map-section.active)")
    explorer_wrapper = _block(
        explorer, ".sidebar-content:has(#sigmagraph-section.active)"
    )
    assert _declaration(designer_wrapper, "padding") == _declaration(
        explorer_wrapper, "padding"
    ) == "0"

    section = _block(designer, "#map-section.active")
    assert _declaration(section, "padding") == "1rem"
    assert _declaration(section, "padding-bottom") == "0"


def test_placeholder_shares_the_explorer_empty_state_styling():
    css = _read(PANELS_CSS)
    block = _block(css, ".shared-detail-panel .panel-placeholder")

    assert _declaration(block, "display") == "flex"
    assert _declaration(block, "justify-content") == "center"


def test_assistant_offset_tracks_the_permanent_panel():
    """The FAB offset can no longer key off `panel-open`, which is always absent."""
    js = _read(ASSISTANT_JS)

    assert not re.search(
        r"classList\.contains\('panel-open'\)", js
    ), "offset watcher must measure the panel directly"


def test_explorer_reference_values_are_unchanged():
    """Guard the source of truth this contract is copied from."""
    css = _read(SIGMAGRAPH_CSS)
    block = _block(css, "#sigmagraph-section .details-panel")

    assert _declaration(block, "width") == EXPLORER_PANEL_WIDTH
    assert _declaration(block, "min-width") == PANEL_MIN_WIDTH


def test_frontend_rule_documents_the_split_layout():
    rule = _read(FRONTEND_RULE)

    assert "split-panel" in rule.lower()
    assert PANEL_WIDTH in rule
