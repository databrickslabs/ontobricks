"""Graph Explorer pane separator: same thin divider as Ontology / Mapping Studio."""

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMAGRAPH_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"
HANDLE = "#sigmagraph-section .resize-handle"


def _block(css: str, selector: str) -> str:
    match = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert match, selector
    return match.group(1)


def _declaration(block: str, prop: str) -> str:
    match = re.search(r"(?:^|;|\s)" + re.escape(prop) + r"\s*:\s*([^;]+);", block)
    return match.group(1).strip() if match else ""


def test_handle_is_a_1px_track_overlapping_half_the_gutter():
    handle = _block(SIGMAGRAPH_CSS.read_text(encoding="utf-8"), HANDLE)
    assert _declaration(handle, "width") == "1px"
    assert _declaration(handle, "margin-left") == "-0.25rem"
    assert _declaration(handle, "margin-right") == "-0.25rem"
    assert _declaration(handle, "cursor") == "col-resize"


def test_handle_keeps_a_wider_invisible_hit_area():
    css = SIGMAGRAPH_CSS.read_text(encoding="utf-8")
    hit = _block(css, HANDLE + "::before")
    assert _declaration(hit, "inset") == "0 -4px"


def test_grip_is_a_hairline_in_token_colours():
    css = SIGMAGRAPH_CSS.read_text(encoding="utf-8")
    bar = _block(css, HANDLE + " .resize-handle-bar")
    assert _declaration(bar, "width") == "0.5px"
    assert _declaration(bar, "height") == "40px"
    assert _declaration(bar, "background") == "var(--db-border)"
    active = _block(css, HANDLE + ".active .resize-handle-bar")
    assert _declaration(active, "background") == "var(--db-primary)"
