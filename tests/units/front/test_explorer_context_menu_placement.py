"""The Explorer right-click menus stay inside the viewport wherever you click."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
SIGMA_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"


def _place(x, y, menu_w, menu_h, vw=1200, vh=800):
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")
    js = SIGMA_JS.read_text(encoding="utf-8")
    body = js.split("function _placeContextMenu", 1)[1].split("\n    }\n", 1)[0]
    margin = re.search(r"var MENU_MARGIN = (\d+);", js).group(1)
    script = f"""
var MENU_MARGIN = {margin};
var window = {{ innerWidth: {vw}, innerHeight: {vh} }};
function _placeContextMenu{body}
}}
var menu = {{ style: {{}}, offsetWidth: {menu_w}, offsetHeight: {menu_h} }};
_placeContextMenu(menu, {x}, {y});
process.stdout.write(JSON.stringify(menu.style));
"""
    out = subprocess.run([node, "-e", script], capture_output=True, text=True, check=True)
    style = json.loads(out.stdout)
    return int(style["left"].rstrip("px")), int(style["top"].rstrip("px")), style


def test_menu_opens_at_the_click_when_it_fits():
    left, top, style = _place(100, 100, 200, 300)
    assert (left, top) == (100, 100)
    assert style["display"] == "block"
    assert style["visibility"] == ""


def test_menu_flips_up_near_the_bottom_edge():
    left, top, _ = _place(100, 780, 200, 300)
    assert top == 780 - 300
    assert top + 300 <= 800


def test_menu_flips_left_near_the_right_edge():
    left, _, _ = _place(1150, 100, 200, 300)
    assert left == 1150 - 200


def test_menu_taller_than_the_space_is_clamped_inside_the_viewport():
    _, top, _ = _place(100, 400, 200, 700)
    assert top >= 8
    assert top + 700 <= 800 - 8


def test_menu_taller_than_the_viewport_is_capped_and_scrolls():
    _, top, style = _place(100, 400, 200, 2000)
    assert style["maxHeight"] == "784px"
    assert top == 8


def test_both_menus_use_the_placement_helper():
    js = SIGMA_JS.read_text(encoding="utf-8")
    node_menu = js.split("function _showNodeContextMenu", 1)[1].split("\n    function ", 1)[0]
    canvas_menu = js.split("function _showContextMenu", 1)[1].split("\n    function ", 1)[0]
    assert "_placeContextMenu(nodeMenu" in node_menu
    assert "_placeContextMenu(menu" in canvas_menu


def test_long_menus_scroll_instead_of_overflowing():
    css = SIGMA_CSS.read_text(encoding="utf-8")
    for menu in ("#sgContextMenu", "#sgNodeContextMenu"):
        block = css.split(menu + " {", 1)[1].split("}", 1)[0]
        assert "overflow-y: auto" in block
