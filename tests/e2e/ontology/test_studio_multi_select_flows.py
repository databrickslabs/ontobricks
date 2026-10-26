"""
E2E — Ontology Studio › multi-select regressions (real browser pointer input).

A browser fires ``click`` on the common ancestor right after the ``pointerup``
that ends a Ctrl/Cmd marquee. Source-level contracts cannot see that event
ordering, so these flows drive the real D3 canvas with Playwright:

* a Ctrl/Cmd marquee keeps the selection it just made (the trailing click must
  not clear it);
* a marquee that contains no entity leaves the current selection unchanged;
* the multi-select context menu closes when the selection changes (modifier
  toggle) or on Escape;
* a plain click on empty canvas still clears the selection.

Setup: a throw-away session imports a four-class OWL (one object property)
through the existing ``/ontology/import-owl`` endpoint, then opens
Ontology › Studio (``SidebarNav.switchTo('map')``).
"""

from __future__ import annotations

import json

import pytest


_OWL = """
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@base <http://e2emulti.test/> .

<http://e2emulti.test/>      a owl:Ontology .
<http://e2emulti.test/Alpha> a owl:Class ; rdfs:label "Alpha" .
<http://e2emulti.test/Beta>  a owl:Class ; rdfs:label "Beta" .
<http://e2emulti.test/Gamma> a owl:Class ; rdfs:label "Gamma" .
<http://e2emulti.test/hasBeta> a owl:ObjectProperty ;
    rdfs:domain <http://e2emulti.test/Alpha> ;
    rdfs:range  <http://e2emulti.test/Beta> ;
    rdfs:label  "hasBeta" .
"""

_ALL = ["Alpha", "Beta", "Gamma"]
_MODIFIERS = [
    pytest.param("Control", id="ctrl"),
    pytest.param("Meta", id="cmd"),
]


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    token = cookies.get("csrf_token")
    if token:
        headers["X-CSRF-Token"] = token
    return headers


@pytest.fixture
def studio(page, live_server):
    """Open Ontology Studio with three classes laid out on the D3 canvas."""
    page.set_viewport_size({"width": 1600, "height": 1000})
    page.goto(live_server)
    page.wait_for_load_state("domcontentloaded")
    resp = page.context.request.post(
        f"{live_server}/ontology/import-owl",
        headers=_csrf_headers(page.context),
        data=json.dumps({"content": _OWL}),
    )
    assert resp.status == 200

    page.goto(f"{live_server}/ontology")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function("typeof SidebarNav !== 'undefined'")
    page.evaluate("SidebarNav.switchTo('map')")
    page.wait_for_selector(".map-node", timeout=20000)
    page.wait_for_selector("#ontologyMapLoading", state="hidden")
    # Let the force simulation settle so node centres are stable.
    page.wait_for_timeout(2500)
    return page


def _geometry(page) -> dict:
    return page.evaluate(
        """() => {
            const svg = document.querySelector('#ontology-map-container > svg');
            const s = svg.getBoundingClientRect();
            const nodes = {};
            document.querySelectorAll('.map-node').forEach(n => {
                const r = n.getBoundingClientRect();
                nodes[n.__data__.name] = {
                    x: r.x + r.width / 2,
                    y: r.y + r.height / 2,
                };
            });
            return { svg: { x: s.x, y: s.y, w: s.width, h: s.height }, nodes };
        }"""
    )


def _selected(page) -> list:
    return sorted(
        page.evaluate(
            "() => [...document.querySelectorAll('.map-node.selected')]"
            ".map(n => n.__data__.name)"
        )
    )


def _drag(page, modifier, start, end) -> None:
    """Modifier-held drag using real mouse input, then let the click settle."""
    if modifier:
        page.keyboard.down(modifier)
    page.mouse.move(*start)
    page.mouse.down()
    page.mouse.move(
        (start[0] + end[0]) / 2, (start[1] + end[1]) / 2, steps=4
    )
    page.mouse.move(*end, steps=4)
    page.mouse.up()
    if modifier:
        page.keyboard.up(modifier)
    # The trailing browser ``click`` is dispatched right after pointerup.
    page.wait_for_timeout(250)


def _marquee_around_all(page, modifier) -> None:
    geo = _geometry(page)
    xs = [n["x"] for n in geo["nodes"].values()]
    ys = [n["y"] for n in geo["nodes"].values()]
    _drag(
        page,
        modifier,
        (min(xs) - 90, min(ys) - 70),
        (max(xs) + 90, max(ys) + 70),
    )


def _empty_marquee(page, modifier) -> None:
    geo = _geometry(page)
    ys = [n["y"] for n in geo["nodes"].values()]
    left = geo["svg"]["x"] + 200
    top = max(ys) + 200
    _drag(page, modifier, (left, top), (left + 120, top + 90))


@pytest.mark.parametrize("modifier", _MODIFIERS)
def test_modifier_marquee_keeps_selection_after_browser_click(studio, modifier):
    assert _selected(studio) == []
    _marquee_around_all(studio, modifier)
    assert _selected(studio) == _ALL


@pytest.mark.parametrize("modifier", _MODIFIERS)
def test_empty_modifier_marquee_leaves_selection_unchanged(studio, modifier):
    _marquee_around_all(studio, modifier)
    assert _selected(studio) == _ALL

    _empty_marquee(studio, modifier)
    assert _selected(studio) == _ALL


def test_plain_canvas_click_still_clears_selection(studio):
    _marquee_around_all(studio, "Control")
    assert _selected(studio) == _ALL

    geo = _geometry(studio)
    ys = [n["y"] for n in geo["nodes"].values()]
    studio.mouse.click(geo["svg"]["x"] + 200, max(ys) + 200)
    studio.wait_for_timeout(250)
    assert _selected(studio) == []


def _open_multi_menu(page) -> None:
    geo = _geometry(page)
    alpha = geo["nodes"]["Alpha"]
    page.mouse.click(alpha["x"], alpha["y"], button="right")
    page.wait_for_selector("#mapContextMenu", state="visible")
    assert "3 entities" in page.locator("#mapContextMenu").inner_text()


def test_multi_menu_closes_when_modifier_toggle_changes_selection(studio):
    _marquee_around_all(studio, "Control")
    _open_multi_menu(studio)

    # macOS Control-click reaches the node as ``contextmenu`` (no ``click``),
    # which toggles membership. The open menu would otherwise keep the stale
    # three-name set and could delete the wrong entities.
    studio.evaluate(
        """() => {
            const beta = [...document.querySelectorAll('.map-node')]
                .find(n => n.__data__.name === 'Beta');
            const r = beta.getBoundingClientRect();
            beta.dispatchEvent(new MouseEvent('contextmenu', {
                ctrlKey: true, bubbles: true, cancelable: true,
                clientX: r.x + r.width / 2, clientY: r.y + r.height / 2,
            }));
        }"""
    )
    studio.wait_for_timeout(150)

    assert _selected(studio) == ["Alpha", "Gamma"]
    assert studio.locator("#mapContextMenu").count() == 0


def test_multi_menu_closes_on_escape_and_clears_selection(studio):
    _marquee_around_all(studio, "Control")
    _open_multi_menu(studio)

    studio.keyboard.press("Escape")
    studio.wait_for_timeout(150)

    assert studio.locator("#mapContextMenu").count() == 0
    assert _selected(studio) == []


def test_escape_in_delete_confirmation_keeps_selection(studio):
    """Escape belongs to the Bootstrap modal, not to the Studio selection.

    Bootstrap drops ``.show`` before the document keydown listener runs, so a
    ``.modal.show`` probe alone is not enough.
    """
    _marquee_around_all(studio, "Control")
    _open_multi_menu(studio)
    studio.locator("#mapContextMenu [data-action='delete']").click()
    studio.wait_for_selector(".modal.show", state="visible")
    # Bootstrap only handles Escape once the fade-in finished and it moved
    # focus into the dialog (``shown.bs.modal``).
    studio.wait_for_function(
        "() => document.activeElement && document.activeElement.closest('.modal')"
    )

    studio.keyboard.press("Escape")
    # Bootstrap dismisses the dialog (and drops ``.show``) on this Escape.
    studio.wait_for_selector(".modal.show", state="detached")
    studio.wait_for_timeout(400)

    assert _selected(studio) == _ALL
    assert len(studio.evaluate("OntologyState.config.classes")) == 3


def test_escape_in_studio_without_modal_still_clears_selection(studio):
    _marquee_around_all(studio, "Control")
    assert _selected(studio) == _ALL
    studio.keyboard.press("Escape")
    studio.wait_for_timeout(150)
    assert _selected(studio) == []


def test_escape_outside_studio_keeps_selection(studio):
    _marquee_around_all(studio, "Control")
    assert _selected(studio) == _ALL
    studio.evaluate("SidebarNav.switchTo('entities')")
    studio.wait_for_timeout(300)
    studio.keyboard.press("Escape")
    studio.wait_for_timeout(150)
    assert _selected(studio) == _ALL


def test_select_toolbar_toggles_without_modifier(studio):
    studio.locator("#mapToggleSelect").click()
    studio.wait_for_timeout(100)
    assert studio.locator("#mapToggleSelect").get_attribute("aria-pressed") == "true"

    geo = _geometry(studio)
    studio.mouse.click(geo["nodes"]["Alpha"]["x"], geo["nodes"]["Alpha"]["y"])
    studio.wait_for_timeout(150)
    studio.mouse.click(geo["nodes"]["Beta"]["x"], geo["nodes"]["Beta"]["y"])
    studio.wait_for_timeout(150)
    assert _selected(studio) == ["Alpha", "Beta"]

    _marquee_around_all(studio, None)
    assert _selected(studio) == _ALL
