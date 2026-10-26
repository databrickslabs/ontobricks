"""Contracts for the shared section layout inside the entity panel tabs.

Every section renders the same way: a header (icon, title, count, optional
effect chip, "+ Add"), a one-line subtitle with the long help behind an info
button, then flat item rows or an actionable empty state. Descriptions are
borderless fields that show a border on focus. Equivalent To / Disjoint With
are chip pickers driving the original (hidden) multi-selects.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
PANELS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-shared-panels.css"

_HELPERS_START = "// ---- Panel section helpers ----"
_HELPERS_END = "// ---- End panel section helpers ----"
_ORDER = ("details", "attributes", "model", "behaviour", "links")


def _js() -> str:
    return PANELS_JS.read_text(encoding="utf-8")


def _css() -> str:
    return PANELS_CSS.read_text(encoding="utf-8")


def _entity_form(js: str) -> str:
    start = js.index('<form id="sharedEntityForm">')
    return js[start : js.index("</form>", start)]


def _pane(js: str, tab: str) -> str:
    form = _entity_form(js)
    start = form.index(f'data-form-tab-content="{tab}"')
    nxt = _ORDER.index(tab) + 1
    end = form.index(f'data-form-tab-content="{_ORDER[nxt]}"', start) if nxt < len(_ORDER) else len(form)
    return form[start:end]


def _run_helpers(expr: str):
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")
    js = _js()
    helpers = js[js.index(_HELPERS_START) : js.index(_HELPERS_END)]
    stub = (
        "function escapeHtml(t){return String(t==null?'':t)"
        ".replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')"
        ".replace(/\"/g,'&quot;').replace(/'/g,'&#039;');}\n"
    )
    script = stub + helpers + f"\nprocess.stdout.write(JSON.stringify({expr}));"
    out = subprocess.run([node, "-e", script], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


# ---------------------------------------------------------------------------
# Helpers (executed with node)
# ---------------------------------------------------------------------------


def test_header_has_icon_title_count_chip_add_and_help():
    html = _run_helpers(
        "panelSectionHeader({targetId: 'sharedEntityActions', icon: 'bi-lightning-charge',"
        " title: 'Functions', chip: 'result', subtitle: 'Run on demand.',"
        " help: 'Takes exactly one parameter.', onAdd: 'openActionSelectorModal()'})"
    )
    assert '<i class="bi bi-lightning-charge me-1"></i>Functions</span>' in html
    assert 'data-section-count-for="sharedEntityActions"' in html
    assert "Returns a result" in html and "bg-secondary-subtle" in html
    assert 'onclick="openActionSelectorModal()"' in html
    assert '<i class="bi bi-plus"></i> Add</button>' in html
    assert 'data-section-add-for="sharedEntityActions"' in html
    assert "Run on demand." in html
    assert 'aria-label="About Functions: Takes exactly one parameter."' in html
    assert "data-panel-passive" in html


def test_graph_write_chip_is_warning_toned_with_icon_and_label():
    html = _run_helpers("panelSectionHeader({targetId: 'x', icon: 'bi-diagram-3', title: 'Rules', chip: 'graphWrite'})")
    assert "bg-warning-subtle" in html
    assert '<i class="bi bi-pencil-square me-1"></i>Writes to the graph' in html


def test_view_only_header_has_no_add_control():
    html = _run_helpers(
        "panelSectionHeader({targetId: 'x', icon: 'bi-x', title: 'T', onAdd: 'go()', viewOnly: true})"
    )
    assert "go()" not in html
    assert "> Add<" not in html


def test_header_menu_renders_a_dropdown_of_add_choices():
    html = _run_helpers(
        "panelSectionHeader({targetId: 'sharedEntityAttributes', icon: 'bi-tags', title: 'Mapped attributes',"
        " menu: [{icon: 'bi-database', label: 'From data assets', onclick: 'openMetadataAttributePicker()'},"
        " {icon: 'bi-pencil', label: 'Manually', onclick: 'addSharedEntityAttribute()'}]})"
    )
    assert 'data-bs-toggle="dropdown"' in html
    assert "dropdown-menu" in html
    assert 'onclick="openMetadataAttributePicker()"' in html
    assert "From data assets" in html and "Manually" in html


def test_header_title_can_label_a_control():
    html = _run_helpers("panelSectionHeader({targetId: 'p', icon: 'bi-diagram-2', title: 'Inherits from', labelFor: 'sharedEntityParent'})")
    assert '<label class="panel-section-title" for="sharedEntityParent">' in html


def test_empty_state_is_actionable_unless_view_only():
    edit = _run_helpers("panelEmptyState('No functions yet.', 'Add a function', 'openActionSelectorModal()', false)")
    assert 'class="panel-empty-state"' in edit
    assert "No functions yet." in edit
    assert 'onclick="openActionSelectorModal()"' in edit and "Add a function" in edit
    view = _run_helpers("panelEmptyState('No functions yet.', 'Add a function', 'openActionSelectorModal()', true)")
    assert "openActionSelectorModal()" not in view


def test_item_row_escapes_text_and_offers_removal():
    html = _run_helpers(
        "panelItemRow({icon: 'bi-lightning-charge', title: '<b>f</b>', meta: 'c.s.f',"
        " removeOnclick: 'removeSharedEntityAction(0)', removeTitle: 'Remove function'})"
    )
    assert "&lt;b&gt;f&lt;/b&gt;" in html
    assert 'class="panel-item"' in html
    assert 'onclick="removeSharedEntityAction(0)"' in html
    assert 'aria-label="Remove function"' in html
    view = _run_helpers("panelItemRow({icon: 'bi-x', title: 't', removeOnclick: 'r()', viewOnly: true})")
    assert "r()" not in view


def test_inline_field_is_a_borderless_textarea_or_plain_text():
    edit = _run_helpers(
        "panelInlineField({value: 'a<b', placeholder: 'What does it do?', oninput: 'onX(0, this.value)'})"
    )
    assert 'class="form-control form-control-sm panel-inline-field"' in edit
    assert "a&lt;b" in edit
    assert 'oninput="onX(0, this.value)"' in edit
    assert 'aria-label="What does it do?"' in edit
    assert _run_helpers("panelInlineField({value: 'hello', viewOnly: true})") == '<div class="panel-item-desc">hello</div>'
    assert _run_helpers("panelInlineField({value: '', viewOnly: true})") == ""


# ---------------------------------------------------------------------------
# Entity form markup
# ---------------------------------------------------------------------------


def test_entity_form_has_no_framed_grey_boxes_or_mixed_add_labels():
    form = _entity_form(_js())
    assert "bg-light rounded border" not in form
    assert "Assign</button>" not in form
    assert "Select</button>" not in form


@pytest.mark.parametrize(
    "tab,count",
    [("attributes", 2), ("model", 4), ("behaviour", 3), ("links", 2)],
)
def test_each_tab_uses_the_shared_section(tab, count):
    pane = _pane(_js(), tab)
    assert pane.count('<section class="panel-section">') == count
    assert pane.count("${panelSectionHeader({") == count


def test_general_puts_icon_and_name_on_one_row():
    pane = _pane(_js(), "details")
    row = pane[pane.index('class="panel-identity-row"') :]
    assert row.index('id="sharedEntityIcon"') < row.index('id="sharedEntityName"')
    assert pane.index('id="sharedEntityName"') < pane.index('id="sharedEntityGroup"')


def test_attributes_add_offers_data_assets_or_manual():
    pane = _pane(_js(), "attributes")
    assert "openMetadataAttributePicker()" in pane
    assert "addSharedEntityAttribute()" in pane
    assert "menu: [" in pane


def test_equivalent_and_disjoint_are_chip_pickers_over_hidden_selects():
    js = _js()
    pane = _pane(js, "model")
    for sel in ("sharedEntityEquivalentTo", "sharedEntityDisjointWith"):
        assert f'id="{sel}"' in pane
        assert f'id="{sel}Chips"' in pane
        assert f'id="{sel}Add"' in pane
        assert f"addSharedAxiomChip('{sel}', this)" in pane
        assert f"renderSharedAxiomChips('{sel}', viewOnly)" in js
    assert 'class="d-none" multiple' in pane
    assert "function renderSharedAxiomChips(" in js
    assert "function addSharedAxiomChip(" in js
    assert "function removeSharedAxiomChip(" in js


@pytest.mark.parametrize(
    "render,empty",
    [
        ("renderSharedEntityAttributes", "No mapped attributes yet."),
        ("renderSharedEntityVirtualAttributes", "No virtual attributes yet."),
        ("renderSharedEntityActions", "No functions yet."),
        ("renderSharedEntityJobs", "No jobs yet."),
        ("renderSharedEntityBusinessRules", "No inference rules yet."),
        ("renderSharedEntityBridges", "No bridges to other domains yet."),
        ("renderSharedEntityDashboard", "No dashboard linked yet."),
        ("renderSharedEntityDataset", "No dataset linked yet."),
    ],
)
def test_render_functions_use_the_empty_state_and_update_the_header(render, empty):
    js = _js()
    body = js[js.index(f"function {render}(") :]
    body = body[: body.index("\n}\n")]
    assert f"panelEmptyState('{empty}'" in body
    assert "_updatePanelSection(" in body


@pytest.mark.parametrize(
    "render", ["renderSharedEntityActions", "renderSharedEntityJobs", "renderSharedEntityVirtualAttributes"]
)
def test_item_descriptions_use_the_borderless_field(render):
    js = _js()
    body = js[js.index(f"function {render}(") :]
    body = body[: body.index("\n}\n")]
    assert "panelInlineField({" in body
    assert "<textarea" not in body


def test_passive_buttons_do_not_mark_the_panel_dirty():
    js = _js()
    tracking = js[js.index("function attachDirtyTracking(") :]
    tracking = tracking[: tracking.index("\n}\n")]
    assert "data-panel-passive" in tracking


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------


def test_section_styles_use_shared_tokens():
    css = _css()
    for selector in (
        ".panel-section",
        ".panel-section-header",
        ".panel-section-subtitle",
        ".panel-item",
        ".panel-empty-state",
        ".panel-inline-field",
        ".panel-axiom-chip",
        ".panel-identity-row",
    ):
        assert selector in css
    empty = css[css.index(".panel-empty-state {") :]
    assert "dashed" in empty[: empty.index("}")]
    field = css[css.index("\n.shared-detail-panel .panel-inline-field {") :]
    assert "border-color: transparent" in field[: field.index("}")]
    assert ".panel-inline-field:focus" in css
    assert "--db-radius-chip" in css
