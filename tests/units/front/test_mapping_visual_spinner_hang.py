"""Visual spinner must not stick on "Loading mapping design...".

Three failure modes seen in 0.10.0:
1. Overlay CSS defaulted to display:flex, so a skipped init left it visible.
2. Mapping section change armed the spinner even for Informations/List, then
   returned early when Visual was not active — overlay never cleared.
3. MappingStudioTabs._onShown('visual') only called init when MappingState was
   already initialized; opening Visual a moment too early left the spinner forever.
"""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
INIT_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-init.js"
DESIGN_HTML = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
MAPPING_CSS = REPO_ROOT / "src/front/static/global/css/mapping.css"


def test_overlay_starts_hidden_in_markup():
    html = DESIGN_HTML.read_text(encoding="utf-8")
    idx = html.index('id="mappingDesignerLoading"')
    tag = html[idx : idx + 160]
    assert "display:none" in tag.replace(" ", "")


def test_overlay_css_defaults_to_none():
    css = MAPPING_CSS.read_text(encoding="utf-8")
    block = css[
        css.index(".mapping-designer-container .designer-loading-overlay") :
    ]
    block = block[: block.index("}")]
    assert "display: none" in block
    assert "display: flex" not in block


def test_section_change_does_not_arm_spinner_for_informations():
    js = INIT_JS.read_text(encoding="utf-8")
    body = js[js.index("onSectionChange:") :]
    body = body[: body.index("window.MappingPageHooks")]
    # Informations / List exit before showMappingDesignerLoading(true).
    assert "MappingStudioTabs.current() !== 'visual'" in body
    show_true = body.index("showMappingDesignerLoading(true)")
    early_return = body.index("MappingStudioTabs.current() !== 'visual'")
    assert early_return < show_true


def test_visual_tab_show_owns_init_without_duplicate_spinner_arm():
    js = INIT_JS.read_text(encoding="utf-8")
    # ?tab=visual path must not fall through to the legacy spinner+tryInit path.
    assert "MappingStudioTabs.show('visual')" in js
    visual_branch = js[js.index("if (tab === 'visual')") : js.index("Informations / List")]
    assert "return;" in visual_branch


def test_visual_tab_shown_always_inits_even_before_mapping_state():
    js = DESIGN_JS.read_text(encoding="utf-8")
    on_shown = js[js.index("if (name === 'visual')") :]
    on_shown = on_shown[: on_shown.index("\n    },")]
    assert "MappingState.initialized)" not in on_shown
    assert "&& MappingState.initialized" not in on_shown
    assert "showMappingDesignerLoading(true)" in on_shown
    assert "initMappingDesigner()" in on_shown


def test_map_layout_fetch_is_abortable():
    js = DESIGN_JS.read_text(encoding="utf-8")
    body = re.search(r"async function loadMapLayout\(\)\s*\{(.*?)\n\}", js, re.DOTALL)
    assert body, "loadMapLayout missing"
    text = body.group(1)
    assert "AbortController" in text
    assert "abort()" in text
    assert "8000" in text


def test_init_finally_always_clears_overlay():
    js = DESIGN_JS.read_text(encoding="utf-8")
    # The silent drift re-init must still clear a stuck overlay.
    assert re.search(
        r"finally\s*\{[^}]*showMappingDesignerLoading\(false\)",
        js,
        re.DOTALL,
    )
    assert "else hide()" in js or "else hide();" in js
