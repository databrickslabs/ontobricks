"""Contract tests for the graphical direction picker in the relationship panel.

The Forward/Reverse ``<select>`` was replaced by two clickable cards that draw
the relationship the way the canvas will. The contract that matters is that
``saveSharedRelationship()`` keeps reading a single ``#sharedRelDirection``
value, and that the picker stays read-only in view mode.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
PANELS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-shared-panels.css"


def _js() -> str:
    return PANELS_JS.read_text(encoding="utf-8")


def _css() -> str:
    return PANELS_CSS.read_text(encoding="utf-8")


class TestPickerMarkup:
    def test_dropdown_is_gone(self):
        assert "<select" not in _js()[
            _js().index("sharedRelDirection") - 400 : _js().index("sharedRelDirection")
        ], "the direction control must no longer be a <select>"

    def test_value_still_lives_in_shared_rel_direction(self):
        """saveSharedRelationship() reads it by id — keep the contract."""
        js = _js()
        assert 'id="sharedRelDirection"' in js
        assert "panelGetById('sharedRelDirection')?.value" in js

    def test_both_directions_are_offered(self):
        js = _js()
        assert "_relDirectionOption('forward'" in js
        assert "_relDirectionOption('reverse'" in js

    def test_arrows_use_bootstrap_icons(self):
        js = _js()
        assert "'bi-arrow-right'" in js
        assert "'bi-arrow-left'" in js

    def test_options_are_radios_for_assistive_tech(self):
        js = _js()
        assert 'role="radiogroup"' in js
        assert 'role="radio"' in js
        assert "aria-checked" in js

    def test_options_do_not_submit_the_form(self):
        """The picker sits inside #sharedRelationshipForm."""
        body = re.search(
            r"function _relDirectionOption\([^)]*\)\s*\{(.*?)\n\}", _js(), re.S
        )
        assert body, "_relDirectionOption() must exist"
        assert 'type="button"' in body.group(1)

    def test_view_only_renders_a_disabled_preview(self):
        body = re.search(
            r"function _relDirectionOption\([^)]*\)\s*\{(.*?)\n\}", _js(), re.S
        )
        assert "viewOnly ? 'disabled' : ''" in body.group(1)


class TestPickerBehaviour:
    def test_selection_mirrors_into_the_hidden_input(self):
        body = re.search(
            r"function setSharedRelDirection\([^)]*\)\s*\{(.*?)\n\}", _js(), re.S
        )
        assert body, "setSharedRelDirection() must exist"
        assert "panelGetById('sharedRelDirection')" in body.group(1)
        assert "classList.toggle('selected'" in body.group(1)

    def test_unknown_values_fall_back_to_forward(self):
        body = re.search(
            r"function setSharedRelDirection\([^)]*\)\s*\{(.*?)\n\}", _js(), re.S
        )
        assert "=== 'reverse' ? 'reverse' : 'forward'" in body.group(1)

    def test_saved_direction_is_applied_on_open(self):
        assert "setSharedRelDirection(prop?.direction || 'forward')" in _js()

    def test_chips_follow_the_source_target_and_name_fields(self):
        js = _js()
        assert "refreshSharedRelDirectionLabels" in js
        for field in ("sharedRelDomain", "sharedRelRange", "sharedRelName"):
            assert field in js
        body = re.search(
            r"function refreshSharedRelDirectionLabels\(\)\s*\{(.*?)\n\}", js, re.S
        )
        assert "textContent" in body.group(1), "labels must not be injected as HTML"

    def test_listeners_are_skipped_in_view_only_mode(self):
        js = _js()
        idx = js.index("sharedRelDirectionPicker')?.addEventListener")
        assert "if (!viewOnly)" in js[idx - 200 : idx]


class TestPickerStyling:
    def test_styles_live_in_the_panel_stylesheet(self):
        css = _css()
        for rule in (
            ".rel-direction-picker",
            ".rel-direction-option",
            ".rel-direction-option.selected",
            ".rel-direction-node",
            ".rel-direction-arrow",
        ):
            assert rule in css, f"{rule} must be styled"

    def test_uses_design_tokens_not_hard_coded_brand_colours(self):
        block = _css()[_css().index(".rel-direction-picker") :]
        assert "var(--db-" in block
        assert "#FF3621" not in block

    def test_endpoint_chips_truncate_in_the_narrow_panel(self):
        block = _css()[_css().index(".rel-direction-node") :][:400]
        assert "text-overflow: ellipsis" in block
