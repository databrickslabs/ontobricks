"""Button design contract: focus states, size ramp, clusters and CTA hierarchy.

Covers the regressions found by the button audit of the Clarity refresh:
outline buttons filling solid on focus, small joined controls falling back to
Bootstrap's legacy radius, section headers that could not wrap, and the two
button "design islands" (the ontology assistant FAB and the Graph Chat).
"""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
ASSISTANT_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-assistant.css"
CHAT_CSS = REPO_ROOT / "src/front/static/query/css/query-chat.css"
MAP_PARTIAL = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_map.html"
SETTINGS_HTML = REPO_ROOT / "src/front/templates/settings.html"
TEMPLATES_DIR = REPO_ROOT / "src/front/templates"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_comments(css_text: str) -> str:
    return re.sub(r"/\*.*?\*/", "", css_text, flags=re.DOTALL)


def _blocks_for_selector(css_text: str, selector: str) -> list[str]:
    """Declaration bodies of every rule listing `selector` exactly."""
    blocks: list[str] = []
    for selectors, declarations in re.findall(
        r"([^{}]+)\{([^{}]*)\}", _strip_comments(css_text), flags=re.DOTALL
    ):
        if any(part.strip() == selector for part in selectors.split(",")):
            blocks.append(declarations)
    return blocks


def _has_declaration(blocks: list[str], prop: str, value: str) -> bool:
    return any(
        re.search(rf"\b{prop}\s*:\s*{value}\s*;", block, flags=re.IGNORECASE)
        for block in blocks
    )


NEUTRAL_BUTTON_VARIANTS = [
    ".btn-secondary",
    ".btn-success",
    ".btn-danger",
    ".btn-warning",
    ".btn-info",
    ".btn-outline-primary",
    ".btn-outline-secondary",
    ".btn-outline-success",
    ".btn-outline-danger",
]


@pytest.mark.parametrize("variant", NEUTRAL_BUTTON_VARIANTS)
def test_unfilled_buttons_stay_unfilled_when_focused(variant):
    """Bootstrap's `:focus-visible` repaints a button with its hover palette,
    which for these variants is the solid fill the design drops. Focus must
    keep the resting background and carry the state in the ring alone."""
    css = _read(COMPONENTS_CSS)

    for state in (f"{variant}:focus", f"{variant}:focus-visible"):
        blocks = _blocks_for_selector(css, state)
        assert blocks, f"No focus rule pins {state}"
        assert _has_declaration(blocks, "background-color", "transparent"), (
            f"{state} does not pin a transparent background"
        )

    ring_blocks = _blocks_for_selector(css, f"{variant}:focus")
    assert _has_declaration(ring_blocks, "box-shadow", r"var\(--db-focus-ring\)")


def test_outline_danger_keeps_its_red_lettering_on_focus():
    """The shared focus rule neutralises colour; danger opts back out."""
    css = _read(COMPONENTS_CSS)
    blocks = _blocks_for_selector(css, ".btn-outline-danger:focus")

    assert _has_declaration(blocks, "color", r"var\(--db-status-danger\)")


def test_small_joined_controls_use_the_control_radius_token():
    """Bootstrap's `-sm` ramp sets `border-radius` as a property with enough
    specificity to beat the base rule, so small input groups fell back to its
    legacy 4px while full-size siblings used the token."""
    css = _read(MAIN_CSS)

    for selector in (
        ".input-group-sm > .btn",
        ".input-group-sm > .form-control",
        ".form-control-sm",
    ):
        blocks = _blocks_for_selector(css, selector)
        assert blocks, f"No radius rule for {selector}"
        assert _has_declaration(blocks, "border-radius", r"var\(--db-radius-control\)")


def test_section_headers_wrap_their_control_cluster():
    """`justify-content-between` does not wrap: below a tablet the cluster used
    to push its last buttons off-screen."""
    css = _read(MAIN_CSS)

    assert _has_declaration(
        _blocks_for_selector(css, ".section-header.d-flex"), "flex-wrap", "wrap"
    )
    assert _has_declaration(
        _blocks_for_selector(css, ".section-header .d-flex"), "flex-wrap", "wrap"
    )


def test_grouped_buttons_become_pills_when_they_have_to_wrap():
    """A joined group cannot stay joined across rows — Bootstrap shrank its
    members instead, breaking labels into buttons three lines tall."""
    css = _strip_comments(_read(MAIN_CSS))

    media = re.search(
        r"@media\s*\(max-width:\s*767\.98px\)\s*\{(.*?)\n\}", css, flags=re.DOTALL
    )
    assert media, "No narrow-viewport block for section header groups"
    body = media.group(1)

    assert re.search(r"\.section-header\s+\.btn-group\s*\{[^}]*flex-wrap:\s*wrap", body)
    assert re.search(
        r"\.section-header\s+\.btn-group\s*>\s*\.btn\s*\{[^}]*"
        r"border-radius:\s*var\(--db-radius-control\)",
        body,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.section-header\s+\.btn-group\s*>\s*\.btn\s*\{[^}]*white-space:\s*nowrap",
        body,
        flags=re.DOTALL,
    )


def test_ontology_assistant_has_no_floating_action_button():
    """The FAB was a circular, hard-coded-orange island; the assistant is now
    opened from a regular icon button in the section header."""
    assert "assistant-fab" not in _read(ASSISTANT_CSS)
    assert "assistant-fab" not in _read(MAP_PARTIAL)

    partial = _read(MAP_PARTIAL)
    assert 'id="mapToggleAssistant"' in partial
    # Declared in the header cluster, above the canvas shell.
    assert partial.index('id="mapToggleAssistant"') < partial.index("ob-split-shell")
    assert "btn btn-sm btn-outline-secondary" in partial


def test_permission_hidden_buttons_stay_out_of_joined_groups():
    """`permissions.css` hides these outright for viewers; inside a group a
    hidden member leaves its neighbour with a squared edge."""
    partial = _read(MAP_PARTIAL)

    groups = re.findall(r'<div class="btn-group[^"]*"[^>]*>(.*?)</div>', partial, re.DOTALL)
    grouped = "".join(groups)
    for button_id in ("mapAutoAssignIcons", "mapToggleAssistant"):
        assert f'id="{button_id}"' in partial
        assert f'id="{button_id}"' not in grouped


def test_graph_chat_buttons_use_clarity_tokens():
    """The chat was styled on Databricks orange with its own focus ring."""
    css = _read(CHAT_CSS)

    assert "FF3621" not in css.upper()
    assert "255, 54, 33" not in css
    assert "#64748b" not in css.lower()
    # The header action defers to the global button contract, keeping only the
    # chat-specific icon size.
    assert ".graph-chat-header-actions .btn-outline-danger:hover" not in css
    assert _has_declaration(
        _blocks_for_selector(css, ".graph-chat-input-area .assistant-input-wrapper:focus-within"),
        "box-shadow",
        r"var\(--db-focus-ring\)",
    )


def test_settings_header_buttons_carry_the_small_size():
    """Section headers use `btn-sm`; without it these were 110px tall on a
    phone."""
    html = _read(SETTINGS_HTML)

    assert "btn btn-primary btn-save-settings" not in html
    assert "btn btn-sm btn-primary btn-save-settings" in html
    assert re.search(
        r'class="btn btn-sm btn-outline-primary" id="btnOpenProvisionModal"', html
    )


def test_no_button_is_hidden_with_an_inline_style():
    """Initial visibility belongs to a class, not a `style` attribute."""
    offenders = []
    for path in sorted(TEMPLATES_DIR.rglob("*.html")):
        for tag in re.findall(r"<button\b[^>]*>", _read(path), flags=re.DOTALL):
            if re.search(r'style="[^"]*display\s*:\s*none', tag):
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {tag[:90]}")

    assert not offenders, "Buttons hidden with inline styles:\n" + "\n".join(offenders)


def test_section_header_primary_cta_is_the_right_most_button():
    """The rightmost button is the section's most important action, so a
    utility like the discussion shortcut never sits after it."""
    sections = {
        "partials/ontology/_ontology_entities.html": "ontology-edit-btn",
        "partials/ontology/_ontology_relationships.html": "ontology-edit-btn",
        "partials/ontology/_ontology_wizard.html": "wizardTopGenerateBtn",
        "partials/domain/_domain_metadata.html": "loadMetadataBtn",
        "partials/mapping/_mapping_diagnostics.html": "runDiagnosticsBtn",
        "partials/ontology/_pitfalls.html": "pitfallsRunBtn",
        "partials/ontology/_ontology_cohorts.html": "cohortSaveBtn",
    }

    for relative_path, cta_marker in sections.items():
        html = _read(TEMPLATES_DIR / relative_path)
        assert "onto-discuss-btn" in html, relative_path
        assert html.index("onto-discuss-btn") < html.index(cta_marker), (
            f"{relative_path}: the discussion shortcut sits after the primary CTA"
        )
        # And the CTA is the filled variant, not another outline button.
        cta_tag = re.search(
            rf"<button[^>]*{re.escape(cta_marker)}[^>]*>", html, flags=re.DOTALL
        )
        assert cta_tag, relative_path
        assert "btn-primary" in cta_tag.group(0), (
            f"{relative_path}: primary CTA is not filled"
        )
