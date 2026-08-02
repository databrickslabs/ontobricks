"""Contract: showConfirmDialog stacks above an open Bootstrap modal.

When a confirmation opens while another ``.modal.show`` exists, the parent
gets ``ob-modal-underlying`` (blur/dim), the confirm gets ``ob-modal-stacked``,
and its backdrop gets ``ob-modal-stacked-backdrop``. Cleanup is idempotent on
``hidden.bs.modal``.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"

_STACK_CLASSES = (
    "ob-modal-underlying",
    "ob-modal-stacked",
    "ob-modal-stacked-backdrop",
)


def _utils() -> str:
    return UTILS_JS.read_text(encoding="utf-8")


def _confirm_body() -> str:
    source = _utils()
    start = source.index("function showConfirmDialog")
    # Next exported helper after showConfirmDialog
    end = source.index("function showInfoDialog", start)
    return source[start:end]


def _css() -> str:
    return COMPONENTS_CSS.read_text(encoding="utf-8")


def _rule_body(css: str, selector: str) -> str:
    """Return the CSS declaration block for an exact selector occurrence.

    Prefer matching the selector followed by ``{`` (allowing whitespace),
    and reject matches where the selector is only a prefix of a longer
    class (e.g. ``.ob-modal-stacked`` vs ``.ob-modal-stacked-backdrop``).
    """
    pattern = re.escape(selector)
    for match in re.finditer(pattern, css):
        start = match.end()
        if start < len(css) and css[start] in "-_a-zA-Z0-9":
            continue
        i = start
        while i < len(css) and css[i] in " \t\n\r":
            i += 1
        if i >= len(css) or css[i] != "{":
            continue
        i += 1
        depth = 1
        body_start = i
        while i < len(css) and depth > 0:
            if css[i] == "{":
                depth += 1
            elif css[i] == "}":
                depth -= 1
            i += 1
        if depth == 0:
            return css[body_start : i - 1]
    return ""


def _rule_body_any(css: str, *selectors: str) -> str:
    for selector in selectors:
        body = _rule_body(css, selector)
        if body:
            return body
    return ""


def test_confirm_detects_visible_parent_modal():
    body = _confirm_body()
    assert ".modal.show" in body
    assert "ob-modal-underlying" in body


def test_confirm_marks_itself_stacked():
    body = _confirm_body()
    assert "ob-modal-stacked" in body
    assert "ob-modal-stacked-backdrop" in body


def test_confirm_cleans_stack_on_hidden():
    body = _confirm_body()
    assert "hidden.bs.modal" in body
    hidden_idx = body.index("hidden.bs.modal")
    cleanup_slice = body[hidden_idx:]
    removals = re.findall(r"classList\.remove\([^)]+\)", cleanup_slice)
    assert removals, "classList.remove cleanup required on hidden.bs.modal"
    removal_text = " ".join(removals)
    for cls in _STACK_CLASSES:
        assert cls in removal_text, (
            f"{cls} must be removed via classList.remove on hidden.bs.modal"
        )


def test_css_defines_underlying_blur():
    css = _css()
    body = _rule_body_any(
        css,
        ".modal.ob-modal-underlying .modal-content",
        ".ob-modal-underlying",
    )
    assert body, "underlying blur rule missing from components.css"
    assert "blur(" in body, "blur() must be defined in the underlying-modal rule"
    assert "prefers-reduced-motion" in css, (
        "prefers-reduced-motion must be present in stacked modal styles"
    )


def test_css_raises_stacked_z_index():
    css = _css()
    stacked_body = _rule_body_any(css, ".modal.ob-modal-stacked", ".ob-modal-stacked")
    backdrop_body = _rule_body_any(
        css,
        ".modal-backdrop.ob-modal-stacked-backdrop",
        ".ob-modal-stacked-backdrop",
    )
    assert stacked_body, ".ob-modal-stacked rule missing from components.css"
    assert backdrop_body, ".ob-modal-stacked-backdrop rule missing from components.css"
    assert "1065" in stacked_body, ".ob-modal-stacked must set z-index: 1065"
    assert "1060" in backdrop_body, (
        ".ob-modal-stacked-backdrop must set z-index: 1060"
    )
