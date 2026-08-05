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


def _brace_block_after(source: str, start: int) -> str:
    """Return the inner text of the ``{ ... }`` block opened at ``start``."""
    i = start
    while i < len(source) and source[i] != "{":
        i += 1
    if i >= len(source):
        return ""
    i += 1
    depth = 1
    body_start = i
    while i < len(source) and depth > 0:
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
        i += 1
    return source[body_start : i - 1] if depth == 0 else ""


def _hidden_modal_handler_body(body: str) -> str:
    """Return the callback body for the ``hidden.bs.modal`` listener."""
    idx = body.index("hidden.bs.modal")
    slice_ = body[idx:]
    match = re.search(r"(?:=>\s*\{|function\s*\([^)]*\)\s*\{)", slice_)
    if not match:
        return ""
    return _brace_block_after(slice_, match.end() - 1)


def _function_body(source: str, name: str) -> str:
    """Return the body of ``const name = () => { ... }`` in ``source``."""
    match = re.search(
        rf"const\s+{re.escape(name)}\s*=\s*\([^)]*\)\s*=>\s*\{{",
        source,
    )
    if not match:
        return ""
    return _brace_block_after(source, match.end() - 1)


def _named_function_body(source: str, name: str) -> str:
    match = re.search(
        rf"function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{",
        source,
    )
    if not match:
        return ""
    return _brace_block_after(source, match.end() - 1)


def _stack_helper_body() -> str:
    return _named_function_body(_utils(), "showStackedModal")


def _removals_cover_classes(text: str, classes: tuple[str, ...]) -> bool:
    removal_text = " ".join(re.findall(r"classList\.remove\([^)]+\)", text))
    return all(cls in removal_text for cls in classes)


def test_shared_helper_detects_visible_parent_modal():
    body = _stack_helper_body()
    assert body, "showStackedModal helper must exist"
    assert ".modal.show" in body
    assert "ob-modal-underlying" in body


def test_shared_helper_marks_child_and_backdrop_stacked():
    body = _stack_helper_body()
    assert "ob-modal-stacked" in body
    assert "ob-modal-stacked-backdrop" in body


def test_shared_helper_cleans_stack_on_hidden():
    body = _stack_helper_body()
    assert "hidden.bs.modal" in body
    assert re.search(
        r"addEventListener\(\s*['\"]hidden\.bs\.modal['\"]\s*,\s*clearStack",
        body,
    ), "hidden.bs.modal must invoke clearStack"
    clear_body = _function_body(body, "clearStack")
    assert clear_body, "clearStack helper must be defined"
    assert _removals_cover_classes(clear_body, _STACK_CLASSES), (
        "clearStack must remove every stacked-modal class"
    )


def test_confirm_uses_shared_stacked_modal_helper():
    assert "showStackedModal(modalEl)" in _confirm_body()


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
    assert re.search(r"z-index\s*:\s*1065", stacked_body), (
        ".ob-modal-stacked must set z-index: 1065"
    )
    assert re.search(r"z-index\s*:\s*1060", backdrop_body), (
        ".ob-modal-stacked-backdrop must set z-index: 1060"
    )
