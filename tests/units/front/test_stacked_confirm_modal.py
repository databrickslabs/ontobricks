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


def _scoped_css(css: str, selector: str, radius: int = 800) -> str:
    idx = css.find(selector)
    if idx == -1:
        return ""
    return css[idx : idx + radius]


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
    removals = re.findall(r"classList\.remove\([^)]+\)", body)
    assert removals, "classList.remove cleanup required on hidden.bs.modal"
    removal_text = " ".join(removals)
    for cls in _STACK_CLASSES:
        assert cls in removal_text, (
            f"{cls} must be removed via classList.remove on hidden.bs.modal"
        )


def test_css_defines_underlying_blur():
    css = _css()
    block = _scoped_css(css, ".ob-modal-underlying")
    assert block, ".ob-modal-underlying selector missing from components.css"
    assert "blur(" in block, "blur() must be defined in the .ob-modal-underlying rule"
    assert "prefers-reduced-motion" in block, (
        "prefers-reduced-motion must be scoped to stacked modal styles"
    )


def test_css_raises_stacked_z_index():
    css = _css()
    stacked_block = _scoped_css(css, ".ob-modal-stacked", radius=200)
    backdrop_block = _scoped_css(css, ".ob-modal-stacked-backdrop", radius=200)
    assert stacked_block, ".ob-modal-stacked selector missing from components.css"
    assert backdrop_block, (
        ".ob-modal-stacked-backdrop selector missing from components.css"
    )
    assert re.search(r"z-index\s*:\s*1065", stacked_block), (
        ".ob-modal-stacked must set z-index: 1065"
    )
    assert re.search(r"z-index\s*:\s*1060", backdrop_block), (
        ".ob-modal-stacked-backdrop must set z-index: 1060"
    )
