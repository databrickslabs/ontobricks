"""Contract: showConfirmDialog stacks above an open Bootstrap modal.

When a confirmation opens while another ``.modal.show`` exists, the parent
gets ``ob-modal-underlying`` (blur/dim), the confirm gets ``ob-modal-stacked``,
and its backdrop gets ``ob-modal-stacked-backdrop``. Cleanup is idempotent on
``hidden.bs.modal``.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"


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
    assert "classList.remove" in body
    assert "ob-modal-underlying" in body
    # Cleanup must not leave the parent marked after close
    assert body.count("ob-modal-underlying") >= 2


def test_css_defines_underlying_blur():
    css = _css()
    assert ".ob-modal-underlying" in css
    assert "blur(" in css
    assert "prefers-reduced-motion" in css


def test_css_raises_stacked_z_index():
    css = _css()
    assert ".ob-modal-stacked" in css
    assert ".ob-modal-stacked-backdrop" in css
    assert "1065" in css
    assert "1060" in css
