"""Contract for the edit-lock auto-resume after an idle expiry (edit-lock.js).

The behaviour lives in browser JS, so these tests pin the source-level
guarantees that matter: which inputs resume, that the re-acquire is
non-forcing, that a reload only happens once the lock is ours, and that a
lock lost to a take-over is never auto-reclaimed.
"""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
EDIT_LOCK_JS = REPO_ROOT / "src/front/static/global/js/edit-lock.js"


def _js() -> str:
    return EDIT_LOCK_JS.read_text(encoding="utf-8")


def _function_body(js: str, name: str) -> str:
    """Return the body of ``function name(...) { ... }`` (4-space indented)."""
    match = re.search(
        rf"\n    (?:async )?function {name}\(\) \{{\n(.*?)\n    \}}\n",
        js,
        flags=re.DOTALL,
    )
    assert match, f"function {name} not found in edit-lock.js"
    return match.group(1)


def test_auto_resume_only_listens_to_deliberate_inputs():
    js = _js()
    match = re.search(r"var AUTO_RESUME_EVENTS = \[(.*?)\];", js)
    assert match, "AUTO_RESUME_EVENTS not declared"
    events = re.findall(r"'([a-z]+)'", match.group(1))
    assert events == ["click", "keydown", "touchstart"]
    assert "mousemove" not in events
    assert "scroll" not in events


def test_auto_resume_uses_non_forcing_acquire():
    body = _function_body(_js(), "tryAutoResume").replace("async ", "")
    assert "'/domain/edit-lock/acquire'" in _js()
    assert "force: false" in body
    assert "force: true" not in body


def test_auto_resume_reloads_only_when_lock_is_ours():
    body = _function_body(_js(), "tryAutoResume")
    # Reload is gated on mode === 'edit' (lock granted to this browser).
    assert re.search(r"data\.mode === 'edit'[\s\S]*?window\.location\.reload\(\)", body)
    # The "held by someone else" branch notifies and does not reload.
    view_branch = body.split("data.mode === 'view'", 1)[1]
    assert "reload" not in view_branch.split("finally", 1)[0]


def test_auto_resume_is_armed_only_on_idle_expiry_paths():
    js = _js()
    # Idle expiry arms it.
    assert "armAutoResume();" in _function_body(js, "expireByIdle")
    # A page load that finds the sticky flag (still expired) arms it too.
    assert "armAutoResume();" in js.split("if (lockKey && stickyGet() === lockKey)", 1)[1].split(
        "} else {", 1
    )[0]
    # A lease lost to a take-over must never arm it (no auto-reclaim).
    assert "armAutoResume" not in _function_body(js, "onLeaseLost")


def test_resume_button_fallback_is_kept():
    js = _js()
    assert '<span class="ob-subnav-label">Resume</span>' in js
    assert "function onResumeEditingClick()" in js
