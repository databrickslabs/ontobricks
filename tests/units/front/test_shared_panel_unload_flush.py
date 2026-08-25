"""The detail panel's pending edit must survive leaving the page.

The panel is an edit buffer, flushed on every other exit: closing it, switching
sidebar section, opening another item. Leaving the page for a different route
(Knowledge Graph, Mapping, Domain) went through none of those, so a pending
edit was dropped — and the registry auto-save on unload then persisted the
state that never saw it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
CORE_JS = REPO_ROOT / "src/front/static/global/js/ontology-core.js"
DESIGN_JS = REPO_ROOT / "src/front/static/global/js/ontology-design.js"

pytestmark = pytest.mark.unit


def test_pending_edit_is_flushed_on_unload():
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "function flushSharedPanelOnUnload" in js
    assert "window.addEventListener('beforeunload', flushSharedPanelOnUnload)" in js
    assert "window.addEventListener('pagehide', flushSharedPanelOnUnload)" in js


def test_flush_is_a_no_op_when_there_is_nothing_to_save():
    """It runs on every navigation and is called twice by design, so it must
    not save a clean or read-only panel."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "if (!sharedPanelDirty || sharedPanelViewOnly) return;" in js


def test_session_save_survives_page_teardown():
    """A plain fetch is cancelled during unload, which is what silently lost
    the edit."""
    core = CORE_JS.read_text(encoding="utf-8")
    assert "keepalive: Boolean(options.keepalive)" in core

    panels = PANELS_JS.read_text(encoding="utf-8")
    assert panels.count("saveConfigToSession({ keepalive: options.keepalive })") == 2
    assert "saveSharedPanelItem({ keepalive: true })" in panels


def test_flush_precedes_the_registry_save():
    """The panel module loads after the design module, so its own unload
    handler runs second; the registry would otherwise be written from a
    session that never saw the pending edit."""
    js = DESIGN_JS.read_text(encoding="utf-8")
    start = js.index("function saveRegistryOnUnload()")
    body = js[start : js.index("window.addEventListener", start)]
    assert body.index("flushSharedPanelOnUnload") < body.index(
        "if (!registryDirty) return;"
    )


def test_the_other_exits_still_flush():
    """Regression fence: the unload path is an addition, not a replacement."""
    js = PANELS_JS.read_text(encoding="utf-8")
    assert "async function guardedCloseSharedPanel" in js
    assert "async function checkDirtyBeforeSwitch" in js
    assert "await saveSharedPanelItem();" in js
