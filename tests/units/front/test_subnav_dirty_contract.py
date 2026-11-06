"""Static contract for the Save dirty indicator in the L2 subnav."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
ONTOLOGY_JS = REPO_ROOT / "src/front/static/global/js/ontology-design.js"
MAPPING_JS = REPO_ROOT / "src/front/static/global/js/mapping-core.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_navbar_exposes_dirty_bus_and_applies_class_to_save():
    js = _read(NAVBAR_JS)

    assert "window.setDomainDirty = setDomainDirty" in js
    assert "window.clearAllDomainDirty = clearAllDomainDirty" in js
    assert "classList.toggle('is-dirty'" in js
    # A successful registry save clears every dirty source.
    assert "clearAllDomainDirty();" in js


def test_ontology_publishes_registry_dirty_state():
    js = _read(ONTOLOGY_JS)

    assert "setDomainDirty('ontology', true)" in js
    assert "setDomainDirty('ontology', false)" in js


def test_mapping_publishes_registry_dirty_state():
    js = _read(MAPPING_JS)

    assert "setDomainDirty('mapping', true)" in js
    assert "setDomainDirty('mapping', false)" in js


def test_save_kbd_label_follows_platform():
    js = _read(NAVBAR_JS)

    assert "[data-os-shortcut]" in js
    assert "navigator.platform" in js


def test_shortcut_is_not_registered_twice():
    # Ctrl/Cmd+S is already handled globally in base-ui-handlers.js; the
    # navbar must not add a second keydown handler for it.
    js = _read(NAVBAR_JS)

    assert "addEventListener('keydown'" not in js
