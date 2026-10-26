"""Contracts for remapping Ontology Studio entity icons after confirmation."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"


def _auto_assign_block():
    js = MAP_JS.read_text(encoding="utf-8")
    start = js.index("async function autoAssignEntityIcons()")
    end = js.index("function showMapRelationshipDialog", start)
    return js[start:end]


def test_running_task_guard_runs_before_remap_confirmation():
    block = _auto_assign_block()
    task_guard = block.index("sessionStorage.getItem(ICONS_TASK_KEY)")
    confirm = block.index("showConfirmDialog")
    assert task_guard < confirm


def test_default_icon_candidates_skip_remap_confirmation():
    block = _auto_assign_block()
    filter_line = block.index(
        "classes.filter(cls => (cls.emoji || defaultEmoji) === defaultEmoji)"
    )
    confirm = block.index("showConfirmDialog")
    assert filter_line < confirm
    assert "candidates.length === 0" in block


def test_all_custom_icons_open_shared_confirmation_dialog():
    block = _auto_assign_block()
    assert "title: 'Remap entity icons?'" in block
    assert (
        "message: 'All entities already have custom icons. "
        "Remapping will replace every current icon.'"
    ) in block
    assert "confirmText: 'Remap all'" in block
    assert "cancelText: 'Cancel'" in block
    assert "await showConfirmDialog(" in block


def test_cancel_remap_does_not_start_icon_task():
    block = _auto_assign_block()
    assert "if (!confirmed) return;" in block
    confirm = block.index("if (!confirmed) return;")
    fetch = block.index("fetch('/ontology/auto-assign-icons'")
    assert confirm < fetch


def test_confirm_remap_submits_every_entity_name():
    block = _auto_assign_block()
    assert "candidates = classes;" in block
    assert "const entityNames = candidates.map(cls => cls.name);" in block
    assert "JSON.stringify({ entity_names: entityNames })" in block


def test_missing_confirm_helper_does_not_replace_custom_icons():
    block = _auto_assign_block()
    assert "typeof showConfirmDialog !== 'function'" in block
    helper_guard = block.index("typeof showConfirmDialog !== 'function'")
    fetch = block.index("fetch('/ontology/auto-assign-icons'")
    assert helper_guard < fetch
