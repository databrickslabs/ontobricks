"""Contract tests for the Designer "Create Relationship" quick-create dialog
(`showMapRelationshipDialog` in ontology-map.js).

The dialog has two editable fields: Label (focused by default) and ID
(mirrors the Label as camelCase until edited directly). The ID must be
non-empty and unique across the whole ontology's properties, otherwise the
Create button is blocked with an inline error.
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
MAP_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-map.js"


def _dialog_block():
    js = MAP_JS.read_text(encoding="utf-8")
    start = js.index("function showMapRelationshipDialog(")
    end = js.index("\n}\n", start) + 3
    return js[start:end]


def test_dialog_has_two_fields_label_and_id():
    block = _dialog_block()
    assert 'id="mapRelationshipLabel"' in block
    assert 'id="mapRelationshipName"' in block
    assert 'for="mapRelationshipLabel">Label<' in block
    assert 'for="mapRelationshipName">ID' in block


def test_label_gets_default_focus():
    block = _dialog_block()
    # Bootstrap's modal focus-trap grabs focus once the shown transition
    # completes, overriding a synchronous .focus() called right after
    # .show(). Must wait for 'shown.bs.modal' instead.
    assert "modalEl.addEventListener('shown.bs.modal', () => {\n            labelInput.focus();" in block
    # autofocus belongs to the Label input, not the ID input.
    label_tag_start = block.index('id="mapRelationshipLabel"')
    label_tag_end = block.index('>', label_tag_start)
    assert "autofocus" in block[label_tag_start:label_tag_end]
    name_tag_start = block.index('id="mapRelationshipName"')
    name_tag_end = block.index('>', name_tag_start)
    assert "autofocus" not in block[name_tag_start:name_tag_end]


def test_id_mirrors_label_until_manually_edited():
    block = _dialog_block()
    assert "let idManuallyEdited = false;" in block
    assert "labelInput.addEventListener('input'" in block
    assert "if (idManuallyEdited) return;" in block
    assert "columnToCamelCase(labelInput.value)" in block
    assert "nameInput.addEventListener('input', () => {\n            idManuallyEdited = true;" in block


def test_id_uniqueness_validated_and_blocks_create():
    block = _dialog_block()
    assert "const isDuplicateId = (id) =>" in block
    assert "props.some(p => p.name === id)" in block
    assert "if (!validateName()) return;" in block
    assert "already exists in the ontology" in block


def test_resolve_returns_name_and_label_object():
    block = _dialog_block()
    assert "resolve({ name: nameInput.value.trim(), label: labelInput.value.trim() });" in block


def test_create_relationship_from_map_uses_global_uniqueness_and_label():
    js = MAP_JS.read_text(encoding="utf-8")
    start = js.index("async function createRelationshipFromMap(")
    end = js.index("\n}\n", start) + 3
    block = js[start:end]
    assert "OntologyState.config.properties.find(p => p.name === relationshipName);" in block
    assert "label: relationshipLabel" in block
