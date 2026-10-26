"""Studio Import entity — JS helpers executed with node, plus source contracts."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
JS = REPO_ROOT / "src/front/static/ontology/js/ontology-entity-import.js"

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node required")

CATALOG = {
    "entities": [
        {"name": "Person", "parent": "", "exists": False},
        {"name": "Customer", "parent": "Person", "exists": False},
        {"name": "Order", "parent": "", "exists": False},
        {"name": "Address", "parent": "", "exists": True},
    ],
    "relationships": [
        {"name": "places", "domain": "Customer", "range": "Order"},
        {"name": "livesAt", "domain": "Person", "range": "Address"},
    ],
}

_HARNESS = """
const fs = require('fs');
global.window = {};
global.document = { addEventListener: () => {}, getElementById: () => null };
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const EI = window.EntityImport;
const catalog = __CATALOG__;
const result = (() => { __BODY__ })();
process.stdout.write(JSON.stringify(result));
"""


def _eval(body):
    script = (
        _HARNESS.replace("__SOURCE__", json.dumps(str(JS)))
        .replace("__CATALOG__", json.dumps(CATALOG))
        .replace("__BODY__", body)
    )
    done = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


@needs_node
def test_related_entities_cover_relations_parent_and_children():
    rel = _eval("return EI.relatedEntities(catalog, 'Customer');")
    assert {"name": "Order", "relation": "places", "direction": "out"} in rel
    assert {"name": "Person", "relation": "subClassOf", "direction": "parent"} in rel
    rel_person = _eval("return EI.relatedEntities(catalog, 'Person');")
    assert {"name": "Customer", "relation": "subClassOf", "direction": "child"} in rel_person
    assert {"name": "Address", "relation": "livesAt", "direction": "out"} in rel_person


@needs_node
def test_preview_counts_follow_backend_rules():
    assert _eval("return EI.previewImport(catalog, ['Customer'], {});") == {
        "entities": 1, "relationships": 0}
    assert _eval("return EI.previewImport(catalog, ['Customer', 'Order'], {});") == {
        "entities": 2, "relationships": 1}
    # Address exists in the target: the relationship points at it, Address is not imported.
    assert _eval("return EI.previewImport(catalog, ['Person', 'Address'], {});") == {
        "entities": 1, "relationships": 1}
    assert _eval("return EI.previewImport(catalog, ['Address'], {Address: 'SalesAddress'});") == {
        "entities": 1, "relationships": 0}


@needs_node
def test_is_valid_name():
    assert _eval("return ['Ok_1', '', 'a b', '1x'].map(EI.isValidName);") == [
        True, False, False, False]


ONTOLOGY_HTML = REPO_ROOT / "src/front/templates/ontology.html"
MAP_HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_map.html"
MODAL_HTML = REPO_ROOT / "src/front/templates/partials/ontology/_ontology_entity_import_modal.html"


def test_studio_header_has_edit_only_import_button():
    html = MAP_HTML.read_text(encoding="utf-8")
    assert 'id="mapImportEntity"' in html
    idx = html.index('id="mapImportEntity"')
    assert "ontology-edit-btn" in html[idx - 200 : idx]
    assert "Import entity" in html


def test_modal_partial_and_script_are_included():
    page = ONTOLOGY_HTML.read_text(encoding="utf-8")
    assert "partials/ontology/_ontology_entity_import_modal.html" in page
    assert page.index("ontology/js/ontology-map.js") < page.index("ontology/js/ontology-entity-import.js")
    modal = MODAL_HTML.read_text(encoding="utf-8")
    for needle in ('id="entityImportModal"', 'id="entityImportDomain"', 'id="entityImportList"',
                   'id="entityImportRelated"', 'id="entityImportSummary"', 'id="entityImportConfirm"',
                   'data-bs-dismiss="modal"', "modal-lg"):
        assert needle in modal
    assert 'style="' not in modal


def test_wiring_uses_routes_and_refreshes_studio():
    js = JS.read_text(encoding="utf-8")
    assert "/ontology/bridges/domains" in js
    assert "/ontology/entity-import/domains/" in js
    assert "'/ontology/entity-import'" in js
    assert "bootstrap.Modal.getOrCreateInstance" in js
    assert "loadOntologyFromSession" in js and "initOntologyMap" in js
