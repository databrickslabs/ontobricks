"""Simple-rule editor: a Remove rule can name a relationship that is also a condition.

The IF-THEN editor keeps one role per element in normal (assert) mode, so a
relationship used as a condition could not be added to THEN and the rule could
not be saved. In Remove mode the same relationship may be both: the IF part
binds its variables and the THEN part names the triple to delete.

``IfThenModule`` is driven under node against a stubbed DOM, the same way the
data-quality front-end tests run their modules.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
IF_THEN_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-if-then.js"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the frontend module"
)

_HARNESS = """
const fs = require('fs');
const action = __ACTION__;
global.window = global;  // the module refers to both window.X and bare X
global.document = {
    addEventListener() {},
    getElementById: (id) => (id === 'ifThenAction' ? { value: action } : null),
};
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const M = window.IfThenModule;

// Graph: Meter -meterOf-> Customer, with a second link (status) on Meter.
M._graphLinks = [
    { linkId: 'L1', name: 'meterOf', source: 'Meter', target: 'Customer' },
    { linkId: 'L2', name: 'status', source: 'Meter', target: 'Status' },
];
M.nodeVars = new Map();
M.ifNodes = new Set();
M.thenNodes = new Set();
M.ifLinks = new Set();
M.thenLinks = new Set();
M.VAR_NAMES = ['?x', '?y', '?z'];
__SETUP__

const out = {
    canThenLink: M._canAddToThen('L1'),
    canThenNode: M._canAddToThen('Meter'),
    ifLinks: [...M.ifLinks],
    thenLinks: [...M.thenLinks],
    ifNodes: [...M.ifNodes].sort(),
    thenNodes: [...M.thenNodes].sort(),
};
process.stdout.write(JSON.stringify(out));
"""


def _run(action, setup=""):
    script = (
        _HARNESS.replace("__SOURCE__", json.dumps(str(IF_THEN_JS)))
        .replace("__ACTION__", json.dumps(action))
        .replace("__SETUP__", setup)
    )
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


class TestRemoveMode:
    def test_a_condition_relationship_can_be_added_to_then(self):
        out = _run("retract", setup="M._selectLink('L1', 'if');")
        assert out["canThenLink"] is True

    def test_adding_to_then_keeps_the_relationship_in_if(self):
        out = _run("retract", setup="M._selectLink('L1', 'if'); M._selectLink('L1', 'then');")
        assert out["ifLinks"] == ["L1"]
        assert out["thenLinks"] == ["L1"]

    def test_endpoints_bind_in_if_not_in_then(self):
        out = _run("retract", setup="M._selectLink('L1', 'then');")
        assert out["thenNodes"] == []
        assert out["ifNodes"] == ["Customer", "Meter"]

    def test_a_relationship_already_in_then_cannot_be_added_again(self):
        out = _run("retract", setup="M._selectLink('L1', 'then');")
        assert out["canThenLink"] is False

    def test_a_class_in_if_can_be_added_to_then_and_stays_in_if(self):
        out = _run("retract", setup="M._selectNode('Meter', 'if'); M._selectNode('Meter', 'then');")
        assert out["canThenNode"] is False  # already in THEN now
        assert out["ifNodes"] == ["Meter"]
        assert out["thenNodes"] == ["Meter"]

    def test_a_class_outside_if_is_bound_in_if_when_added_to_then(self):
        out = _run("retract", setup="M._selectNode('Meter', 'then');")
        assert out["ifNodes"] == ["Meter"]
        assert out["thenNodes"] == ["Meter"]


class TestAssertModeUnchanged:
    def test_a_condition_relationship_cannot_be_added_to_then(self):
        out = _run("assert", setup="M._selectLink('L1', 'if');")
        assert out["canThenLink"] is False

    def test_adding_to_then_moves_the_relationship_out_of_if(self):
        out = _run("assert", setup="M._selectLink('L1', 'if'); M._selectLink('L1', 'then');")
        assert out["ifLinks"] == []
        assert out["thenLinks"] == ["L1"]

    def test_endpoints_follow_the_relationship_into_then(self):
        out = _run("assert", setup="M._selectLink('L1', 'then');")
        assert out["thenNodes"] == ["Customer", "Meter"]
        assert out["ifNodes"] == []

    def test_an_unselected_class_can_be_added_to_then(self):
        out = _run("assert")
        assert out["canThenNode"] is True

    def test_a_class_in_if_cannot_be_added_to_then(self):
        out = _run("assert", setup="M._selectNode('Meter', 'if');")
        assert out["canThenNode"] is False
