"""Data quality lists every rule a run executes, one row per rule.

The list endpoint returns SHACL shapes under ``shapes`` and the Simple (IF-THEN)
rules under ``rules``. Both are handed to the shared phase tree under their
dimension, so a Simple rule has a row and picking a few rules runs only those.

``_loadShapes`` is executed under node against a recording stub of PhaseTree.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DQ_EXEC_JS = REPO_ROOT / "src/front/static/query/js/query-dataquality.js"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the frontend module"
)

_HARNESS = """
const fs = require('fs');
global.window = { __TRIPLESTORE_CONFIG: { view_table: 'cat.sch.triples' } };
global.showNotification = () => {};
global.document = {
    querySelector: () => null,
    querySelectorAll: () => [],
    getElementById: () => null,
};
global.fetch = async () => ({ json: async () => (__RESPONSE__) });

eval(fs.readFileSync(__SOURCE__, 'utf8'));
const dq = window.DQExecModule;

// Records the items each dimension hands to the tree.
const rendered = {};
dq._tree = { renderRules(key, items) { rendered[key] = items; } };

dq._loadShapes().then(() => {
    process.stdout.write(JSON.stringify({ rendered, cache: dq._shapesCache }));
});
"""


def _load(response):
    script = _HARNESS.replace("__SOURCE__", json.dumps(str(DQ_EXEC_JS))).replace(
        "__RESPONSE__", json.dumps(response)
    )
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


RESPONSE = {
    "success": True,
    "shapes": [{"id": "shape_1", "label": "Email present", "category": "completeness"}],
    "rules": [
        {
            "id": "if_then:no_orphans",
            "label": "No orphans",
            "category": "structural",
            "family": "if_then",
        },
    ],
}


def _values(items):
    return [item["value"] for item in items]


class TestRuleItems:
    def test_a_if_then_rule_gets_a_row(self):
        outcome = _load(RESPONSE)
        assert _values(outcome["rendered"]["structural"]) == ["if_then:no_orphans"]

    def test_the_rule_is_listed_under_the_dimension_it_reports_in(self):
        outcome = _load(RESPONSE)
        assert "if_then:no_orphans" not in _values(outcome["rendered"]["conformance"])

    def test_the_shapes_are_still_listed(self):
        outcome = _load(RESPONSE)
        assert _values(outcome["rendered"]["completeness"]) == ["shape_1"]

    def test_rows_show_the_label_and_keep_the_id_as_title(self):
        outcome = _load(RESPONSE)
        row = outcome["rendered"]["structural"][0]
        assert row["label"] == "No orphans"
        assert row["title"] == "if_then:no_orphans"

    def test_a_rule_says_which_family_it_comes_from(self):
        """A dimension counts shapes and Simple rules together, so the ones
        that are not shapes have to say where they were authored."""
        outcome = _load(RESPONSE)
        badge = outcome["rendered"]["structural"][0]["badge"]
        assert badge["text"] == "Simple"
        assert "Ontology" in badge["title"]

    def test_a_shape_carries_no_family_badge(self):
        outcome = _load(RESPONSE)
        assert outcome["rendered"]["completeness"][0]["badge"] is None

    def test_a_response_without_rules_still_works(self):
        """An older server, or a domain with no rule of those families."""
        outcome = _load({"success": True, "shapes": RESPONSE["shapes"]})
        assert _values(outcome["rendered"]["completeness"]) == ["shape_1"]
        assert len(outcome["cache"]) == 1
