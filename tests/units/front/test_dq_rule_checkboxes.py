"""The run page offers a checkbox for every rule a run executes.

The list endpoint returns SHACL shapes under ``shapes`` and the other rule
families under ``rules``. The page has to render both or a SWRL rule has no
checkbox, and picking a few rules still runs every one of them.

``_loadShapes`` is executed under node against a stubbed DOM and fetch.
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

// One rule list container per category, recording what gets rendered into it.
const containers = {};
const makeContainer = () => ({
    innerHTML: '',
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    querySelectorAll: () => [],
});
const makeToggle = () => ({
    classList: { add() {}, remove() {} },
    querySelector: () => ({ textContent: '' }),
});

// _escHtml escapes by round-tripping through an element.
const makeEscaper = () => ({
    set textContent(value) {
        this.innerHTML = String(value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    },
    innerHTML: '',
});

global.document = {
    createElement: () => makeEscaper(),
    querySelector: (selector) => {
        const list = selector.match(/\\[data-dq-rulelist="([^"]+)"\\]/);
        if (list) {
            containers[list[1]] = containers[list[1]] || makeContainer();
            return containers[list[1]];
        }
        if (selector.startsWith('[data-dq-expand=')) return makeToggle();
        return null;
    },
    querySelectorAll: () => ({ forEach: () => {} }),
    getElementById: () => null,
};
global.fetch = async () => ({ json: async () => (__RESPONSE__) });

eval(fs.readFileSync(__SOURCE__, 'utf8'));
const dq = window.DQExecModule;

dq._loadShapes().then(() => {
    const rendered = {};
    Object.keys(containers).forEach(cat => { rendered[cat] = containers[cat].innerHTML; });
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
            "id": "swrl:no_orphans",
            "label": "No orphans",
            "category": "structural",
            "family": "swrl",
        },
        {
            "id": "dt:pricing",
            "label": "Pricing table",
            "category": "conformance",
            "family": "dt",
        },
    ],
}


class TestRuleCheckboxes:
    def test_a_swrl_rule_gets_a_checkbox(self):
        outcome = _load(RESPONSE)
        assert 'data-dq-rule-id="swrl:no_orphans"' in outcome["rendered"]["structural"]

    def test_a_decision_table_gets_a_checkbox(self):
        outcome = _load(RESPONSE)
        assert 'data-dq-rule-id="dt:pricing"' in outcome["rendered"]["conformance"]

    def test_the_rule_is_listed_under_the_dimension_it_reports_in(self):
        outcome = _load(RESPONSE)
        assert "no_orphans" not in outcome["rendered"].get("conformance", "")

    def test_the_shapes_are_still_rendered(self):
        outcome = _load(RESPONSE)
        assert 'data-dq-rule-id="shape_1"' in outcome["rendered"]["completeness"]

    def test_a_rule_says_which_family_it_comes_from(self):
        """A dimension counts shapes and business rules together, so the ones
        that are not shapes have to say where they were authored."""
        outcome = _load(RESPONSE)
        assert "Decision table" in outcome["rendered"]["conformance"]
        assert "SWRL" in outcome["rendered"]["structural"]

    def test_a_shape_carries_no_family_badge(self):
        outcome = _load(RESPONSE)
        assert "dq-rule-family" not in outcome["rendered"]["completeness"]

    def test_a_response_without_rules_still_works(self):
        """An older server, or a domain with no rule of those families."""
        outcome = _load({"success": True, "shapes": RESPONSE["shapes"]})
        assert 'data-dq-rule-id="shape_1"' in outcome["rendered"]["completeness"]
        assert len(outcome["cache"]) == 1
