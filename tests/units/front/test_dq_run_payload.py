"""The run request always names the selected dimensions.

SWRL rules, decision tables and aggregate rules have no checkbox: the backend
includes them based on the dimensions. Sending only ``shape_ids`` — which is
what happened as soon as a domain had any shape — left the backend with no
selection to apply, so every SWRL rule ran under Structural even when only
Conformance was ticked.

``runAllChecks`` is executed under node against a stubbed DOM and fetch.
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
global.window = { __TRIPLESTORE_CONFIG: __CONFIG__ };
global.showNotification = (message) => { notifications.push(message); };
const notifications = [];
let sent = null;

const dimensionBoxes = __DIMENSIONS__.map(
    ([dimension, checked]) => ({ dataset: { dimension }, checked, indeterminate: false })
);
const ruleBoxes = __RULES__.map(
    ([dqRuleId, checked]) => ({ dataset: { dqRuleId }, checked })
);

const query = (selector) => {
    if (selector === '[data-dimension]') return dimensionBoxes;
    if (selector === '.dq-rule-cb:checked') return ruleBoxes.filter(cb => cb.checked);
    if (selector === '.dq-rule-cb') return ruleBoxes;
    return [];
};
global.document = {
    querySelectorAll: (selector) => ({ forEach: (fn) => query(selector).forEach(fn) }),
    querySelector: () => null,
    getElementById: () => ({ classList: { add() {}, remove() {} }, style: {}, value: '10' }),
};
global.fetch = async (url, options) => {
    sent = { url, body: JSON.parse(options.body) };
    return { json: async () => ({ success: false, message: 'stopped in the test' }) };
};

eval(fs.readFileSync(__SOURCE__, 'utf8'));
const dq = window.DQExecModule;
dq._shapesLoaded = true;
dq._shapesCache = __SHAPES__;
dq._showError = () => {};

dq.runAllChecks().then(() => {
    process.stdout.write(JSON.stringify({ sent, notifications }));
});
"""


def _run_checks(dimensions, rules, shapes=None, config=None):
    """Click Run with the given tick state and return the request that went out."""
    script = (
        _HARNESS.replace("__SOURCE__", json.dumps(str(DQ_EXEC_JS)))
        .replace("__DIMENSIONS__", json.dumps(dimensions))
        .replace("__RULES__", json.dumps(rules))
        .replace("__SHAPES__", json.dumps(shapes if shapes is not None else [{"id": "s1"}]))
        .replace(
            "__CONFIG__",
            json.dumps(
                config if config is not None else {"view_table": "cat.sch.triples"}
            ),
        )
    )
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


class TestPayload:
    def test_the_dimensions_travel_with_the_rule_ids(self):
        outcome = _run_checks(
            dimensions=[["conformance", True], ["structural", False]],
            rules=[["shape_conformance", True]],
        )
        assert outcome["sent"]["body"]["dimensions"] == ["conformance"]
        assert outcome["sent"]["body"]["shape_ids"] == ["shape_conformance"]

    def test_an_unticked_dimension_is_left_out(self):
        outcome = _run_checks(
            dimensions=[["conformance", True], ["structural", False]],
            rules=[["shape_conformance", True]],
        )
        assert "structural" not in outcome["sent"]["body"]["dimensions"]

    def test_a_partly_ticked_dimension_still_counts(self):
        """Its remaining rules are selected, so its rule families apply too."""
        outcome = _run_checks(
            dimensions=[["conformance", True], ["structural", False]],
            rules=[["a", True], ["b", False]],
        )
        assert outcome["sent"]["body"]["dimensions"] == ["conformance"]

    def test_a_dimension_with_no_shape_can_run_alone(self):
        """Structural may hold only SWRL rules, which have no checkbox."""
        outcome = _run_checks(
            dimensions=[["structural", True]], rules=[], shapes=[{"id": "s1"}]
        )
        assert outcome["sent"]["body"]["dimensions"] == ["structural"]
        assert "shape_ids" not in outcome["sent"]["body"]

    def test_nothing_selected_sends_no_request(self):
        outcome = _run_checks(dimensions=[["conformance", False]], rules=[["a", False]])
        assert outcome["sent"] is None
        assert "Select at least one" in outcome["notifications"][0]


class TestRuleFamilySelection:
    """A picked SWRL rule reaches the request the way a picked shape does."""

    def test_a_family_rule_id_travels_as_a_shape_id(self):
        outcome = _run_checks(
            dimensions=[["structural", True]],
            rules=[["swrl:no_orphans", True], ["swrl:unique_ids", False]],
        )
        assert outcome["sent"]["body"]["shape_ids"] == ["swrl:no_orphans"]


class TestExecutionTarget:
    """The run has one target, so the request no longer negotiates one."""

    def test_no_backend_is_named(self):
        outcome = _run_checks(
            dimensions=[["conformance", True]], rules=[["shape_conformance", True]]
        )
        assert "backend" not in outcome["sent"]["body"]

    def test_the_table_is_left_to_the_server(self):
        """The server resolves the VIEW; a client-supplied name could disagree."""
        outcome = _run_checks(
            dimensions=[["conformance", True]], rules=[["shape_conformance", True]]
        )
        assert "triplestore_table" not in outcome["sent"]["body"]

    def test_a_graph_without_a_view_cannot_run(self):
        outcome = _run_checks(
            dimensions=[["conformance", True]],
            rules=[["shape_conformance", True]],
            config={"graph_name": "Cust360_V5"},
        )
        assert outcome["sent"] is None
        assert "Build the Knowledge Graph first" in outcome["notifications"][0]
