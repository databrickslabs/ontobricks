"""A data quality check that did not run must not report a pass rate.

An untranslatable rule comes back with no violations, which is
indistinguishable from a clean result unless the status is taken into account.
Reading it as "100%" told the user their data was perfect on a rule that never
executed, next to the warning icon saying it hadn't.

The row is rendered by ``DQExecModule._renderResultRow``; these tests run it
under node.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DQ_EXEC_JS = REPO_ROOT / "src/front/static/query/js/query-dataquality.js"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the frontend module"
)

# `_escHtml` escapes through a detached element, so createElement has to
# reproduce the textContent → innerHTML conversion.
_HARNESS = """
const fs = require('fs');
global.window = {};
global.document = {
    createElement: () => ({
        set textContent(value) {
            this.innerHTML = String(value == null ? '' : value)
                .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        },
        innerHTML: '',
    }),
};
eval(fs.readFileSync(__SOURCE__, 'utf8'));
process.stdout.write(window.DQExecModule._renderResultRow(__RESULT__));
"""


def _row(result: dict) -> str:
    script = _HARNESS.replace("__SOURCE__", json.dumps(str(DQ_EXEC_JS))).replace(
        "__RESULT__", json.dumps(result)
    )
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


def _percent_cell(html: str) -> str:
    match = re.search(r'<td class="dq-col-pct([^"]*)"[^>]*>([^<]*)</td>', html)
    assert match, f"no percentage cell in {html}"
    return match.group(2).strip()


def _percent_classes(html: str) -> str:
    return re.search(r'<td class="dq-col-pct([^"]*)"', html).group(1)


def _check(status, **extra):
    return {
        "name": "Monthly fee between 1 and 10",
        "category": "conformance",
        "shape_id": "shape_conformance_Contract_monthlyFee_abc123",
        "status": status,
        "message": "",
        "violations": [],
        **extra,
    }


class TestChecksThatDidNotRun:
    @pytest.mark.parametrize("status", ["info", "warning"])
    def test_no_pass_rate_is_shown(self, status):
        assert _percent_cell(_row(_check(status))) == "-"

    @pytest.mark.parametrize("status", ["info", "warning"])
    def test_the_cell_is_muted_rather_than_green(self, status):
        classes = _percent_classes(_row(_check(status)))
        assert "text-muted" in classes
        assert "text-success" not in classes

    def test_an_untranslatable_rule_never_reads_as_100(self):
        html = _row(_check("info", message="Cannot translate to SQL"))
        assert "100%" not in html
        assert "Cannot translate to SQL" in html

    def test_the_reason_is_offered_as_a_tooltip(self):
        assert "This check did not run" in _row(_check("info"))


class TestChecksThatRan:
    def test_a_clean_check_still_reads_as_100(self):
        html = _row(_check("success"))
        assert _percent_cell(html) == "100%"
        assert "text-success" in _percent_classes(html)

    def test_a_measured_pass_rate_is_shown(self):
        html = _row(
            _check(
                "error",
                violations=[{"s": "x"}],
                violation_total=1,
                total_population=100,
                pass_pct=99.0,
            )
        )
        assert _percent_cell(html) == "99%"

    def test_a_failing_check_without_a_population_reads_as_unknown(self):
        html = _row(_check("error", violations=[{"s": "x"}]))
        assert _percent_cell(html) == "-"

    def test_a_low_pass_rate_is_flagged(self):
        html = _row(
            _check(
                "error",
                violations=[{"s": "x"}],
                violation_total=50,
                total_population=100,
                pass_pct=50.0,
            )
        )
        assert "text-danger" in _percent_classes(html)
