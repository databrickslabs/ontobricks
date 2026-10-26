"""Behavior contract: stage-transition focus management in
``ontology-wizard.js``'s ``setWizardStage``.

Fixes a review finding on plan task 5 of ``staged-ontology-generate``:
screen-reader/keyboard users must land on the new stage's heading whenever
the wizard genuinely transitions to a different stage — but the very first
call (resuming a running task or an existing draft on page load) must not
steal focus from wherever the browser naturally placed it, and re-calling
the *same* stage must not re-focus (e.g. `resumeDraftFromServer`'s own
`setWizardStage('review')` right after a fresh detection already called it
once).

Same Node ``vm.runInContext`` pattern as ``test_no_llm_save_gate_sync.py``
(no JS test runner in this repo) — `setWizardStage` is a normal top-level
function in ``ontology-wizard.js``, directly reachable on the vm context.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
WIZARD_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-wizard.js"

pytestmark = pytest.mark.unit


def _run_stage_calls(stages: list[str]) -> list[str]:
    stage_calls = "\n".join(f"context.setWizardStage({json.dumps(s)});" for s in stages)
    harness = f"""
const fs = require('fs');
const vm = require('vm');

const focusLog = [];
const elements = {{}};
function makeEl(id) {{
    const classes = new Set();
    return {{
        id,
        classList: {{
            add(c) {{ classes.add(c); }},
            remove(c) {{ classes.delete(c); }},
            toggle(c, force) {{
                if (force === undefined) force = !classes.has(c);
                if (force) classes.add(c); else classes.delete(c);
                return force;
            }},
            contains(c) {{ return classes.has(c); }},
        }},
        dataset: {{}},
        setAttribute() {{}},
        removeAttribute() {{}},
        addEventListener() {{}},
        querySelectorAll: () => [],
        querySelector: () => null,
        focus() {{ focusLog.push(id); }},
    }};
}}
function getElementById(id) {{
    if (!elements[id]) elements[id] = makeEl(id);
    return elements[id];
}}

const context = {{
    console,
    document: {{
        getElementById,
        readyState: 'complete',
        addEventListener: () => {{}},
        querySelectorAll: () => [],
        querySelector: () => null,
    }},
    sessionStorage: {{ getItem: () => null, setItem: () => {{}}, removeItem: () => {{}} }},
    window: {{}},
}};
context.window = context;

vm.createContext(context);
vm.runInContext(fs.readFileSync({json.dumps(str(WIZARD_JS))}, 'utf8'), context);

{stage_calls}

process.stdout.write(JSON.stringify(focusLog));
"""
    result = subprocess.run(
        ["node", "-e", harness],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_initial_stage_render_on_page_load_does_not_steal_focus():
    """The very first `setWizardStage` call — resuming a task/draft on page
    load — must never move focus, no matter which stage it resumes to."""
    assert _run_stage_calls(["review"]) == []
    assert _run_stage_calls(["complete"]) == []


def test_a_real_transition_after_load_focuses_the_new_stage_heading():
    assert _run_stage_calls(["configure", "review"]) == ["wizardReviewHeading"]


def test_recalling_the_same_stage_does_not_refocus():
    """`resumeDraftFromServer` calls `setWizardStage('review')` a second
    time right after a fresh detection already transitioned there — that
    redundant re-call must not yank focus away from the pane again."""
    assert _run_stage_calls(["configure", "review", "review"]) == [
        "wizardReviewHeading"
    ]


def test_multiple_real_transitions_focus_each_heading_in_order():
    assert _run_stage_calls(["configure", "review", "complete"]) == [
        "wizardReviewHeading",
        "wizardCompleteHeading",
    ]
