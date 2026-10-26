"""Behavior contract: Stage 2 draft mutations are serialized through one
FIFO queue in ``ontology-wizard-review.js``.

Fixes a review finding on plan task 5 of ``staged-ontology-generate``:
rapid edits (typing across two fields, toggling include while an evidence
edit is in flight, ...) used to read ``_draft.draft_revision`` synchronously
before any request was sent, so two calls fired back-to-back always shared
the *same* revision — the second request's response could silently clobber
the first, and a rejected/expired (409) request had no defined effect on
requests already queued behind it.

There is no JS test runner in this repository — real browser-runtime
assertions for `navbar.js`/`domain.js` already run the actual production
JS in Node via `vm.runInContext` (see `test_no_llm_save_gate_sync.py`);
this module follows the exact same pattern against
`ontology-wizard-review.js`, stubbing only the DOM/global surface the
module touches (`document.getElementById`, `fetch`, `showNotification`,
`showConfirmDialog`). `postDraftUpdate` is exposed on
`window.WizardReview` purely for this contract — no markup ever calls it
directly, every real call site goes through the module's own delegated
event handlers.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
REVIEW_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-wizard-review.js"

pytestmark = pytest.mark.unit


def _run(script_body: str) -> list:
    harness = f"""
const fs = require('fs');
const vm = require('vm');

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
        _listeners: {{}},
        addEventListener(type, fn) {{ this._listeners[type] = fn; }},
        querySelectorAll: () => [],
        querySelector: () => null,
        setAttribute() {{}},
        removeAttribute() {{}},
        appendChild() {{}},
        set innerHTML(v) {{ this._innerHTML = v; }},
        get innerHTML() {{ return this._innerHTML || ''; }},
        textContent: '',
        value: '',
        disabled: false,
    }};
}}
const elements = {{}};
function getElementById(id) {{
    if (!elements[id]) elements[id] = makeEl(id);
    return elements[id];
}}

const context = {{
    console,
    setTimeout,
    clearTimeout,
    Promise,
    document: {{
        getElementById,
        querySelectorAll: () => [],
        querySelector: () => null,
    }},
    showNotification: () => {{}},
    showConfirmDialog: async () => true,
    window: {{}},
}};
context.window = context;

vm.createContext(context);
vm.runInContext(fs.readFileSync({json.dumps(str(REVIEW_JS))}, 'utf8'), context);

{script_body}
"""
    result = subprocess.run(
        ["node", "-e", harness],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _draft(revision: int, **overrides) -> str:
    base = {
        "draft_revision": revision,
        "id": "draft-1",
        "stage": "reviewing",
        "stale": False,
        "existing_anchors": [],
        "candidate_entities": [],
    }
    base.update(overrides)
    return json.dumps(base)


def test_rapid_calls_do_not_share_a_revision():
    """Three calls fired back-to-back (no await between them) must each
    read the *current* revision at the time their turn in the queue
    arrives, not all at call time — otherwise they'd all submit revision 0
    and the server would only ever apply the first one."""
    script = f"""
const requestLog = [];
context.fetch = async (url, opts) => {{
    const body = JSON.parse(opts.body);
    requestLog.push(body.revision);
    const newRevision = body.revision + 1;
    return {{
        ok: true,
        status: 200,
        json: async () => ({{
            success: true,
            draft: {{ draft_revision: newRevision, id: 'draft-1', stage: 'reviewing', existing_anchors: [], candidate_entities: [] }},
        }}),
    }};
}};
context.window.WizardReview.render(JSON.parse('{_draft(0)}'));
const p1 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'a', updates: {{}} }});
const p2 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'a', updates: {{}} }});
const p3 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'a', updates: {{}} }});
Promise.all([p1, p2, p3]).then(() => {{
    process.stdout.write(JSON.stringify(requestLog));
}});
"""
    assert _run(script) == [0, 1, 2]


def test_interaction_order_is_preserved_under_the_queue():
    """Three distinct edits issued in order a, b, c must be sent to the
    server in that same order — the queue must never reorder or
    interleave concurrent mutations."""
    script = f"""
const requestLog = [];
context.fetch = async (url, opts) => {{
    const body = JSON.parse(opts.body);
    requestLog.push(body.entity_id);
    const newRevision = body.revision + 1;
    return {{
        ok: true,
        status: 200,
        json: async () => ({{
            success: true,
            draft: {{ draft_revision: newRevision, id: 'draft-1', stage: 'reviewing', existing_anchors: [], candidate_entities: [] }},
        }}),
    }};
}};
context.window.WizardReview.render(JSON.parse('{_draft(0)}'));
const p1 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'a', updates: {{}} }});
const p2 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'b', updates: {{}} }});
const p3 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'c', updates: {{}} }});
Promise.all([p1, p2, p3]).then(() => {{
    process.stdout.write(JSON.stringify(requestLog));
}});
"""
    assert _run(script) == ["a", "b", "c"]


def test_queue_continues_after_a_409_rejection_without_losing_later_edits():
    """A 409 on the second queued call must not wedge the queue: the third
    call still gets sent, and it carries the revision the client reloaded
    after the conflict (not the stale one that caused the 409)."""
    script = f"""
const requestLog = [];
let call = 0;
context.fetch = async (url, opts) => {{
    call += 1;
    if (url === '/ontology/wizard/generate/draft') {{
        // Reload after the 409 — server says the real current revision is 5.
        return {{
            ok: true,
            status: 200,
            json: async () => ({{
                success: true,
                draft: {{ draft_revision: 5, id: 'draft-1', stage: 'reviewing', stale: false, existing_anchors: [], candidate_entities: [] }},
            }}),
        }};
    }}
    const body = JSON.parse(opts.body);
    requestLog.push(body.revision);
    if (call === 2) {{
        return {{ ok: false, status: 409, json: async () => ({{ success: false, message: 'conflict' }}) }};
    }}
    const newRevision = body.revision + 1;
    return {{
        ok: true,
        status: 200,
        json: async () => ({{
            success: true,
            draft: {{ draft_revision: newRevision, id: 'draft-1', stage: 'reviewing', existing_anchors: [], candidate_entities: [] }},
        }}),
    }};
}};
context.window.WizardReview.render(JSON.parse('{_draft(0)}'));
const p1 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'a', updates: {{}} }});
const p2 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'b', updates: {{}} }});
const p3 = context.window.WizardReview.postDraftUpdate({{ op: 'update', entity_id: 'c', updates: {{}} }});
Promise.all([p1, p2, p3]).then(() => {{
    process.stdout.write(JSON.stringify(requestLog));
}});
"""
    # Call 1 succeeds at revision 0 -> server bumps to 1.
    # Call 2 is issued at revision 1 but the server rejects with 409 (some
    # other change landed) -> client reloads and learns the real revision (5).
    # Call 3 must still fire (queue not stuck) and use the reloaded revision.
    assert _run(script) == [0, 1, 5]
