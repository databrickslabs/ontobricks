"""Complex rule assistant: the question behind the offered answers is always shown.

The assistant can return answer buttons whose question lives only in
``options.question``; the reply text may not repeat it. The editor must show
that question in the chat bubble and keep it in the history sent back on the
next turn, otherwise the user sees buttons with nothing to answer.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
JS = REPO_ROOT / "src/front/static/ontology/js/ontology-business-rules.js"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the frontend module"
)

_HARNESS = """
const fs = require('fs');
global.window = global;
global.document = { addEventListener() {}, getElementById: () => null };
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const M = window.BusinessRulesModule;
process.stdout.write(JSON.stringify(M._craReplyWithQuestion(__DATA__)));
"""


def _reply(data):
    script = _HARNESS.replace("__SOURCE__", json.dumps(str(JS))).replace(
        "__DATA__", json.dumps(data)
    )
    completed = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


QUESTION = "What should disappear when a contract is terminated?"
OPTIONS = {"question": QUESTION, "options": ["Only the link", "The meter itself"], "multi": False}


def test_a_question_missing_from_the_reply_is_appended():
    text = _reply({"reply": "I'll wait for your answer.", "options": OPTIONS})
    assert text == "I'll wait for your answer.\n\n" + QUESTION


def test_a_question_alone_becomes_the_reply():
    assert _reply({"reply": "", "options": OPTIONS}) == QUESTION


def test_a_reply_that_already_asks_the_question_is_unchanged():
    reply = "Sure. " + QUESTION
    assert _reply({"reply": reply, "options": OPTIONS}) == reply


def test_no_options_keeps_the_reply():
    assert _reply({"reply": "Done.", "options": None}) == "Done."
    assert _reply({"reply": "Done.", "options": {"question": QUESTION, "options": []}}) == "Done."


def test_the_history_and_the_bubble_use_the_question():
    source = JS.read_text(encoding="utf-8")
    assert "content: this._craReplyWithQuestion(data)" in source
    assert "const text = this._craReplyWithQuestion(data);" in source
