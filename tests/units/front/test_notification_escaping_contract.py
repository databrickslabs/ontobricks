"""Shared toast / notification / dialog helpers escape text by default."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FRONT = Path(__file__).resolve().parents[3] / "src" / "front"
UTILS = (FRONT / "static/global/js/utils.js").read_text()


def test_toast_escapes_message():
    assert "'<div class=\"flex-grow-1\">' + escapeHtml(message) + '</div>'" in UTILS


def test_notification_center_stores_escaped_html_unless_trusted():
    assert "html: isHtml ? String(message) : escapeHtml(message)" in UTILS
    assert "+ m.html +" in UTILS
    assert "+ m.message +" not in UTILS


def test_confirm_and_info_dialogs_escape_message():
    assert '<p class="mb-0">${messageHtml || escapeHtml(message)}</p>' in UTILS
    assert '<p class="mb-0">${escapeHtml(message)}</p>' in UTILS
    assert '<p class="mb-0">${message}</p>' not in UTILS


def test_task_tracker_marks_its_prebuilt_markup_as_trusted():
    tracker = (FRONT / "static/global/js/task-tracker.js").read_text()
    assert "NotificationCenter.add(body, type, true)" in tracker


def _dialog_messages():
    files = list((FRONT / "static").rglob("*.js")) + list((FRONT / "templates").rglob("*.html"))
    for path in files:
        src = path.read_text()
        for call in re.finditer(r"show(?:Confirm|Info)Dialog\(", src):
            segment = src[call.end(): call.end() + 1500]
            match = re.search(
                r"(?<!\w)message\s*:\s*(.*?)(?=,\s*\n\s*\w+\s*:|\n\s*\}\s*\)|\}\))",
                segment,
                re.S,
            )
            if match:
                line = src[: call.start()].count("\n") + 1
                yield f"{path.relative_to(FRONT)}:{line}", match.group(1)


def test_plain_dialog_messages_carry_no_markup_or_pre_escaping():
    offenders = [
        where
        for where, message in _dialog_messages()
        if re.search(r"<\w", message) or "escapeHtml(" in message
    ]
    assert offenders == []
