"""Date/time display format: ``YYYY/MM/DD HH:MM:SS`` everywhere in the UI.

The formatter lives in ``global/js/utils.js``. These tests run its source block
under Node with a fixed ``TZ=UTC`` so the expected strings are deterministic.
"""

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = ROOT / "src/front/static/global/js/utils.js"
STATIC = ROOT / "src/front/static"

_START = "// ========== DATE / TIME FORMATTING"
_END = "// ========== STRING UTILITIES"


def _run_node(body: str) -> None:
    runner = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        const src = fs.readFileSync(process.argv[1], 'utf8');
        const start = src.indexOf(process.argv[2]);
        const end = src.indexOf(process.argv[3]);
        if (start < 0 || end < 0) throw new Error('formatter block not found');
        const {formatDateTime, formatDateOnly} = new Function(
            src.slice(start, end) + '; return {formatDateTime, formatDateOnly};'
        )();
        """
    ) + textwrap.dedent(body)
    result = subprocess.run(
        ["node", "-e", runner, str(UTILS_JS), _START, _END],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "TZ": "UTC"},
    )
    assert result.returncode == 0, result.stderr


def test_python_isoformat_with_microseconds_is_formatted():
    _run_node(
        """
        assert.equal(formatDateTime('2026-11-05T07:36:01.730419+00:00'), '2026/11/05 07:36:01');
        """
    )


def test_offset_is_converted_to_browser_local_time():
    _run_node(
        """
        assert.equal(formatDateTime('2026-11-05T09:36:01+02:00'), '2026/11/05 07:36:01');
        assert.equal(formatDateTime('2026-11-05T07:36:01Z'), '2026/11/05 07:36:01');
        """
    )


def test_naive_iso_is_read_as_utc():
    _run_node(
        """
        assert.equal(formatDateTime('2026-11-05T07:36:01'), '2026/11/05 07:36:01');
        assert.equal(formatDateTime('2026-11-05 07:36:01.123456'), '2026/11/05 07:36:01');
        """
    )


def test_date_objects_and_epoch_ms_are_supported():
    _run_node(
        """
        assert.equal(formatDateTime(new Date(2026, 10, 5, 7, 36, 1)), '2026/11/05 07:36:01');
        assert.equal(formatDateTime(Date.UTC(2026, 10, 5, 7, 36, 1)), '2026/11/05 07:36:01');
        assert.equal(formatDateOnly(new Date(2026, 10, 5, 7, 36, 1)), '2026/11/05');
        """
    )


def test_bare_date_is_shown_without_time():
    _run_node(
        """
        assert.equal(formatDateTime('2026-11-05'), '2026/11/05');
        assert.equal(formatDateOnly('2026-11-05'), '2026/11/05');
        assert.equal(formatDateOnly('2026-11-05T07:36:01.730419+00:00'), '2026/11/05');
        """
    )


def test_empty_and_unparseable_values():
    _run_node(
        """
        assert.equal(formatDateTime(null), '');
        assert.equal(formatDateTime(undefined), '');
        assert.equal(formatDateTime(''), '');
        assert.equal(formatDateTime('not-a-date'), 'not-a-date');
        assert.equal(formatDateOnly('not-a-date'), 'not-a-date');
        """
    )


def test_call_sites_use_shared_formatter_not_raw_values():
    versions_js = (STATIC / "domain/js/domain-versions.js").read_text(encoding="utf-8")
    assert "formatDateTime(version.last_update)" in versions_js
    assert "formatDateTime(version.last_build)" in versions_js
    assert "version.last_update ||" not in versions_js
    assert "version.last_build ||" not in versions_js

    timed_log = (STATIC / "query/js/timed-task-log.js").read_text(encoding="utf-8")
    assert "formatDateTime(task.created_at)" in timed_log
    assert '"Exported at     : " + formatDateTime(new Date())' in timed_log
