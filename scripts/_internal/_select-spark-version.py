#!/usr/bin/env python3
"""Print the realtime stream job's runtime: the newest DBR 19 key the workspace lists, LTS first.

Reads ``databricks clusters spark-versions -o json`` on stdin and prints one
``spark_version`` key — the newest 19.x LTS, else the newest 19.x — or nothing
when there is none (or the input is not the CLI's JSON), so
``scripts/deploy.sh`` keeps the bundle default. ML, GPU, Photon and ARM
runtimes are skipped: the stream job runs on the standard engine. Exit status
is always 0.

Why 19: Kinesis polling in Real-Time Mode needs DBR 19, and the Python
``transformWithState`` low-throughput latency fix needs 18.2 or later.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable, Mapping
from typing import Any, TextIO

_MAJOR = "19"
_SPECIAL_KEY = re.compile(r"(^|-)(cpu-ml|gpu-ml|ml|gpu|photon|aarch64)(-|$)", re.IGNORECASE)
_SPECIAL_NAME = re.compile(r"\b(ML|GPU|Photon|ARM)\b")


class SparkVersionPicker:
    """Pick the stream job's ``spark_version`` key out of the workspace's runtime list."""

    @staticmethod
    def pick(versions: Iterable[Mapping[str, Any]], major: str = _MAJOR) -> str:
        """The newest standard *major*.x key, an LTS when one is listed; ``""`` when none is."""
        candidates = [
            v
            for v in versions
            if str(v.get("key") or "").startswith(f"{major}.")
            and not _SPECIAL_KEY.search(str(v.get("key") or ""))
            and not _SPECIAL_NAME.search(str(v.get("name") or ""))
        ]
        lts = [v for v in candidates if "LTS" in str(v.get("name") or "")]
        pool = lts or candidates
        return str(max(pool, key=SparkVersionPicker._order)["key"]) if pool else ""

    @staticmethod
    def _order(version: Mapping[str, Any]) -> tuple[int, ...]:
        """``19.1.x-scala2.13`` → ``(19, 1, -1)``: numeric parts compare, ``x`` sorts first."""
        release = str(version.get("key") or "").split("-", 1)[0]
        return tuple(int(part) if part.isdigit() else -1 for part in release.split("."))

    @staticmethod
    def versions(text: str) -> list:
        """The ``versions`` list of the CLI's JSON (``[]`` for anything else)."""
        try:
            payload = json.loads(text)
        except ValueError:
            return []
        rows = payload.get("versions") if isinstance(payload, dict) else payload
        return [row for row in rows or [] if isinstance(row, dict)]


def main(stdin: TextIO = sys.stdin) -> int:
    key = SparkVersionPicker.pick(SparkVersionPicker.versions(stdin.read()))
    if key:
        print(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
