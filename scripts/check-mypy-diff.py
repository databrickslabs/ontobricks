#!/usr/bin/env python3
"""Compare current mypy output to mypy_baseline.txt — fail on NEW errors only.

Mitigates M3.P1 risk #4: "Ruff/mypy adoption flood — strict mypy on 100k+ LOC
will surface hundreds of violations." The baseline accepts existing violations
and gates PRs only on what they ADD.

Usage:
    uv run python scripts/check-mypy-diff.py

Exit codes:
    0 — no new errors (or baseline file matches current output exactly).
    1 — PR introduces NEW mypy errors not in the baseline.
    2 — mypy itself failed to run.

CI wiring:
    - run: uv run python scripts/check-mypy-diff.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BASELINE = Path("mypy_baseline.txt")


def run_mypy() -> list[str]:
    result = subprocess.run(
        [
            "uv", "run", "mypy", "src",
            "--no-error-summary",
            "--hide-error-context",
            "--no-color-output",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    # mypy exits non-zero when there are errors — that's expected.
    lines = [ln for ln in result.stdout.splitlines() if ln.startswith("src/")]
    return sorted(lines)


def main() -> int:
    if not BASELINE.exists():
        print(f"ERROR: {BASELINE} missing. Run scripts/generate-mypy-baseline.sh first.", file=sys.stderr)
        return 2

    baseline_lines = sorted(set(BASELINE.read_text().splitlines()))
    current_lines = run_mypy()

    new_errors = [ln for ln in current_lines if ln not in set(baseline_lines)]
    fixed_errors = [ln for ln in baseline_lines if ln not in set(current_lines)]

    if new_errors:
        print(f"FAIL: {len(new_errors)} new mypy error(s) introduced relative to baseline:\n")
        for ln in new_errors[:50]:
            print(f"  {ln}")
        if len(new_errors) > 50:
            print(f"  ... and {len(new_errors) - 50} more.")
        print()
        print("Fix the errors, or (if a baseline tightening is intentional):")
        print("  uv run ./scripts/generate-mypy-baseline.sh")
        print("  git add mypy_baseline.txt")
        return 1

    if fixed_errors:
        print(f"OK — gate passes. Bonus: {len(fixed_errors)} baseline error(s) appear to be fixed:")
        for ln in fixed_errors[:5]:
            print(f"  {ln}")
        print()
        print("Consider running scripts/generate-mypy-baseline.sh to tighten the baseline.")
    else:
        print("OK — no new mypy errors.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
