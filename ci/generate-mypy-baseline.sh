#!/usr/bin/env bash
# Regenerate mypy_baseline.txt — the accepted-violation baseline that CI compares against.
#
# Run when you intentionally accept current mypy violations (initial onboarding).
# CI will then fail PRs that introduce NEW violations relative to this baseline.
#
# Usage:
#   ./scripts/generate-mypy-baseline.sh
#
# Then `git add mypy_baseline.txt && git commit`.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

echo "Regenerating mypy_baseline.txt against src/..."
# `--no-error-summary` strips the trailing 'Found N errors' line; we want the
# per-file errors as the baseline.
uv run mypy src \
    --no-error-summary \
    --hide-error-context \
    --no-color-output \
    2>&1 \
  | grep -E '^src/' \
  | sort \
  > mypy_baseline.txt || true

n=$(wc -l <mypy_baseline.txt)
echo "Wrote $n baseline error lines to mypy_baseline.txt."
echo ""
echo "Next:"
echo "  git diff mypy_baseline.txt   # inspect what's now accepted"
echo "  git add mypy_baseline.txt"
echo "  uv run python scripts/check-mypy-diff.py   # dry-run the gate"
