#!/usr/bin/env bash
# Pre-commit hook: if any staged file under src/ or tests/ changed, require a
# changelogs/ diff in the same commit. Bypass with: `SKIP=changelog-presence git commit ...`
#
# Mirrors the planned CI gate (M3.P2). Catches the mistake locally so PRs don't
# fail on the GitHub Actions equivalent.
set -euo pipefail

staged=$(git diff --cached --name-only)

needs_changelog=false
has_changelog=false

while IFS= read -r f; do
  case "$f" in
    src/*|tests/*)
      needs_changelog=true
      ;;
    changelogs/*)
      has_changelog=true
      ;;
  esac
done <<<"$staged"

if [ "$needs_changelog" = true ] && [ "$has_changelog" = false ]; then
  echo "ERROR: src/ or tests/ changed but no changelogs/ entry staged."
  echo ""
  echo "Fix:"
  echo "  1. Edit (or create) changelogs/$(date -u +%Y-%m-%d).log."
  echo "  2. Add a '## <Title>' section per the template in changelogs/README.md."
  echo "  3. git add changelogs/$(date -u +%Y-%m-%d).log"
  echo "  4. Re-run the commit."
  echo ""
  echo "Bypass once: SKIP=changelog-presence git commit ..."
  exit 1
fi

exit 0
