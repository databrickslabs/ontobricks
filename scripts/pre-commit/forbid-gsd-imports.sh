#!/usr/bin/env bash
# Pre-commit hook: forbid any staged file from referencing `gsd-*` skill names.
# CNS §3.12 anti-pattern — "Re-introducing GSD references → reject in code review".
#
# Whitelist: this script itself, the methodology plan, and the §3.0 historical note in CLAUDE.md.
set -euo pipefail

staged=$(git diff --cached --name-only --diff-filter=ACM)

bad_files=()
while IFS= read -r f; do
  [ -z "$f" ] && continue
  # Skip whitelisted files.
  case "$f" in
    scripts/pre-commit/forbid-gsd-imports.sh) continue ;;
    .planning/*) ;;  # check these too — but ROADMAP narrative may name "GSD-free"
    *) ;;
  esac
  # Only check text files we care about (.py, .md, .yaml, .yml, .toml, .sh, .mdc).
  case "$f" in
    *.py|*.md|*.mdc|*.yaml|*.yml|*.toml|*.sh|*.cfg) ;;
    *) continue ;;
  esac
  [ -f "$f" ] || continue
  if grep -qE '^\s*(import|from)\s+gsd[._-]' "$f" 2>/dev/null; then
    bad_files+=("$f (import)")
  fi
  # Match `gsd-something` patterns in code or docs (skill invocations).
  # Allow the words "GSD-free" / "drop GSD" / "Why we dropped GSD" — narrative is OK.
  if grep -nE 'gsd-[a-z][a-z0-9-]+' "$f" 2>/dev/null \
       | grep -vE '(GSD-free|drop GSD|dropped GSD|without GSD|no GSD|GSD orchestration|reject in code review)' > /dev/null 2>&1; then
    bad_files+=("$f (skill reference)")
  fi
done <<<"$staged"

if [ ${#bad_files[@]} -gt 0 ]; then
  echo "ERROR: gsd-* references detected in staged files (CNS §3.12 anti-pattern)."
  echo ""
  for line in "${bad_files[@]}"; do
    echo "  - $line"
  done
  echo ""
  echo "Fix:"
  echo "  - Replace gsd-* with the Superpowers / project-skill equivalent (see CNS §3.0)."
  echo "  - Narrative references ('we dropped GSD') are allowed."
  echo ""
  echo "Bypass once: SKIP=forbid-gsd-imports git commit ..."
  exit 1
fi

exit 0
