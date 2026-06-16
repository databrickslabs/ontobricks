#!/usr/bin/env python3
"""Render ``app.yaml`` from ``app.yaml.template`` + the env exported
by ``scripts/deploy.config.sh``.

Called by ``scripts/deploy.sh`` immediately before
``databricks bundle deploy``. Uses ``string.Template.substitute`` (not
``safe_substitute``) so a missing variable fails loudly instead of
silently shipping a literal ``${FOO}`` to Databricks Apps.

Optional env-var entries
------------------------
Any ``env`` block item whose ``value`` renders to an empty string is
**removed from the output** so that Databricks Apps never sees it.
This prevents a stale value from a previous deploy persisting when the
operator later clears the override.  Mark optional entries with an
inline comment ``# optional`` on the ``- name:`` line:

    - name: ONTOBRICKS_SYNC_UC_CATALOG  # optional
      value: "${APP_SYNC_UC_CATALOG}"

Any item *without* the ``# optional`` tag that renders to an empty
string is left as-is (so required vars that happen to be empty still
produce a validation error at runtime rather than being silently
omitted).
"""

from __future__ import annotations

import os
import re
import string
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

_RENDERS: list[tuple[Path, Path]] = [
    (REPO_ROOT / "app.yaml.template",                     REPO_ROOT / "app.yaml"),
    (REPO_ROOT / "src/mcp-server/app.yaml.template",      REPO_ROOT / "src/mcp-server/app.yaml"),
]

# Matches a 2-line optional env-var block:
#   - name: FOO  # optional
#     value: ""
# The value must be an empty string (after substitution).
_OPTIONAL_EMPTY_RE = re.compile(
    r"[ \t]*- name:[ \t]+\S+[ \t]+#[ \t]*optional\n"
    r"[ \t]+value:[ \t]*[\"']{0,1}[\"']{0,1}[ \t]*\n",
)


def _strip_empty_optional(text: str) -> tuple[str, list[str]]:
    """Remove optional env-var entries whose value is empty.

    Returns the cleaned text and a list of removed variable names for
    logging.
    """
    removed: list[str] = []

    def _sub(m: re.Match) -> str:
        name_match = re.search(r"- name:[ \t]+(\S+)", m.group(0))
        if name_match:
            removed.append(name_match.group(1))
        return ""

    cleaned = _OPTIONAL_EMPTY_RE.sub(_sub, text)
    return cleaned, removed


def _render_one(template_path: Path, output_path: Path) -> int:
    if not template_path.exists():
        print(f"ERROR: missing {template_path}", file=sys.stderr)
        return 1

    template = string.Template(template_path.read_text())
    try:
        rendered = template.substitute(os.environ)
    except KeyError as exc:
        print(
            f"ERROR: {template_path.name} references ${{{exc.args[0]}}} but the "
            "variable is not set. Add it to scripts/deploy.config.sh or "
            "export it before running deploy.",
            file=sys.stderr,
        )
        return 2
    except ValueError as exc:
        print(f"ERROR: malformed placeholder in {template_path.name}: {exc}", file=sys.stderr)
        return 3

    rendered, omitted = _strip_empty_optional(rendered)
    for name in omitted:
        print(f"  omitted empty optional env var: {name}")

    output_path.write_text(rendered)
    print(f"  rendered {output_path.relative_to(REPO_ROOT)} from "
          f"{template_path.relative_to(REPO_ROOT)}")
    return 0


def main() -> int:
    rc = 0
    for tpl, out in _RENDERS:
        rc = rc or _render_one(tpl, out)
    return rc


if __name__ == "__main__":
    sys.exit(main())
