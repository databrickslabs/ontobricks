#!/usr/bin/env python3
"""Render ``app.yaml`` from ``app.yaml.template`` + the env exported
by ``scripts/deploy.config.sh``.

Called by ``scripts/deploy.sh`` immediately before
``databricks bundle deploy``. Uses ``string.Template.substitute`` (not
``safe_substitute``) so a missing variable fails loudly instead of
silently shipping a literal ``${FOO}`` to Databricks Apps.
"""

from __future__ import annotations

import os
import string
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = REPO_ROOT / "app.yaml.template"
OUTPUT_PATH = REPO_ROOT / "app.yaml"


def main() -> int:
    if not TEMPLATE_PATH.exists():
        print(f"ERROR: missing {TEMPLATE_PATH}", file=sys.stderr)
        return 1

    template = string.Template(TEMPLATE_PATH.read_text())
    try:
        rendered = template.substitute(os.environ)
    except KeyError as exc:
        print(
            f"ERROR: app.yaml.template references ${{{exc.args[0]}}} but the "
            "variable is not set. Add it to scripts/deploy.config.sh or "
            f"export it before running deploy.",
            file=sys.stderr,
        )
        return 2
    except ValueError as exc:
        print(f"ERROR: malformed placeholder in template: {exc}", file=sys.stderr)
        return 3

    OUTPUT_PATH.write_text(rendered)
    print(f"  rendered {OUTPUT_PATH.relative_to(REPO_ROOT)} from "
          f"{TEMPLATE_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
