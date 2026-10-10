#!/usr/bin/env python3
"""Regenerate the table of contents at the top of every docs/*.md guide.

The TOC lives between ``<!-- toc -->`` and ``<!-- /toc -->`` just before the
first ``##`` section. It lists every ``##`` section; guides with at most
``NESTED_MAX_H2`` sections also list their ``###`` subsections. Anchors follow
GitHub's heading slugs (the in-app Help viewer assigns the same ids).

Usage: python3 scripts/_internal/_docs-toc.py [--check] [docs_dir]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

START = "<!-- toc -->"
END = "<!-- /toc -->"
SKIP_FILES = {"README.md"}
NESTED_MAX_H2 = 4
SKIP_TITLES = {"table of contents"}

_FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_TOC_BLOCK = re.compile(
    re.escape(START) + r".*?" + re.escape(END) + r"\n*", re.DOTALL
)


def heading_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", "", raw)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    return re.sub(r"[`*]", "", text).strip()


def slugify(text: str) -> str:
    return re.sub(r"[^\w\s-]", "", text.lower()).replace(" ", "-")


def headings(lines: list[str]) -> list[tuple[int, int, str, str]]:
    """(line_index, level, text, anchor) for headings outside code fences."""
    out, seen, fence = [], {}, None
    for i, line in enumerate(lines):
        m = _FENCE.match(line)
        if m:
            tick = m.group(1)
            if fence is None:
                fence = tick
            elif tick[0] == fence[0] and len(tick) >= len(fence):
                fence = None
            continue
        if fence:
            continue
        h = _HEADING.match(line)
        if not h:
            continue
        text = heading_text(h.group(2))
        base = slugify(text)
        if base in seen:
            seen[base] += 1
            anchor = f"{base}-{seen[base]}"
        else:
            seen[base] = 0
            anchor = base
        out.append((i, len(h.group(1)), text, anchor))
    return out


def build_toc(heads: list[tuple[int, int, str, str]]) -> str:
    h2_count = sum(1 for _, lvl, _, _ in heads if lvl == 2)
    max_level = 3 if h2_count <= NESTED_MAX_H2 else 2
    items = [
        f"{'  ' * (lvl - 2)}- [{text}](#{anchor})"
        for _, lvl, text, anchor in heads
        if 2 <= lvl <= max_level and text.lower() not in SKIP_TITLES
    ]
    return "\n".join([START, "**Contents**", "", *items, END]) + "\n\n"


def render(source: str) -> str:
    stripped = _TOC_BLOCK.sub("", source)
    lines = stripped.splitlines(keepends=True)
    heads = headings(lines)
    first_h2 = next((i for i, lvl, _, _ in heads if lvl == 2), None)
    if first_h2 is None:
        return source
    insert_at = first_h2
    while insert_at > 0 and lines[insert_at - 1].strip() in ("", "---"):
        insert_at -= 1
    before = "".join(lines[:insert_at]).rstrip("\n") + "\n\n"
    return before + build_toc(heads) + "".join(lines[insert_at:]).lstrip("\n")


def main(argv: list[str]) -> int:
    check = "--check" in argv
    args = [a for a in argv if a != "--check"]
    docs = Path(args[0] if args else "docs")
    stale = []
    for path in sorted(docs.glob("*.md")):
        if path.name in SKIP_FILES:
            continue
        current = path.read_text(encoding="utf-8")
        updated = render(current)
        if updated == current:
            continue
        stale.append(path)
        if not check:
            path.write_text(updated, encoding="utf-8")
    verb = "stale" if check else "updated"
    for path in stale:
        print(f"{verb}: {path}")
    return 1 if check and stale else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
