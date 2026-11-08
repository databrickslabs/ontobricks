"""Serialize / parse OntoBricks SWRL text (IF → THEN blocks).

Format (blank-line separated)::

    # Rule: <name>
    # Description: <optional>
    <antecedent> -> <consequent>
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

_RULE_RE = re.compile(r"^#\s*Rule:\s*(.*)$", re.IGNORECASE)
_DESC_RE = re.compile(r"^#\s*Description:\s*(.*)$", re.IGNORECASE)
_ARROW_RE = re.compile(r"\s*->\s*")


def serialize_rules(rules: List[Dict[str, Any]]) -> str:
    """Render SWRL rule dicts as OntoBricks SWRL text."""
    blocks: List[str] = []
    for rule in rules or []:
        name = (rule.get("name") or "").strip()
        if not name:
            continue
        lines = [f"# Rule: {name}"]
        desc = (rule.get("description") or "").strip()
        if desc:
            lines.append(f"# Description: {desc}")
        ant = (rule.get("antecedent") or "").strip()
        cons = (rule.get("consequent") or "").strip()
        lines.append(f"{ant} -> {cons}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def parse_rules(text: str) -> List[Dict[str, Any]]:
    """Parse OntoBricks SWRL text into rule dicts.

    Raises:
        ValueError: if any non-empty block is malformed (fail closed).
    """
    raw = (text or "").strip()
    if not raw:
        return []

    blocks = re.split(r"\n\s*\n", raw)
    rules: List[Dict[str, Any]] = []
    for i, block in enumerate(blocks, start=1):
        name = ""
        description = ""
        body_lines: List[str] = []
        for line in block.splitlines():
            s = line.strip()
            if not s:
                continue
            m_rule = _RULE_RE.match(s)
            if m_rule:
                name = m_rule.group(1).strip()
                continue
            m_desc = _DESC_RE.match(s)
            if m_desc:
                description = m_desc.group(1).strip()
                continue
            if s.startswith("#"):
                continue
            body_lines.append(s)

        if not body_lines:
            if name or description:
                raise ValueError(
                    f"Rule block {i}: missing implication (antecedent -> consequent)"
                )
            continue

        body = " ".join(body_lines)
        parts = _ARROW_RE.split(body, maxsplit=1)
        if len(parts) != 2:
            raise ValueError(
                f"Rule block {i}: missing implication (antecedent -> consequent)"
            )
        antecedent, consequent = parts[0].strip(), parts[1].strip()
        if not antecedent:
            raise ValueError(f"Rule block {i}: empty antecedent")
        if not consequent:
            raise ValueError(f"Rule block {i}: empty consequent")
        if not name:
            name = f"Imported rule {i}"
        rules.append(
            {
                "name": name,
                "description": description,
                "antecedent": antecedent,
                "consequent": consequent,
            }
        )
    return rules
