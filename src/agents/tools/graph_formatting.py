"""
Shared knowledge-graph formatting helpers.

Converts raw triple / GraphQL API payloads into readable text suitable
for LLM consumption.  Extracted to be reusable from both the MCP server
and the Graph Chat agent (``agent_dtwin_chat``).  The MCP server keeps
its own copy because it is shipped as an independent wheel with only
``server/`` on its Python path.
"""

from __future__ import annotations

import re
from typing import Iterable

from back.core.w3c.rdf_utils import uri_local_name

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


# ── URI helpers ───────────────────────────────────────────────────────────


def local_name(uri: str) -> str:
    """Extract the human-readable local name from a URI.

    Thin wrapper around :func:`back.core.w3c.rdf_utils.uri_local_name`
    that preserves the original URI for pathological trailing-separator
    inputs (``"http://foo/"`` → ``"http://foo/"``) so that the
    LLM-facing formatter never renders an empty entity label.

    ``https://ontobricks.com/ontology/Customer/CUST00094``  →  ``CUST00094``
    ``http://www.w3.org/1999/02/22-rdf-syntax-ns#type``     →  ``type``
    """
    name = uri_local_name(uri)
    return name if name else uri


def pretty_predicate(uri: str) -> str:
    """Turn a predicate URI into a readable attribute name."""
    name = local_name(uri)
    m = re.match(r"^ontology(.+)$", name, re.IGNORECASE)
    if m:
        name = m.group(1)
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return name.replace("_", " ").strip()


def is_uri(value: str) -> bool:
    return isinstance(value, str) and (
        value.startswith("http://") or value.startswith("https://")
    )


def is_label_predicate(pred: str) -> bool:
    ln = local_name(pred).lower()
    return ln in ("label", "name") or pred == RDFS_LABEL


# ── Triple formatting ─────────────────────────────────────────────────────


def _format_entity_block(entity_uri: str, triples: list[dict]) -> str:
    """Build a human-readable text block for one entity."""
    lines: list[str] = []
    entity_label = local_name(entity_uri)
    types: list[str] = []
    labels: list[str] = []
    attributes: list[tuple[str, str]] = []
    relationships: list[tuple[str, str]] = []

    for t in triples:
        pred = t.get("predicate", "")
        obj = t.get("object", "")

        if pred == RDF_TYPE:
            types.append(local_name(obj))
        elif is_label_predicate(pred):
            labels.append(obj)
        elif is_uri(obj):
            relationships.append((pretty_predicate(pred), local_name(obj)))
        else:
            attributes.append((pretty_predicate(pred), obj))

    display_name = labels[0] if labels else entity_label
    type_str = ", ".join(types) if types else "Unknown type"
    lines.append(f"■ {display_name}  ({type_str})")
    lines.append(f"  URI: {entity_uri}")

    if labels and len(labels) > 1:
        for lbl in labels[1:]:
            lines.append(f"  Also known as: {lbl}")

    if attributes:
        lines.append("  Attributes:")
        for attr_name, attr_val in attributes:
            lines.append(f"    • {attr_name}: {attr_val}")

    if relationships:
        lines.append("  Relationships:")
        for rel_name, target in relationships:
            lines.append(f"    → {rel_name}: {target}")

    return "\n".join(lines)


def _merge_uri_aliases(by_subject: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Merge triples from URI aliases into a single entity."""
    groups: dict[str, list[str]] = {}
    for uri in by_subject:
        lid = local_name(uri)
        groups.setdefault(lid, []).append(uri)

    merged: dict[str, list[dict]] = {}
    for _, uris in groups.items():
        canonical = max(uris, key=lambda u: len(by_subject.get(u, [])))
        combined: list[dict] = []
        seen: set[tuple] = set()
        for u in uris:
            for t in by_subject.get(u, []):
                key = (t.get("predicate"), t.get("object"))
                if key not in seen:
                    seen.add(key)
                    combined.append(t)
        merged[canonical] = combined
    return merged


def format_find_response(data: dict) -> str:
    """Convert a /triples/find JSON response into a full-text description."""
    if not data.get("success"):
        return data.get("message", "Search failed.")

    seed_count = data.get("seed_count", 0)
    if seed_count == 0:
        return data.get("message") or "No matching entities found."

    triples = data.get("triples", [])
    depth = data.get("depth", 1)
    total = data.get("total", len(triples))

    by_subject: dict[str, list[dict]] = {}
    for t in triples:
        by_subject.setdefault(t["subject"], []).append(t)

    by_subject = _merge_uri_aliases(by_subject)

    seed_uris: set[str] = set()
    related_uris: set[str] = set()
    for uri, subj_triples in by_subject.items():
        has_attributes = any(
            not is_uri(t.get("object", "")) and t.get("predicate") != RDF_TYPE
            for t in subj_triples
        )
        if has_attributes and len(seed_uris) < seed_count:
            seed_uris.add(uri)
        else:
            related_uris.add(uri)

    if not seed_uris:
        seed_uris = set(list(by_subject.keys())[:seed_count])
        related_uris = set(by_subject.keys()) - seed_uris

    unique_entities = len(by_subject)
    parts: list[str] = []
    parts.append(
        f"Found {seed_count} matching entit{'y' if seed_count == 1 else 'ies'} "
        f"({total} triples across {unique_entities} entities, depth={depth})\n"
    )

    parts.append("── Matching Entities ──")
    for uri in seed_uris:
        parts.append(_format_entity_block(uri, by_subject.get(uri, [])))
        parts.append("")

    if related_uris:
        parts.append("── Related Entities (neighbors) ──")
        for uri in related_uris:
            parts.append(_format_entity_block(uri, by_subject.get(uri, [])))
            parts.append("")

    if total > len(triples):
        parts.append(
            f"(Showing {len(triples)} of {total} triples — "
            f"increase limit or use pagination for more)"
        )

    return "\n".join(parts)


def _format_graphql_entity(lines: list[str], entity: dict, indent: int = 0) -> None:
    """Recursively format a GraphQL entity dict as readable text."""
    prefix = " " * indent
    for key, value in entity.items():
        if value is None:
            continue
        if isinstance(value, list):
            if not value:
                continue
            if isinstance(value[0], dict):
                lines.append(f"{prefix}{key}:")
                for sub in value:
                    _format_graphql_entity(lines, sub, indent=indent + 4)
                    lines.append(f"{prefix}    ---")
                if lines[-1].endswith("---"):
                    lines.pop()
            else:
                lines.append(
                    f"{prefix}{key}: {', '.join(str(v) for v in value)}"
                )
        elif isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            _format_graphql_entity(lines, value, indent=indent + 4)
        else:
            lines.append(f"{prefix}{key}: {value}")


def format_graphql_response(data: dict, domain_name: str) -> str:
    """Convert a GraphQL JSON response into LLM-friendly text."""
    errors = data.get("errors")
    result_data = data.get("data")

    if errors and not result_data:
        error_lines = [f"  • {e.get('message', str(e))}" for e in errors]
        return "GraphQL errors:\n" + "\n".join(error_lines)

    if not result_data:
        return "GraphQL query returned no data."

    lines: list[str] = []
    lines.append(f"GraphQL Result — {domain_name}")
    lines.append("=" * 50)

    for field_name, field_data in result_data.items():
        if isinstance(field_data, list):
            lines.append(f"\n{field_name} ({len(field_data)} results)")
            lines.append("-" * 40)
            for i, item in enumerate(field_data):
                if isinstance(item, dict):
                    _format_graphql_entity(lines, item, indent=2)
                else:
                    lines.append(f"  {item}")
                if i < len(field_data) - 1:
                    lines.append("")
        elif isinstance(field_data, dict):
            lines.append(f"\n{field_name}")
            lines.append("-" * 40)
            _format_graphql_entity(lines, field_data, indent=2)
        elif field_data is None:
            lines.append(f"\n{field_name}: (not found)")
        else:
            lines.append(f"\n{field_name}: {field_data}")

    if errors:
        lines.append("\nWarnings:")
        for e in errors:
            lines.append(f"  • {e.get('message', str(e))}")

    return "\n".join(lines)


def format_sparql_rows(columns: Iterable[str], rows: Iterable[Iterable]) -> str:
    """Render SPARQL SELECT results as a compact Markdown-friendly table.

    ``columns`` are the header names.  ``rows`` is an iterable of row
    values (same length as columns).  Truncates long values.
    """
    cols = list(columns)
    if not cols:
        return "(no columns)"

    max_val = 80
    out_rows: list[list[str]] = []
    for row in rows:
        vals = []
        for v in row:
            s = "" if v is None else str(v)
            if len(s) > max_val:
                s = s[: max_val - 1] + "…"
            vals.append(s)
        out_rows.append(vals)

    if not out_rows:
        return f"(empty result — columns: {', '.join(cols)})"

    widths = [len(c) for c in cols]
    for row in out_rows:
        for i, v in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(v))

    def _row_str(vals: list[str]) -> str:
        return "| " + " | ".join(
            vals[i].ljust(widths[i]) if i < len(widths) else vals[i]
            for i in range(len(vals))
        ) + " |"

    header = _row_str(cols)
    sep = "|" + "|".join("-" * (w + 2) for w in widths) + "|"
    lines = [header, sep]
    lines.extend(_row_str(r) for r in out_rows)
    return "\n".join(lines)
