"""LLM-friendly text formatting for OntoBricks MCP responses.

Converts the JSON payloads returned by the OntoBricks external REST / GraphQL
surface into the readable text blocks the model consumes: entity descriptions,
node context, virtual-attribute listings, action results and GraphQL results.
Pure functions — depend only on :mod:`server.constants` and
:mod:`server.uri_helpers`.
"""

from __future__ import annotations

from typing import Callable, Optional

from server.constants import RDF_TYPE
from server.uri_helpers import _is_label_predicate, _is_uri, _local_name


def _format_entity_block(
    entity_uri: str,
    triples: list[dict],
    label_or_local: "Callable[[str], str] | None" = None,
) -> str:
    """Build a human-readable text block for one entity."""
    _resolve = label_or_local or _local_name
    lines: list[str] = []
    entity_label = _local_name(entity_uri)
    types: list[str] = []
    labels: list[str] = []
    attributes: list[tuple[str, str]] = []
    relationships: list[tuple[str, str]] = []

    for t in triples:
        pred = t["predicate"]
        obj = t["object"]

        if pred == RDF_TYPE:
            types.append(_resolve(obj))
        elif _is_label_predicate(pred):
            labels.append(obj)
        elif _is_uri(obj):
            relationships.append((_resolve(pred), _local_name(obj)))
        else:
            attributes.append((_resolve(pred), obj))

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


def _preferred(context_policy: Optional[dict], feature: str) -> bool:
    """True when the domain marked *feature* as preferred.

    Preferred only changes the wording of the follow-up hint the model reads:
    a neutral "you may" mention becomes a directive instruction. Section
    order and payload content are untouched.
    """
    return bool(context_policy) and context_policy.get(feature) == "preferred"


def _hint(
    context_policy: Optional[dict], feature: str, *, directive: str, neutral: str
) -> str:
    """Pick the wording of a follow-up hint for *feature*."""
    return directive if _preferred(context_policy, feature) else neutral


def _format_class_context_block(
    local_id: str, cls_actions: dict, context_policy: Optional[dict] = None
) -> str:
    """Append a [Context] block for a node's class Actions metadata."""
    dataset = cls_actions.get("dataset")
    bridges = cls_actions.get("bridges") or []
    actions = cls_actions.get("actions") or []
    virtual = cls_actions.get("virtualAttributes") or []
    if not dataset and not bridges and not actions and not virtual:
        return ""

    lines: list[str] = []
    lines.append(f"  [Context — class: {cls_actions.get('name', '')}]")

    if dataset and dataset.get("fullName"):
        key_col = dataset.get("key_column")
        if key_col:
            lines.append(f"  Dataset: {dataset['fullName']}  (key: {key_col} = '{local_id}')")
            lines.append(
                _hint(
                    context_policy,
                    "dataset",
                    directive="    → ALWAYS call get_entity_context("
                    "fetch_dataset_rows=True) to retrieve the rows before "
                    "answering — this dataset is the authoritative source for "
                    "this entity",
                    neutral="    → call get_entity_context(fetch_dataset_rows=True) "
                    "to retrieve rows",
                )
            )
        else:
            lines.append(f"  Dataset: {dataset['fullName']}  (key_column not configured)")
        purpose = (dataset.get("description") or "").strip()
        if purpose:
            lines.append(f"  Description: {purpose}")

    if bridges:
        lines.append("  Bridges:")
        for b in bridges:
            target_domain = b.get("target_domain", "")
            target = f"{target_domain} / {b.get('target_class_name', '')}"
            label = f"  \"{b['label']}\"" if b.get("label") else ""
            lines.append(f"    → {target}{label}")
            target_desc = (b.get("target_domain_description") or "").strip()
            if target_desc:
                lines.append(f"      Target domain: {target_desc}")
        lines.append(
            _hint(
                context_policy,
                "bridges",
                directive="    → ALWAYS follow these bridges before concluding: "
                "the answer likely spans the target domain. Call "
                "select_domain(<target_domain>) then re-run describe_entity or "
                "GraphQL there. get_entity_context(follow_bridges=True) only "
                "peeks — it does NOT switch the session.",
                neutral="    → to query the target domain, call "
                "select_domain(<target_domain>) then re-run describe_entity or "
                "GraphQL there. get_entity_context(follow_bridges=True) only "
                "peeks — it does NOT switch the session.",
            )
        )

    if actions:
        lines.append("  Actions:")
        for a in actions:
            lines.append(f"    → {a.get('fullName', '')}: {a.get('description', '')}")
        lines.append(
            _hint(
                context_policy,
                "actions",
                directive="    → PREFER invoke_entity_action(entity_uri, action) "
                "over answering from the graph alone — these actions return "
                "live, authoritative results",
                neutral="    → call invoke_entity_action(entity_uri, action) to run one",
            )
        )

    if virtual:
        names = [
            attr.get("name", "")
            for group in virtual
            for attr in (group.get("attributes") or [])
        ]
        lines.append(
            f"  Virtual attributes ({len(names)}, computed on demand): "
            + ", ".join(n for n in names if n)
        )
        lines.append(
            _hint(
                context_policy,
                "virtual_attributes",
                directive="    → ALWAYS call compute_virtual_attributes("
                "entity_uri) before answering — these values are not in the "
                "graph and are computed live",
                neutral="    → call compute_virtual_attributes(entity_uri) "
                "to compute their values",
            )
        )

    return "\n".join(lines)


def _format_node_context_response(
    data: dict, context_policy: Optional[dict] = None
) -> str:
    """Format the /nodes/context JSON response as LLM-friendly text."""
    if not data.get("success"):
        return data.get("message", "Could not retrieve node context.")

    entity_uri = data.get("entity_uri", "")
    local_id = data.get("entity_local_id", "") or _local_name(entity_uri)
    class_name = data.get("class_name", "Unknown")

    lines: list[str] = [
        f"Node Context — {local_id}  ({class_name})",
        f"URI: {entity_uri}",
        "",
    ]

    dataset = data.get("dataset")
    if dataset:
        lines.append(f"Dataset: {dataset.get('fullName', '')}")
        key_col = dataset.get("key_column")
        if key_col:
            lines.append(f"  Key: {key_col} = '{local_id}'")
        purpose = (dataset.get("description") or "").strip()
        if purpose:
            lines.append(f"  Description: {purpose}")
        if dataset.get("key_column_missing"):
            lines.append("  ⚠ key_column not configured — row fetch skipped")
        rows = dataset.get("rows")
        if rows:
            lines.append(f"  Rows ({len(rows)}):")
            for row in rows:
                lines.append("    " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
        lines.append("")

    bridges = data.get("bridges") or []
    if bridges:
        lines.append("Cross-domain Bridges:")
        for b in bridges:
            target_domain = b.get("target_domain", "")
            target = f"{target_domain} / {b.get('target_class_name', '')}"
            label = f"  \"{b['label']}\"" if b.get("label") else ""
            lines.append(f"  → {target}{label}")
            target_desc = (b.get("target_domain_description") or "").strip()
            if target_desc:
                lines.append(f"    Target domain: {target_desc}")
            entities = b.get("entities")
            if entities:
                lines.append(f"    Entities ({len(entities)}):")
                for e in entities:
                    lines.append(f"      • {_local_name(e.get('uri', ''))}  {e.get('predicate', '')} → {e.get('object', '')}")
        lines.append(
            _hint(
                context_policy,
                "bridges",
                directive="  → ALWAYS follow these bridges before concluding: "
                "call select_domain(<target_domain>) then describe_entity / "
                "GraphQL there. follow_bridges only peeks; it does not switch "
                "the session.",
                neutral="  → to actually query one of these targets, call "
                "select_domain(<target_domain>) then describe_entity / GraphQL "
                "there. follow_bridges only peeks; it does not switch the session.",
            )
        )
        lines.append("")

    actions = data.get("actions") or []
    if actions:
        lines.append("Actions (Unity Catalog functions):")
        for a in actions:
            lines.append(f"  → {a.get('fullName', '')}")
            desc = (a.get("description") or "").strip()
            if desc:
                lines.append(f"    Description: {desc}")
        lines.append(
            _hint(
                context_policy,
                "actions",
                directive="  → PREFER running one of these actions over answering "
                "from the graph alone — call invoke_entity_action(entity_uri, "
                f"action) with the entity's ID ('{local_id}')",
                neutral="  → call invoke_entity_action(entity_uri, action) with "
                f"the entity's ID ('{local_id}')",
            )
        )
        lines.append("")

    virtual = data.get("virtual_attributes") or []
    if virtual:
        lines.extend(_format_virtual_attribute_lines(virtual, context_policy))
        lines.append("")

    return "\n".join(lines)


def _format_virtual_attribute_lines(
    groups: list, context_policy: Optional[dict] = None
) -> list:
    """Render the virtual attribute block of a node-context response.

    A group carries ``values`` only once computed, so the same block serves
    both the declaration-only listing and the computed one; the trailing hint
    is emitted only while at least one group is still uncomputed, otherwise it
    would ask the model to redo work it just did.
    """
    lines = ["Virtual Attributes (computed on demand):"]
    pending = False
    for group in groups:
        values = group.get("values")
        computed = isinstance(values, dict) and bool(values)
        lines.append(f"  → {group.get('fullName', '')}")
        desc = (group.get("description") or "").strip()
        if desc:
            lines.append(f"    Description: {desc}")
        for attr in group.get("attributes") or []:
            name = attr.get("name", "")
            dtype = attr.get("dataType")
            suffix = f"  ({dtype})" if dtype else ""
            if computed:
                lines.append(f"    {name} = {values.get(name)!r}{suffix}")
            else:
                lines.append(f"    {name}{suffix}  — not computed")
        error = (group.get("error") or "").strip()
        if error:
            lines.append(f"    ⚠ computation failed: {error}")
        message = (group.get("message") or "").strip()
        if message:
            lines.append(f"    {message}")
        if not computed and not error:
            pending = True

    if pending:
        lines.append(
            _hint(
                context_policy,
                "virtual_attributes",
                directive="  → ALWAYS call compute_virtual_attributes("
                "entity_uri) before answering: these values are not stored "
                "in the graph and only a computation can produce them",
                neutral="  → call compute_virtual_attributes(entity_uri) "
                "to compute these values",
            )
        )
    return lines


def _format_virtual_attributes_response(
    data: dict, context_policy: Optional[dict] = None
) -> str:
    """Format the dedicated virtual-attribute compute endpoint for LLMs."""
    if not data.get("success"):
        return (
            data.get("message")
            or (data.get("error") if isinstance(data.get("error"), str) else None)
            or "Could not compute virtual attributes."
        )

    entity_uri = data.get("entity_uri", "")
    local_id = data.get("entity_local_id", "") or _local_name(entity_uri)
    class_name = data.get("class_name", "Unknown")

    lines: list[str] = [
        f"Virtual Attributes — {local_id}  ({class_name})",
        f"URI: {entity_uri}",
        "",
    ]
    virtual = data.get("virtual_attributes") or []
    if virtual:
        lines.extend(_format_virtual_attribute_lines(virtual, context_policy))
    else:
        lines.append("No virtual attributes declared on this class.")
    return "\n".join(lines)


def _format_node_action_response(data: dict) -> str:
    """Format the /nodes/action JSON response as LLM-friendly text."""
    if not data.get("success"):
        return (
            data.get("message")
            or (data.get("error") if isinstance(data.get("error"), str) else None)
            or "Could not invoke the action."
        )

    local_id = data.get("entity_local_id", "")
    lines: list[str] = [
        f"Action: {data.get('action', '')}",
        f"Entity: {local_id}  ({data.get('class_name', 'Unknown')})",
        "",
    ]

    rows = data.get("rows") or []
    if not rows:
        lines.append("Completed — no result rows returned.")
        return "\n".join(lines)

    lines.append(f"Result ({len(rows)} row{'s' if len(rows) != 1 else ''}):")
    for row in rows:
        lines.append("  " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
    return "\n".join(lines)


def _merge_uri_aliases(by_subject: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Merge triples from URI aliases into a single entity.

    R2RML mappings may produce different URI patterns for the same
    entity (e.g. ``…/Customer/CUST00094`` and ``…/CUST00094``).  Group
    them by their local identifier and pick the richest URI as the
    canonical one.
    """
    groups: dict[str, list[str]] = {}
    for uri in by_subject:
        lid = _local_name(uri)
        groups.setdefault(lid, []).append(uri)

    merged: dict[str, list[dict]] = {}
    for lid, uris in groups.items():
        canonical = max(uris, key=lambda u: len(by_subject.get(u, [])))
        combined: list[dict] = []
        seen: set[tuple] = set()
        for u in uris:
            for t in by_subject.get(u, []):
                key = (t["predicate"], t["object"])
                if key not in seen:
                    seen.add(key)
                    combined.append(t)
        merged[canonical] = combined
    return merged


def _format_find_response(
    data: dict,
    label_or_local: "Callable[[str], str] | None" = None,
    class_actions: "dict | None" = None,
    context_policy: Optional[dict] = None,
) -> str:
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
            not _is_uri(t["object"]) and t["predicate"] != RDF_TYPE
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
        block = _format_entity_block(uri, by_subject.get(uri, []), label_or_local)
        parts.append(block)
        # Append class Actions context if available
        if class_actions:
            triples_for_uri = by_subject.get(uri, [])
            type_uris = [t["object"] for t in triples_for_uri if t["predicate"] == RDF_TYPE]
            for type_uri in type_uris:
                if type_uri in class_actions:
                    ctx = _format_class_context_block(
                        _local_name(uri), class_actions[type_uri], context_policy
                    )
                    if ctx:
                        parts.append(ctx)
                    break
        parts.append("")

    if related_uris:
        parts.append("── Related Entities (neighbors) ──")
        for uri in related_uris:
            parts.append(_format_entity_block(uri, by_subject.get(uri, []), label_or_local))
            parts.append("")

    if total > len(triples):
        parts.append(
            f"(Showing {len(triples)} of {total} triples — "
            f"increase limit or use pagination for more)"
        )

    return "\n".join(parts)


def _format_graphql_response(data: dict, domain_name: str) -> str:
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
                lines.append(f"{prefix}{key}: {', '.join(str(v) for v in value)}")
        elif isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            _format_graphql_entity(lines, value, indent=indent + 4)
        else:
            lines.append(f"{prefix}{key}: {value}")
