"""Reset of a domain's mappings, optionally keeping the user's exclusions.

An *excluded* entity/relationship is rendered grey (hidden) in Mapping Studio,
while an unmapped, non-excluded one is rendered red ("not mapped"). The
``excluded`` / ``excluded_attributes`` flags live on the same entries as the SQL
mappings, so wiping the mapping lists also wipes the exclusions and every hidden
item reappears red. :func:`reset_mappings` can therefore keep them.
"""

from __future__ import annotations

from typing import Any, Dict, List


def exclusion_stubs(
    entries: List[Dict[str, Any]], key: str, label_key: str
) -> List[Dict[str, Any]]:
    """Reduce *entries* to the user's exclusions, dropping the mapping itself.

    An entry survives only when it carries ``excluded`` and/or
    ``excluded_attributes``. What is kept is the key, the label and those flags;
    the SQL, ID/label columns and attribute assignments are removed. This is the
    same minimal shape ``Mapping.toggle_exclude_items`` creates for an item that
    was never mapped.
    """
    stubs: List[Dict[str, Any]] = []
    for entry in entries or []:
        ref = entry.get(key)
        if not ref:
            continue
        keep: Dict[str, Any] = {}
        if entry.get("excluded"):
            keep["excluded"] = True
        if entry.get("excluded_attributes"):
            keep["excluded_attributes"] = list(entry["excluded_attributes"])
        if not keep:
            continue
        stub: Dict[str, Any] = {key: ref}
        if entry.get(label_key):
            stub[label_key] = entry[label_key]
        stub.update(keep)
        stubs.append(stub)
    return stubs


def reset_mappings(domain: Any, keep_exclusions: bool = False) -> None:
    """Delete every entity/relationship mapping of *domain* and save it.

    Args:
        domain: The domain session.
        keep_exclusions: Keep hidden entities/relationships and excluded
            attributes as exclusion-only entries; only the mappings go.
    """
    assignment = domain.assignment
    if keep_exclusions:
        entities = exclusion_stubs(
            assignment.get("entities"), "ontology_class", "ontology_class_label"
        )
        relationships = exclusion_stubs(
            assignment.get("relationships"), "property", "property_label"
        )
    else:
        entities, relationships = [], []
    assignment["entities"] = entities
    assignment["relationships"] = relationships
    assignment["r2rml_output"] = ""
    domain.clear_generated_content()
    domain.record_change(
        "mapping_reset",
        entity_type="mapping",
        summary=(
            "All mappings reset (exclusions kept)"
            if keep_exclusions
            else "All mappings reset"
        ),
    )
    domain.save()
