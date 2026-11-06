"""Validate and apply Auto-fix operations against a domain session."""

from __future__ import annotations

import copy
from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger
from back.objects.ontology.ClassModel import ClassModel
from back.objects.ontology.Editor import Editor
from back.objects.ontology.autofix_ops import ALLOWED_OPS

logger = get_logger(__name__)


def apply_operations(domain, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply confirmed Auto-fix ops atomically to ``domain``.

    Works on deep copies; commits only when every op succeeds. Rename/merge
    triggers URI rewrite and mapping remap.
    """
    if not operations:
        return {"success": False, "error": "No operations to apply.", "applied": 0}

    for op in operations:
        if op.get("op") not in ALLOWED_OPS:
            return {
                "success": False,
                "error": f"Unknown operation: {op.get('op')}",
                "applied": 0,
            }

    old_classes = copy.deepcopy(domain.get_classes())
    old_properties = copy.deepcopy(domain.get_properties())
    old_groups = copy.deepcopy(list(domain.groups or []))

    classes = copy.deepcopy(old_classes)
    properties = copy.deepcopy(old_properties)
    groups = copy.deepcopy(old_groups)

    try:
        for op in operations:
            _apply_one(op, classes, properties, groups)
    except _ApplyError as exc:
        return {"success": False, "error": str(exc), "applied": 0}

    class_renames, property_renames = ClassModel.rewrite_renamed_uris(
        old_classes, classes, old_properties, properties
    )

    ont = domain._data.setdefault("ontology", {})
    ont["classes"] = classes
    ont["properties"] = properties
    ont["groups"] = groups

    editor = Editor(domain)
    editor.remap_mapping_uris(class_renames, property_renames)

    class_uris = {c.get("uri") for c in classes if c.get("uri")}
    property_uris = {p.get("uri") for p in properties if p.get("uri")}
    editor.prune_mappings_to_ontology_uris(class_uris, property_uris)

    if hasattr(domain, "save"):
        domain.save()

    config = {
        "classes": classes,
        "properties": properties,
        "groups": groups,
        "base_uri": ont.get("base_uri", ""),
        "name": ont.get("name", ""),
        "description": ont.get("description", ""),
    }
    logger.info("Applied %d Auto-fix operation(s)", len(operations))
    return {
        "success": True,
        "applied": len(operations),
        "uri_renames": {"classes": class_renames, "properties": property_renames},
        "attribute_uris": ClassModel.attribute_uris(config),
        "findings_remaining_hint": True,
    }


class _ApplyError(Exception):
    """Raised when a single op cannot be applied; triggers atomic abort."""


def _find_by_name(items: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    for item in items:
        if item.get("name") == name:
            return item
    return None


def _existing_names(
    classes: List[Dict[str, Any]], properties: List[Dict[str, Any]]
) -> set[str]:
    names = {str(c.get("name") or "") for c in classes}
    names.update(str(p.get("name") or "") for p in properties)
    names.discard("")
    return names


def _target_name(op: Dict[str, Any]) -> str:
    return str((op.get("target") or {}).get("name") or "")


def _prop_noun(op: Dict[str, Any]) -> str:
    kind = (op.get("target") or {}).get("kind") or ""
    if kind == "attribute":
        return "Attribute"
    if kind == "property":
        return "Relationship"
    return "Attribute or relationship"


def _resolve_item(
    kind: str,
    name: str,
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if kind == "class":
        return _find_by_name(classes, name)
    if kind in ("property", "attribute"):
        return _find_by_name(properties, name)
    return _find_by_name(classes, name) or _find_by_name(properties, name)


def _rewrite_name_refs(
    old: str,
    new: str,
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> None:
    for cls in classes:
        if cls.get("parent") == old:
            cls["parent"] = new
        for attr in cls.get("dataProperties") or []:
            if attr.get("name") == old:
                attr["name"] = new
                if "localName" in attr:
                    attr["localName"] = new
    for prop in properties:
        if prop.get("domain") == old:
            prop["domain"] = new
        if prop.get("range") == old:
            prop["range"] = new
    for group in groups:
        members = group.get("members") or []
        group["members"] = [new if m == old else m for m in members]


def _op_clear_parent(op, classes, properties, groups) -> None:
    name = _target_name(op)
    cls = _find_by_name(classes, name)
    if not cls:
        raise _ApplyError(f"Entity '{name}' not found.")
    cls["parent"] = ""
    cls.pop("parent_uri", None)


def _op_set_parent(op, classes, properties, groups) -> None:
    name = _target_name(op)
    parent = str((op.get("params") or {}).get("parent") or "")
    cls = _find_by_name(classes, name)
    if not cls:
        raise _ApplyError(f"Entity '{name}' not found.")
    if parent and not _find_by_name(classes, parent):
        raise _ApplyError(f"Parent entity '{parent}' not found.")
    cls["parent"] = parent


def _op_clear_domain(op, classes, properties, groups) -> None:
    name = _target_name(op)
    prop = _find_by_name(properties, name)
    if not prop:
        raise _ApplyError(f"{_prop_noun(op)} '{name}' not found.")
    prop["domain"] = ""


def _is_attribute(prop: Dict[str, Any]) -> bool:
    return str(prop.get("type") or "").replace("owl:", "") == "DatatypeProperty"


def _sync_attribute_owner(
    prop: Dict[str, Any], owner: str, classes: List[Dict[str, Any]]
) -> None:
    """Keep ``dataProperties`` in step with an attribute's owning entity.

    Drops the attribute from every entity that lists it, then adds it to
    ``owner`` when one is given. Idempotent, so re-applying is safe.
    """
    name = prop.get("name") or ""
    for cls in classes:
        attrs = cls.get("dataProperties") or []
        cls["dataProperties"] = [a for a in attrs if a.get("name") != name]
    if not owner:
        return
    cls = _find_by_name(classes, owner)
    attrs = list(cls.get("dataProperties") or [])
    attrs.append(
        {
            "name": name,
            "localName": name,
            "label": prop.get("label") or name,
            "uri": prop.get("uri") or "",
        }
    )
    cls["dataProperties"] = attrs


def _op_set_domain(op, classes, properties, groups) -> None:
    name = _target_name(op)
    domain_name = str((op.get("params") or {}).get("domain") or "")
    prop = _find_by_name(properties, name)
    if not prop:
        raise _ApplyError(f"{_prop_noun(op)} '{name}' not found.")
    if domain_name and not _find_by_name(classes, domain_name):
        raise _ApplyError(f"Entity '{domain_name}' not found.")
    prop["domain"] = domain_name
    if _is_attribute(prop):
        _sync_attribute_owner(prop, domain_name, classes)


def _op_clear_range(op, classes, properties, groups) -> None:
    name = _target_name(op)
    prop = _find_by_name(properties, name)
    if not prop:
        raise _ApplyError(f"{_prop_noun(op)} '{name}' not found.")
    prop["range"] = ""


def _op_set_range(op, classes, properties, groups) -> None:
    name = _target_name(op)
    range_name = str((op.get("params") or {}).get("range") or "")
    prop = _find_by_name(properties, name)
    if not prop:
        raise _ApplyError(f"{_prop_noun(op)} '{name}' not found.")
    prop["range"] = range_name


def _op_remove_group_member(op, classes, properties, groups) -> None:
    params = op.get("params") or {}
    group_name = str(params.get("group") or _target_name(op))
    member = str(params.get("member") or "")
    group = _find_by_name(groups, group_name)
    if not group:
        raise _ApplyError(f"Group '{group_name}' not found.")
    if not member:
        raise _ApplyError("remove_group_member requires params.member.")
    members = list(group.get("members") or [])
    if member not in members:
        raise _ApplyError(f"Member '{member}' not in group '{group_name}'.")
    group["members"] = [m for m in members if m != member]


def _op_attach_attribute(op, classes, properties, groups) -> None:
    name = _target_name(op)
    entity = str((op.get("params") or {}).get("entity") or "")
    prop = _find_by_name(properties, name)
    cls = _find_by_name(classes, entity)
    if not prop:
        raise _ApplyError(f"Attribute '{name}' not found.")
    if not cls:
        raise _ApplyError(f"Entity '{entity}' not found.")
    attrs = list(cls.get("dataProperties") or [])
    if any(a.get("name") == name for a in attrs):
        raise _ApplyError(f"Entity '{entity}' already has attribute '{name}'.")
    prop["domain"] = entity
    attrs.append(
        {
            "name": name,
            "localName": name,
            "label": prop.get("label") or name,
            "uri": prop.get("uri") or "",
        }
    )
    cls["dataProperties"] = attrs


def _op_remove_property(op, classes, properties, groups) -> None:
    name = _target_name(op)
    idx = next((i for i, p in enumerate(properties) if p.get("name") == name), -1)
    if idx < 0:
        raise _ApplyError(f"{_prop_noun(op)} '{name}' not found.")
    properties.pop(idx)
    for cls in classes:
        attrs = cls.get("dataProperties") or []
        cls["dataProperties"] = [a for a in attrs if a.get("name") != name]


def _op_set_label(op, classes, properties, groups) -> None:
    kind = (op.get("target") or {}).get("kind") or ""
    name = _target_name(op)
    item = _resolve_item(kind, name, classes, properties)
    if not item:
        raise _ApplyError(f"Target '{name}' not found.")
    item["label"] = str((op.get("params") or {}).get("label") or "")


def _op_set_description(op, classes, properties, groups) -> None:
    kind = (op.get("target") or {}).get("kind") or ""
    name = _target_name(op)
    item = _resolve_item(kind, name, classes, properties)
    if not item:
        raise _ApplyError(f"Target '{name}' not found.")
    item["description"] = str((op.get("params") or {}).get("description") or "")


def _op_rename(op, classes, properties, groups) -> None:
    kind = (op.get("target") or {}).get("kind") or ""
    name = _target_name(op)
    new_name = str((op.get("params") or {}).get("new_name") or "")
    if not new_name:
        raise _ApplyError("rename requires params.new_name.")
    if new_name in _existing_names(classes, properties):
        raise _ApplyError(f"Rename collision: '{new_name}' already exists.")
    item = _resolve_item(kind, name, classes, properties)
    if not item:
        raise _ApplyError(f"Target '{name}' not found.")
    item["name"] = new_name
    if "localName" in item:
        item["localName"] = new_name
    _rewrite_name_refs(name, new_name, classes, properties, groups)


def _op_merge_into(op, classes, properties, groups) -> None:
    kind = (op.get("target") or {}).get("kind") or ""
    name = _target_name(op)
    survivor = str((op.get("params") or {}).get("survivor") or "")
    if not survivor:
        raise _ApplyError("merge_into requires params.survivor.")
    if survivor == name:
        raise _ApplyError("Cannot merge an item into itself.")
    if not _resolve_item(kind, name, classes, properties):
        raise _ApplyError(f"Merge source '{name}' not found.")
    if not _resolve_item(kind, survivor, classes, properties):
        raise _ApplyError(f"Merge survivor '{survivor}' not found.")
    _rewrite_name_refs(name, survivor, classes, properties, groups)
    if kind == "class":
        classes[:] = [c for c in classes if c.get("name") != name]
        return
    properties[:] = [p for p in properties if p.get("name") != name]
    for cls in classes:
        attrs = cls.get("dataProperties") or []
        cls["dataProperties"] = [a for a in attrs if a.get("name") != name]


_HANDLERS: Dict[str, Callable] = {
    "clear_parent": _op_clear_parent,
    "set_parent": _op_set_parent,
    "clear_domain": _op_clear_domain,
    "set_domain": _op_set_domain,
    "clear_range": _op_clear_range,
    "set_range": _op_set_range,
    "remove_group_member": _op_remove_group_member,
    "attach_attribute": _op_attach_attribute,
    "remove_property": _op_remove_property,
    "set_label": _op_set_label,
    "set_description": _op_set_description,
    "rename": _op_rename,
    "merge_into": _op_merge_into,
}


def _apply_one(
    op: Dict[str, Any],
    classes: List[Dict[str, Any]],
    properties: List[Dict[str, Any]],
    groups: List[Dict[str, Any]],
) -> None:
    handler = _HANDLERS.get(op.get("op") or "")
    if not handler:
        raise _ApplyError(f"Unhandled operation: {op.get('op')}")
    handler(op, classes, properties, groups)
