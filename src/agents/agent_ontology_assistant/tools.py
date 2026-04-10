"""
Ontology Assistant Agent – tool definitions and handlers.

Provides read and mutation tools that let the LLM inspect and modify
the in-memory ontology (classes, properties, inheritance).
"""

import json
from typing import Callable, Dict, List

from agents.tools.context import ToolContext
from back.core.logging import get_logger

logger = get_logger(__name__)


# =====================================================
# Read tools
# =====================================================

def tool_get_ontology_classes(ctx: ToolContext, **_kwargs) -> str:
    """Return all classes in the current ontology."""
    classes = ctx.ontology_classes
    summary = []
    for cls in classes:
        entry = {
            "name": cls.get("name", ""),
            "label": cls.get("label", ""),
            "description": cls.get("description", ""),
            "parent": cls.get("parent", ""),
            "emoji": cls.get("emoji", ""),
            "dataProperties": [
                dp.get("name", dp.get("localName", ""))
                for dp in cls.get("dataProperties", [])
            ],
        }
        summary.append(entry)
    return json.dumps({"classes": summary, "count": len(summary)})


def tool_get_ontology_properties(ctx: ToolContext, **_kwargs) -> str:
    """Return all object properties (relationships) in the current ontology."""
    props = ctx.ontology_properties
    summary = []
    for p in props:
        entry = {
            "name": p.get("name", ""),
            "label": p.get("label", ""),
            "description": p.get("description", ""),
            "domain": p.get("domain", ""),
            "range": p.get("range", ""),
            "direction": p.get("direction", "forward"),
            "type": p.get("type", ""),
        }
        summary.append(entry)
    return json.dumps({"properties": summary, "count": len(summary)})


# =====================================================
# Mutation tools – entities (classes)
# =====================================================

def tool_add_entity(ctx: ToolContext, *, name: str, label: str = "",
                    description: str = "", parent: str = "",
                    emoji: str = "📦", **_kwargs) -> str:
    """Add a new entity (class) to the ontology."""
    classes = ctx.ontology_classes
    if any(c.get("name") == name for c in classes):
        return json.dumps({"error": f"Entity '{name}' already exists"})

    base_uri = ctx.ontology_base_uri
    uri = f"{base_uri}{name}" if base_uri else name

    new_class = {
        "uri": uri,
        "name": name,
        "label": label or name,
        "description": description,
        "parent": parent,
        "emoji": emoji,
        "properties": [],
        "dataProperties": [],
    }
    classes.append(new_class)
    ctx.ontology_dirty = True
    logger.info("tool_add_entity: added '%s'", name)
    return json.dumps({"success": True, "entity": name})


def tool_remove_entity(ctx: ToolContext, *, name: str, **_kwargs) -> str:
    """Remove an entity and cascade-delete its relationships and child inheritance."""
    classes = ctx.ontology_classes
    properties = ctx.ontology_properties

    idx = next((i for i, c in enumerate(classes) if c.get("name") == name), None)
    if idx is None:
        return json.dumps({"error": f"Entity '{name}' not found"})

    classes.pop(idx)

    removed_props = [p.get("name") for p in properties
                     if p.get("domain") == name or p.get("range") == name]
    ctx.ontology_properties[:] = [
        p for p in properties
        if p.get("domain") != name and p.get("range") != name
    ]

    for c in classes:
        if c.get("parent") == name:
            c["parent"] = ""

    ctx.ontology_dirty = True
    logger.info("tool_remove_entity: removed '%s', cascade-removed %d relationships", name, len(removed_props))
    return json.dumps({
        "success": True,
        "removed_entity": name,
        "removed_relationships": removed_props,
    })


def tool_rename_entity(ctx: ToolContext, *, old_name: str, new_name: str,
                       new_label: str = "", **_kwargs) -> str:
    """Rename an entity and update all references (properties, inheritance)."""
    classes = ctx.ontology_classes
    properties = ctx.ontology_properties

    cls = next((c for c in classes if c.get("name") == old_name), None)
    if cls is None:
        return json.dumps({"error": f"Entity '{old_name}' not found"})

    if any(c.get("name") == new_name for c in classes):
        return json.dumps({"error": f"Entity '{new_name}' already exists"})

    base_uri = ctx.ontology_base_uri
    cls["name"] = new_name
    cls["label"] = new_label or new_name
    if base_uri:
        cls["uri"] = f"{base_uri}{new_name}"

    for p in properties:
        if p.get("domain") == old_name:
            p["domain"] = new_name
        if p.get("range") == old_name:
            p["range"] = new_name

    for c in classes:
        if c.get("parent") == old_name:
            c["parent"] = new_name

    ctx.ontology_dirty = True
    logger.info("tool_rename_entity: '%s' -> '%s'", old_name, new_name)
    return json.dumps({"success": True, "old_name": old_name, "new_name": new_name})


def tool_update_entity(ctx: ToolContext, *, name: str, label: str = None,
                       description: str = None, parent: str = None,
                       emoji: str = None, **_kwargs) -> str:
    """Update fields of an existing entity."""
    classes = ctx.ontology_classes
    cls = next((c for c in classes if c.get("name") == name), None)
    if cls is None:
        return json.dumps({"error": f"Entity '{name}' not found"})

    updated = []
    if label is not None:
        cls["label"] = label
        updated.append("label")
    if description is not None:
        cls["description"] = description
        updated.append("description")
    if parent is not None:
        cls["parent"] = parent
        updated.append("parent")
    if emoji is not None:
        cls["emoji"] = emoji
        updated.append("emoji")

    if updated:
        ctx.ontology_dirty = True
    logger.info("tool_update_entity: '%s' updated fields: %s", name, updated)
    return json.dumps({"success": True, "entity": name, "updated_fields": updated})


def tool_add_attribute(ctx: ToolContext, *, entity_name: str,
                       attribute_name: str, attribute_type: str = "xsd:string",
                       description: str = "", **_kwargs) -> str:
    """Add a data property (attribute) to an entity."""
    classes = ctx.ontology_classes
    cls = next((c for c in classes if c.get("name") == entity_name), None)
    if cls is None:
        return json.dumps({"error": f"Entity '{entity_name}' not found"})

    data_props = cls.setdefault("dataProperties", [])
    if any(dp.get("name") == attribute_name for dp in data_props):
        return json.dumps({"error": f"Attribute '{attribute_name}' already exists on '{entity_name}'"})

    base_uri = ctx.ontology_base_uri
    data_props.append({
        "uri": f"{base_uri}{attribute_name}" if base_uri else attribute_name,
        "name": attribute_name,
        "localName": attribute_name,
        "range": attribute_type,
        "description": description,
    })
    ctx.ontology_dirty = True
    logger.info("tool_add_attribute: added '%s' to entity '%s'", attribute_name, entity_name)
    return json.dumps({"success": True, "entity": entity_name, "attribute": attribute_name})


def tool_remove_attribute(ctx: ToolContext, *, entity_name: str,
                          attribute_name: str, **_kwargs) -> str:
    """Remove a data property (attribute) from an entity."""
    classes = ctx.ontology_classes
    cls = next((c for c in classes if c.get("name") == entity_name), None)
    if cls is None:
        return json.dumps({"error": f"Entity '{entity_name}' not found"})

    data_props = cls.get("dataProperties", [])
    before = len(data_props)
    cls["dataProperties"] = [dp for dp in data_props if dp.get("name") != attribute_name]
    if len(cls["dataProperties"]) == before:
        return json.dumps({"error": f"Attribute '{attribute_name}' not found on '{entity_name}'"})

    ctx.ontology_dirty = True
    logger.info("tool_remove_attribute: removed '%s' from entity '%s'", attribute_name, entity_name)
    return json.dumps({"success": True, "entity": entity_name, "removed_attribute": attribute_name})


# =====================================================
# Mutation tools – relationships (properties)
# =====================================================

def tool_add_relationship(ctx: ToolContext, *, name: str, domain: str,
                          range: str, label: str = "",
                          description: str = "",
                          direction: str = "forward", **_kwargs) -> str:
    """Add a new relationship (object property) to the ontology."""
    properties = ctx.ontology_properties
    classes = ctx.ontology_classes

    class_names = {c.get("name") for c in classes}
    if domain not in class_names:
        return json.dumps({"error": f"Domain entity '{domain}' not found"})
    if range not in class_names:
        return json.dumps({"error": f"Range entity '{range}' not found"})

    base_uri = ctx.ontology_base_uri
    uri = f"{base_uri}{name}" if base_uri else name

    new_prop = {
        "uri": uri,
        "name": name,
        "label": label or name,
        "description": description,
        "domain": domain,
        "range": range,
        "direction": direction,
        "type": "ObjectProperty",
        "properties": [],
    }
    properties.append(new_prop)
    ctx.ontology_dirty = True
    logger.info("tool_add_relationship: added '%s' (%s -> %s)", name, domain, range)
    return json.dumps({"success": True, "relationship": name, "domain": domain, "range": range})


def tool_remove_relationship(ctx: ToolContext, *, name: str,
                             domain: str = "", range: str = "",
                             **_kwargs) -> str:
    """Remove a relationship (object property). If domain/range given, match exactly."""
    properties = ctx.ontology_properties

    matches = [
        (i, p) for i, p in enumerate(properties)
        if p.get("name") == name
        and (not domain or p.get("domain") == domain)
        and (not range or p.get("range") == range)
    ]
    if not matches:
        return json.dumps({"error": f"Relationship '{name}' not found"})

    for i, _ in reversed(matches):
        properties.pop(i)

    ctx.ontology_dirty = True
    logger.info("tool_remove_relationship: removed %d instance(s) of '%s'", len(matches), name)
    return json.dumps({"success": True, "removed_relationship": name, "count": len(matches)})


def tool_update_relationship(ctx: ToolContext, *, name: str,
                             new_name: str = None, label: str = None,
                             description: str = None, domain: str = None,
                             range: str = None, direction: str = None,
                             **_kwargs) -> str:
    """Update fields of an existing relationship."""
    properties = ctx.ontology_properties
    prop = next((p for p in properties if p.get("name") == name), None)
    if prop is None:
        return json.dumps({"error": f"Relationship '{name}' not found"})

    updated = []
    if new_name is not None:
        prop["name"] = new_name
        prop["label"] = new_name
        if ctx.ontology_base_uri:
            prop["uri"] = f"{ctx.ontology_base_uri}{new_name}"
        updated.append("name")
    if label is not None:
        prop["label"] = label
        updated.append("label")
    if description is not None:
        prop["description"] = description
        updated.append("description")
    if domain is not None:
        prop["domain"] = domain
        updated.append("domain")
    if range is not None:
        prop["range"] = range
        updated.append("range")
    if direction is not None:
        prop["direction"] = direction
        updated.append("direction")

    if updated:
        ctx.ontology_dirty = True
    logger.info("tool_update_relationship: '%s' updated fields: %s", name, updated)
    return json.dumps({"success": True, "relationship": name, "updated_fields": updated})


def tool_set_inheritance(ctx: ToolContext, *, child: str, parent: str, **_kwargs) -> str:
    """Set or change the parent (superclass) of an entity."""
    classes = ctx.ontology_classes
    child_cls = next((c for c in classes if c.get("name") == child), None)
    if child_cls is None:
        return json.dumps({"error": f"Entity '{child}' not found"})
    if parent and not any(c.get("name") == parent for c in classes):
        return json.dumps({"error": f"Parent entity '{parent}' not found"})

    old_parent = child_cls.get("parent", "")
    child_cls["parent"] = parent
    ctx.ontology_dirty = True
    logger.info("tool_set_inheritance: '%s' parent changed from '%s' to '%s'", child, old_parent, parent)
    return json.dumps({"success": True, "child": child, "old_parent": old_parent, "new_parent": parent})


# =====================================================
# Tool definitions (OpenAI function-calling format)
# =====================================================

_GET_CLASSES_DEF = {
    "type": "function",
    "function": {
        "name": "get_ontology_classes",
        "description": "Get all entity classes in the current ontology with their attributes, parent, and emoji.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

_GET_PROPERTIES_DEF = {
    "type": "function",
    "function": {
        "name": "get_ontology_properties",
        "description": "Get all relationships (object properties) in the current ontology with domain and range.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

_ADD_ENTITY_DEF = {
    "type": "function",
    "function": {
        "name": "add_entity",
        "description": "Add a new entity (class) to the ontology.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Entity class name (PascalCase, e.g. 'Customer')"},
                "label": {"type": "string", "description": "Human-readable label"},
                "description": {"type": "string", "description": "Short description"},
                "parent": {"type": "string", "description": "Parent entity name for inheritance (empty if none)"},
                "emoji": {"type": "string", "description": "Icon emoji for the entity"},
            },
            "required": ["name"],
        },
    },
}

_REMOVE_ENTITY_DEF = {
    "type": "function",
    "function": {
        "name": "remove_entity",
        "description": "Remove an entity and cascade-delete its relationships and inheritance references.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Entity name to remove"},
            },
            "required": ["name"],
        },
    },
}

_RENAME_ENTITY_DEF = {
    "type": "function",
    "function": {
        "name": "rename_entity",
        "description": "Rename an entity and update all references (properties, inheritance).",
        "parameters": {
            "type": "object",
            "properties": {
                "old_name": {"type": "string", "description": "Current entity name"},
                "new_name": {"type": "string", "description": "New entity name"},
                "new_label": {"type": "string", "description": "New human-readable label (defaults to new_name)"},
            },
            "required": ["old_name", "new_name"],
        },
    },
}

_UPDATE_ENTITY_DEF = {
    "type": "function",
    "function": {
        "name": "update_entity",
        "description": "Update fields (label, description, parent, emoji) of an existing entity.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Entity name to update"},
                "label": {"type": "string", "description": "New label"},
                "description": {"type": "string", "description": "New description"},
                "parent": {"type": "string", "description": "New parent entity name (empty to remove)"},
                "emoji": {"type": "string", "description": "New icon emoji"},
            },
            "required": ["name"],
        },
    },
}

_ADD_ATTRIBUTE_DEF = {
    "type": "function",
    "function": {
        "name": "add_attribute",
        "description": "Add a data property (attribute) to an entity.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_name": {"type": "string", "description": "Entity to add the attribute to"},
                "attribute_name": {"type": "string", "description": "Attribute name (camelCase)"},
                "attribute_type": {"type": "string", "description": "XSD type (e.g. xsd:string, xsd:integer, xsd:date)"},
                "description": {"type": "string", "description": "Short description"},
            },
            "required": ["entity_name", "attribute_name"],
        },
    },
}

_REMOVE_ATTRIBUTE_DEF = {
    "type": "function",
    "function": {
        "name": "remove_attribute",
        "description": "Remove a data property (attribute) from an entity.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_name": {"type": "string", "description": "Entity to remove the attribute from"},
                "attribute_name": {"type": "string", "description": "Attribute name to remove"},
            },
            "required": ["entity_name", "attribute_name"],
        },
    },
}

_ADD_RELATIONSHIP_DEF = {
    "type": "function",
    "function": {
        "name": "add_relationship",
        "description": "Add a new relationship (object property) between two entities.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Relationship name (camelCase, e.g. 'belongsTo')"},
                "domain": {"type": "string", "description": "Source entity name"},
                "range": {"type": "string", "description": "Target entity name"},
                "label": {"type": "string", "description": "Human-readable label"},
                "description": {"type": "string", "description": "Short description"},
                "direction": {"type": "string", "enum": ["forward", "inverse"], "description": "Relationship direction"},
            },
            "required": ["name", "domain", "range"],
        },
    },
}

_REMOVE_RELATIONSHIP_DEF = {
    "type": "function",
    "function": {
        "name": "remove_relationship",
        "description": "Remove a relationship (object property) from the ontology.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Relationship name to remove"},
                "domain": {"type": "string", "description": "Optional: filter by domain entity"},
                "range": {"type": "string", "description": "Optional: filter by range entity"},
            },
            "required": ["name"],
        },
    },
}

_UPDATE_RELATIONSHIP_DEF = {
    "type": "function",
    "function": {
        "name": "update_relationship",
        "description": "Update fields of an existing relationship.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Current relationship name"},
                "new_name": {"type": "string", "description": "New name (if renaming)"},
                "label": {"type": "string", "description": "New label"},
                "description": {"type": "string", "description": "New description"},
                "domain": {"type": "string", "description": "New domain entity"},
                "range": {"type": "string", "description": "New range entity"},
                "direction": {"type": "string", "enum": ["forward", "inverse"], "description": "New direction"},
            },
            "required": ["name"],
        },
    },
}

_SET_INHERITANCE_DEF = {
    "type": "function",
    "function": {
        "name": "set_inheritance",
        "description": "Set or change the parent (superclass) of an entity. Use empty parent to remove inheritance.",
        "parameters": {
            "type": "object",
            "properties": {
                "child": {"type": "string", "description": "Child entity name"},
                "parent": {"type": "string", "description": "Parent entity name (empty to remove inheritance)"},
            },
            "required": ["child", "parent"],
        },
    },
}


# =====================================================
# Aggregated lists for the engine
# =====================================================

TOOL_DEFINITIONS: List[dict] = [
    _GET_CLASSES_DEF,
    _GET_PROPERTIES_DEF,
    _ADD_ENTITY_DEF,
    _REMOVE_ENTITY_DEF,
    _RENAME_ENTITY_DEF,
    _UPDATE_ENTITY_DEF,
    _ADD_ATTRIBUTE_DEF,
    _REMOVE_ATTRIBUTE_DEF,
    _ADD_RELATIONSHIP_DEF,
    _REMOVE_RELATIONSHIP_DEF,
    _UPDATE_RELATIONSHIP_DEF,
    _SET_INHERITANCE_DEF,
]

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_ontology_classes": tool_get_ontology_classes,
    "get_ontology_properties": tool_get_ontology_properties,
    "add_entity": tool_add_entity,
    "remove_entity": tool_remove_entity,
    "rename_entity": tool_rename_entity,
    "update_entity": tool_update_entity,
    "add_attribute": tool_add_attribute,
    "remove_attribute": tool_remove_attribute,
    "add_relationship": tool_add_relationship,
    "remove_relationship": tool_remove_relationship,
    "update_relationship": tool_update_relationship,
    "set_inheritance": tool_set_inheritance,
}
