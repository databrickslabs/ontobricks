"""Class/property dict hygiene extracted from :class:`Ontology`.

Fowler Extract Class. ``Ontology`` keeps one-line delegators.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

class OntologyClassModel:
    """URI hygiene, class/property builders, completeness checks."""

    @staticmethod
    def ensure_uris(config: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure all classes and properties have URIs.

        Args:
            config: Ontology configuration dict

        Returns:
            dict: Configuration with URIs ensured
        """
        base_uri = config.get("base_uri", "http://example.org/")
        if not base_uri.endswith("#") and not base_uri.endswith("/"):
            base_uri = base_uri + "#"

        for cls in config.get("classes", []):
            if not cls.get("uri") and cls.get("name"):
                cls["uri"] = base_uri + cls["name"]
            if not cls.get("localName") and cls.get("name"):
                cls["localName"] = cls["name"]

        for prop in config.get("properties", []):
            if not prop.get("uri") and prop.get("name"):
                prop["uri"] = base_uri + prop["name"]
            if not prop.get("localName") and prop.get("name"):
                prop["localName"] = prop["name"]

        return config

    @staticmethod
    def prune_orphaned_datatype_properties(config: Dict[str, Any]) -> int:
        """Drop datatype properties whose attribute was removed from its class.

        Inverse of :meth:`sync_class_data_properties`: a class's
        ``dataProperties`` is the authoritative editor view, but each datatype
        attribute is *also* mirrored as a ``DatatypeProperty`` in
        ``config['properties']`` (carrying a ``domain``). The editor removes the
        attribute from the class only — so the mirror survives and
        ``sync_class_data_properties`` resurrects it on the next load. Here we
        remove any datatype property whose ``domain`` names an existing class in
        which the attribute is no longer present. Object properties and
        properties whose domain is empty or points to an unknown class are left
        untouched. Returns the number of properties removed.
        """
        classes = config.get("classes", [])
        properties = config.get("properties", [])
        if not classes or not properties:
            return 0

        attrs_by_class = {
            c.get("name"): {
                p.get("name")
                for p in c.get("dataProperties", []) or []
                if p.get("name")
            }
            for c in classes
            if c.get("name")
        }

        kept: List[Dict[str, Any]] = []
        removed = 0
        for prop in properties:
            prop_type = prop.get("type", "")
            if prop_type not in ("DatatypeProperty", "Property", ""):
                kept.append(prop)
                continue
            domain = prop.get("domain", "")
            class_attrs = attrs_by_class.get(domain)
            if class_attrs is None:
                # No such class (or no domain) — not a resurrectable orphan.
                kept.append(prop)
                continue
            pname = prop.get("name") or prop.get("localName")
            if pname and pname not in class_attrs:
                removed += 1
                continue
            kept.append(prop)

        if removed:
            config["properties"] = kept
        return removed

    @staticmethod
    def sync_class_data_properties(config: Dict[str, Any]) -> None:
        """Ensure ``classes[].dataProperties`` includes datatype attributes.

        Merges datatype properties declared on ``config['properties']`` (when
        they carry a ``domain``) into the matching class.  Idempotent.
        """
        classes = config.get("classes", [])
        properties = config.get("properties", [])
        if not classes:
            return

        by_name = {c.get("name"): c for c in classes if c.get("name")}

        for prop in properties:
            prop_type = prop.get("type", "")
            if prop_type == "ObjectProperty":
                continue
            if prop_type not in ("DatatypeProperty", "Property", ""):
                continue

            domain = prop.get("domain", "")
            if not domain:
                continue

            cls = by_name.get(domain)
            if not cls:
                continue

            pname = prop.get("name") or prop.get("localName")
            if not pname:
                continue

            data_props = cls.setdefault("dataProperties", [])
            if any(p.get("name") == pname for p in data_props):
                continue

            data_props.append(
                {
                    "name": pname,
                    "localName": prop.get("localName", pname),
                    "label": prop.get("label", pname),
                    "uri": prop.get("uri", ""),
                }
            )

    @staticmethod
    def finalize_class_attributes(config: Dict[str, Any]) -> None:
        """Sync datatype properties onto classes and propagate inheritance."""
        from back.core.w3c.owl.OntologyParser import OntologyParser

        OntologyClassModel.sync_class_data_properties(config)
        classes = config.get("classes", [])
        if classes:
            OntologyParser._propagate_inherited_properties(classes)

    _PRIMITIVE_RANGES = frozenset({
        "string", "integer", "int", "long", "float", "double", "decimal",
        "boolean", "date", "datetime", "time", "duration",
        "anyuri", "literal", "plainliteral", "langstring",
        "xsd:string", "xsd:integer", "xsd:int", "xsd:long", "xsd:float",
        "xsd:double", "xsd:decimal", "xsd:boolean", "xsd:date",
        "xsd:datetime", "xsd:time", "xsd:duration", "xsd:anyuri",
        "rdfs:literal",
    })

    @staticmethod
    def ancestor_names(classes: List[Dict[str, Any]], class_name: str) -> List[str]:
        """Nearest-first parent chain, excluding *class_name*. Cycles stop."""
        by_name = {c.get("name"): c for c in classes if c.get("name")}
        chain: List[str] = []
        visited = {class_name}
        current = by_name.get(class_name)
        while current:
            parent = current.get("parent") or ""
            if not parent or parent in visited:
                break
            visited.add(parent)
            chain.append(parent)
            current = by_name.get(parent)
        return chain

    @staticmethod
    def _is_object_property(prop: Dict[str, Any]) -> bool:
        ptype = (prop.get("type") or "").replace("owl:", "")
        if ptype == "DatatypeProperty":
            return False
        if ptype in ("ObjectProperty",):
            return True
        range_val = (prop.get("range") or "").lower()
        return range_val not in OntologyClassModel._PRIMITIVE_RANGES

    @staticmethod
    def outgoing_relations_for_class(
        config: Dict[str, Any], class_name: str
    ) -> List[Dict[str, Any]]:
        """Own + inherited outgoing object properties for *class_name*.

        Does not mutate ``config``. Inherited copies set ``inherited`` /
        ``inheritedFrom`` (declaring ancestor name).
        """
        classes = config.get("classes") or []
        properties = config.get("properties") or []
        declaring = {class_name, *OntologyClassModel.ancestor_names(classes, class_name)}
        out: List[Dict[str, Any]] = []
        for prop in properties:
            domain = prop.get("domain") or ""
            if domain not in declaring:
                continue
            if not OntologyClassModel._is_object_property(prop):
                continue
            if not prop.get("range"):
                continue
            row = dict(prop)
            inherited = domain != class_name
            row["inherited"] = inherited
            row["inheritedFrom"] = domain if inherited else ""
            out.append(row)
        return out

    @staticmethod
    def normalize_property_domain_range(
        ontology_config: Dict[str, Any],
        *,
        on_replace: Optional[Callable[[Dict[str, Any], str, Any, Any], None]] = None,
    ) -> bool:
        """Align property ``domain`` / ``range`` with canonical class names (case-insensitive).

        Mutates ``ontology_config['properties']`` in place. If ``on_replace`` is set, it is
        called as ``(prop_dict, field_name, old_value, new_value)`` for each change.

        Returns:
            True if any property field was updated.
        """
        classes = ontology_config.get("classes", [])
        properties = ontology_config.get("properties", [])
        class_name_lookup = {
            c["name"].lower(): c["name"] for c in classes if c.get("name")
        }
        modified = False
        for prop in properties:
            for field in ("domain", "range"):
                val = prop.get(field, "")
                if val and val not in class_name_lookup.values():
                    canonical = class_name_lookup.get(str(val).lower())
                    if canonical:
                        if on_replace is not None:
                            on_replace(prop, field, val, canonical)
                        prop[field] = canonical
                        modified = True
        return modified

    @staticmethod
    def build_class_from_data(
        data: Dict[str, Any], existing: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Build a class dict from request data.

        Args:
            data: Request data
            existing: Existing class data (for updates)

        Returns:
            dict: Built class
        """
        existing = existing or {}
        return {
            "uri": data.get("uri", existing.get("uri", "")),
            "name": data.get("name", existing.get("name", "")),
            "label": data.get("label", data.get("name", existing.get("label", ""))),
            "description": data.get("description", existing.get("description", "")),
            "parent": data.get("parent", existing.get("parent", "")),
            "emoji": data.get("emoji", existing.get("emoji", "📦")),
            "properties": data.get("properties", existing.get("properties", [])),
            "dataProperties": data.get(
                "dataProperties", existing.get("dataProperties", [])
            ),
            "dashboard": data.get("dashboard", existing.get("dashboard", "")),
            "dashboardParams": data.get(
                "dashboardParams", existing.get("dashboardParams", {})
            ),
            "bridges": data.get("bridges", existing.get("bridges", [])),
            "dataset": data.get("dataset", existing.get("dataset", None)),
            "actions": data.get("actions", existing.get("actions", [])),
            "business_rules": data.get(
                "business_rules", existing.get("business_rules", [])
            ),
            "virtualAttributes": data.get(
                "virtualAttributes", existing.get("virtualAttributes", [])
            ),
            # First-class synonym storage (design:
            # docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md
            # §Synonyms as first-class alternate labels). Populated by the
            # Generate merge for newly-appended entities; preserved verbatim
            # on manual edits via the existing/`existing` fallback.
            "alternate_labels": data.get(
                "alternate_labels", existing.get("alternate_labels", [])
            ),
        }

    @staticmethod
    def build_property_from_data(
        data: Dict[str, Any], existing: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Build a property dict from request data.

        Args:
            data: Request data
            existing: Existing property data (for updates)

        Returns:
            dict: Built property
        """
        existing = existing or {}
        return {
            "uri": data.get("uri", existing.get("uri", "")),
            "name": data.get("name", existing.get("name", "")),
            "label": data.get("label", data.get("name", existing.get("label", ""))),
            "description": data.get("description", existing.get("description", "")),
            "type": data.get("type", existing.get("type", "")),
            "domain": data.get("domain", existing.get("domain", "")),
            "range": data.get("range", existing.get("range", "")),
            "direction": data.get("direction", existing.get("direction", "forward")),
            "properties": data.get("properties", existing.get("properties", [])),
        }

    @staticmethod
    def validate_classes(classes: List[Dict[str, Any]]) -> tuple:
        """Check ontology classes for completeness.

        Returns:
            ``(is_valid, issues)`` where *issues* is a list of human-readable
            strings and *is_valid* is ``True`` when ``classes`` is non-empty
            and all entries have at least a URI, name, or localName.
        """
        issues: List[str] = []
        for cls in classes:
            if not cls.get("uri") and not cls.get("name") and not cls.get("localName"):
                issues.append(f"Entity '{cls.get('label', 'Unknown')}' has no URI")
        if not classes:
            issues.append("No entities defined")
        return (len(classes) > 0 and len(issues) == 0), issues
