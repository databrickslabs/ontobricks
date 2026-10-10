"""Editor CRUD and design-layout sync extracted from :class:`Ontology`.

Fowler Extract Class. ``Ontology`` keeps one-line delegators.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from back.core.errors import NotFoundError, ValidationError
from back.core.logging import get_logger
from back.objects.ontology.OntologyClassModel import OntologyClassModel
from back.objects.ontology.OntologyOwl import OntologyOwl

if TYPE_CHECKING:
    from back.objects.session.DomainSession import DomainSession

logger = get_logger(__name__)


class OntologyEditor:
    """Persist ontology edits against the current domain session."""

    def __init__(self, session: "DomainSession") -> None:
        self._domain = session

    def prune_mappings_to_ontology_uris(
        self,
        class_uris: Set[str],
        property_uris: Set[str],
    ) -> Dict[str, int]:
        """Drop entity/relationship mappings whose URIs are not in the given sets.

        Updates session assignment only when rows are removed.

        Returns:
            Counts ``entity_mappings_removed`` and ``relationship_mappings_removed``.
        """
        s = self._domain
        entity_mappings = s.get_entity_mappings()
        cleaned_entity = [
            m for m in entity_mappings if m.get("ontology_class") in class_uris
        ]
        removed_entity = len(entity_mappings) - len(cleaned_entity)

        rel_mappings = s.get_relationship_mappings()
        cleaned_rel = [m for m in rel_mappings if m.get("property") in property_uris]
        removed_rel = len(rel_mappings) - len(cleaned_rel)

        if removed_entity > 0:
            s._data["assignment"]["entities"] = cleaned_entity
        if removed_rel > 0:
            s._data["assignment"]["relationships"] = cleaned_rel

        return {
            "entity_mappings_removed": removed_entity,
            "relationship_mappings_removed": removed_rel,
        }

    @staticmethod
    def _diff_by_uri(
        old_list: Optional[List[Dict[str, Any]]],
        new_list: Optional[List[Dict[str, Any]]],
    ) -> Tuple[list, list, list]:
        """Return (added, updated, removed) ``(uri, name)`` for URI-keyed items."""
        old_map = {i.get("uri"): i for i in (old_list or []) if i.get("uri")}
        new_map = {i.get("uri"): i for i in (new_list or []) if i.get("uri")}
        added = [
            (u, n.get("name") or u) for u, n in new_map.items() if u not in old_map
        ]
        removed = [
            (u, o.get("name") or u) for u, o in old_map.items() if u not in new_map
        ]
        updated = [
            (u, new_map[u].get("name") or u)
            for u in new_map
            if u in old_map and new_map[u] != old_map[u]
        ]
        return added, updated, removed

    def _record_ontology_diff(
        self,
        old_classes: Optional[List[Dict[str, Any]]],
        new_classes: Optional[List[Dict[str, Any]]],
        old_props: Optional[List[Dict[str, Any]]],
        new_props: Optional[List[Dict[str, Any]]],
        *,
        source: str = "user",
    ) -> None:
        """Buffer per-entity change events for a bulk ontology replacement."""
        s = self._domain
        for entity_type, old, new in (
            ("class", old_classes, new_classes),
            ("property", old_props, new_props),
        ):
            added, updated, removed = self._diff_by_uri(old, new)
            old_map = {i.get("uri"): i for i in (old or []) if i.get("uri")}
            new_map = {i.get("uri"): i for i in (new or []) if i.get("uri")}
            for verb, items in (("added", added), ("updated", updated),
                                ("removed", removed)):
                for uri, name in items:
                    meta = {}
                    if verb == "updated":
                        meta = s.diff_meta(old_map.get(uri), new_map.get(uri))
                        if not meta:
                            continue
                    elif verb == "added":
                        meta = s.diff_meta({}, new_map.get(uri))
                    elif verb == "removed":
                        meta = s.diff_meta(old_map.get(uri), {})
                    s.record_change(
                        f"{entity_type}_{verb}",
                        entity_type=entity_type,
                        entity_ref=uri,
                        summary=name,
                        source=source,
                        meta=meta,
                    )

    def save_ontology_config_from_editor(
        self, raw_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Persist ontology from the visual editor API (wrapped or bare config dict)."""
        s = self._domain
        old_classes = list(s.get_classes())
        old_props = list(s.get_properties())
        ontology_config = raw_body.get("config", raw_body)
        ontology_config = OntologyClassModel.ensure_uris(ontology_config)

        def _on_replace(prop: Dict[str, Any], field: str, old: Any, new: Any) -> None:
            logger.debug(
                "Normalizing property %s.%s: %r → %r",
                prop.get("name"),
                field,
                old,
                new,
            )

        OntologyClassModel.normalize_property_domain_range(
            ontology_config, on_replace=_on_replace
        )

        # Remove datatype properties whose attribute the editor just deleted from
        # its class. Without this the mirror in ``properties`` survives and
        # ``sync_class_data_properties`` resurrects the attribute on the next
        # load (the "deleted attribute keeps coming back" bug).
        orphaned_props = OntologyClassModel.prune_orphaned_datatype_properties(ontology_config)
        if orphaned_props:
            logger.info(
                "Pruned %d orphaned datatype property(ies) removed in the editor",
                orphaned_props,
            )

        existing_constraints = s.constraints
        existing_swrl_rules = s.swrl_rules
        existing_axioms = s.axioms
        existing_expressions = s.expressions

        new_class_uris = {
            c.get("uri") for c in ontology_config.get("classes", []) if c.get("uri")
        }
        new_property_uris = {
            p.get("uri") for p in ontology_config.get("properties", []) if p.get("uri")
        }

        entity_before = s.get_entity_mappings()
        removed_counts = self.prune_mappings_to_ontology_uris(
            new_class_uris, new_property_uris
        )
        removed_entity = removed_counts["entity_mappings_removed"]
        removed_rel = removed_counts["relationship_mappings_removed"]

        if removed_entity > 0 or removed_rel > 0:
            orphaned_uris = [
                m.get("ontology_class")
                for m in entity_before
                if m.get("ontology_class") not in new_class_uris
            ]
            logger.warning(
                "Orphan cleanup: removing %d entity mappings (orphan URIs: %s) and %d rel mappings. "
                "New class URIs: %s",
                removed_entity,
                orphaned_uris,
                removed_rel,
                list(new_class_uris)[:10],
            )

        s.clear_generated_content()
        domain_name = (s.info.get("name") or "").strip()
        ontology_name = (s.ontology.get("name") or "").strip()
        rederive_name = bool(domain_name) and (
            not ontology_name or ontology_name.lower() == domain_name.lower()
        )
        canonical_name = (
            domain_name.lower()
            if rederive_name
            else ontology_name or ontology_config.get("name", "")
        )
        if rederive_name:
            s.ontology["label_lang"] = None
        s.ontology.update(
            {
                "name": canonical_name,
                "base_uri": ontology_config.get("base_uri", ""),
                "description": ontology_config.get("description", ""),
                "classes": ontology_config.get("classes", []),
                "properties": ontology_config.get("properties", []),
                "constraints": ontology_config.get("constraints", existing_constraints),
                "swrl_rules": ontology_config.get("swrl_rules", existing_swrl_rules),
                "axioms": ontology_config.get("axioms", existing_axioms),
                "expressions": ontology_config.get("expressions", existing_expressions),
            }
        )
        self._record_ontology_diff(
            old_classes,
            ontology_config.get("classes", []),
            old_props,
            ontology_config.get("properties", []),
        )
        # Keep the design-layout views in sync with the ontology we just
        # persisted. Each view stores its own copy of the structural content
        # (attributes, relationships, inheritances) alongside layout, and those
        # copies are NOT touched by the editor — so a stale copy flows back into
        # the ontology the next time the designer canvas is serialised,
        # resurrecting a just-removed attribute/relationship/parent. Reconciling
        # here makes /ontology/save authoritative regardless of the UI path.
        # Isolated: this defence-in-depth reconciliation must never break the
        # ontology save itself (view schemas vary across sessions).
        try:
            self._sync_design_layout_with_ontology()
        except Exception:  # noqa: BLE001
            logger.exception(
                "design-layout reconciliation failed — ontology still saved"
            )
        s.save()

        return {
            "success": True,
            "message": "Ontology saved",
            "stats": OntologyOwl.get_ontology_stats(ontology_config),
            "mappings_cleaned": {
                "entity_mappings_removed": removed_entity,
                "relationship_mappings_removed": removed_rel,
            },
        }

    def _sync_design_layout_with_ontology(self) -> None:
        """Reconcile design-layout views with the ontology (prune stale copies).

        Every design view embeds a full copy of the ontology's structural
        content alongside its layout: ``entities[].properties`` (attributes),
        ``relationships`` and ``inheritances`` — independent of the ontology
        ``classes``/``properties``. The editor only writes the ontology copy, so
        these view copies drift and later overwrite the ontology when the
        designer canvas is serialised back to config (the bug where a removed
        attribute/relationship/parent reappears after leaving and returning to
        the Designer).

        This makes ``/ontology/save`` authoritative: for every view we
          - drop entities whose class no longer exists,
          - reconcile each surviving entity's attribute set with the class
            (survivors keep their canvas metadata, removals are dropped, new
            attributes are appended with defaults),
          - drop relationships / inheritances that no longer match the ontology,
          - prune dangling visibility references.
        Additions of new entities/relationships are intentionally NOT synthesised
        here (no server-side layout to invent) — the designer's merge-load branch
        adds them from the ontology with fresh positions. Mutates
        ``design_layout`` in place; the caller saves.
        """
        s = self._domain
        views = (s.design_layout or {}).get("views") or {}
        if not views:
            return

        classes = s.get_classes()
        class_names = {c.get("name") for c in classes if c.get("name")}
        class_attrs: Dict[str, List[str]] = {}
        parent_by_child: Dict[str, str] = {}
        for cls in classes:
            name = cls.get("name")
            if not name:
                continue
            class_attrs[name] = [
                (dp.get("name") or dp.get("localName"))
                for dp in (cls.get("dataProperties") or [])
                if (dp.get("name") or dp.get("localName"))
            ]
            parent = cls.get("parent") or cls.get("parentClass")
            if parent:
                parent_by_child[name] = parent

        # Ontology object properties as {name, frozenset(domain, range)} for
        # orientation-agnostic matching against view relationships.
        object_prop_keys: Set[Tuple[str, frozenset]] = set()
        for prop in s.get_properties():
            is_object = prop.get("type") == "ObjectProperty" or (
                prop.get("domain") and prop.get("range")
            )
            if is_object and prop.get("name"):
                object_prop_keys.add(
                    (prop["name"], frozenset({prop.get("domain"), prop.get("range")}))
                )

        for view in views.values():
            # 1. Entities: drop deleted classes, reconcile survivors' attributes.
            surviving_entities = []
            id_to_name: Dict[str, str] = {}
            for entity in view.get("entities") or []:
                ename = entity.get("name")
                if ename not in class_names:
                    continue  # class deleted from the ontology
                if entity.get("id"):
                    id_to_name[entity["id"]] = ename
                existing = {
                    p.get("name"): p
                    for p in (entity.get("properties") or [])
                    if p.get("name")
                }
                entity["properties"] = [
                    existing.get(
                        attr_name,
                        {
                            "name": attr_name,
                            "type": "string",
                            "isRequired": False,
                            "isPrimaryKey": False,
                        },
                    )
                    for attr_name in class_attrs.get(ename, [])
                ]
                surviving_entities.append(entity)
            view["entities"] = surviving_entities

            # 2. Relationships: keep only those matching an ontology object
            # property (by name + endpoint class names, orientation-agnostic).
            surviving_rels = []
            for rel in view.get("relationships") or []:
                src = id_to_name.get(rel.get("sourceEntityId"))
                tgt = id_to_name.get(rel.get("targetEntityId"))
                if not src or not tgt:
                    continue  # endpoint entity was removed
                if (rel.get("name"), frozenset({src, tgt})) in object_prop_keys:
                    surviving_rels.append(rel)
            view["relationships"] = surviving_rels

            # 3. Inheritances: keep only pairs that still exist as class parents.
            surviving_inh = []
            for inh in view.get("inheritances") or []:
                src = id_to_name.get(inh.get("sourceEntityId"))
                tgt = id_to_name.get(inh.get("targetEntityId"))
                if not src or not tgt:
                    continue
                if inh.get("direction") == "forward":
                    parent_name, child_name = src, tgt
                else:
                    parent_name, child_name = tgt, src
                if parent_by_child.get(child_name) == parent_name:
                    surviving_inh.append(inh)
            view["inheritances"] = surviving_inh

            # 4. Prune dangling visibility references. Only the name-list keys
            # (hiddenEntities / collapsedEntities) are string lists; the
            # inheritance/relationship visibility keys hold {source, target}
            # dicts and are left untouched (harmless if dangling).
            visibility = view.get("visibility")
            if isinstance(visibility, dict):
                surviving_names = {e.get("name") for e in surviving_entities}
                for key in ("hiddenEntities", "collapsedEntities"):
                    if isinstance(visibility.get(key), list):
                        visibility[key] = [
                            n
                            for n in visibility[key]
                            if not isinstance(n, str) or n in surviving_names
                        ]

    def delete_class_by_uri(self, class_uri: Optional[str]) -> Dict[str, Any]:
        """Remove a class by URI and drop entity mappings that reference it."""
        if not class_uri:
            raise ValidationError("Class URI is required")
        s = self._domain
        classes = list(s.get_classes())
        original_len = len(classes)
        classes = [c for c in classes if c.get("uri") != class_uri]
        if len(classes) >= original_len:
            raise NotFoundError("Class not found")

        removed = next(
            (c for c in s.get_classes() if c.get("uri") == class_uri),
            None,
        )
        removed_name = (removed or {}).get("name") or class_uri
        s.ontology["classes"] = classes
        entity_mappings = s.get_entity_mappings()
        original_mapping_len = len(entity_mappings)
        entity_mappings = [
            m for m in entity_mappings if m.get("ontology_class") != class_uri
        ]
        if len(entity_mappings) < original_mapping_len:
            s._data["assignment"]["entities"] = entity_mappings

        s.clear_generated_content()
        s.record_change(
            "class_removed", entity_type="class",
            entity_ref=class_uri, summary=removed_name or class_uri,
            meta=s.diff_meta(removed or {}, {}),
        )
        s.save()
        return {
            "success": True,
            "mapping_removed": len(entity_mappings) < original_mapping_len,
        }

    def delete_property_by_uri(self, property_uri: Optional[str]) -> Dict[str, Any]:
        """Remove an object property by URI and drop relationship mappings that reference it."""
        if not property_uri:
            raise ValidationError("Property URI is required")
        s = self._domain
        properties = list(s.get_properties())
        original_len = len(properties)
        properties = [p for p in properties if p.get("uri") != property_uri]
        if len(properties) >= original_len:
            raise NotFoundError("Property not found")

        removed = next(
            (p for p in s.get_properties() if p.get("uri") == property_uri),
            None,
        )
        removed_name = (removed or {}).get("name") or property_uri
        s.ontology["properties"] = properties
        rel_mappings = s.get_relationship_mappings()
        original_mapping_len = len(rel_mappings)
        rel_mappings = [m for m in rel_mappings if m.get("property") != property_uri]
        if len(rel_mappings) < original_mapping_len:
            s._data["assignment"]["relationships"] = rel_mappings

        s.clear_generated_content()
        s.record_change(
            "property_removed", entity_type="property",
            entity_ref=property_uri, summary=removed_name or property_uri,
            meta=s.diff_meta(removed or {}, {}),
        )
        s.save()
        return {
            "success": True,
            "mapping_removed": len(rel_mappings) < original_mapping_len,
        }

    def add_class(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build a class from *data*, append if unique URI and name, clear cache and save."""
        s = self._domain
        classes = list(s.get_classes())
        new_class = OntologyClassModel.build_class_from_data(data)
        if any(c.get("uri") == new_class["uri"] for c in classes):
            raise ValidationError("Class with this URI already exists")
        if any(c.get("name") == new_class["name"] for c in classes):
            raise ValidationError("Class with this name already exists")
        classes.append(new_class)
        s.ontology["classes"] = classes
        s.clear_generated_content()
        s.record_change(
            "class_added", entity_type="class",
            entity_ref=new_class.get("uri", ""), summary=new_class.get("name", ""),
            meta=s.diff_meta({}, new_class),
        )
        s.save()
        return {"success": True, "class": new_class}

    def update_class(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Find class by *uri* in data, merge updates, clear cache and save."""
        s = self._domain
        classes = list(s.get_classes())
        class_uri = data.get("uri")
        new_name = data.get("name")
        for i, cls in enumerate(classes):
            if cls.get("uri") == class_uri:
                if new_name and new_name != cls.get("name"):
                    if any(c.get("name") == new_name for j, c in enumerate(classes) if j != i):
                        raise ValidationError("Class with this name already exists")
                old_cls = dict(cls)
                classes[i] = OntologyClassModel.build_class_from_data(data, cls)
                s.ontology["classes"] = classes
                s.clear_generated_content()
                meta = s.diff_meta(old_cls, classes[i])
                if meta:
                    s.record_change(
                        "class_updated", entity_type="class",
                        entity_ref=classes[i].get("uri", ""),
                        summary=classes[i].get("name", ""),
                        meta=meta,
                    )
                s.save()
                return {"success": True, "class": classes[i]}
        raise NotFoundError("Class not found")

    def add_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build a property from *data*, append if unique URI and name, clear cache and save."""
        s = self._domain
        properties = list(s.get_properties())
        new_property = OntologyClassModel.build_property_from_data(data)
        if any(p.get("uri") == new_property["uri"] for p in properties):
            raise ValidationError("Property with this URI already exists")
        if any(p.get("name") == new_property["name"] for p in properties):
            raise ValidationError("Property with this name already exists")
        properties.append(new_property)
        s.ontology["properties"] = properties
        s.clear_generated_content()
        s.record_change(
            "property_added", entity_type="property",
            entity_ref=new_property.get("uri", ""),
            summary=new_property.get("name", ""),
            meta=s.diff_meta({}, new_property),
        )
        s.save()
        return {"success": True, "property": new_property}

    def update_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Find property by *uri* in data, merge updates, clear cache and save."""
        s = self._domain
        properties = list(s.get_properties())
        property_uri = data.get("uri")
        new_name = data.get("name")
        for i, prop in enumerate(properties):
            if prop.get("uri") == property_uri:
                if new_name and new_name != prop.get("name"):
                    if any(p.get("name") == new_name for j, p in enumerate(properties) if j != i):
                        raise ValidationError("Property with this name already exists")
                old_prop = dict(prop)
                properties[i] = OntologyClassModel.build_property_from_data(data, prop)
                s.ontology["properties"] = properties
                s.clear_generated_content()
                meta = s.diff_meta(old_prop, properties[i])
                if meta:
                    s.record_change(
                        "property_updated", entity_type="property",
                        entity_ref=properties[i].get("uri", ""),
                        summary=properties[i].get("name", ""),
                        meta=meta,
                    )
                s.save()
                return {"success": True, "property": properties[i]}
        raise NotFoundError("Property not found")

    def rename_relationship_references(
        self, old_name: str, new_name: str
    ) -> Dict[str, int]:
        """Rename a relationship across mappings, constraints, and axioms. Saves session."""
        s = self._domain
        updates: Dict[str, int] = {
            "mappings_updated": 0,
            "constraints_updated": 0,
            "axioms_updated": 0,
        }
        for rel_mapping in s.get_relationship_mappings():
            if rel_mapping.get("property_label") == old_name:
                rel_mapping["property_label"] = new_name
                updates["mappings_updated"] += 1
        for constraint in s.constraints:
            if constraint.get("property") == old_name:
                constraint["property"] = new_name
                updates["constraints_updated"] += 1
        for axiom in s.axioms:
            if axiom.get("property") == old_name:
                axiom["property"] = new_name
                updates["axioms_updated"] += 1
        s.save()
        return updates
