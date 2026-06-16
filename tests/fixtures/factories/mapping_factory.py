"""R2RMLMappingFactory — builds R2RML mapping config dicts for tests.

Mirrors the shape consumed by `back.objects.mapping.MappingService` and
`back.core.w3c.r2rml.R2RMLGenerator`. Supports controlled join cardinality,
circular refs, and conditional rules so the complex-mapping integration tests
(T-M2.P3) have a single source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class R2RMLMappingFactory:
    """Build R2RML mapping config dicts.

    Args:
        entity_count: Number of entities (table → class mappings).
        relationship_count: Number of relationships (FK → ObjectProperty mappings).
        base_uri: Base IRI for class URIs.
        catalog: UC catalog name placeholder.
        schema: UC schema name placeholder.
        circular: If True, the last relationship loops back to the first entity (creates a cycle).
        with_conditions: If True, attach a `condition` clause to each relationship.
        seed: Deterministic seed.
    """

    entity_count: int = 2
    relationship_count: int = 1
    base_uri: str = "http://test.org/ontology#"
    catalog: str = "test_catalog"
    schema: str = "test_schema"
    circular: bool = False
    with_conditions: bool = False
    seed: int = 0

    @classmethod
    def build(cls, **overrides: Any) -> dict[str, Any]:
        return cls(**overrides)._build()

    def _build(self) -> dict[str, Any]:
        entities = self._entities()
        relationships = self._relationships(entities)
        return {"entities": entities, "relationships": relationships}

    def _entities(self) -> list[dict[str, Any]]:
        defaults = ["Customer", "Order", "Product", "Address", "Invoice", "LineItem"]
        names = defaults[: self.entity_count]
        if self.entity_count > len(defaults):
            names = defaults + [f"Entity{i}" for i in range(len(defaults), self.entity_count)]
        return [
            {
                "ontology_class": f"{self.base_uri}{name}",
                "ontology_class_label": name,
                "sql_query": f"SELECT * FROM {self.catalog}.{self.schema}.{name.lower()}s",
                "id_column": f"{name.lower()}_id",
                "label_column": "name",
                "catalog": self.catalog,
                "schema": self.schema,
                "table": f"{name.lower()}s",
                "attribute_mappings": {
                    f"{name.lower()}Attr0": "name",
                    f"{name.lower()}Attr1": "created_at",
                },
            }
            for name in names
        ]

    def _relationships(self, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not entities or self.relationship_count <= 0:
            return []
        relationships: list[dict[str, Any]] = []
        for i in range(self.relationship_count):
            src_idx = i % len(entities)
            tgt_idx = (i + 1) % len(entities) if (self.circular or i < self.relationship_count - 1) else src_idx
            src = entities[src_idx]
            tgt = entities[tgt_idx]
            rel = {
                "property": f"{self.base_uri}has{tgt['ontology_class_label']}",
                "property_label": f"has{tgt['ontology_class_label']}",
                "sql_query": (
                    f"SELECT s.{src['id_column']}, t.{tgt['id_column']} "
                    f"FROM {src['table']} s JOIN {tgt['table']} t "
                    f"ON s.{tgt['id_column']} = t.{tgt['id_column']}"
                ),
                "source_class": src["ontology_class"],
                "source_class_label": src["ontology_class_label"],
                "target_class": tgt["ontology_class"],
                "target_class_label": tgt["ontology_class_label"],
                "source_id_column": src["id_column"],
                "target_id_column": tgt["id_column"],
                "direction": "forward",
            }
            if self.with_conditions:
                rel["condition"] = "active = true"
            relationships.append(rel)
        return relationships
