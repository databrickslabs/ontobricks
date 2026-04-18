"""LadybugDB graph schema DTOs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class NodeTableDef:
    """Definition for a node table."""

    name: str
    class_uri: str
    properties: List[str]
    property_uris: Dict[str, str] = field(default_factory=dict)

    def to_cypher(self) -> str:
        from back.core.graphdb.ladybugdb.GraphSchema import GraphSchema

        prop_defs = ["uri STRING PRIMARY KEY"]
        prop_defs.append("label STRING")
        for p in self.properties:
            safe = GraphSchema.safe_identifier(p)
            prop_defs.append(f"{safe} STRING")
        return (
            f"CREATE NODE TABLE IF NOT EXISTS {self.name}" f"({', '.join(prop_defs)})"
        )


@dataclass
class RelTableDef:
    """Definition for a relationship table."""

    name: str
    property_uri: str
    from_table: str
    to_table: str

    def to_cypher(self) -> str:
        return (
            f"CREATE REL TABLE IF NOT EXISTS {self.name}"
            f"(FROM {self.from_table} TO {self.to_table})"
        )
