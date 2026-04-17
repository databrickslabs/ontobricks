"""Static helpers for building and using graph schemas (backward-compat API surface)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.core.graphdb.ladybugdb.GraphSchema import GraphSchema


class GraphSchemaBuilder:
    """Wraps schema generation and classification for callers that prefer static methods."""

    @staticmethod
    def generate_graph_schema(
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        relationships: Optional[List[Dict[str, Any]]] = None,
        base_uri: str = "",
    ) -> GraphSchema:
        return GraphSchema.from_ontology(
            classes, properties, relationships=relationships, base_uri=base_uri,
        )

    @staticmethod
    def generate_ddl(schema: GraphSchema) -> List[str]:
        return schema.generate_ddl()

    @staticmethod
    def classify_triples(
        triples: List[Dict[str, str]],
        schema: GraphSchema,
    ) -> Tuple[
        Dict[str, List[Dict[str, Any]]],
        List[Dict[str, Any]],
        Dict[str, List[Dict[str, str]]],
    ]:
        return schema.classify_triples(triples)

    @staticmethod
    def resolve_table_name(
        name: str,
        name_to_uri: Dict[str, str],
        schema: GraphSchema,
        base_uri: str,
    ) -> str:
        return GraphSchema._resolve_table_name(name, name_to_uri, schema, base_uri)
