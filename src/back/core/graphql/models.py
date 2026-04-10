"""GraphQL data transfer objects."""
from dataclasses import dataclass, field
from typing import Dict, Tuple


@dataclass
class TypeInfo:
    """Metadata for a single GraphQL entity type."""

    name: str
    cls_uri: str
    gql_type: type
    predicate_to_field: Dict[str, str] = field(default_factory=dict)
    field_to_predicate: Dict[str, str] = field(default_factory=dict)
    relationships: Dict[str, Tuple[str, str]] = field(default_factory=dict)
