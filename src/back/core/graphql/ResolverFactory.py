"""GraphQL resolver factory functions."""

from typing import Callable, List, Optional

import strawberry
from strawberry.types import Info

from back.core.graphql.SchemaMetadata import SchemaMetadata
from back.core.logging import get_logger

logger = get_logger(__name__)


class ResolverFactory:
    """Factory for creating GraphQL resolvers from schema metadata."""

    @staticmethod
    def make_list_resolver(
        metadata: SchemaMetadata,
        type_name: str,
        gql_type: type,
    ) -> Callable:
        """Create a root list resolver for a given entity type."""

        def resolver(
            info: Info,
            limit: int = 50,
            offset: int = 0,
            search: Optional[str] = None,
        ) -> List[gql_type]:  # type: ignore[valid-type]
            store = info.context["triplestore"]
            table = info.context["table_name"]
            depth = info.context.get("depth")
            return metadata.resolve_list(
                store, table, type_name, limit, offset, search, depth=depth
            )

        resolver.__name__ = f"resolve_{type_name}_list"
        return resolver

    @staticmethod
    def make_single_resolver(
        metadata: SchemaMetadata,
        type_name: str,
        gql_type: type,
    ) -> Callable:
        """Create a root single-entity resolver for a given type."""

        def resolver(
            info: Info,
            id: strawberry.ID,
        ) -> Optional[gql_type]:  # type: ignore[valid-type]
            store = info.context["triplestore"]
            table = info.context["table_name"]
            depth = info.context.get("depth")
            return metadata.resolve_single(
                store, table, type_name, str(id), depth=depth
            )

        resolver.__name__ = f"resolve_{type_name}_single"
        return resolver
