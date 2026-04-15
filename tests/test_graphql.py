"""Tests for back.core.graphql — schema builder and resolvers."""
import pytest
from unittest.mock import MagicMock

from back.core.graphql.GraphQLSchemaBuilder import (
    GraphQLSchemaBuilder,
    build_schema_for_domain,
    invalidate_cache,
    _safe_name,
    _extract_local,
    _pluralize,
    _normalize_base,
    _topo_sort,
)
from back.core.graphql.SchemaMetadata import SchemaMetadata
from back.core.graphql.models import TypeInfo
from back.core.helpers import extract_local_name as res_extract_local
from back.core.helpers import sql_escape as _sql_escape


BASE_URI = "http://test.org/ontology#"

CLASSES = [
    {
        "uri": f"{BASE_URI}Customer",
        "name": "Customer",
        "label": "Customer",
        "dataProperties": [
            {"name": "firstName", "uri": f"{BASE_URI}firstName"},
            {"name": "lastName", "uri": f"{BASE_URI}lastName"},
        ],
    },
    {
        "uri": f"{BASE_URI}Order",
        "name": "Order",
        "label": "Order",
        "dataProperties": [
            {"name": "orderDate", "uri": f"{BASE_URI}orderDate"},
        ],
    },
]

PROPERTIES = [
    {
        "uri": f"{BASE_URI}hasOrder",
        "name": "hasOrder",
        "domain": f"{BASE_URI}Customer",
        "range": f"{BASE_URI}Order",
    }
]


# ------------------------------------------------------------------
# Static helper tests (via backward-compat aliases)
# ------------------------------------------------------------------

class TestSchemaHelpers:
    def test_safe_name_basic(self):
        assert _safe_name("hello") == "hello"

    def test_safe_name_special_chars(self):
        assert _safe_name("has-order") == "has_order"

    def test_safe_name_leading_digit(self):
        assert _safe_name("1abc").startswith("_")

    def test_safe_name_empty(self):
        assert _safe_name("") == "_unnamed"

    def test_extract_local_hash(self):
        assert _extract_local("http://x.org#Foo") == "Foo"

    def test_extract_local_slash(self):
        assert _extract_local("http://x.org/Bar") == "Bar"

    def test_pluralize(self):
        assert _pluralize("Customer") == "customers"
        assert _pluralize("Class") == "classes"
        assert _pluralize("Entity") == "entities"
        assert _pluralize("Bus") == "buses"

    def test_normalize_base(self):
        assert _normalize_base("http://x.org#") == "http://x.org/"
        assert _normalize_base("http://x.org/") == "http://x.org/"
        assert _normalize_base("http://x.org") == "http://x.org/"

    def test_topo_sort_simple(self):
        names = {"A", "B", "C"}
        deps = {"A": {"B"}, "B": {"C"}}
        order = _topo_sort(names, deps)
        assert order.index("C") < order.index("B") < order.index("A")

    def test_topo_sort_cycle(self):
        names = {"A", "B"}
        deps = {"A": {"B"}, "B": {"A"}}
        order = _topo_sort(names, deps)
        assert set(order) == {"A", "B"}


# ------------------------------------------------------------------
# GraphQLSchemaBuilder class tests
# ------------------------------------------------------------------

class TestGraphQLSchemaBuilder:
    def setup_method(self):
        self.builder = GraphQLSchemaBuilder()

    def test_static_helpers_accessible(self):
        assert GraphQLSchemaBuilder.safe_name("has-order") == "has_order"
        assert GraphQLSchemaBuilder.extract_local("http://x.org#Foo") == "Foo"
        assert GraphQLSchemaBuilder.pluralize("Customer") == "customers"
        assert GraphQLSchemaBuilder.normalize_base("http://x.org#") == "http://x.org/"

    def test_ontology_hash_deterministic(self):
        h1 = GraphQLSchemaBuilder.ontology_hash(CLASSES, PROPERTIES)
        h2 = GraphQLSchemaBuilder.ontology_hash(CLASSES, PROPERTIES)
        assert h1 == h2

    def test_ontology_hash_changes(self):
        h1 = GraphQLSchemaBuilder.ontology_hash(CLASSES, PROPERTIES)
        h2 = GraphQLSchemaBuilder.ontology_hash(CLASSES, [])
        assert h1 != h2

    def test_build_for_domain(self):
        result = self.builder.build_for_domain(CLASSES, PROPERTIES, BASE_URI, "test_builder")
        assert result is not None
        schema, metadata = result
        assert schema is not None
        assert "Customer" in metadata.types
        assert "Order" in metadata.types

    def test_empty_classes_returns_none(self):
        result = self.builder.build_for_domain([], PROPERTIES, BASE_URI, "empty_builder")
        assert result is None

    def test_cache_hit(self):
        self.builder.build_for_domain(CLASSES, PROPERTIES, BASE_URI, "cached_builder")
        result = self.builder.build_for_domain(CLASSES, PROPERTIES, BASE_URI, "cached_builder")
        assert result is not None

    def test_cache_invalidation(self):
        self.builder.build_for_domain(CLASSES, PROPERTIES, BASE_URI, "inv_builder")
        self.builder.invalidate_cache("inv_builder")
        assert "inv_builder" not in self.builder._cache

    def test_metadata_has_type_info(self):
        _, metadata = self.builder.build_for_domain(CLASSES, PROPERTIES, BASE_URI, "meta_builder")
        ti = metadata.types["Customer"]
        assert ti.name == "Customer"
        assert ti.cls_uri == f"{BASE_URI}Customer"

    def test_relationship_registered(self):
        _, metadata = self.builder.build_for_domain(CLASSES, PROPERTIES, BASE_URI, "rel_builder")
        ti = metadata.types["Customer"]
        assert "hasOrder" in ti.relationships

    def test_topo_sort_method(self):
        names = {"X", "Y"}
        deps = {"X": {"Y"}}
        order = GraphQLSchemaBuilder.topo_sort(names, deps)
        assert order.index("Y") < order.index("X")


# ------------------------------------------------------------------
# Module-level delegate tests (backward compat)
# ------------------------------------------------------------------

class TestBuildSchema:
    def test_basic_build(self):
        result = build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "test_domain_graphql")
        assert result is not None
        schema, metadata = result
        assert schema is not None
        assert "Customer" in metadata.types
        assert "Order" in metadata.types

    def test_empty_classes_returns_none(self):
        result = build_schema_for_domain([], PROPERTIES, BASE_URI, "empty")
        assert result is None

    def test_cache_hit(self):
        build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "cached_domain")
        result = build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "cached_domain")
        assert result is not None

    def test_cache_invalidation(self):
        build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "inv_domain")
        invalidate_cache("inv_domain")
        result = build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "inv_domain")
        assert result is not None

    def test_metadata_has_type_info(self):
        _, metadata = build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "meta_domain")
        ti = metadata.types["Customer"]
        assert ti.name == "Customer"
        assert ti.cls_uri == f"{BASE_URI}Customer"
        assert "firstName" in [GraphQLSchemaBuilder.safe_name(k) for k in ti.predicate_to_field.values()]

    def test_relationship_registered(self):
        _, metadata = build_schema_for_domain(CLASSES, PROPERTIES, BASE_URI, "rel_domain")
        ti = metadata.types["Customer"]
        assert "hasOrder" in ti.relationships


# ------------------------------------------------------------------
# Resolver helpers
# ------------------------------------------------------------------

class TestResolverHelpers:
    def test_sql_escape(self):
        assert _sql_escape("it's") == "it''s"
        assert _sql_escape("back\\slash") == "back\\\\slash"

    def test_extract_local(self):
        assert res_extract_local("http://x.org#Foo") == "Foo"
        assert res_extract_local("http://x.org/Bar") == "Bar"


class TestSchemaMetadata:
    def test_register_and_lookup(self):
        meta = SchemaMetadata(base_uri=BASE_URI)
        ti = TypeInfo(name="Test", cls_uri=f"{BASE_URI}Test", gql_type=type("Test", (), {}))
        meta.register(ti)
        assert "Test" in meta.types

    def test_resolve_list_unknown_type(self):
        meta = SchemaMetadata(base_uri=BASE_URI)
        store = MagicMock()
        result = meta.resolve_list(store, "table", "Unknown")
        assert result == []

    def test_resolve_single_unknown_type(self):
        meta = SchemaMetadata(base_uri=BASE_URI)
        store = MagicMock()
        result = meta.resolve_single(store, "table", "Unknown", "id1")
        assert result is None
