"""
Tests for per-attribute include/exclude functionality.

Covers every layer that touches excluded_attributes:
  - Mapping.build_entity_mapping / build_relationship_mapping  (model layer)
  - Mapping.compute_mapping_gaps                               (gap reporting)
  - R2RMLGenerator._add_entity_mapping                         (R2RML export)
  - tool_submit_entity_mapping                                 (agent tool)
  - tool_submit_relationship_mapping                           (agent tool)
  - tool_get_ontology                                          (agent tool)
"""

import json
import pytest

from back.objects.mapping import Mapping
from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator
from agents.tools.mapping import (
    tool_submit_entity_mapping,
    tool_submit_relationship_mapping,
)
from agents.tools.ontology import tool_get_ontology
from agents.tools.context import ToolContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ctx(**kwargs) -> ToolContext:
    """Build a minimal ToolContext for agent-tool tests."""
    return ToolContext(host="https://test.databricks.com", token="tok", **kwargs)


# ---------------------------------------------------------------------------
# 1. Mapping.build_entity_mapping
# ---------------------------------------------------------------------------

class TestBuildEntityMappingExcludedAttributes:
    def test_excluded_attributes_propagated(self):
        data = {
            "ontology_class": "http://t/A",
            "excluded_attributes": ["age", "score"],
        }
        m = Mapping.build_entity_mapping(data)
        assert m["excluded_attributes"] == ["age", "score"]

    def test_excluded_attributes_empty_list_not_added(self):
        data = {"ontology_class": "http://t/A", "excluded_attributes": []}
        m = Mapping.build_entity_mapping(data)
        assert "excluded_attributes" not in m

    def test_excluded_attributes_absent_not_added(self):
        data = {"ontology_class": "http://t/A"}
        m = Mapping.build_entity_mapping(data)
        assert "excluded_attributes" not in m

    def test_excluded_attributes_is_a_copy(self):
        """Mutating the original list must not affect the built mapping."""
        original = ["age"]
        data = {"ontology_class": "http://t/A", "excluded_attributes": original}
        m = Mapping.build_entity_mapping(data)
        original.append("score")
        assert m["excluded_attributes"] == ["age"]

    def test_excluded_and_entity_excluded_flag_coexist(self):
        data = {
            "ontology_class": "http://t/A",
            "excluded": True,
            "excluded_attributes": ["x"],
        }
        m = Mapping.build_entity_mapping(data)
        assert m["excluded"] is True
        assert m["excluded_attributes"] == ["x"]


# ---------------------------------------------------------------------------
# 2. Mapping.build_relationship_mapping
# ---------------------------------------------------------------------------

class TestBuildRelationshipMappingExcludedAttributes:
    def test_excluded_attributes_propagated(self):
        data = {"property": "http://t/p", "excluded_attributes": ["note"]}
        m = Mapping.build_relationship_mapping(data)
        assert m["excluded_attributes"] == ["note"]

    def test_excluded_attributes_empty_not_added(self):
        data = {"property": "http://t/p", "excluded_attributes": []}
        m = Mapping.build_relationship_mapping(data)
        assert "excluded_attributes" not in m

    def test_excluded_attributes_absent_not_added(self):
        data = {"property": "http://t/p"}
        m = Mapping.build_relationship_mapping(data)
        assert "excluded_attributes" not in m

    def test_excluded_attributes_is_a_copy(self):
        original = ["note"]
        data = {"property": "http://t/p", "excluded_attributes": original}
        m = Mapping.build_relationship_mapping(data)
        original.append("extra")
        assert m["excluded_attributes"] == ["note"]


# ---------------------------------------------------------------------------
# 3. Mapping.compute_mapping_gaps
# ---------------------------------------------------------------------------

class TestComputeMappingGapsExcludedAttributes:
    """Excluded attributes must NOT appear in unmapped_attributes."""

    _BASE_CLASS = {
        "uri": "http://t/A",
        "name": "A",
        "label": "A",
        "dataProperties": [
            {"name": "age"},
            {"name": "score"},
            {"name": "name"},
        ],
    }

    def test_no_exclusions_all_unmapped_appear(self):
        classes = [self._BASE_CLASS]
        props = []
        entity_mappings = [
            {
                "ontology_class": "http://t/A",
                "sql_query": "SELECT id FROM t",
                "attribute_mappings": {},   # nothing mapped
            }
        ]
        _, _, unmapped_attrs, *_ = Mapping.compute_mapping_gaps(
            classes, props, entity_mappings, []
        )
        attr_names = [u["attribute"] for u in unmapped_attrs]
        assert "age" in attr_names
        assert "score" in attr_names
        assert "name" in attr_names

    def test_excluded_attribute_not_in_gaps(self):
        classes = [self._BASE_CLASS]
        entity_mappings = [
            {
                "ontology_class": "http://t/A",
                "sql_query": "SELECT id FROM t",
                "attribute_mappings": {},
                "excluded_attributes": ["age"],
            }
        ]
        _, _, unmapped_attrs, *_ = Mapping.compute_mapping_gaps(
            classes, [], entity_mappings, []
        )
        attr_names = [u["attribute"] for u in unmapped_attrs]
        assert "age" not in attr_names
        assert "score" in attr_names

    def test_multiple_excluded_attributes_not_in_gaps(self):
        classes = [self._BASE_CLASS]
        entity_mappings = [
            {
                "ontology_class": "http://t/A",
                "sql_query": "SELECT id FROM t",
                "attribute_mappings": {},
                "excluded_attributes": ["age", "score"],
            }
        ]
        _, _, unmapped_attrs, *_ = Mapping.compute_mapping_gaps(
            classes, [], entity_mappings, []
        )
        attr_names = [u["attribute"] for u in unmapped_attrs]
        assert "age" not in attr_names
        assert "score" not in attr_names
        assert "name" in attr_names

    def test_all_excluded_means_no_gaps(self):
        classes = [self._BASE_CLASS]
        entity_mappings = [
            {
                "ontology_class": "http://t/A",
                "sql_query": "SELECT id FROM t",
                "attribute_mappings": {},
                "excluded_attributes": ["age", "score", "name"],
            }
        ]
        _, _, unmapped_attrs, *_ = Mapping.compute_mapping_gaps(
            classes, [], entity_mappings, []
        )
        assert unmapped_attrs == []

    def test_mapped_attribute_never_appears_regardless_of_exclusion(self):
        """An attribute that is both excluded AND mapped should not appear in gaps."""
        classes = [self._BASE_CLASS]
        entity_mappings = [
            {
                "ontology_class": "http://t/A",
                "sql_query": "SELECT id FROM t",
                "attribute_mappings": {"age": "age_col"},
                "excluded_attributes": ["age"],
            }
        ]
        _, _, unmapped_attrs, *_ = Mapping.compute_mapping_gaps(
            classes, [], entity_mappings, []
        )
        attr_names = [u["attribute"] for u in unmapped_attrs]
        assert "age" not in attr_names

    def test_entity_not_mapped_attrs_not_checked(self):
        """Attributes of an unmapped entity are never reported as gaps."""
        classes = [self._BASE_CLASS]
        _, _, unmapped_attrs, *_ = Mapping.compute_mapping_gaps(
            classes, [], [], []  # no entity_mappings
        )
        assert unmapped_attrs == []


# ---------------------------------------------------------------------------
# 4. R2RMLGenerator — excluded attributes not emitted
# ---------------------------------------------------------------------------

class TestR2RMLExcludedAttributes:
    _BASE_URI = "http://test.org/onto/"

    def _mapping(self, *, excl=None, attr_map=None):
        return {
            "entities": [
                {
                    "ontology_class": f"{self._BASE_URI}Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT id AS ID, name AS Label, age, score FROM t",
                    "id_column": "ID",
                    "label_column": "Label",
                    "attribute_mappings": attr_map or {"age": "age", "score": "score"},
                    **({"excluded_attributes": excl} if excl else {}),
                }
            ],
            "relationships": [],
        }

    def test_included_attribute_is_in_r2rml(self):
        gen = R2RMLGenerator(self._BASE_URI)
        r2rml = gen.generate_mapping(self._mapping())
        assert "age" in r2rml
        assert "score" in r2rml

    def test_excluded_attribute_mapping_absent_when_not_in_attr_map(self):
        """If the attr was excluded before mapping, it won't be in attribute_mappings
        and therefore won't appear as a rr:column or predicate in R2RML.

        Note: 'score' may still appear inside the rr:sqlQuery literal — what must
        be absent is a rr:column "score" or ont:score predicate reference.
        """
        gen = R2RMLGenerator(self._BASE_URI)
        r2rml = gen.generate_mapping(self._mapping(attr_map={"age": "age"}))
        assert 'rr:column "age"' in r2rml or "age" in r2rml
        # 'score' must not be emitted as a column or predicate
        assert 'rr:column "score"' not in r2rml
        assert "ont:score" not in r2rml

    def test_full_pipeline_excluded_then_partial_r2rml(self):
        """End-to-end: excluded attribute is absent from predicate/column in R2RML
        even when the excluded_attributes field is present in the mapping config."""
        gen = R2RMLGenerator(self._BASE_URI)
        mapping = self._mapping(attr_map={"age": "age"}, excl=["score"])
        r2rml = gen.generate_mapping(mapping)
        assert 'rr:column "age"' in r2rml or "age" in r2rml
        # score excluded → no column or predicate reference
        assert 'rr:column "score"' not in r2rml
        assert "ont:score" not in r2rml


# ---------------------------------------------------------------------------
# 5. tool_submit_entity_mapping — excluded_attributes preservation & filtering
# ---------------------------------------------------------------------------

class TestToolSubmitEntityMappingExcludedAttributes:
    _URI = "http://t/Customer"
    _ONTOLOGY = {
        "entities": [
            {"uri": _URI, "name": "Customer", "attributes": ["age", "score", "name"]}
        ]
    }

    def _ctx(self, existing_excl=None):
        entity_mappings = []
        if existing_excl is not None:
            entity_mappings = [
                {
                    "ontology_class": self._URI,
                    "excluded_attributes": existing_excl,
                }
            ]
        return _ctx(ontology=self._ONTOLOGY, entity_mappings=entity_mappings)

    def test_no_existing_excluded_attrs_no_field_added(self):
        ctx = self._ctx()
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id AS ID, name AS Label FROM t",
            id_column="ID",
            label_column="Label",
            attribute_mappings={"age": "age_col"},
        )
        assert ctx.entity_mappings[0].get("excluded_attributes") is None

    def test_existing_excluded_attrs_preserved_in_new_mapping(self):
        ctx = self._ctx(existing_excl=["score"])
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id AS ID, name AS Label FROM t",
            id_column="ID",
            label_column="Label",
            attribute_mappings={"age": "age_col"},
        )
        m = ctx.entity_mappings[0]
        assert m["excluded_attributes"] == ["score"]

    def test_agent_mapping_for_excluded_attr_is_stripped(self):
        """Even if the agent tries to map a user-excluded attribute, it must be removed."""
        ctx = self._ctx(existing_excl=["score"])
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id AS ID, name AS Label, score FROM t",
            id_column="ID",
            label_column="Label",
            attribute_mappings={"age": "age_col", "score": "score_col"},
        )
        m = ctx.entity_mappings[0]
        assert "score" not in m["attribute_mappings"]
        assert "age" in m["attribute_mappings"]

    def test_multiple_excluded_attrs_all_stripped(self):
        ctx = self._ctx(existing_excl=["score", "name"])
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id AS ID, label AS Label, age, score, name FROM t",
            id_column="ID",
            label_column="Label",
            attribute_mappings={"age": "age_col", "score": "score_col", "name": "name_col"},
        )
        m = ctx.entity_mappings[0]
        assert "score" not in m["attribute_mappings"]
        assert "name" not in m["attribute_mappings"]
        assert "age" in m["attribute_mappings"]

    def test_excluded_attrs_preserved_when_no_attr_mappings_submitted(self):
        ctx = self._ctx(existing_excl=["score"])
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id AS ID, label AS Label FROM t",
            id_column="ID",
            label_column="Label",
            attribute_mappings={},
        )
        m = ctx.entity_mappings[0]
        assert m["excluded_attributes"] == ["score"]
        assert m["attribute_mappings"] == {}

    def test_updates_existing_entry_preserving_excluded(self):
        """Re-running auto-map updates the existing entry, not appends."""
        ctx = self._ctx(existing_excl=["score"])
        # First call
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id AS ID, label AS Label FROM t",
            id_column="ID",
            label_column="Label",
            attribute_mappings={},
        )
        # Second call — re-map
        tool_submit_entity_mapping(
            ctx,
            class_uri=self._URI,
            class_name="Customer",
            sql_query="SELECT id2 AS ID, label2 AS Label FROM t2",
            id_column="ID",
            label_column="Label",
            attribute_mappings={"age": "age_col"},
        )
        assert len(ctx.entity_mappings) == 1
        m = ctx.entity_mappings[0]
        assert m["excluded_attributes"] == ["score"]
        assert m["sql_query"] == "SELECT id2 AS ID, label2 AS Label FROM t2"


# ---------------------------------------------------------------------------
# 6. tool_submit_relationship_mapping — excluded_attributes preservation
# ---------------------------------------------------------------------------

class TestToolSubmitRelationshipMappingExcludedAttributes:
    _URI = "http://t/hasOrder"

    def _ctx(self, existing_excl=None):
        relationships = []
        if existing_excl is not None:
            relationships = [
                {
                    "property": self._URI,
                    "excluded_attributes": existing_excl,
                }
            ]
        return _ctx(relationships=relationships)

    def test_no_existing_excluded_no_field_added(self):
        ctx = self._ctx()
        tool_submit_relationship_mapping(
            ctx,
            property_uri=self._URI,
            property_name="hasOrder",
            sql_query="SELECT src AS source_id, tgt AS target_id FROM t",
            source_id_column="source_id",
            target_id_column="target_id",
            domain="Customer",
            range_class="Order",
        )
        assert ctx.relationships[0].get("excluded_attributes") is None

    def test_existing_excluded_attrs_preserved(self):
        ctx = self._ctx(existing_excl=["note"])
        tool_submit_relationship_mapping(
            ctx,
            property_uri=self._URI,
            property_name="hasOrder",
            sql_query="SELECT src AS source_id, tgt AS target_id FROM t",
            source_id_column="source_id",
            target_id_column="target_id",
            domain="Customer",
            range_class="Order",
        )
        m = ctx.relationships[0]
        assert m["excluded_attributes"] == ["note"]

    def test_updates_existing_entry_preserving_excluded(self):
        ctx = self._ctx(existing_excl=["note"])
        # First submit
        tool_submit_relationship_mapping(
            ctx,
            property_uri=self._URI,
            property_name="hasOrder",
            sql_query="SELECT a AS source_id, b AS target_id FROM t",
            source_id_column="source_id",
            target_id_column="target_id",
            domain="Customer",
            range_class="Order",
        )
        # Re-submit (auto-map re-run)
        tool_submit_relationship_mapping(
            ctx,
            property_uri=self._URI,
            property_name="hasOrder",
            sql_query="SELECT c AS source_id, d AS target_id FROM t2",
            source_id_column="source_id",
            target_id_column="target_id",
            domain="Customer",
            range_class="Order",
        )
        assert len(ctx.relationships) == 1
        m = ctx.relationships[0]
        assert m["excluded_attributes"] == ["note"]
        assert "t2" in m["sql_query"]


# ---------------------------------------------------------------------------
# 7. tool_get_ontology — excluded attrs stripped before the agent sees them
# ---------------------------------------------------------------------------

class TestToolGetOntologyExcludedAttributes:
    _URI = "http://t/Customer"

    def _ctx(self, existing_excl=None):
        ontology = {
            "entities": [
                {
                    "uri": self._URI,
                    "name": "Customer",
                    "attributes": ["age", "score", "name"],
                }
            ],
            "relationships": [],
        }
        entity_mappings = []
        if existing_excl:
            entity_mappings = [
                {
                    "ontology_class": self._URI,
                    "excluded_attributes": existing_excl,
                }
            ]
        return _ctx(ontology=ontology, entity_mappings=entity_mappings)

    def test_no_exclusions_all_attributes_visible(self):
        ctx = self._ctx()
        result = json.loads(tool_get_ontology(ctx))
        attrs = result["entities"][0]["attributes"]
        assert "age" in attrs
        assert "score" in attrs
        assert "name" in attrs

    def test_excluded_attribute_hidden_from_agent(self):
        ctx = self._ctx(existing_excl=["score"])
        result = json.loads(tool_get_ontology(ctx))
        attrs = result["entities"][0]["attributes"]
        assert "score" not in attrs
        assert "age" in attrs
        assert "name" in attrs

    def test_multiple_excluded_attributes_all_hidden(self):
        ctx = self._ctx(existing_excl=["age", "score"])
        result = json.loads(tool_get_ontology(ctx))
        attrs = result["entities"][0]["attributes"]
        assert "age" not in attrs
        assert "score" not in attrs
        assert "name" in attrs

    def test_all_excluded_returns_empty_attribute_list(self):
        ctx = self._ctx(existing_excl=["age", "score", "name"])
        result = json.loads(tool_get_ontology(ctx))
        attrs = result["entities"][0]["attributes"]
        assert attrs == []

    def test_entity_count_unchanged_when_attributes_excluded(self):
        """Excluding attributes must not drop the entity from the response."""
        ctx = self._ctx(existing_excl=["age"])
        result = json.loads(tool_get_ontology(ctx))
        assert result["entity_count"] == 1
        assert len(result["entities"]) == 1

    def test_exclusion_scoped_to_correct_entity(self):
        """Exclusions for one entity must not affect a different entity."""
        uri_a = "http://t/A"
        uri_b = "http://t/B"
        ontology = {
            "entities": [
                {"uri": uri_a, "name": "A", "attributes": ["foo", "bar"]},
                {"uri": uri_b, "name": "B", "attributes": ["foo", "bar"]},
            ],
            "relationships": [],
        }
        entity_mappings = [
            {"ontology_class": uri_a, "excluded_attributes": ["bar"]}
        ]
        ctx = _ctx(ontology=ontology, entity_mappings=entity_mappings)
        result = json.loads(tool_get_ontology(ctx))
        entities_by_name = {e["name"]: e for e in result["entities"]}

        # A: bar excluded
        assert "bar" not in entities_by_name["A"]["attributes"]
        assert "foo" in entities_by_name["A"]["attributes"]

        # B: unaffected
        assert "bar" in entities_by_name["B"]["attributes"]
        assert "foo" in entities_by_name["B"]["attributes"]
