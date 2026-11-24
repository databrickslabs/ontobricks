"""Unmapping must not silently un-hide excluded entities / relationships.

An *excluded* item is shown grey in the Mapping designer and is skipped by
validation and R2RML generation. An unmapped, non-excluded item is shown red.
Wiping the whole mapping (``Unmap all``) used to delete the exclusion stubs as
well, so every hidden entity and relationship came back red.
"""

import inspect
from unittest.mock import MagicMock

import pytest

from back.objects.mapping import Mapping

pytestmark = pytest.mark.unit

ENT = "http://t/Ent"
REL = "http://t/rel"


def _domain(entities, relationships):
    domain = MagicMock()
    domain.assignment = {
        "entities": list(entities),
        "relationships": list(relationships),
        "r2rml_output": "<old r2rml>",
    }
    return domain


def _entities():
    return [
        {"ontology_class": f"{ENT}A", "ontology_class_label": "A", "sql_query": "SELECT 1"},
        {
            "ontology_class": f"{ENT}B",
            "ontology_class_label": "B",
            "sql_query": "SELECT 2",
            "id_column": "id",
            "excluded": True,
        },
        {
            "ontology_class": f"{ENT}C",
            "ontology_class_label": "C",
            "sql_query": "",
            "excluded_attributes": ["email"],
        },
        {"ontology_class": f"{ENT}D", "excluded": True},
        {"ontology_class_label": "no key at all", "excluded": True},
    ]


def _relationships():
    return [
        {"property": f"{REL}a", "property_label": "a", "sql_query": "SELECT 1"},
        {
            "property": f"{REL}b",
            "property_label": "b",
            "sql_query": "SELECT 2",
            "source_id_column": "s",
            "excluded": True,
        },
        {"property": f"{REL}c", "sql_query": "", "excluded_attributes": ["since"]},
        {"property": f"{REL}d", "excluded": True},
    ]


class TestResetKeepsExclusions:
    def test_unmapped_items_are_dropped_exclusions_survive(self):
        domain = _domain(_entities(), _relationships())

        Mapping(domain).reset_mapping(keep_exclusions=True)

        kept_entities = {m["ontology_class"]: m for m in domain.assignment["entities"]}
        assert set(kept_entities) == {f"{ENT}B", f"{ENT}C", f"{ENT}D"}
        kept_rels = {m["property"]: m for m in domain.assignment["relationships"]}
        assert set(kept_rels) == {f"{REL}b", f"{REL}c", f"{REL}d"}

    def test_stub_keeps_only_exclusions_and_the_label(self):
        domain = _domain(_entities(), _relationships())

        Mapping(domain).reset_mapping(keep_exclusions=True)

        kept = {m["ontology_class"]: m for m in domain.assignment["entities"]}
        # Excluded entity that was mapped: the SQL/ID mapping is gone, the
        # exclusion and label stay.
        assert kept[f"{ENT}B"] == {
            "ontology_class": f"{ENT}B",
            "ontology_class_label": "B",
            "excluded": True,
        }
        # Attribute-only exclusions survive without flagging the entity itself.
        assert kept[f"{ENT}C"] == {
            "ontology_class": f"{ENT}C",
            "ontology_class_label": "C",
            "excluded_attributes": ["email"],
        }
        assert kept[f"{ENT}D"] == {"ontology_class": f"{ENT}D", "excluded": True}

        rels = {m["property"]: m for m in domain.assignment["relationships"]}
        assert rels[f"{REL}b"] == {
            "property": f"{REL}b",
            "property_label": "b",
            "excluded": True,
        }
        assert rels[f"{REL}c"] == {
            "property": f"{REL}c",
            "excluded_attributes": ["since"],
        }

    def test_generated_output_is_still_cleared_and_change_recorded(self):
        domain = _domain(_entities(), _relationships())

        Mapping(domain).reset_mapping(keep_exclusions=True)

        assert domain.assignment["r2rml_output"] == ""
        domain.clear_generated_content.assert_called_once()
        domain.record_change.assert_called_once()
        domain.save.assert_called_once()

    def test_stub_does_not_alias_the_original_attribute_list(self):
        entities = _entities()
        domain = _domain(entities, [])

        Mapping(domain).reset_mapping(keep_exclusions=True)

        stub = next(
            m for m in domain.assignment["entities"] if m["ontology_class"] == f"{ENT}C"
        )
        stub["excluded_attributes"].append("phone")
        assert entities[2]["excluded_attributes"] == ["email"]


class TestResetDefaultIsUnchanged:
    def test_default_reset_still_wipes_everything(self):
        domain = _domain(_entities(), _relationships())

        Mapping(domain).reset_mapping()

        assert domain.assignment["entities"] == []
        assert domain.assignment["relationships"] == []
        assert domain.assignment["r2rml_output"] == ""


class TestEndpointsExposeTheFlag:
    @pytest.mark.parametrize("name", ["reset_mapping_endpoint", "clear_mapping"])
    def test_endpoint_accepts_keep_exclusions_defaulting_to_false(self, name):
        from api.routers.internal import mapping as router

        param = inspect.signature(getattr(router, name)).parameters["keep_exclusions"]
        assert param.default is False
