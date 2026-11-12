"""Instance IRI scope: root-class segments and sibling collision detection."""

import pytest

from back.core.w3c.r2rml.InstanceIdentity import InstanceIdentity

pytestmark = pytest.mark.unit

B = "http://test.org/onto#"


def _ontology(scope="hierarchy", extra_classes=()):
    return {
        "identity_scope": scope,
        "classes": [
            {"name": "Contratto", "uri": B + "Contratto"},
            {
                "name": "ContrattoDiFornitura",
                "uri": B + "ContrattoDiFornitura",
                "parent": "Contratto",
            },
            {"name": "Parte", "uri": B + "Parte"},
            {
                "name": "Azienda",
                "uri": B + "Azienda",
                "parent": "Parte",
                "label": "Company",
            },
            {"name": "Sede", "uri": B + "Sede"},
            *extra_classes,
        ],
    }


def _entities(*names):
    return [{"ontology_class": B + n, "ontology_class_label": n} for n in names]


class TestNormalizeScope:
    @pytest.mark.parametrize("raw", [None, "", "bogus", "CLASS", 3])
    def test_unknown_values_fall_back_to_class(self, raw):
        assert InstanceIdentity.normalize_scope(raw) == "class"

    def test_hierarchy_is_case_insensitive(self):
        assert InstanceIdentity.normalize_scope(" Hierarchy ") == "hierarchy"


class TestSegmentMap:
    def test_class_scope_returns_empty_map(self):
        assert InstanceIdentity.segment_map(_ontology("class")) == {}

    def test_missing_scope_returns_empty_map(self):
        ont = _ontology()
        ont.pop("identity_scope")
        assert InstanceIdentity.segment_map(ont) == {}

    def test_subclasses_map_to_their_root(self):
        seg = InstanceIdentity.segment_map(_ontology())
        assert seg["ContrattoDiFornitura"] == "Contratto"
        assert seg["Azienda"] == "Parte"

    def test_roots_and_standalone_classes_map_to_themselves(self):
        seg = InstanceIdentity.segment_map(_ontology())
        assert seg["Contratto"] == "Contratto"
        assert seg["Sede"] == "Sede"

    def test_label_is_a_lower_priority_key(self):
        seg = InstanceIdentity.segment_map(_ontology())
        assert seg["Company"] == "Parte"

    def test_grandchild_maps_to_top_root(self):
        extra = ({"name": "Spa", "uri": B + "Spa", "parent": "Azienda"},)
        assert (
            InstanceIdentity.segment_map(_ontology(extra_classes=extra))["Spa"]
            == "Parte"
        )

    def test_parent_cycle_terminates(self):
        extra = (
            {"name": "CycleA", "uri": B + "CycleA", "parent": "CycleB"},
            {"name": "CycleB", "uri": B + "CycleB", "parent": "CycleA"},
        )
        seg = InstanceIdentity.segment_map(_ontology(extra_classes=extra))
        assert seg["CycleA"] in {"CycleA", "CycleB"}

    def test_missing_parent_keeps_the_class_as_root(self):
        extra = ({"name": "Orphan", "uri": B + "Orphan", "parent": "Missing"},)
        assert (
            InstanceIdentity.segment_map(_ontology(extra_classes=extra))["Orphan"]
            == "Orphan"
        )


class TestSiblingCollisions:
    def test_parent_and_child_with_same_id_is_not_a_collision(self):
        ents = _entities(
            "Contratto", "ContrattoDiFornitura", "Parte", "Azienda", "Sede"
        )
        assert InstanceIdentity.sibling_collisions(_ontology(), ents) == {}

    def test_unrelated_siblings_under_one_root_collide(self):
        extra = (
            {
                "name": "ContrattoDiServizi",
                "uri": B + "ContrattoDiServizi",
                "parent": "Contratto",
            },
        )
        ents = _entities("Contratto", "ContrattoDiFornitura", "ContrattoDiServizi")
        result = InstanceIdentity.sibling_collisions(
            _ontology(extra_classes=extra), ents
        )
        assert result == {
            B + "ContrattoDiFornitura": ["ContrattoDiServizi"],
            B + "ContrattoDiServizi": ["ContrattoDiFornitura"],
        }

    def test_class_scope_never_reports_collisions(self):
        extra = (
            {
                "name": "ContrattoDiServizi",
                "uri": B + "ContrattoDiServizi",
                "parent": "Contratto",
            },
        )
        ents = _entities("ContrattoDiFornitura", "ContrattoDiServizi")
        assert (
            InstanceIdentity.sibling_collisions(_ontology("class", extra), ents) == {}
        )

    def test_excluded_mappings_are_ignored(self):
        extra = (
            {
                "name": "ContrattoDiServizi",
                "uri": B + "ContrattoDiServizi",
                "parent": "Contratto",
            },
        )
        ents = _entities("ContrattoDiFornitura", "ContrattoDiServizi")
        ents[1]["excluded"] = True
        assert (
            InstanceIdentity.sibling_collisions(_ontology(extra_classes=extra), ents)
            == {}
        )


class TestFlagCollisions:
    def test_appends_warning_and_downgrades_ok_status(self):
        extra = (
            {
                "name": "ContrattoDiServizi",
                "uri": B + "ContrattoDiServizi",
                "parent": "Contratto",
            },
        )
        ents = _entities("ContrattoDiFornitura", "ContrattoDiServizi")
        results = [{"status": "ok", "checks": []}, {"status": "error", "checks": []}]
        InstanceIdentity.flag_collisions(
            _ontology(extra_classes=extra), ents, results
        )
        assert results[0]["status"] == "warning"
        assert results[0]["checks"][0]["check"] == "identity_scope"
        assert "ContrattoDiServizi" in results[0]["checks"][0]["detail"]
        assert results[1]["status"] == "error"


class TestApplyScope:
    def test_absent_key_is_a_no_op(self):
        ont = {}
        assert InstanceIdentity.apply_scope(ont, {"name": "X"}) is False
        assert "identity_scope" not in ont

    def test_switch_to_hierarchy_reports_change(self):
        ont = {}
        assert InstanceIdentity.apply_scope(ont, {"identity_scope": "hierarchy"}) is True
        assert ont["identity_scope"] == "hierarchy"

    def test_default_class_on_legacy_ontology_is_not_a_change(self):
        ont = {}
        assert InstanceIdentity.apply_scope(ont, {"identity_scope": "class"}) is False
        assert "identity_scope" not in ont
