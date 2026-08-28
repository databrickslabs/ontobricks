"""Tests for :class:`back.objects.ontology.Ontology` helpers used by ontology routes."""

import copy

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.objects.ontology import Ontology


class TestEnsureUris:
    def test_adds_uri_to_class(self):
        config = {
            "base_uri": "http://test.org/ontology#",
            "classes": [{"name": "Customer"}],
            "properties": [],
        }
        result = Ontology.ensure_uris(config)
        assert result["classes"][0]["uri"] == "http://test.org/ontology#Customer"

    def test_adds_local_name(self):
        config = {
            "base_uri": "http://test.org/ontology#",
            "classes": [{"name": "Customer"}],
            "properties": [],
        }
        result = Ontology.ensure_uris(config)
        assert result["classes"][0]["localName"] == "Customer"

    def test_preserves_existing_uri(self):
        config = {
            "base_uri": "http://test.org/ontology#",
            "classes": [{"name": "Customer", "uri": "http://other.org/Customer"}],
            "properties": [],
        }
        result = Ontology.ensure_uris(config)
        assert result["classes"][0]["uri"] == "http://other.org/Customer"

    def test_adds_separator_if_missing(self):
        config = {
            "base_uri": "http://test.org/ontology",
            "classes": [{"name": "Foo"}],
            "properties": [],
        }
        result = Ontology.ensure_uris(config)
        assert "#" in result["classes"][0]["uri"]

    def test_property_uris(self):
        config = {
            "base_uri": "http://test.org/ontology#",
            "classes": [],
            "properties": [{"name": "hasOrder"}],
        }
        result = Ontology.ensure_uris(config)
        assert result["properties"][0]["uri"] == "http://test.org/ontology#hasOrder"


class TestGetOntologyStats:
    def test_empty_config(self):
        stats = Ontology.get_ontology_stats({})
        assert stats == {
            "classes": 0,
            "properties": 0,
            "constraints": 0,
            "swrl_rules": 0,
            "axioms": 0,
            "expressions": 0,
        }

    def test_with_data(self):
        config = {
            "classes": [{"name": "A"}, {"name": "B"}],
            "properties": [{"name": "p"}],
            "constraints": [{"type": "functional"}],
            "swrl_rules": [],
            "axioms": [{"type": "equivalentClass"}],
        }
        stats = Ontology.get_ontology_stats(config)
        assert stats["classes"] == 2
        assert stats["properties"] == 1
        assert stats["constraints"] == 1
        assert stats["axioms"] == 1


class TestBuildClassFromData:
    def test_new_class(self):
        data = {"name": "Customer", "label": "Customer", "description": "Test"}
        cls = Ontology.build_class_from_data(data)
        assert cls["name"] == "Customer"
        assert cls["label"] == "Customer"
        assert cls["description"] == "Test"
        assert cls["emoji"] == "📦"

    def test_merge_with_existing(self):
        existing = {"name": "Cust", "emoji": "👤", "uri": "http://old"}
        data = {"name": "Customer"}
        cls = Ontology.build_class_from_data(data, existing)
        assert cls["name"] == "Customer"
        assert cls["emoji"] == "👤"

    def test_dataset_field(self):
        dataset = {
            "catalog": "main",
            "schema": "sales",
            "asset": "orders",
            "type": "TABLE",
            "fullName": "main.sales.orders",
        }
        cls = Ontology.build_class_from_data({"name": "Customer", "dataset": dataset})
        assert cls["dataset"] == dataset

    def test_dataset_defaults_none(self):
        cls = Ontology.build_class_from_data({"name": "Customer"})
        assert cls["dataset"] is None

    def test_dataset_preserved_from_existing(self):
        dataset = {"catalog": "c", "schema": "s", "asset": "t", "type": "VIEW"}
        existing = {"name": "Customer", "dataset": dataset}
        cls = Ontology.build_class_from_data({"name": "Customer"}, existing)
        assert cls["dataset"] == dataset

    def test_actions_field(self):
        actions = [
            {
                "catalog": "main",
                "schema": "ops",
                "function": "recompute_risk",
                "fullName": "main.ops.recompute_risk",
                "description": "Recompute the risk score",
                "returns_table": False,
            }
        ]
        cls = Ontology.build_class_from_data({"name": "Customer", "actions": actions})
        assert cls["actions"] == actions

    def test_actions_defaults_empty(self):
        cls = Ontology.build_class_from_data({"name": "Customer"})
        assert cls["actions"] == []

    def test_actions_preserved_from_existing(self):
        actions = [{"fullName": "main.ops.f", "description": "d"}]
        existing = {"name": "Customer", "actions": actions}
        cls = Ontology.build_class_from_data({"name": "Customer"}, existing)
        assert cls["actions"] == actions

    def test_virtual_attributes_field(self):
        virtual_attributes = [
            {
                "catalog": "main",
                "schema": "kg",
                "function": "customer_risk",
                "fullName": "main.kg.customer_risk",
                "description": "Live credit risk",
                "returns_table": True,
                "attributes": [
                    {"name": "risk_score", "column": "risk_score", "dataType": "DOUBLE"}
                ],
            }
        ]
        cls = Ontology.build_class_from_data(
            {"name": "Customer", "virtualAttributes": virtual_attributes}
        )
        assert cls["virtualAttributes"] == virtual_attributes

    def test_virtual_attributes_stay_out_of_data_properties(self):
        """They are computed on demand, so the mapping layer, R2RML and the
        build pipeline must never see them as attributes."""
        cls = Ontology.build_class_from_data(
            {
                "name": "Customer",
                "virtualAttributes": [
                    {
                        "fullName": "main.kg.customer_risk",
                        "attributes": [{"name": "risk_score", "column": "risk_score"}],
                    }
                ],
            }
        )
        assert cls["dataProperties"] == []

    def test_virtual_attributes_defaults_empty(self):
        cls = Ontology.build_class_from_data({"name": "Customer"})
        assert cls["virtualAttributes"] == []

    def test_virtual_attributes_preserved_from_existing(self):
        """A save from a panel that does not know about them (or an older
        client) must not wipe the declarations."""
        virtual_attributes = [
            {"fullName": "main.kg.f", "attributes": [{"name": "x", "column": "x"}]}
        ]
        existing = {"name": "Customer", "virtualAttributes": virtual_attributes}
        cls = Ontology.build_class_from_data({"name": "Customer"}, existing)
        assert cls["virtualAttributes"] == virtual_attributes


class TestBuildPropertyFromData:
    def test_new_property(self):
        data = {"name": "hasOrder", "domain": "Customer", "range": "Order"}
        prop = Ontology.build_property_from_data(data)
        assert prop["name"] == "hasOrder"
        assert prop["domain"] == "Customer"
        assert prop["direction"] == "forward"

    def test_merge_existing(self):
        existing = {"name": "old", "direction": "reverse"}
        data = {"name": "hasOrder"}
        prop = Ontology.build_property_from_data(data, existing)
        assert prop["name"] == "hasOrder"
        assert prop["direction"] == "reverse"


class TestValidateConstraint:
    def test_missing_type(self):
        assert Ontology.validate_constraint({}) == "Constraint type is required"

    def test_cardinality_missing_property(self):
        err = Ontology.validate_constraint({"type": "minCardinality"})
        assert "property" in err.lower() or "Relationship" in err

    def test_cardinality_missing_value(self):
        err = Ontology.validate_constraint({"type": "minCardinality", "property": "p"})
        assert "value" in err.lower()

    def test_valid_cardinality(self):
        assert (
            Ontology.validate_constraint(
                {"type": "minCardinality", "property": "p", "cardinalityValue": 1}
            )
            is None
        )

    def test_value_check_missing_class(self):
        err = Ontology.validate_constraint({"type": "valueCheck"})
        assert err is not None

    def test_global_rule_no_class_needed(self):
        assert Ontology.validate_constraint({"type": "globalRule"}) is None

    def test_valid_functional(self):
        assert (
            Ontology.validate_constraint({"type": "functional", "property": "p"})
            is None
        )

    def test_functional_missing_property(self):
        err = Ontology.validate_constraint({"type": "functional"})
        assert err is not None


class TestGenerateOwl:
    def test_generates_valid_turtle(self):
        data = {
            "base_uri": "http://test.org/ontology#",
            "name": "TestOntology",
            "classes": [{"name": "Customer", "label": "Customer"}],
            "properties": [],
        }
        owl = Ontology.generate_owl(data)
        assert "@prefix" in owl
        assert "Customer" in owl


class TestParseOwl:
    def test_parse_basic(self):
        owl = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test.org/ontology#> .

<http://test.org/ontology> a owl:Ontology ; rdfs:label "Test" .
:Foo a owl:Class ; rdfs:label "Foo" .
"""
        info, classes, props, constraints, swrl, axioms, expressions, groups = (
            Ontology.parse_owl(owl)
        )
        assert info["label"] == "Test"
        assert len(classes) == 1
        assert classes[0]["name"] == "Foo"

    def test_parse_without_advanced(self):
        owl = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test.org/ontology#> .

<http://test.org/ontology> a owl:Ontology .
:Bar a owl:Class ; rdfs:label "Bar" .
"""
        result = Ontology.parse_owl(owl, extract_advanced=False)
        assert len(result) == 3


class TestNormalizePropertyDomainRange:
    def test_fixes_domain_range_case(self):
        config = {
            "classes": [{"name": "Customer"}, {"name": "Order"}],
            "properties": [
                {"name": "hasX", "domain": "customer", "range": "ORDER"},
            ],
        }
        assert Ontology.normalize_property_domain_range(config) is True
        assert config["properties"][0]["domain"] == "Customer"
        assert config["properties"][0]["range"] == "Order"

    def test_no_change_when_already_canonical(self):
        config = {
            "classes": [{"name": "Customer"}],
            "properties": [{"domain": "Customer", "range": ""}],
        }
        assert Ontology.normalize_property_domain_range(config) is False

    def test_on_replace_callback(self):
        seen = []

        def on_replace(prop, field, old, new):
            seen.append((prop.get("name"), field, old, new))

        config = {
            "classes": [{"name": "A"}],
            "properties": [{"name": "p", "domain": "a"}],
        }
        Ontology.normalize_property_domain_range(config, on_replace=on_replace)
        assert seen == [("p", "domain", "a", "A")]


class TestPruneMappingsToOntologyUris:
    def test_removes_stale_entity_and_rel_mappings(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["assignment"]["entities"] = copy.deepcopy(
            sample_mapping_config["entities"]
        )
        ps._data["assignment"]["relationships"] = copy.deepcopy(
            sample_mapping_config["relationships"]
        )
        counts = Ontology(ps).prune_mappings_to_ontology_uris(
            {"http://test.org/ontology#Customer"},
            set(),
        )
        assert counts["entity_mappings_removed"] == 1
        assert counts["relationship_mappings_removed"] == 1
        assert len(ps.get_entity_mappings()) == 1
        assert len(ps.get_relationship_mappings()) == 0


class TestSaveOntologyConfigFromEditor:
    def test_save_and_prune_orphans(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["assignment"]["entities"] = copy.deepcopy(
            sample_mapping_config["entities"]
        )
        ps._data["assignment"]["relationships"] = copy.deepcopy(
            sample_mapping_config["relationships"]
        )
        cfg = copy.deepcopy(sample_ontology_config)
        cfg["classes"] = cfg["classes"][:1]
        out = Ontology(ps).save_ontology_config_from_editor({"config": cfg})
        assert out["success"]
        assert out["mappings_cleaned"]["entity_mappings_removed"] == 1
        assert len(ps.get_classes()) == 1

    def test_save_syncs_stale_attribute_out_of_design_views(
        self, domain_session, sample_ontology_config
    ):
        """Removing an attribute must also drop it from design-view copies.

        Each design view embeds a full attribute list per entity. If that copy
        is not reconciled on save, the removed attribute flows back into the
        ontology the next time the designer canvas is serialised — the bug where
        a deleted attribute reappears after leaving and returning to the
        Designer. See Ontology._sync_design_layout_with_ontology.
        """
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        # A design view whose Customer copy still carries a stale "customerid"
        # (with canvas metadata) plus the two real attributes.
        ps._data["design_layout"]["views"]["default"] = {
            "entities": [
                {
                    "id": "entity_customer",
                    "name": "Customer",
                    "x": 10,
                    "y": 20,
                    "properties": [
                        {
                            "id": "p1",
                            "name": "firstName",
                            "type": "string",
                            "isRequired": True,
                            "isPrimaryKey": False,
                        },
                        {"id": "p2", "name": "lastName", "type": "string"},
                        {"id": "stale", "name": "customerid", "type": "string"},
                    ],
                }
            ]
        }

        # Save the ontology with "customerid" removed from Customer.
        cfg = copy.deepcopy(sample_ontology_config)
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})

        view_props = ps._data["design_layout"]["views"]["default"]["entities"][0][
            "properties"
        ]
        names = [p["name"] for p in view_props]
        assert "customerid" not in names
        assert names == ["firstName", "lastName"]
        # Survivor canvas metadata is preserved (not blindly rebuilt).
        assert view_props[0]["isRequired"] is True
        assert view_props[0]["id"] == "p1"

    def test_save_adds_new_attribute_to_design_views(
        self, domain_session, sample_ontology_config
    ):
        """A newly added ontology attribute is appended to design-view copies."""
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["design_layout"]["views"]["default"] = {
            "entities": [
                {
                    "id": "entity_customer",
                    "name": "Customer",
                    "properties": [{"id": "p1", "name": "firstName"}],
                }
            ]
        }

        cfg = copy.deepcopy(sample_ontology_config)
        # Customer gains a new attribute the view has never seen.
        cfg["classes"][0]["dataProperties"].append(
            {"name": "email", "localName": "email"}
        )
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})

        names = [
            p["name"]
            for p in ps._data["design_layout"]["views"]["default"]["entities"][0][
                "properties"
            ]
        ]
        assert names == ["firstName", "lastName", "email"]

    def test_save_drops_orphaned_datatype_property(
        self, domain_session, sample_ontology_config
    ):
        """A datatype attribute removed from a class must also leave ``properties``.

        Each datatype attribute is mirrored as a ``DatatypeProperty`` (with a
        ``domain``) in ``config['properties']``. The editor only removes it from
        the class, so without pruning the mirror survives and
        ``finalize_class_attributes`` resurrects the attribute on the next load —
        the "deleted attribute keeps coming back" bug. See
        Ontology.prune_orphaned_datatype_properties.
        """
        ps = domain_session
        cfg = copy.deepcopy(sample_ontology_config)
        # Customer carries "customerid" both on the class and as its datatype
        # property mirror.
        cfg["classes"][0]["dataProperties"].append(
            {"name": "customerid", "localName": "customerid", "label": "Customer ID"}
        )
        cfg["properties"].append(
            {
                "uri": "http://test.org/ontology#customerid",
                "name": "customerid",
                "localName": "customerid",
                "type": "DatatypeProperty",
                "domain": "Customer",
                "range": "string",
            }
        )
        ps._data["ontology"].update(copy.deepcopy(cfg))

        # Editor removes "customerid" from the class only (mirror still present).
        edited = copy.deepcopy(cfg)
        edited["classes"][0]["dataProperties"] = [
            p
            for p in edited["classes"][0]["dataProperties"]
            if p["name"] != "customerid"
        ]
        Ontology(ps).save_ontology_config_from_editor({"config": edited})

        prop_names = [p.get("name") for p in ps.get_properties()]
        assert "customerid" not in prop_names
        # The object property is untouched.
        assert "hasOrder" in prop_names

        # Re-finalizing (what happens on the next session load) must NOT
        # resurrect the attribute.
        Ontology.finalize_class_attributes(ps._data["ontology"])
        customer = next(c for c in ps.get_classes() if c["name"] == "Customer")
        assert "customerid" not in [p["name"] for p in customer["dataProperties"]]

    def test_prune_orphaned_datatype_properties_keeps_valid_and_unknown(self):
        """Prune only datatype mirrors whose attribute is gone from a known class."""
        config = {
            "classes": [
                {"name": "Customer", "dataProperties": [{"name": "email"}]},
            ],
            "properties": [
                # Removed attribute on a known class -> pruned.
                {"name": "customerid", "type": "DatatypeProperty", "domain": "Customer"},
                # Still present on the class -> kept.
                {"name": "email", "type": "DatatypeProperty", "domain": "Customer"},
                # Object property -> never touched.
                {"name": "hasOrder", "type": "ObjectProperty", "domain": "Customer"},
                # Domain points to an unknown class -> left alone.
                {"name": "ghost", "type": "DatatypeProperty", "domain": "Missing"},
                # No domain -> left alone.
                {"name": "loose", "type": "DatatypeProperty", "domain": ""},
            ],
        }
        removed = Ontology.prune_orphaned_datatype_properties(config)
        assert removed == 1
        names = [p["name"] for p in config["properties"]]
        assert names == ["email", "hasOrder", "ghost", "loose"]

    def _seed_view_with_full_graph(self, ps, sample_ontology_config):
        """Populate ontology + a design view mirroring it (Customer, Order, hasOrder)."""
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["design_layout"]["views"]["default"] = {
            "entities": [
                {"id": "e_cust", "name": "Customer", "properties": []},
                {"id": "e_order", "name": "Order", "properties": []},
            ],
            "relationships": [
                {
                    "id": "r1",
                    "name": "hasOrder",
                    "sourceEntityId": "e_cust",
                    "targetEntityId": "e_order",
                    "direction": "forward",
                }
            ],
            "inheritances": [
                {
                    "id": "i1",
                    "direction": "forward",
                    "sourceEntityId": "e_cust",
                    "targetEntityId": "e_order",
                }
            ],
            # Real sessions store name-lists AND {source,target} dicts here; the
            # reconciliation must handle both without raising (regression:
            # TypeError unhashable dict crashed the whole /ontology/save).
            "visibility": {
                "hiddenEntities": ["Order"],
                "collapsedEntities": [],
                "hiddenInheritances": [{"source": "Customer", "target": "Order"}],
                "hiddenRelationships": [{"source": "Customer", "target": "Order"}],
            },
        }

    def test_save_prunes_deleted_relationship_from_views(
        self, domain_session, sample_ontology_config
    ):
        """A relationship removed from the ontology is dropped from design views."""
        ps = domain_session
        self._seed_view_with_full_graph(ps, sample_ontology_config)
        cfg = copy.deepcopy(sample_ontology_config)
        cfg["properties"] = []  # hasOrder removed
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})
        view = ps._data["design_layout"]["views"]["default"]
        assert view["relationships"] == []

    def test_save_prunes_deleted_entity_and_dependents_from_views(
        self, domain_session, sample_ontology_config
    ):
        """Deleting a class removes its view entity plus rels/inheritances touching it."""
        ps = domain_session
        self._seed_view_with_full_graph(ps, sample_ontology_config)
        cfg = copy.deepcopy(sample_ontology_config)
        cfg["classes"] = [c for c in cfg["classes"] if c["name"] != "Order"]
        cfg["properties"] = []  # hasOrder depends on Order
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})
        view = ps._data["design_layout"]["views"]["default"]
        assert [e["name"] for e in view["entities"]] == ["Customer"]
        assert view["relationships"] == []  # endpoint gone
        assert view["inheritances"] == []  # endpoint gone
        assert view["visibility"]["hiddenEntities"] == []  # Order pruned

    def test_save_prunes_stale_inheritance_from_views(
        self, domain_session, sample_ontology_config
    ):
        """An inheritance not backed by an ontology parent link is dropped."""
        ps = domain_session
        self._seed_view_with_full_graph(ps, sample_ontology_config)
        # No class declares a parent, so the seeded inheritance is stale.
        cfg = copy.deepcopy(sample_ontology_config)
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})
        view = ps._data["design_layout"]["views"]["default"]
        assert view["inheritances"] == []

    def test_save_keeps_valid_relationship_and_inheritance(
        self, domain_session, sample_ontology_config
    ):
        """Content still present in the ontology survives reconciliation."""
        ps = domain_session
        self._seed_view_with_full_graph(ps, sample_ontology_config)
        cfg = copy.deepcopy(sample_ontology_config)
        # Make Order a child of Customer so the seeded inheritance is valid.
        for c in cfg["classes"]:
            if c["name"] == "Order":
                c["parent"] = "Customer"
        Ontology(ps).save_ontology_config_from_editor({"config": cfg})
        view = ps._data["design_layout"]["views"]["default"]
        assert [r["name"] for r in view["relationships"]] == ["hasOrder"]
        assert len(view["inheritances"]) == 1


class TestDeleteClassAndPropertyByUri:
    def test_delete_class_cascades_entity_mapping(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["assignment"]["entities"] = copy.deepcopy(
            sample_mapping_config["entities"]
        )
        r = Ontology(ps).delete_class_by_uri("http://test.org/ontology#Customer")
        assert r["success"]
        assert r["mapping_removed"] is True
        assert len(ps.get_classes()) == 1

    def test_delete_class_missing_uri(self, domain_session):
        with pytest.raises(ValidationError):
            Ontology(domain_session).delete_class_by_uri(None)

    def test_delete_property_cascades_rel_mapping(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["assignment"]["relationships"] = copy.deepcopy(
            sample_mapping_config["relationships"]
        )
        r = Ontology(ps).delete_property_by_uri("http://test.org/ontology#hasOrder")
        assert r["success"]
        assert r["mapping_removed"] is True
        assert all(
            p.get("uri") != "http://test.org/ontology#hasOrder"
            for p in ps.get_properties()
        )

    def test_delete_property_missing_uri(self, domain_session):
        with pytest.raises(ValidationError):
            Ontology(domain_session).delete_property_by_uri("")


class TestAddUpdateClassProperty:
    def test_add_class(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        r = Ontology(ps).add_class(
            {"name": "Product", "uri": "http://test.org/ontology#Product"}
        )
        assert r["success"]
        assert r["class"]["name"] == "Product"
        assert len(ps.get_classes()) == 3

    def test_add_class_duplicate(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        with pytest.raises(ValidationError):
            Ontology(ps).add_class(
                {"name": "Customer", "uri": "http://test.org/ontology#Customer"}
            )

    def test_update_class(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        r = Ontology(ps).update_class(
            {"uri": "http://test.org/ontology#Customer", "name": "Client"}
        )
        assert r["success"]
        assert r["class"]["name"] == "Client"

    def test_update_class_not_found(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        with pytest.raises(NotFoundError):
            Ontology(ps).update_class({"uri": "http://test.org/ontology#Missing"})

    def test_add_property(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        r = Ontology(ps).add_property(
            {"name": "hasPart", "uri": "http://test.org/ontology#hasPart"}
        )
        assert r["success"]
        assert len(ps.get_properties()) == 2

    def test_add_property_duplicate(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        with pytest.raises(ValidationError):
            Ontology(ps).add_property(
                {"name": "hasOrder", "uri": "http://test.org/ontology#hasOrder"}
            )

    def test_update_property(self, domain_session, sample_ontology_config):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        r = Ontology(ps).update_property(
            {"uri": "http://test.org/ontology#hasOrder", "name": "placeOrder"}
        )
        assert r["success"]
        assert r["property"]["name"] == "placeOrder"


class TestIngestOwl:
    def test_ingest_owl_import(self, domain_session, sample_owl_content):
        r = Ontology(domain_session).ingest_owl(sample_owl_content, outcome="import")
        assert r["success"]
        assert r["stats"]["classes"] >= 1

    def test_ingest_owl_parse(self, domain_session, sample_owl_content):
        r = Ontology(domain_session).ingest_owl(sample_owl_content, outcome="parse")
        assert r["success"]
        assert "ontology" in r

    def test_ingest_owl_load_file(self, domain_session, sample_owl_content):
        r = Ontology(domain_session).ingest_owl(
            sample_owl_content,
            name_fallback_to_domain=False,
            outcome="load_file",
        )
        assert r["success"]


class TestApplyParsedRdfs:
    def test_basic(self, domain_session):
        rdfs = """@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix : <http://example.org/> .

<http://example.org/> a owl:Ontology ; rdfs:label "MyVocab" .
:Foo a owl:Class ; rdfs:label "Foo" .
"""
        r = Ontology(domain_session).apply_parsed_rdfs_to_domain(rdfs)
        assert r["success"]
        assert r["stats"]["classes"] >= 1


class TestRenameRelationshipReferences:
    def test_renames_across_mappings_and_constraints(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["ontology"]["constraints"] = [
            {"property": "hasOrder", "type": "functional"}
        ]
        ps._data["ontology"]["axioms"] = [{"property": "hasOrder", "type": "reflexive"}]
        ps._data["assignment"]["relationships"] = copy.deepcopy(
            sample_mapping_config["relationships"]
        )
        updates = Ontology(ps).rename_relationship_references("hasOrder", "placesOrder")
        assert updates["mappings_updated"] == 1
        assert updates["constraints_updated"] == 1
        assert updates["axioms_updated"] == 1


class TestApplyAgentOntologyChanges:
    def test_with_prune(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["assignment"]["entities"] = copy.deepcopy(
            sample_mapping_config["entities"]
        )
        config = Ontology(ps).apply_agent_ontology_changes(
            [{"name": "Customer", "uri": "http://test.org/ontology#Customer"}],
            [],
            prune_orphan_mappings=True,
        )
        assert config["classes"][0]["name"] == "Customer"
        assert len(ps.get_entity_mappings()) == 1

    def test_without_prune(
        self, domain_session, sample_ontology_config, sample_mapping_config
    ):
        ps = domain_session
        ps._data["ontology"].update(copy.deepcopy(sample_ontology_config))
        ps._data["assignment"]["entities"] = copy.deepcopy(
            sample_mapping_config["entities"]
        )
        Ontology(ps).apply_agent_ontology_changes(
            [{"name": "NewOnly", "uri": "http://test.org/ontology#NewOnly"}],
            [],
            prune_orphan_mappings=False,
        )
        assert len(ps.get_entity_mappings()) == 2


class _StubDomain:
    """Minimal domain exposing only what agent_ontology_context needs."""

    def __init__(self, classes, properties):
        self._classes = classes
        self._properties = properties

    def get_classes(self):
        return self._classes

    def get_properties(self):
        return self._properties


class TestAgentOntologyContext:
    classes = [
        {"name": "Customer", "uri": "http://t/Customer", "dataProperties": [{"name": "age"}]},
        {"name": "Order", "uri": "http://t/Order", "dataProperties": []},
        {"name": "Lonely", "uri": "http://t/Lonely", "dataProperties": [{"name": "x"}]},
        {"name": "Child", "uri": "http://t/Child", "parent": "Customer", "dataProperties": []},
    ]
    properties = [
        {"name": "placesOrder", "type": "ObjectProperty", "domain": "Customer", "range": "Order"},
    ]

    def _onto(self):
        onto = Ontology.__new__(Ontology)
        onto._domain = _StubDomain(self.classes, self.properties)
        return onto

    def test_default_keeps_all_entities(self):
        ctx = self._onto().agent_ontology_context()
        assert {e["name"] for e in ctx["entities"]} == {
            "Customer",
            "Order",
            "Lonely",
            "Child",
        }

    def test_connected_only_drops_relationshipless_and_inheritance_only(self):
        ctx = self._onto().agent_ontology_context(connected_only=True)
        # Lonely (no rels) and Child (inheritance only) are excluded.
        assert {e["name"] for e in ctx["entities"]} == {"Customer", "Order"}
        assert len(ctx["relationships"]) == 1


class TestValidateSwrlRule:
    def test_valid(self):
        assert (
            Ontology.validate_swrl_rule(
                {"name": "r", "antecedent": "A", "consequent": "B"}
            )
            == []
        )

    def test_missing_fields(self):
        errors = Ontology.validate_swrl_rule({})
        assert len(errors) == 3


class TestMergeIconSuggestions:
    def test_case_insensitive(self):
        result = Ontology.merge_icon_suggestions(
            ["Foo", "bar"], {"foo": "🔧", "BAR": "📦"}
        )
        assert result == {"Foo": "🔧", "bar": "📦"}

    def test_missing_name(self):
        result = Ontology.merge_icon_suggestions(["Foo", "Baz"], {"Foo": "🔧"})
        assert "Baz" not in result


class TestPostprocessGeneratedOwl:
    def test_clean_and_stats(self):
        content = """```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .
:Foo a owl:Class .
:bar a owl:ObjectProperty .
```"""
        turtle, stats = Ontology.postprocess_generated_owl(content)
        assert "@prefix" in turtle
        assert stats["classes"] == 1
        assert stats["properties"] == 1
