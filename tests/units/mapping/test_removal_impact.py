"""Tests for the data-source deletion guard's referrer lookup."""

from unittest.mock import MagicMock

from back.objects.mapping import Mapping


def _mock_domain(entities=None, relationships=None):
    domain = MagicMock()
    domain.assignment = {
        "entities": list(entities or []),
        "relationships": list(relationships or []),
    }
    domain.ontology = {}
    domain.get_entity_mappings.side_effect = lambda: domain.assignment["entities"]
    domain.get_relationship_mappings.side_effect = lambda: domain.assignment[
        "relationships"
    ]
    return domain


class TestFindMappingsReferencing:
    def test_explicit_catalog_schema_table_triple(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                }
            ]
        )
        impact = Mapping(domain).find_mappings_referencing(["cat.sch.customers"])
        assert impact == {"cat.sch.customers": ["Entity: Customer"]}

    def test_sql_query_only_reference(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Order",
                    "ontology_class_label": "Order",
                    "sql_query": "SELECT id FROM cat.sch.orders WHERE active",
                }
            ]
        )
        impact = Mapping(domain).find_mappings_referencing(["cat.sch.orders"])
        assert impact == {"cat.sch.orders": ["Entity: Order"]}

    def test_relationship_source_and_target_tables(self):
        domain = _mock_domain(
            relationships=[
                {
                    "property": "http://t/buys",
                    "property_label": "buys",
                    "source_table": "cat.sch.customers",
                    "target_table": "cat.sch.products",
                }
            ]
        )
        impact = Mapping(domain).find_mappings_referencing(
            ["cat.sch.customers", "cat.sch.products"]
        )
        assert impact == {
            "cat.sch.customers": ["Rel: buys (source)"],
            "cat.sch.products": ["Rel: buys (target)"],
        }

    def test_multiple_referrers_for_one_table(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                }
            ],
            relationships=[
                {
                    "property": "http://t/buys",
                    "property_label": "buys",
                    "source_table": "cat.sch.customers",
                }
            ],
        )
        impact = Mapping(domain).find_mappings_referencing(["cat.sch.customers"])
        assert impact["cat.sch.customers"] == [
            "Entity: Customer",
            "Rel: buys (source)",
        ]

    def test_excluded_mappings_are_skipped(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                    "excluded": True,
                }
            ]
        )
        assert Mapping(domain).find_mappings_referencing(["cat.sch.customers"]) == {}

    def test_unreferenced_table_is_omitted(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                }
            ]
        )
        impact = Mapping(domain).find_mappings_referencing(["cat.sch.unused"])
        assert impact == {}

    def test_no_mappings_at_all(self):
        domain = _mock_domain()
        assert Mapping(domain).find_mappings_referencing(["cat.sch.customers"]) == {}

    def test_bare_table_name_matches_on_table_segment(self):
        """The Data Sources UI can supply short names when full_name is absent."""
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                }
            ]
        )
        impact = Mapping(domain).find_mappings_referencing(["customers"])
        assert impact == {"customers": ["Entity: Customer"]}

    def test_bare_name_match_is_case_insensitive(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "Customers",
                }
            ]
        )
        assert Mapping(domain).find_mappings_referencing(["customers"])

    def test_empty_and_blank_inputs(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                }
            ]
        )
        assert Mapping(domain).find_mappings_referencing([]) == {}
        assert Mapping(domain).find_mappings_referencing(["", None]) == {}

    def test_backticked_reference(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Customer",
                    "ontology_class_label": "Customer",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                }
            ]
        )
        impact = Mapping(domain).find_mappings_referencing(["`cat`.`sch`.`customers`"])
        assert impact == {"`cat`.`sch`.`customers`": ["Entity: Customer"]}
