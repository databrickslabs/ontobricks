"""Tests for schema-drift detection against the live source table schema."""

from unittest.mock import MagicMock

from back.objects.mapping import Mapping


def _mock_domain(entities=None, relationships=None, ontology=None):
    domain = MagicMock()
    domain.assignment = {
        "entities": list(entities or []),
        "relationships": list(relationships or []),
    }
    domain.ontology = dict(ontology) if ontology is not None else {}
    domain.get_entity_mappings.side_effect = lambda: domain.assignment["entities"]
    domain.get_relationship_mappings.side_effect = lambda: domain.assignment[
        "relationships"
    ]
    return domain


def _client(columns_by_table):
    """A client whose DESCRIBE returns *columns_by_table* keyed by table name."""
    client = MagicMock()
    client.get_table_columns.side_effect = lambda cat, sch, tbl: [
        {"name": c, "type": "string"} for c in columns_by_table.get(tbl, [])
    ]
    client.execute_query.return_value = []
    return client


def _entity(**overrides):
    ent = {
        "ontology_class": "http://t/Customer",
        "ontology_class_label": "Customer",
        "catalog": "cat",
        "schema": "sch",
        "table": "customers",
        "id_column": "id",
    }
    ent.update(overrides)
    return ent


def _drift_checks(result_item):
    return [
        c
        for c in result_item["checks"]
        if c["check"].startswith("schema_drift:")
    ]


class TestEntityDrift:
    def test_dropped_attribute_column_is_warned(self):
        domain = _mock_domain(
            entities=[_entity(attribute_mappings={"email": "email_addr"})]
        )
        client = _client({"customers": ["id", "name"]})
        result = Mapping(domain).run_diagnostics(client=client)
        checks = _drift_checks(result["entities"][0])
        assert [c["check"] for c in checks] == ["schema_drift:attribute:email"]
        assert checks[0]["status"] == "warning"
        assert "email_addr" in checks[0]["detail"]
        assert "cat.sch.customers" in checks[0]["detail"]

    def test_dropped_id_column_is_warned(self):
        domain = _mock_domain(entities=[_entity(id_column="cust_id")])
        client = _client({"customers": ["id", "name"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert [c["check"] for c in _drift_checks(result["entities"][0])] == [
            "schema_drift:id_column"
        ]

    def test_dropped_label_column_is_warned(self):
        domain = _mock_domain(entities=[_entity(label_column="full_name")])
        client = _client({"customers": ["id"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert "schema_drift:label_column" in [
            c["check"] for c in _drift_checks(result["entities"][0])
        ]

    def test_no_false_positive_when_schema_matches(self):
        domain = _mock_domain(
            entities=[_entity(attribute_mappings={"email": "email_addr"})]
        )
        client = _client({"customers": ["id", "email_addr"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert _drift_checks(result["entities"][0]) == []

    def test_drift_is_advisory_not_an_error(self):
        """The mapping is still well-formed — the upstream table moved."""
        domain = _mock_domain(
            entities=[_entity(attribute_mappings={"email": "email_addr"})]
        )
        client = _client({"customers": ["id"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert result["entities"][0]["status"] == "warning"

    def test_unreadable_table_is_skipped_rather_than_flagged(self):
        """get_table_columns returns [] for missing tables / no privilege;
        reporting every column as dropped would be noise."""
        domain = _mock_domain(
            entities=[_entity(attribute_mappings={"email": "email_addr"})]
        )
        result = Mapping(domain).run_diagnostics(client=_client({}))
        assert _drift_checks(result["entities"][0]) == []

    def test_parseable_sql_projection_skips_drift(self):
        """An explicit SELECT list is validated against the projection, whose
        aliases legitimately differ from the table's own column names."""
        domain = _mock_domain(
            entities=[
                _entity(
                    sql_query="SELECT id, name AS email_addr FROM cat.sch.customers",
                    attribute_mappings={"email": "email_addr"},
                )
            ]
        )
        client = _client({"customers": ["id", "name"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert _drift_checks(result["entities"][0]) == []

    def test_select_star_still_gets_drift(self):
        """SELECT * cannot be parsed into a projection, so the table's own
        schema is the right thing to compare against."""
        domain = _mock_domain(
            entities=[
                _entity(
                    sql_query="SELECT * FROM cat.sch.customers",
                    attribute_mappings={"email": "email_addr"},
                )
            ]
        )
        client = _client({"customers": ["id", "name"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert [c["check"] for c in _drift_checks(result["entities"][0])] == [
            "schema_drift:attribute:email"
        ]

    def test_schema_fetched_once_per_table_not_per_attribute(self):
        domain = _mock_domain(
            entities=[
                _entity(
                    attribute_mappings={"a": "ca", "b": "cb", "c": "cc"},
                    label_column="lbl",
                )
            ]
        )
        client = _client({"customers": ["id"]})
        Mapping(domain).run_diagnostics(client=client)
        assert client.get_table_columns.call_count == 1

    def test_excluded_entity_is_not_checked(self):
        domain = _mock_domain(
            entities=[
                _entity(excluded=True, attribute_mappings={"email": "email_addr"})
            ]
        )
        client = _client({"customers": ["id"]})
        result = Mapping(domain).run_diagnostics(client=client)
        assert result["entities"] == []


class TestRelationshipDrift:
    def _rel(self, **overrides):
        rel = {
            "property": "http://t/buys",
            "property_label": "buys",
            "source_table": "cat.sch.customers",
            "target_table": "cat.sch.products",
            "source_id_column": "customer_id",
            "target_id_column": "product_id",
        }
        rel.update(overrides)
        return rel

    def test_dropped_source_id_column_is_warned(self):
        domain = _mock_domain(relationships=[self._rel()])
        client = _client(
            {"customers": ["id"], "products": ["product_id"]}
        )
        result = Mapping(domain).run_diagnostics(client=client)
        checks = _drift_checks(result["relationships"][0])
        assert [c["check"] for c in checks] == ["schema_drift:source_id_column"]
        assert "cat.sch.customers" in checks[0]["detail"]

    def test_each_side_checked_against_its_own_table(self):
        """target_id_column existing in the source table must not mask its
        absence from the target table."""
        domain = _mock_domain(relationships=[self._rel()])
        client = _client(
            {"customers": ["customer_id", "product_id"], "products": ["id"]}
        )
        result = Mapping(domain).run_diagnostics(client=client)
        assert [c["check"] for c in _drift_checks(result["relationships"][0])] == [
            "schema_drift:target_id_column"
        ]

    def test_no_false_positive_when_both_sides_match(self):
        domain = _mock_domain(relationships=[self._rel()])
        client = _client(
            {"customers": ["customer_id"], "products": ["product_id"]}
        )
        result = Mapping(domain).run_diagnostics(client=client)
        assert _drift_checks(result["relationships"][0]) == []


class TestGetSchemaDrift:
    def test_reports_entity_columns(self):
        domain = _mock_domain(
            entities=[_entity(attribute_mappings={"email": "email_addr"})]
        )
        client = _client({"customers": ["id"]})
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["success"] is True
        assert drift["entities"]["http://t/Customer"]["columns"] == ["email_addr"]
        assert drift["entities"]["http://t/Customer"]["label"] == "Customer"
        assert drift["tables_checked"] == 1

    def test_clean_mapping_reports_nothing(self):
        domain = _mock_domain(
            entities=[_entity(attribute_mappings={"email": "email_addr"})]
        )
        client = _client({"customers": ["id", "email_addr"]})
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["entities"] == {}
        assert drift["relationships"] == {}

    def test_reports_relationship_columns(self):
        domain = _mock_domain(
            relationships=[
                {
                    "property": "http://t/buys",
                    "property_label": "buys",
                    "source_table": "cat.sch.customers",
                    "source_id_column": "customer_id",
                }
            ]
        )
        client = _client({"customers": ["id"]})
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["relationships"]["http://t/buys"]["columns"] == ["customer_id"]

    def test_aliased_sql_projection_is_not_reported_as_drift(self):
        """Designer/auto-assignment mappings bind to the SELECT aliases
        (``… AS ID``), which never match the table's own column names."""
        domain = _mock_domain(
            entities=[
                _entity(
                    sql_query=(
                        "SELECT customer_id AS ID, last_name AS Label, "
                        "last_name AS lastname FROM cat.sch.customers"
                    ),
                    id_column="ID",
                    label_column="Label",
                    attribute_mappings={"lastname": "lastname"},
                )
            ]
        )
        client = _client({"customers": ["customer_id", "last_name"]})
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["entities"] == {}

    def test_projection_only_mapping_skips_the_warehouse(self):
        """Nothing DESCRIBE could decide — do not pay for the round-trips."""
        domain = _mock_domain(
            entities=[
                _entity(
                    sql_query="SELECT id AS ID FROM cat.sch.customers",
                    id_column="ID",
                )
            ]
        )
        client = _client({"customers": ["id"]})
        Mapping(domain).get_schema_drift(client)
        assert client.get_table_columns.call_count == 0

    def test_select_star_mapping_is_still_checked(self):
        domain = _mock_domain(
            entities=[
                _entity(
                    sql_query="SELECT * FROM cat.sch.customers",
                    attribute_mappings={"email": "email_addr"},
                )
            ]
        )
        client = _client({"customers": ["id"]})
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["entities"]["http://t/Customer"]["columns"] == ["email_addr"]

    def test_aliased_relationship_sql_is_not_reported_as_drift(self):
        domain = _mock_domain(
            relationships=[
                {
                    "property": "http://t/buys",
                    "property_label": "buys",
                    "source_table": "cat.sch.customers",
                    "source_id_column": "SourceID",
                    "sql_query": (
                        "SELECT customer_id AS SourceID, product_id AS TargetID "
                        "FROM cat.sch.customers"
                    ),
                }
            ]
        )
        client = _client({"customers": ["customer_id", "product_id"]})
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["relationships"] == {}

    def test_no_client_is_a_safe_noop(self):
        domain = _mock_domain(entities=[_entity()])
        drift = Mapping(domain).get_schema_drift(None)
        assert drift == {
            "success": True,
            "entities": {},
            "relationships": {},
            "tables_checked": 0,
        }

    def test_describe_failure_does_not_raise(self):
        domain = _mock_domain(entities=[_entity()])
        client = MagicMock()
        client.get_table_columns.side_effect = Exception("warehouse down")
        drift = Mapping(domain).get_schema_drift(client)
        assert drift["entities"] == {}
        assert drift["tables_checked"] == 0
