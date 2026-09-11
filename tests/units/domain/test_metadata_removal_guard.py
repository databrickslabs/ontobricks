"""Tests for the data-source deletion guard on Domain metadata removal."""

from unittest.mock import MagicMock

from back.objects.domain import Domain


def _mock_session(tables=None, entities=None, relationships=None):
    session = MagicMock()
    metadata = {"tables": list(tables or []), "table_count": len(tables or [])}
    session.catalog_metadata = metadata
    session.assignment = {
        "entities": list(entities or []),
        "relationships": list(relationships or []),
    }
    session.ontology = {}
    session.get_entity_mappings.side_effect = lambda: session.assignment["entities"]
    session.get_relationship_mappings.side_effect = lambda: session.assignment[
        "relationships"
    ]
    session._data = {"domain": {"metadata": metadata}}
    return session


def _table(name, catalog="cat", schema="sch"):
    return {"name": name, "full_name": f"{catalog}.{schema}.{name}", "columns": []}


def _entity(label, table, catalog="cat", schema="sch"):
    return {
        "ontology_class": f"http://t/{label}",
        "ontology_class_label": label,
        "catalog": catalog,
        "schema": schema,
        "table": table,
    }


class TestGetRemovalImpact:
    def test_reports_referrers_and_counts(self):
        session = _mock_session(
            tables=[_table("customers")],
            entities=[_entity("Customer", "customers")],
        )
        result = Domain(session).get_removal_impact(["cat.sch.customers"])
        assert result["success"] is True
        assert result["impact"] == {"cat.sch.customers": ["Entity: Customer"]}
        assert result["affected_table_count"] == 1
        assert result["affected_mapping_count"] == 1

    def test_empty_when_nothing_references_the_table(self):
        session = _mock_session(tables=[_table("customers")])
        result = Domain(session).get_removal_impact(["cat.sch.customers"])
        assert result["impact"] == {}
        assert result["affected_mapping_count"] == 0

    def test_is_read_only(self):
        session = _mock_session(
            tables=[_table("customers")],
            entities=[_entity("Customer", "customers")],
        )
        Domain(session).get_removal_impact(["cat.sch.customers"])
        session.save.assert_not_called()
        session.clear_generated_content.assert_not_called()


class TestSaveMetadataTablesInvalidation:
    def test_clears_generated_content_when_referenced_table_removed(self):
        session = _mock_session(
            tables=[_table("customers"), _table("orders")],
            entities=[_entity("Customer", "customers")],
        )
        result = Domain(session).save_metadata_tables([_table("orders")])
        assert result["impact"] == {"cat.sch.customers": ["Entity: Customer"]}
        session.clear_generated_content.assert_called_once()
        session.record_change.assert_called_once()
        session.save.assert_called_once()

    def test_no_invalidation_when_removed_table_is_unreferenced(self):
        session = _mock_session(
            tables=[_table("customers"), _table("orders")],
            entities=[_entity("Customer", "customers")],
        )
        result = Domain(session).save_metadata_tables([_table("customers")])
        assert result["impact"] == {}
        session.clear_generated_content.assert_not_called()
        session.record_change.assert_not_called()
        session.save.assert_called_once()

    def test_no_invalidation_when_nothing_is_removed(self):
        tables = [_table("customers")]
        session = _mock_session(
            tables=tables, entities=[_entity("Customer", "customers")]
        )
        result = Domain(session).save_metadata_tables(list(tables))
        assert result["impact"] == {}
        session.clear_generated_content.assert_not_called()

    def test_removal_still_persists_the_reduced_table_set(self):
        session = _mock_session(
            tables=[_table("customers"), _table("orders")],
            entities=[_entity("Customer", "customers")],
        )
        Domain(session).save_metadata_tables([_table("orders")])
        saved = session._data["domain"]["metadata"]["tables"]
        assert [t["name"] for t in saved] == ["orders"]

    def test_change_event_summarises_the_impact(self):
        session = _mock_session(
            tables=[_table("customers")],
            entities=[_entity("Customer", "customers")],
        )
        Domain(session).save_metadata_tables([])
        kwargs = session.record_change.call_args.kwargs
        assert kwargs["entity_type"] == "metadata_table"
        assert "cat.sch.customers" in kwargs["entity_ref"]
        assert kwargs["meta"]["impact"] == {
            "cat.sch.customers": ["Entity: Customer"]
        }


class TestTablesWithoutFullName:
    """Legacy metadata rows carry only ``name``; the guard must still match."""

    def test_removal_matches_on_the_bare_name(self):
        session = _mock_session(
            tables=[{"name": "customers", "columns": []}],
            entities=[_entity("Customer", "customers")],
        )
        result = Domain(session).save_metadata_tables([])
        assert result["impact"] == {"customers": ["Entity: Customer"]}
        session.clear_generated_content.assert_called_once()

    def test_clear_matches_on_the_bare_name(self):
        session = _mock_session(
            tables=[{"name": "customers", "columns": []}],
            entities=[_entity("Customer", "customers")],
        )
        assert Domain(session).clear_metadata()["impact"] == {
            "customers": ["Entity: Customer"]
        }

    def test_identifier_prefers_full_name_when_present(self):
        session = _mock_session()
        assert Domain(session)._table_identifiers(
            [{"name": "customers", "full_name": "cat.sch.customers"}]
        ) == ["cat.sch.customers"]

    def test_identifier_skips_empty_rows(self):
        session = _mock_session()
        assert Domain(session)._table_identifiers([{}, None]) == []


class TestSqlOnlyReferenceIsGuarded:
    def test_table_referenced_only_from_custom_sql_still_blocks_silently(self):
        """The referrer map covers FQNs parsed out of freeform SQL, so a table
        with no explicit catalog/schema/table binding is still protected."""
        session = _mock_session(
            tables=[_table("orders")],
            entities=[
                {
                    "ontology_class": "http://t/Order",
                    "ontology_class_label": "Order",
                    "sql_query": "SELECT id FROM cat.sch.orders",
                }
            ],
        )
        result = Domain(session).save_metadata_tables([])
        assert result["impact"] == {"cat.sch.orders": ["Entity: Order"]}
        session.clear_generated_content.assert_called_once()


class TestClearMetadataInvalidation:
    def test_clears_generated_content_when_mappings_exist(self):
        session = _mock_session(
            tables=[_table("customers")],
            entities=[_entity("Customer", "customers")],
        )
        result = Domain(session).clear_metadata()
        assert result["impact"] == {"cat.sch.customers": ["Entity: Customer"]}
        session.clear_generated_content.assert_called_once()
        session.save.assert_called_once()
        assert session._data["domain"]["metadata"] == {}

    def test_no_invalidation_when_no_mappings(self):
        session = _mock_session(tables=[_table("customers")])
        result = Domain(session).clear_metadata()
        assert result["impact"] == {}
        session.clear_generated_content.assert_not_called()
        session.save.assert_called_once()

    def test_clearing_empty_metadata_is_a_noop_guard(self):
        session = _mock_session()
        result = Domain(session).clear_metadata()
        assert result["success"] is True
        assert result["impact"] == {}
        session.clear_generated_content.assert_not_called()
