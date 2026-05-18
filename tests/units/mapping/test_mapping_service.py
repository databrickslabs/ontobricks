"""Tests for back.objects.mapping.Mapping domain class."""

import pytest
from unittest.mock import MagicMock
from back.objects.mapping import Mapping


def _mock_domain(entities=None, relationships=None, ontology=None):
    domain = MagicMock()
    domain.assignment = {
        "entities": list(entities or []),
        "relationships": list(relationships or []),
    }
    # ``run_diagnostics`` reads ``domain.ontology`` directly; without an
    # explicit dict, MagicMock returns a MagicMock that breaks ``.get``
    # iteration in the diagnostics path.
    domain.ontology = dict(ontology) if ontology is not None else {}
    domain.get_entity_mappings.side_effect = lambda: domain.assignment["entities"]
    domain.get_relationship_mappings.side_effect = lambda: domain.assignment[
        "relationships"
    ]
    return domain


class TestBuildEntityMapping:
    def test_basic(self):
        data = {
            "ontology_class": "http://t/A",
            "ontology_class_label": "A",
            "sql_query": "SELECT *",
            "id_column": "id",
        }
        m = Mapping.build_entity_mapping(data)
        assert m["ontology_class"] == "http://t/A"
        assert m["id_column"] == "id"
        assert "excluded" not in m

    def test_excluded_flag(self):
        data = {"ontology_class": "A", "excluded": True}
        m = Mapping.build_entity_mapping(data)
        assert m["excluded"] is True


class TestBuildRelationshipMapping:
    def test_basic(self):
        data = {
            "property": "http://t/p",
            "source_class": "A",
            "target_class": "B",
        }
        m = Mapping.build_relationship_mapping(data)
        assert m["property"] == "http://t/p"
        assert m["direction"] == "forward"


class TestAddOrUpdateEntity:
    def test_add_new(self):
        domain = _mock_domain()
        was_update, mapping = Mapping(domain).add_or_update_entity_mapping(
            {
                "ontology_class": "http://t/A",
                "id_column": "id",
            }
        )
        assert was_update is False
        assert len(domain.assignment["entities"]) == 1

    def test_update_existing(self):
        existing = [{"ontology_class": "http://t/A", "id_column": "old"}]
        domain = _mock_domain(entities=existing)
        was_update, mapping = Mapping(domain).add_or_update_entity_mapping(
            {
                "ontology_class": "http://t/A",
                "id_column": "new",
            }
        )
        assert was_update is True
        assert domain.assignment["entities"][0]["id_column"] == "new"


class TestDeleteEntity:
    def test_delete_existing(self):
        existing = [{"ontology_class": "http://t/A"}]
        domain = _mock_domain(entities=existing)
        deleted = Mapping(domain).delete_entity_mapping("http://t/A")
        assert deleted is True

    def test_delete_nonexistent(self):
        domain = _mock_domain()
        deleted = Mapping(domain).delete_entity_mapping("http://t/Nope")
        assert deleted is False


class TestAddOrUpdateRelationship:
    def test_add_new(self):
        domain = _mock_domain()
        was_update, mapping = Mapping(domain).add_or_update_relationship_mapping(
            {
                "property": "http://t/p",
            }
        )
        assert was_update is False
        assert len(domain.assignment["relationships"]) == 1

    def test_update_existing(self):
        existing = [{"property": "http://t/p", "sql_query": "old"}]
        domain = _mock_domain(relationships=existing)
        was_update, mapping = Mapping(domain).add_or_update_relationship_mapping(
            {
                "property": "http://t/p",
                "sql_query": "new",
            }
        )
        assert was_update is True


class TestDeleteRelationship:
    def test_delete_existing(self):
        existing = [{"property": "http://t/p"}]
        domain = _mock_domain(relationships=existing)
        deleted = Mapping(domain).delete_relationship_mapping("http://t/p")
        assert deleted is True

    def test_delete_nonexistent(self):
        domain = _mock_domain()
        deleted = Mapping(domain).delete_relationship_mapping("nope")
        assert deleted is False


class TestGetMappingStats:
    def test_stats(self):
        domain = _mock_domain(
            entities=[{}, {}],
            relationships=[{}],
        )
        stats = Mapping(domain).get_mapping_stats()
        assert stats["entities"] == 2
        assert stats["relationships"] == 1


class TestSaveMappingConfig:
    def test_save(self):
        domain = _mock_domain()
        config = {
            "entities": [{"ontology_class": "A"}],
            "relationships": [{"property": "p"}],
        }
        stats = Mapping(domain).save_mapping_config(config)
        assert stats["entities"] == 1

    def test_reset(self):
        domain = _mock_domain(entities=[{}])
        Mapping(domain).reset_mapping()
        assert domain.assignment["entities"] == []


class TestExtractFqnFromSql:
    def test_simple_from(self):
        triples = Mapping._extract_fqn_from_sql(
            "SELECT * FROM main.bronze.events e"
        )
        assert ("main", "bronze", "events") in triples

    def test_backticked(self):
        triples = Mapping._extract_fqn_from_sql(
            "SELECT id FROM `main`.`bronze`.`events-stream` WHERE 1=1"
        )
        assert ("main", "bronze", "events-stream") in triples

    def test_join(self):
        triples = Mapping._extract_fqn_from_sql(
            "SELECT a.id FROM cat1.sch1.t1 a "
            "JOIN cat2.sch2.t2 b ON a.id = b.id"
        )
        assert ("cat1", "sch1", "t1") in triples
        assert ("cat2", "sch2", "t2") in triples

    def test_two_part_skipped(self):
        # 2-part references depend on the warehouse default catalog;
        # we cannot probe permissions safely so they must be ignored.
        triples = Mapping._extract_fqn_from_sql("SELECT * FROM bronze.events")
        assert triples == []

    def test_empty(self):
        assert Mapping._extract_fqn_from_sql("") == []
        assert Mapping._extract_fqn_from_sql(None) == []


class TestSplitTableRef:
    def test_three_parts(self):
        assert Mapping._split_table_ref("main.bronze.events") == (
            "main",
            "bronze",
            "events",
        )

    def test_with_backticks(self):
        assert Mapping._split_table_ref("`main`.`bronze`.`events`") == (
            "main",
            "bronze",
            "events",
        )

    def test_short_returns_none(self):
        assert Mapping._split_table_ref("bronze.events") is None
        assert Mapping._split_table_ref("events") is None
        assert Mapping._split_table_ref("") is None
        assert Mapping._split_table_ref(None) is None


class TestCollectSourceTables:
    def test_explicit_triple_on_entity(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/Person",
                    "ontology_class_label": "Person",
                    "catalog": "main",
                    "schema": "bronze",
                    "table": "people",
                }
            ]
        )
        result = Mapping(domain)._collect_source_tables()
        assert ("main", "bronze", "people") in result
        assert "Entity: Person" in result[("main", "bronze", "people")]

    def test_excluded_entity_is_skipped(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class_label": "P",
                    "catalog": "main",
                    "schema": "bronze",
                    "table": "people",
                    "excluded": True,
                }
            ]
        )
        assert Mapping(domain)._collect_source_tables() == {}

    def test_relationship_source_target_tables(self):
        domain = _mock_domain(
            relationships=[
                {
                    "property_label": "assignedTo",
                    "source_table": "main.silver.assignments",
                    "target_table": "main.silver.projects",
                }
            ]
        )
        result = Mapping(domain)._collect_source_tables()
        assert ("main", "silver", "assignments") in result
        assert ("main", "silver", "projects") in result
        labels = result[("main", "silver", "assignments")]
        assert any("assignedTo" in l and "source" in l for l in labels)

    def test_sql_query_extracts_fqn(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class_label": "X",
                    "sql_query": "SELECT id FROM dev.bronze.events",
                }
            ],
            relationships=[
                {
                    "property_label": "rel",
                    "sql_query": "SELECT a, b FROM `dev`.`silver`.`joined`",
                }
            ],
        )
        result = Mapping(domain)._collect_source_tables()
        assert ("dev", "bronze", "events") in result
        assert ("dev", "silver", "joined") in result

    def test_referrers_deduplicated(self):
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class_label": "X",
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                    "sql_query": "SELECT * FROM c.s.t",
                }
            ]
        )
        result = Mapping(domain)._collect_source_tables()
        assert result[("c", "s", "t")] == ["Entity: X"]


class TestClassifySqlError:
    def test_permission_denied(self):
        status, detail = Mapping._classify_sql_error(
            Exception("PERMISSION_DENIED: principal lacks SELECT on `main`.`x`.`y`")
        )
        assert status == "error"
        assert "Missing SELECT" in detail

    def test_table_not_found(self):
        status, detail = Mapping._classify_sql_error(
            Exception("[TABLE_OR_VIEW_NOT_FOUND] cannot resolve table")
        )
        assert status == "error"
        assert "Table not found" in detail

    def test_other_error(self):
        status, detail = Mapping._classify_sql_error(Exception("kaboom"))
        assert status == "error"
        assert "Probe failed" in detail


class TestRunPermissionChecks:
    def test_no_client_returns_warning(self):
        domain = _mock_domain(
            entities=[{"catalog": "c", "schema": "s", "table": "t"}]
        )
        section = Mapping(domain)._run_permission_checks(client=None)
        assert section["summary"]["warnings"] == 1
        assert section["checks"][0]["status"] == "warning"
        assert "warehouse" in section["checks"][0]["detail"].lower()

    def test_no_tables_returns_empty(self):
        domain = _mock_domain()
        section = Mapping(domain)._run_permission_checks(client=MagicMock())
        assert section["checks"] == []
        assert section["summary"]["total"] == 0

    def test_select_ok(self):
        client = MagicMock()
        client.execute_query.return_value = []
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class_label": "P",
                    "catalog": "main",
                    "schema": "bronze",
                    "table": "people",
                }
            ]
        )
        section = Mapping(domain)._run_permission_checks(client)
        assert section["summary"]["ok"] == 1
        assert section["checks"][0]["status"] == "ok"
        client.execute_query.assert_called_once()
        assert "LIMIT 0" in client.execute_query.call_args[0][0]
        assert "`main`.`bronze`.`people`" in client.execute_query.call_args[0][0]

    def test_permission_denied_classified_as_error(self):
        client = MagicMock()
        client.execute_query.side_effect = Exception(
            "PERMISSION_DENIED: SELECT on `c`.`s`.`t`"
        )
        domain = _mock_domain(
            entities=[{"catalog": "c", "schema": "s", "table": "t"}]
        )
        section = Mapping(domain)._run_permission_checks(client)
        assert section["summary"]["errors"] == 1
        assert "Missing SELECT" in section["checks"][0]["detail"]


class TestRunDiagnosticsPermissionsSection:
    def test_response_has_permissions_key(self):
        domain = _mock_domain()
        result = Mapping(domain).run_diagnostics()
        assert "permissions" in result
        # Without a client we expect the advisory warning row.
        assert result["permissions"][0]["status"] == "warning"

    def test_summary_rolls_up_permissions(self):
        client = MagicMock()
        client.execute_query.return_value = []
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/A",
                    "ontology_class_label": "A",
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                    "sql_query": "SELECT id FROM c.s.t",
                    "id_column": "id",
                }
            ]
        )
        result = Mapping(domain).run_diagnostics(client=client)
        # 1 entity row + 1 permissions row.
        assert result["summary"]["total"] == 2
        assert result["summary"]["ok"] >= 1
        assert any(p["status"] == "ok" for p in result["permissions"])

    def test_permission_error_counts_in_summary(self):
        client = MagicMock()
        client.execute_query.side_effect = Exception(
            "PERMISSION_DENIED: missing SELECT"
        )
        domain = _mock_domain(
            entities=[
                {
                    "ontology_class": "http://t/A",
                    "ontology_class_label": "A",
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                    "sql_query": "SELECT id FROM c.s.t",
                    "id_column": "id",
                }
            ]
        )
        result = Mapping(domain).run_diagnostics(client=client)
        assert result["summary"]["errors"] >= 1
        assert any(p["status"] == "error" for p in result["permissions"])
