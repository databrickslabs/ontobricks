"""Tests for back.core.triplestore.IncrementalBuildService."""

import pytest
from unittest.mock import MagicMock, call

from back.core.triplestore.IncrementalBuildService import IncrementalBuildService


class TestExtractSourceTables:
    def test_entity_sql(self):
        assignment = {
            "entities": [
                {"sql_query": "SELECT * FROM catalog.schema.customers"},
                {"sql_query": "SELECT id FROM catalog.schema.orders"},
            ],
            "relationships": [],
        }
        tables = IncrementalBuildService.extract_source_tables(assignment)
        assert "catalog.schema.customers" in tables
        assert "catalog.schema.orders" in tables

    def test_relationship_sql_with_join(self):
        assignment = {
            "entities": [],
            "relationships": [
                {
                    "sql_query": "SELECT c.id, o.id FROM catalog.schema.customers c JOIN catalog.schema.orders o ON c.id = o.cid"
                }
            ],
        }
        tables = IncrementalBuildService.extract_source_tables(assignment)
        assert "catalog.schema.customers" in tables
        assert "catalog.schema.orders" in tables

    def test_backtick_quoted_names(self):
        assignment = {
            "entities": [
                {"sql_query": "SELECT * FROM `cat`.`sch`.`tbl`"}
            ],
            "relationships": [],
        }
        tables = IncrementalBuildService.extract_source_tables(assignment)
        assert "cat.sch.tbl" in tables

    def test_no_sql_query(self):
        assignment = {"entities": [{"other_field": "value"}], "relationships": []}
        tables = IncrementalBuildService.extract_source_tables(assignment)
        assert tables == []

    def test_empty_assignment(self):
        assert IncrementalBuildService.extract_source_tables({}) == []

    def test_deduplication(self):
        assignment = {
            "entities": [
                {"sql_query": "SELECT * FROM cat.sch.tbl"},
                {"sql_query": "SELECT id FROM cat.sch.tbl"},
            ],
            "relationships": [],
        }
        tables = IncrementalBuildService.extract_source_tables(assignment)
        assert tables.count("cat.sch.tbl") == 1

    def test_legacy_key_data_source_mappings(self):
        assignment = {
            "data_source_mappings": [
                {"sql_query": "SELECT * FROM cat.sch.legacy_table"}
            ],
        }
        tables = IncrementalBuildService.extract_source_tables(assignment)
        assert "cat.sch.legacy_table" in tables


class TestCheckSourceVersions:
    def test_changed_when_version_differs(self):
        client = MagicMock()
        client.execute_query.return_value = [{"version": 5}]
        svc = IncrementalBuildService(client)

        changed, versions = svc.check_source_versions(
            ["cat.sch.tbl"], {"cat.sch.tbl": 3}
        )
        assert changed is True
        assert versions["cat.sch.tbl"] == 5

    def test_not_changed_when_same(self):
        client = MagicMock()
        client.execute_query.return_value = [{"version": 5}]
        svc = IncrementalBuildService(client)

        changed, versions = svc.check_source_versions(
            ["cat.sch.tbl"], {"cat.sch.tbl": 5}
        )
        assert changed is False

    def test_new_table(self):
        client = MagicMock()
        client.execute_query.return_value = [{"version": 1}]
        svc = IncrementalBuildService(client)

        changed, versions = svc.check_source_versions(["cat.sch.new"], {})
        assert changed is True

    def test_empty_source_tables(self):
        svc = IncrementalBuildService(MagicMock())
        changed, versions = svc.check_source_versions([], {})
        assert changed is True

    def test_history_fails_gracefully(self):
        client = MagicMock()
        client.execute_query.side_effect = Exception("not delta")
        svc = IncrementalBuildService(client)

        changed, versions = svc.check_source_versions(
            ["cat.sch.tbl"], {"cat.sch.tbl": -1}
        )
        assert changed is False
        assert versions["cat.sch.tbl"] == -1


class TestSnapshotTableName:
    def test_basic(self):
        name = IncrementalBuildService.snapshot_table_name(
            "MyDomain", {"catalog": "cat", "schema": "sch"}, version="1"
        )
        assert name == "cat.sch._ob_snapshot_mydomain_v1"

    def test_special_chars_sanitised(self):
        name = IncrementalBuildService.snapshot_table_name(
            "My-Domain!", {"catalog": "c", "schema": "s"}, version="2.0"
        )
        assert "my_domain_" in name
        assert "v2_0" in name

    def test_default_version(self):
        name = IncrementalBuildService.snapshot_table_name(
            "domain", {"catalog": "c", "schema": "s"}
        )
        assert "v1" in name


class TestSnapshotOperations:
    def test_snapshot_exists_true(self):
        client = MagicMock()
        client.execute_query.return_value = []
        svc = IncrementalBuildService(client)
        assert svc.snapshot_exists("cat.sch.snap") is True

    def test_snapshot_exists_false(self):
        client = MagicMock()
        client.execute_query.side_effect = Exception("table not found")
        svc = IncrementalBuildService(client)
        assert svc.snapshot_exists("cat.sch.snap") is False

    def test_create_snapshot(self):
        client = MagicMock()
        client.create_or_replace_table_from_query.return_value = (True, "ok")
        svc = IncrementalBuildService(client)
        svc.create_snapshot("cat.sch.view", "cat.sch.snap")
        client.create_or_replace_table_from_query.assert_called_once()

    def test_create_snapshot_not_fully_qualified(self):
        svc = IncrementalBuildService(MagicMock())
        with pytest.raises(Exception, match="fully qualified"):
            svc.create_snapshot("view", "bad_name")

    def test_drop_snapshot(self):
        client = MagicMock()
        svc = IncrementalBuildService(client)
        svc.drop_snapshot("cat.sch.snap")
        client.execute_query.assert_called_once()


class TestComputeDiff:
    def test_returns_additions_and_removals(self):
        client = MagicMock()
        client.execute_query.side_effect = [
            [{"subject": "A", "predicate": "p", "object": "B"}],
            [{"subject": "C", "predicate": "p", "object": "D"}],
        ]
        svc = IncrementalBuildService(client)
        to_add, to_remove = svc.compute_diff("view", "snapshot")
        assert len(to_add) == 1
        assert len(to_remove) == 1

    def test_empty_diff(self):
        client = MagicMock()
        client.execute_query.side_effect = [[], []]
        svc = IncrementalBuildService(client)
        to_add, to_remove = svc.compute_diff("view", "snapshot")
        assert to_add == []
        assert to_remove == []


class TestShouldFallbackToFull:
    def test_zero_total(self):
        svc = IncrementalBuildService(MagicMock())
        assert svc.should_fallback_to_full(10, 5, 0) is True

    def test_high_change_rate(self):
        svc = IncrementalBuildService(MagicMock())
        assert svc.should_fallback_to_full(80, 10, 100) is True

    def test_low_change_rate(self):
        svc = IncrementalBuildService(MagicMock())
        assert svc.should_fallback_to_full(5, 3, 100) is False

    def test_exactly_at_threshold(self):
        svc = IncrementalBuildService(MagicMock())
        assert svc.should_fallback_to_full(40, 40, 100) is True


class TestCountViewTriples:
    def test_returns_count(self):
        client = MagicMock()
        client.execute_query.return_value = [{"cnt": 42}]
        svc = IncrementalBuildService(client)
        assert svc.count_view_triples("view") == 42

    def test_returns_zero_on_error(self):
        client = MagicMock()
        client.execute_query.side_effect = Exception("fail")
        svc = IncrementalBuildService(client)
        assert svc.count_view_triples("view") == 0
