"""Tests for Unity Catalog schema DDL before synced-table registration."""

from unittest.mock import MagicMock

import pytest

from back.core.graphdb.lakebase._sync_uc_schema import (
    ensure_uc_schema_for_synced_table_fqn,
)


def test_runs_create_schema_with_three_part_fqn():
    client = MagicMock()
    ensure_uc_schema_for_synced_table_fqn(
        client,
        "main.ontobricks_graph.bigcustomers_v1_sync",
    )
    client.execute_statement.assert_called_once()
    ddl = client.execute_statement.call_args[0][0]
    assert "CREATE SCHEMA IF NOT EXISTS" in ddl
    assert "`main`" in ddl
    assert "`ontobricks_graph`" in ddl


def test_rejects_non_three_part_name():
    client = MagicMock()
    with pytest.raises(ValueError, match="catalog.schema.table"):
        ensure_uc_schema_for_synced_table_fqn(client, "only.two")


def test_wraps_execute_failure_as_infrastructure_error():
    from back.core.errors import InfrastructureError

    client = MagicMock()
    client.execute_statement.side_effect = RuntimeError("denied")
    with pytest.raises(InfrastructureError, match="Could not create Unity Catalog schema"):
        ensure_uc_schema_for_synced_table_fqn(
            client,
            "main.ontobricks_graph.bigcustomers_v1_sync",
        )
