"""Unit tests for ``WarehouseStatements`` (safe names, translated warehouse errors)."""

from __future__ import annotations

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.helpers import SQLHelpers, WarehouseStatements

pytestmark = pytest.mark.unit


class _Client:
    def __init__(self, rows=None, error=None):
        self.rows, self.error, self.queries = rows, error, []

    def execute_query(self, sql):
        self.queries.append(sql)
        if self.error is not None:
            raise self.error
        return self.rows


class TestRequireFqn:
    def test_a_qualified_name_passes(self):
        WarehouseStatements.require_fqn("main.ob.table_V1")

    @pytest.mark.parametrize("fqn", ["", "  ", "main.ob.x'", "main.ob.x y", "main.ob.x;--"])
    def test_an_empty_or_unsafe_name_is_refused(self, fqn):
        with pytest.raises(ValidationError):
            WarehouseStatements.require_fqn(fqn)

    def test_the_message_names_what_was_refused(self):
        with pytest.raises(ValidationError, match="Invalid source table name"):
            WarehouseStatements.require_fqn("a'b", what="source table name")


class TestQuery:
    def test_returns_the_rows_as_a_list(self):
        client = _Client(rows=({"a": 1},))

        assert WarehouseStatements.query(client, "SELECT 1", "read") == [{"a": 1}]
        assert client.queries == ["SELECT 1"]

    def test_no_rows_is_an_empty_list(self):
        assert WarehouseStatements.query(_Client(rows=None), "SELECT 1", "read") == []

    def test_a_driver_error_becomes_an_infrastructure_error(self):
        with pytest.raises(InfrastructureError, match="Could not read the history") as info:
            WarehouseStatements.query(
                _Client(error=RuntimeError("boom")), "DESCRIBE HISTORY t", "read the history"
            )
        assert info.value.detail == "boom"

    def test_an_ontobricks_error_passes_through(self):
        with pytest.raises(NotFoundError):
            WarehouseStatements.query(_Client(error=NotFoundError("gone")), "SELECT 1", "read")


class TestObjectHashSql:
    def test_is_the_sha256_of_the_given_expression(self):
        assert SQLHelpers.object_hash_sql("s.object") == "sha2(s.object, 256)"
