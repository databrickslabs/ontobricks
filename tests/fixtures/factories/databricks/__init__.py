"""Databricks surface mocks.

Each mock here exposes the public method surface of its real counterpart in
`back.core.databricks/` but stays fully in-process. Use these instead of
`unittest.mock.MagicMock` when:

- You want method signatures enforced (the mock raises on unknown methods).
- You need stateful behaviour (e.g., SQL Warehouse remembers inserted rows).
- The test asserts on the *interaction*, not the return value.

For "I just need a stub", `MagicMock` is still fine.
"""

from tests.fixtures.factories.databricks.sql_warehouse_mock import MockSQLWarehouse
from tests.fixtures.factories.databricks.uc_metadata_mock import MockUCCatalog
from tests.fixtures.factories.databricks.volumes_mock import MockVolume
from tests.fixtures.factories.databricks.fma_endpoint_mock import MockFoundationModelClient

__all__ = [
    "MockSQLWarehouse",
    "MockUCCatalog",
    "MockVolume",
    "MockFoundationModelClient",
]
