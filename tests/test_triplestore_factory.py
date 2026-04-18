"""Tests for triplestore factory."""

import importlib
import pytest
from unittest.mock import patch, MagicMock
from back.core.triplestore import TripleStoreFactory, get_triplestore

# Submodule (module object), not the class re-exported on back.core.triplestore
_triple_store_factory_mod = importlib.import_module(
    "back.core.triplestore.TripleStoreFactory",
)
_delta_triple_store_mod = importlib.import_module(
    "back.core.triplestore.delta.DeltaTripleStore",
)


def _mock_domain(host="https://h", token="tok", warehouse_id="wh"):
    domain = MagicMock()
    domain.triplestore = {}
    domain.databricks = {"host": host, "token": token, "warehouse_id": warehouse_id}
    domain.info = {"name": "TestDomain"}
    domain.ladybug = {"db_path": "/tmp/ontobricks"}
    return domain


class TestGetTriplestore:
    def test_unknown_backend_returns_none(self):
        domain = _mock_domain()
        result = get_triplestore(domain, backend="unknown")
        assert result is None

    def test_default_backend_is_graph(self):
        """When backend is None, default to graph (GraphDBFactory)."""
        domain = _mock_domain()
        with (
            patch("back.core.graphdb.get_graphdb") as mock_gdb,
            patch.object(
                TripleStoreFactory, "_resolve_graph_engine", return_value="ladybug"
            ),
            patch.object(
                TripleStoreFactory, "_resolve_graph_engine_config", return_value={}
            ),
        ):
            mock_gdb.return_value = MagicMock()
            result = get_triplestore(domain)
            mock_gdb.assert_called_once_with(
                domain, None, engine="ladybug", engine_config={}
            )

    @patch.object(
        _triple_store_factory_mod,
        "get_databricks_host_and_token",
        return_value=("", ""),
    )
    def test_view_missing_host_returns_none(self, mock_get):
        domain = _mock_domain(host="", token="")
        domain.databricks = {"host": "", "token": "", "warehouse_id": "wh"}
        result = get_triplestore(
            domain, settings=MagicMock(databricks_sql_warehouse_id="wh"), backend="view"
        )
        assert result is None

    @patch.object(
        _triple_store_factory_mod,
        "get_databricks_host_and_token",
        return_value=("https://h", "tok"),
    )
    @patch.object(_triple_store_factory_mod, "resolve_warehouse_id", return_value="wh")
    def test_view_success(self, mock_wh, mock_get):
        domain = _mock_domain()
        settings = MagicMock()
        settings.databricks_sql_warehouse_id = "wh"

        with (
            patch("back.core.databricks.DatabricksClient") as mock_client_cls,
            patch.object(_delta_triple_store_mod, "DeltaTripleStore") as mock_delta_cls,
        ):
            mock_client_cls.return_value = MagicMock()
            mock_delta_cls.return_value = MagicMock()
            result = get_triplestore(domain, settings=settings, backend="view")
