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


def _mock_project(host='https://h', token='tok', warehouse_id='wh'):
    project = MagicMock()
    project.triplestore = {}
    project.databricks = {'host': host, 'token': token, 'warehouse_id': warehouse_id}
    project.info = {'name': 'TestProject'}
    project.ladybug = {'db_path': '/tmp/ontobricks'}
    return project


class TestGetTriplestore:
    def test_unknown_backend_returns_none(self):
        project = _mock_project()
        result = get_triplestore(project, backend='unknown')
        assert result is None

    def test_default_backend_is_graph(self):
        """When backend is None, default to graph (LadybugDB)."""
        project = _mock_project()
        with patch.object(
            TripleStoreFactory, '_create_ladybug'
        ) as mock_lb:
            mock_lb.return_value = MagicMock()
            result = get_triplestore(project)
            mock_lb.assert_called_once_with(project, None)

    @patch.object(_triple_store_factory_mod, 'get_databricks_host_and_token', return_value=('', ''))
    def test_view_missing_host_returns_none(self, mock_get):
        project = _mock_project(host='', token='')
        project.databricks = {'host': '', 'token': '', 'warehouse_id': 'wh'}
        result = get_triplestore(project, settings=MagicMock(databricks_sql_warehouse_id='wh'), backend='view')
        assert result is None

    @patch.object(_triple_store_factory_mod, 'get_databricks_host_and_token', return_value=('https://h', 'tok'))
    @patch.object(_triple_store_factory_mod, 'resolve_warehouse_id', return_value='wh')
    def test_view_success(self, mock_wh, mock_get):
        project = _mock_project()
        settings = MagicMock()
        settings.databricks_sql_warehouse_id = 'wh'

        with patch('back.core.databricks.DatabricksClient') as mock_client_cls, \
             patch.object(_delta_triple_store_mod, 'DeltaTripleStore') as mock_delta_cls:
            mock_client_cls.return_value = MagicMock()
            mock_delta_cls.return_value = MagicMock()
            result = get_triplestore(project, settings=settings, backend='view')
