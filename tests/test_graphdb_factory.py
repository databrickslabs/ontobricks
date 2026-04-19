"""Tests for GraphDBFactory, GraphDBBackend, and graph DB capability flags."""

import pytest
from unittest.mock import patch, MagicMock

from back.core.graphdb.GraphDBFactory import GraphDBFactory, _get_factory_singleton
from back.core.graphdb.GraphDBBackend import GraphDBBackend


def _concrete_backend():
    """Build a minimal concrete GraphDBBackend subclass for testing."""
    class _Concrete(GraphDBBackend):
        def get_connection(self): return None
        def close(self): pass
        def create_table(self, n): pass
        def drop_table(self, n): pass
        def insert_triples(self, n, t, **kw): return 0
        def query_triples(self, n, **kw): return []
        def table_exists(self, n): return False
        def count_triples(self, n): return 0
        def get_status(self, n): return {}
        def execute_query(self, q): return []

    return _Concrete()


class TestGraphDBBackend:
    """Test the abstract base class default behaviour / capability flags."""

    def test_supports_cypher_default(self):
        backend = _concrete_backend()
        assert backend.supports_cypher is False
        assert backend.supports_graph_model is False
        assert backend.query_dialect == "sql"

    def test_is_cypher_backend_false(self):
        assert GraphDBBackend.is_cypher_backend(MagicMock(spec=[])) is False

    def test_get_node_table_default(self):
        assert _concrete_backend().get_node_table("test") == "test"

    def test_get_graph_schema_default_none(self):
        assert _concrete_backend().get_graph_schema() is None

    def test_sync_not_supported_by_default(self):
        backend = _concrete_backend()
        ok, msg = backend.sync_to_remote("/path", MagicMock())
        assert ok is False
        ok2, msg2 = backend.sync_from_remote("/path", MagicMock())
        assert ok2 is False
        assert backend.local_path() is None
        assert backend.remote_archive_path("/p") is None

    def test_get_query_translator_default(self):
        translator = _concrete_backend().get_query_translator()
        from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator
        assert isinstance(translator, SWRLSQLTranslator)


class TestGraphDBFactory:
    def test_unknown_engine(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        domain.info = {"name": "Test"}
        result = factory.create(domain, engine="neo4j")
        assert result is None

    def test_singleton(self):
        s1 = _get_factory_singleton()
        s2 = _get_factory_singleton()
        assert s1 is s2

    def test_default_engine_is_ladybug(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        domain.info = {"name": "Test"}
        with patch.object(factory, "_create_ladybug", return_value=MagicMock()) as mock_create:
            factory.create(domain)
            mock_create.assert_called_once()

    def test_explicit_ladybug_engine(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        with patch.object(factory, "_create_ladybug", return_value=MagicMock()) as mock_create:
            factory.create(domain, engine="ladybug")
            mock_create.assert_called_once()

    def test_engine_config_passed_through(self):
        factory = GraphDBFactory()
        with patch.object(factory, "_create_ladybug", return_value=None) as mock:
            factory.create(MagicMock(), engine="ladybug", engine_config={"key": "val"})
            _, kwargs = mock.call_args
            assert kwargs["engine_config"] == {"key": "val"}

    def test_get_graphdb_convenience(self):
        with patch.object(GraphDBFactory, "create", return_value=MagicMock()) as mock_create:
            domain = MagicMock()
            GraphDBFactory.get_graphdb(domain, engine="ladybug")
            mock_create.assert_called_once()
