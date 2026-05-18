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

    def test_default_engine_is_lakebase(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        domain.info = {"name": "Test"}
        with patch.object(factory, "_create_lakebase", return_value=MagicMock()) as mock_create:
            factory.create(domain)
            mock_create.assert_called_once()

    def test_engine_config_passed_through(self):
        factory = GraphDBFactory()
        with patch.object(factory, "_create_lakebase", return_value=None) as mock:
            factory.create(MagicMock(), engine="lakebase", engine_config={"key": "val"})
            _, kwargs = mock.call_args
            assert kwargs["engine_config"] == {"key": "val"}

    def test_lakebase_engine_dispatches(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        with patch.object(factory, "_create_lakebase", return_value=MagicMock()) as mock_lb:
            factory.create(domain, engine="lakebase")
            mock_lb.assert_called_once()

    def test_lakebase_unavailable_returns_none(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        with patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", False):
            assert factory.create(domain, engine="lakebase") is None

    def test_get_graphdb_convenience(self):
        with patch.object(GraphDBFactory, "create", return_value=MagicMock()) as mock_create:
            domain = MagicMock()
            GraphDBFactory.get_graphdb(domain, engine="lakebase")
            mock_create.assert_called_once()

    def test_lakebase_explicit_schema_overrides_registry_volume_schema(self):
        """When graph_engine_config.schema is set, it wins over the registry volume schema."""
        from types import SimpleNamespace

        factory = GraphDBFactory()
        domain = SimpleNamespace(settings={"registry": {}}, info={"name": "Dom"})
        settings = SimpleNamespace(
            registry_catalog="",
            registry_schema="",
            registry_volume="",
            lakebase_schema="ontobricks_registry",
            lakebase_database="",
            registry_volume_path="",
        )
        mock_auth = MagicMock(is_available=True, instance_name="inst", database="ldb")
        with (
            patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", True),
            patch("back.core.databricks.get_lakebase_auth", return_value=mock_auth),
            patch(
                "back.objects.registry.RegistryCfg.from_domain",
                return_value=MagicMock(
                    catalog="team_cat",
                    schema="registry_uc_schema",
                    volume="vol"
                ),
            ),
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.LakebaseFlatStore",
            ) as mock_lb,
        ):
            mock_lb.return_value = MagicMock()
            factory.create(
                domain,
                settings,
                engine="lakebase",
                engine_config={"schema": "ontobricks_graph"},
            )
        assert mock_lb.call_args.kwargs["schema"] == "ontobricks_graph"

    def test_lakebase_schema_falls_back_to_registry_volume_when_not_configured(self):
        """When graph_engine_config.schema is empty, the registry volume schema is used."""
        from types import SimpleNamespace

        factory = GraphDBFactory()
        domain = SimpleNamespace(settings={"registry": {}}, info={"name": "Dom"})
        settings = SimpleNamespace(
            registry_catalog="",
            registry_schema="",
            registry_volume="",
            lakebase_schema="ontobricks_registry",
            lakebase_database="",
            registry_volume_path="",
        )
        mock_auth = MagicMock(is_available=True, instance_name="inst", database="ldb")
        with (
            patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", True),
            patch("back.core.databricks.get_lakebase_auth", return_value=mock_auth),
            patch(
                "back.objects.registry.RegistryCfg.from_domain",
                return_value=MagicMock(
                    catalog="team_cat",
                    schema="registry_uc_schema",
                    volume="vol"
                ),
            ),
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.LakebaseFlatStore",
            ) as mock_lb,
        ):
            mock_lb.return_value = MagicMock()
            factory.create(
                domain,
                settings,
                engine="lakebase",
                engine_config={},  # no schema set — registry wins
            )
        assert mock_lb.call_args.kwargs["schema"] == "registry_uc_schema"

    def test_lakebase_sync_uc_schema_uses_graph_schema(self):
        """sync_uc_schema matches the Postgres graph schema, not the registry schema.

        Lakebase places the _sync foreign table in the Postgres schema named after
        the UC schema segment of the synced-table FQN.  That schema must equal the
        graph schema where all other graph tables live.
        """
        from types import SimpleNamespace

        factory = GraphDBFactory()
        domain = SimpleNamespace(settings={"registry": {}}, info={"name": "Dom"})
        settings = SimpleNamespace(
            registry_catalog="",
            registry_schema="",
            registry_volume="",
            lakebase_schema="ontobricks_registry",
            lakebase_database="",
            registry_volume_path="",
        )
        mock_auth = MagicMock(is_available=True, instance_name="inst", database="ldb")
        with (
            patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", True),
            patch("back.core.databricks.get_lakebase_auth", return_value=mock_auth),
            patch(
                "back.objects.registry.RegistryCfg.from_domain",
                return_value=MagicMock(
                    catalog="benoit_cayla",
                    schema="ontobricks",
                    volume="registry",
                    is_configured=True,
                ),
            ),
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.LakebaseFlatStore",
            ) as mock_lb,
        ):
            mock_lb.return_value = MagicMock()
            factory.create(
                domain,
                settings,
                engine="lakebase",
                engine_config={"schema": "ontobricks_graph"},
            )
        assert mock_lb.call_args.kwargs["schema"] == "ontobricks_graph"
        # sync UC schema must equal the Postgres graph schema (not the registry schema)
        assert mock_lb.call_args.kwargs["sync_uc_schema"] == "ontobricks_graph"

    def test_lakebase_sync_uc_schema_fallback_to_graph_schema_when_registry_empty(self):
        """sync_uc_schema falls back to graph schema when RegistryCfg returns no schema."""
        from types import SimpleNamespace

        factory = GraphDBFactory()
        domain = SimpleNamespace(settings={"registry": {}}, info={"name": "Dom"})
        settings = SimpleNamespace(
            registry_catalog="",
            registry_schema="",
            registry_volume="",
            lakebase_schema="ontobricks_registry",
            lakebase_database="",
            registry_volume_path="",
        )
        mock_auth = MagicMock(is_available=True, instance_name="inst", database="ldb")
        with (
            patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", True),
            patch("back.core.databricks.get_lakebase_auth", return_value=mock_auth),
            patch(
                "back.objects.registry.RegistryCfg.from_domain",
                return_value=MagicMock(
                    catalog="",
                    schema="",  # no registry schema → fallback
                    volume="",
                    is_configured=False,
                ),
            ),
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.LakebaseFlatStore",
            ) as mock_lb,
        ):
            mock_lb.return_value = MagicMock()
            factory.create(
                domain,
                settings,
                engine="lakebase",
                engine_config={"schema": "ontobricks_graph"},
            )
        # registry schema empty → fallback to graph schema
        assert mock_lb.call_args.kwargs["schema"] == "ontobricks_graph"
        assert mock_lb.call_args.kwargs["sync_uc_schema"] == "ontobricks_graph"

    def test_lakebase_sync_uc_schema_explicit_override(self):
        """Explicit sync_uc_schema in engine_config wins over the graph schema."""
        from types import SimpleNamespace

        factory = GraphDBFactory()
        domain = SimpleNamespace(settings={"registry": {}}, info={"name": "Dom"})
        settings = SimpleNamespace(
            registry_catalog="",
            registry_schema="",
            registry_volume="",
            lakebase_schema="ontobricks_registry",
            lakebase_database="",
            registry_volume_path="",
        )
        mock_auth = MagicMock(is_available=True, instance_name="inst", database="ldb")
        with (
            patch("back.core.graphdb.lakebase.LAKEBASE_AVAILABLE", True),
            patch("back.core.databricks.get_lakebase_auth", return_value=mock_auth),
            patch(
                "back.core.graphdb.lakebase.LakebaseFlatStore.LakebaseFlatStore",
            ) as mock_lb,
        ):
            mock_lb.return_value = MagicMock()
            factory.create(
                domain,
                settings,
                engine="lakebase",
                engine_config={
                    "schema": "ontobricks_graph",
                    "sync_uc_schema": "custom_uc_schema",
                },
            )
        assert mock_lb.call_args.kwargs["schema"] == "ontobricks_graph"
        assert mock_lb.call_args.kwargs["sync_uc_schema"] == "custom_uc_schema"

