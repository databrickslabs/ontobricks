"""Tests for DeltaFlatStore factory registration."""

from unittest.mock import MagicMock, patch

from back.core.graphdb.GraphDBFactory import GraphDBFactory


class TestGraphDBFactoryDelta:
    def test_create_delta_engine(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        client = MagicMock()
        with patch(
            "back.core.graphdb.delta.DeltaBase.create_databricks_client",
            return_value=client,
        ):
            store = factory.create(domain, engine="delta")
        assert store is not None
        assert store.get_connection() is client

    def test_create_delta_write_store_routes_client_to_build_warehouse(self):
        factory = GraphDBFactory()
        domain = MagicMock()
        client = MagicMock()
        with patch(
            "back.core.graphdb.delta.DeltaBase.create_databricks_client",
            return_value=client,
        ) as create_client:
            store = factory.create(domain, engine="delta", for_write=True)

        assert store is not None
        create_client.assert_called_once_with(domain, None, for_write=True)

    def test_unknown_engine_returns_none(self):
        factory = GraphDBFactory()
        assert factory.create(MagicMock(), engine="neo4j") is None
