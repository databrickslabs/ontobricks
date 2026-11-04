"""Hat typing + capability-accessor contract for the graph DB factory.

These seams were introduced by the backend cloisonnement refactor and were only
covered indirectly (through migrated call sites). This file pins them directly:

* the two **hats** — a Delta ``view`` store is a read-only
  :class:`VirtualBackend` with no write surface, a Delta ``table`` store is a
  copy-owning :class:`MaterializedGraphBackend`;
* :meth:`GraphDBFactory.classify_store` maps a live store to its backend kind;
* the four capability accessors (``get_builder`` / ``get_health_probe`` /
  ``get_provisioner`` / ``get_simulation``) vend the right per-engine impl.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# Lakebase/Neo4j capability impls and classify_store pull in the optional
# drivers; skip the whole module if they are not installed.
pytest.importorskip("psycopg")
pytest.importorskip("neo4j")

from back.core.graphdb.interfaces.GraphBackend import GraphBackend
from back.core.graphdb.interfaces.MaterializedGraphBackend import MaterializedGraphBackend
from back.core.graphdb.interfaces.VirtualBackend import VirtualBackend
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.graphdb.delta.MaterializedStore import MaterializedStore
from back.core.graphdb.delta.ViewStore import ViewStore

pytestmark = pytest.mark.unit

# The write surface that only a materialized (copy-owning) backend carries.
_WRITE_SURFACE = ("create_table", "drop_table", "insert_triples")


def _domain(backend: str):
    d = MagicMock()
    d.info = {"name": "Dom", "graph_backend": backend}
    d.current_version = 1
    return d


class TestBackendHats:
    """The virtual/materialized split is a *type*, not a runtime flag."""

    def test_delta_view_is_a_virtual_backend(self):
        store = ViewStore(MagicMock())
        assert isinstance(store, VirtualBackend)
        assert isinstance(store, GraphBackend)
        assert not isinstance(store, MaterializedGraphBackend)

    def test_delta_table_is_a_materialized_backend(self):
        store = MaterializedStore(MagicMock())
        assert isinstance(store, MaterializedGraphBackend)
        assert isinstance(store, GraphBackend)
        assert not isinstance(store, VirtualBackend)

    def test_virtual_store_has_no_write_surface(self):
        store = ViewStore(MagicMock())
        for name in _WRITE_SURFACE:
            assert not hasattr(store, name), (
                f"VirtualBackend must not expose write method {name!r}"
            )

    def test_materialized_store_exposes_write_surface(self):
        store = MaterializedStore(MagicMock())
        for name in _WRITE_SURFACE:
            assert callable(getattr(store, name, None)), (
                f"MaterializedGraphBackend must expose write method {name!r}"
            )

    def test_virtual_store_keeps_companion_read_flags(self):
        """A no-copy view still *reads* companions (search-cache governs use)."""
        store = ViewStore(MagicMock())
        assert store.supports_adjacency is True
        assert store.supports_entity_search is True
        assert store.supports_props is True
        # …but it owns no copy, so inference purge is off.
        assert store.supports_materialized_inference_purge is False

    def test_factory_view_path_yields_a_virtual_store(self):
        """``engine="view"`` builds a real ViewStore (not mocked)."""
        from unittest.mock import patch

        domain = _domain("databricks")
        settings = MagicMock()
        with (
            patch(
                "back.core.helpers.get_databricks_host_and_token",
                return_value=("https://h", "tok"),
            ),
            patch("back.core.helpers.resolve_build_warehouse_id", return_value="wh"),
            patch("back.core.helpers.resolve_use_cloud_fetch", return_value=False),
            patch("back.core.databricks.DatabricksClient", return_value=MagicMock()),
        ):
            store = GraphDBFactory().create(domain, settings=settings, engine="view")
        assert isinstance(store, VirtualBackend)
        assert not isinstance(store, MaterializedGraphBackend)


class TestClassifyStore:
    """Maps a live store instance to its backend kind without importing it."""

    def test_delta_materialized_classifies_as_delta(self):
        assert GraphDBFactory.classify_store(MaterializedStore(MagicMock())) == "delta"

    def test_delta_view_classifies_as_delta(self):
        assert GraphDBFactory.classify_store(ViewStore(MagicMock())) == "delta"

    def test_lakebase_classifies_as_lakebase(self):
        from back.core.graphdb.lakebase.FlatStore import FlatStore

        store = object.__new__(FlatStore)  # isinstance only, no ctor deps
        assert GraphDBFactory.classify_store(store) == "lakebase"

    def test_neo4j_classifies_as_neo4j(self):
        from back.core.graphdb.neo4j.Store import Store

        store = object.__new__(Store)
        assert GraphDBFactory.classify_store(store) == "neo4j"

    def test_unknown_store_classifies_as_empty(self):
        assert GraphDBFactory.classify_store(object()) == ""


class TestGetBuilder:
    def test_explicit_engine_selects_impl(self):
        from back.core.graphdb.delta.Builder import Builder
        from back.core.graphdb.lakebase.Builder import Builder as LakebaseBuilder
        from back.core.graphdb.neo4j.Builder import Builder as Neo4jBuilder

        d = _domain("lakebase")
        assert isinstance(GraphDBFactory.get_builder(d, engine="delta"), Builder)
        assert isinstance(
            GraphDBFactory.get_builder(d, engine="lakebase"), LakebaseBuilder
        )
        assert isinstance(GraphDBFactory.get_builder(d, engine="neo4j"), Neo4jBuilder)

    def test_auto_resolution_from_domain_backend(self):
        from back.core.graphdb.delta.Builder import Builder as DeltaBuilder
        from back.core.graphdb.lakebase.Builder import Builder as LakebaseBuilder

        assert isinstance(
            GraphDBFactory.get_builder(_domain("databricks")), DeltaBuilder
        )
        assert isinstance(
            GraphDBFactory.get_builder(_domain("lakebase")), LakebaseBuilder
        )
        # Neo4j has no auto builder engine — analytics forces Delta explicitly.
        assert GraphDBFactory.get_builder(_domain("neo4j")) is None

    def test_unknown_backend_returns_none(self):
        assert GraphDBFactory.get_builder(_domain("none")) is None


class TestGetHealthProbe:
    def test_explicit_engine_selects_impl(self):
        from back.core.graphdb.delta.HealthProbe import HealthProbe
        from back.core.graphdb.lakebase.HealthProbe import HealthProbe as LakebaseHealthProbe
        from back.core.graphdb.neo4j.HealthProbe import HealthProbe as Neo4jHealthProbe

        d = _domain("lakebase")
        assert isinstance(
            GraphDBFactory.get_health_probe(d, engine="delta"), HealthProbe
        )
        assert isinstance(
            GraphDBFactory.get_health_probe(d, engine="lakebase"), LakebaseHealthProbe
        )
        assert isinstance(
            GraphDBFactory.get_health_probe(d, engine="neo4j"), Neo4jHealthProbe
        )

    def test_auto_resolution_from_domain_backend(self):
        from back.core.graphdb.delta.HealthProbe import HealthProbe as DeltaHealthProbe
        from back.core.graphdb.lakebase.HealthProbe import HealthProbe as LakebaseHealthProbe

        assert isinstance(
            GraphDBFactory.get_health_probe(_domain("databricks")), DeltaHealthProbe
        )
        assert isinstance(
            GraphDBFactory.get_health_probe(_domain("lakebase")), LakebaseHealthProbe
        )
        assert GraphDBFactory.get_health_probe(_domain("neo4j")) is None


class TestGetProvisioner:
    def test_explicit_engine_selects_impl(self):
        from back.core.graphdb.lakebase.Provisioner import Provisioner as LakebaseProvisioner
        from back.core.graphdb.neo4j.Provisioner import Provisioner as Neo4jProvisioner

        d = _domain("lakebase")
        assert isinstance(
            GraphDBFactory.get_provisioner(d, engine="lakebase"), LakebaseProvisioner
        )
        assert isinstance(
            GraphDBFactory.get_provisioner(d, engine="neo4j"), Neo4jProvisioner
        )

    def test_delta_has_no_provisioner(self):
        assert GraphDBFactory.get_provisioner(_domain("databricks"), engine="delta") is None

    def test_auto_resolution_from_domain_backend(self):
        from back.core.graphdb.lakebase.Provisioner import Provisioner

        assert isinstance(
            GraphDBFactory.get_provisioner(_domain("lakebase")), Provisioner
        )
        # databricks -> delta engine, which has no provisioner
        assert GraphDBFactory.get_provisioner(_domain("databricks")) is None
        assert GraphDBFactory.get_provisioner(_domain("neo4j")) is None


class TestGetSimulation:
    def test_each_kind_selects_impl(self):
        from back.core.graphdb.lakebase.Simulation import Simulation as LakebaseSimulation
        from back.core.graphdb.delta.Simulation import Simulation
        from back.core.graphdb.neo4j.Simulation import Simulation as Neo4jSimulation

        assert isinstance(GraphDBFactory.get_simulation("lakebase"), LakebaseSimulation)
        assert isinstance(GraphDBFactory.get_simulation("delta"), Simulation)
        # "databricks" is the domain-level alias for the Delta engine.
        assert isinstance(GraphDBFactory.get_simulation("databricks"), Simulation)
        assert isinstance(GraphDBFactory.get_simulation("neo4j"), Neo4jSimulation)

    def test_unknown_kind_returns_none(self):
        assert GraphDBFactory.get_simulation("") is None
        assert GraphDBFactory.get_simulation("mystery") is None
