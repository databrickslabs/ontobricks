"""RegistryFactory: backend selection from settings (lakebase default, sqlite opt-in)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from back.core.graphdb.GraphDBFactory import default_graph_backend, normalize_graph_backend
from back.objects.registry.RegistryCfg import RegistryCfg
from back.objects.registry.store import RegistryFactory

pytestmark = pytest.mark.unit

CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol", lakebase_schema="reg", lakebase_database="db2")


@pytest.fixture
def backend(monkeypatch):
    """Set ``registry_backend`` on the cached settings for one test."""
    from shared.config.settings import get_settings

    def _set(name):
        monkeypatch.setattr(get_settings(), "registry_backend", name)

    return _set


class TestBackendSelection:
    def test_lakebase_is_the_default(self, backend):
        backend("")
        assert RegistryFactory.backend_name() == "lakebase"

    def test_unknown_value_falls_back_to_lakebase(self, backend):
        backend("oracle")
        assert RegistryFactory.backend_name() == "lakebase"

    def test_lakebase_store_gets_schema_and_database(self, backend):
        backend("lakebase")
        with patch.object(RegistryFactory, "lakebase") as lakebase:
            RegistryFactory.from_cfg(CFG)
        lakebase.assert_called_once_with(registry_cfg=CFG, schema="reg", database="db2")

    def test_sqlite_backend_builds_a_sqlite_store(self, backend, tmp_path, monkeypatch):
        backend("sqlite")
        monkeypatch.setenv("REGISTRY_SQLITE_PATH", str(tmp_path / "reg.sqlite"))
        no_volume = RegistryCfg(catalog="", schema="", volume="")
        assert RegistryFactory.from_cfg(no_volume).backend == "sqlite"

    def test_sqlite_snapshot_defaults_to_the_registry_volume(self, monkeypatch):
        monkeypatch.delenv("REGISTRY_SQLITE_SNAPSHOT_PATH", raising=False)
        path = RegistryFactory._sqlite_snapshot_path(CFG, "reg")
        assert path == "/Volumes/cat/sch/vol/_registry/reg.sqlite"

    def test_no_volume_means_no_snapshot(self, monkeypatch):
        monkeypatch.delenv("REGISTRY_SQLITE_SNAPSHOT_PATH", raising=False)
        assert RegistryFactory._sqlite_snapshot_path(RegistryCfg(catalog="", schema="", volume=""), "reg") == ""


class TestGraphBackendDefault:
    def test_default_is_lakebase_with_a_lakebase_registry(self, backend):
        backend("lakebase")
        assert default_graph_backend() == "lakebase"

    def test_default_is_lakehouse_without_lakebase(self, backend):
        backend("sqlite")
        assert default_graph_backend() == "databricks"

    def test_missing_domain_choice_follows_the_default(self, backend):
        backend("sqlite")
        assert normalize_graph_backend(None) == "databricks"

    def test_explicit_domain_choice_is_kept(self, backend):
        backend("sqlite")
        assert normalize_graph_backend("neo4j") == "neo4j"


class TestRegistryCfgWithoutLakebase:
    def test_sqlite_backend_never_looks_up_the_lakebase_row(self, backend, monkeypatch):
        """The Lakebase ``registries`` row lookup would retry Lakebase every minute."""
        from back.objects.registry.store.lakebase import store as _lb_store

        backend("sqlite")
        calls = []
        monkeypatch.setattr(
            _lb_store,
            "fetch_lakebase_registry_triplet",
            lambda schema, database="": calls.append(schema),
        )
        settings = type("S", (), {"registry_volume_path": "/Volumes/c/s/v"})()
        cfg = RegistryCfg.from_domain(None, settings)
        assert calls == [] and (cfg.catalog, cfg.schema, cfg.volume) == ("c", "s", "v")
