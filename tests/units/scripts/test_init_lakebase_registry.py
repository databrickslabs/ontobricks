"""Contracts for scripts/_internal/_init-lakebase-registry.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MOD_PATH = ROOT / "scripts" / "_internal" / "_init-lakebase-registry.py"


def _load():
    spec = importlib.util.spec_from_file_location("init_lakebase_registry", MOD_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_pick_starter_warehouse_by_name():
    mod = _load()
    warehouses = [
        {"id": "aaa", "name": "Pro Warehouse", "enable_serverless_compute": True},
        {"id": "bbb", "name": "Serverless Starter Warehouse", "enable_serverless_compute": True},
    ]
    assert mod.pick_serverless_warehouse(warehouses) == (
        "bbb",
        "Serverless Starter Warehouse",
    )


def test_pick_first_serverless_when_no_starter():
    mod = _load()
    warehouses = [
        {"id": "classic", "name": "Classic", "enable_serverless_compute": False},
        {"id": "srv", "name": "Team Serverless", "enable_serverless_compute": True},
    ]
    assert mod.pick_serverless_warehouse(warehouses) == ("srv", "Team Serverless")


def test_pick_none_when_no_serverless():
    mod = _load()
    warehouses = [{"id": "classic", "name": "Classic", "enable_serverless_compute": False}]
    assert mod.pick_serverless_warehouse(warehouses) is None


def test_parse_wrapped_and_bare_list():
    mod = _load()
    wrapped = '{"warehouses":[{"id":"x","name":"Serverless Starter Warehouse","enable_serverless_compute":true}]}'
    bare = '[{"id":"x","name":"Serverless Starter Warehouse","enable_serverless_compute":true}]'
    assert mod.pick_serverless_warehouse(mod.parse_warehouses_payload(wrapped))[0] == "x"
    assert mod.pick_serverless_warehouse(mod.parse_warehouses_payload(bare))[0] == "x"


def test_initialize_registry_calls_store():
    mod = _load()
    calls = {}

    class FakeStore:
        def is_initialized(self):
            return False

        def initialize(self):
            calls["ok"] = True
            return True, "initialized"

    def factory(**kwargs):
        calls["kwargs"] = kwargs
        return FakeStore()

    ok, msg = mod.initialize_registry(
        catalog="cat",
        schema="sch",
        volume="OntoBricksRegistry",
        lakebase_schema="reg_sc",
        lakebase_database="reg_db",
        store_factory=factory,
    )
    assert ok is True
    assert "initialized" in msg
    cfg = calls["kwargs"]["registry_cfg"]
    assert cfg.catalog == "cat"
    assert cfg.schema == "sch"
    assert cfg.volume == "OntoBricksRegistry"
    assert calls["kwargs"]["schema"] == "reg_sc"
    assert calls["kwargs"]["database"] == "reg_db"
    assert calls.get("ok") is True


def test_initialize_skips_when_already_initialized():
    mod = _load()
    calls = {}

    class FakeStore:
        def is_initialized(self):
            return True

        def initialize(self):
            calls["initialized"] = True
            return True, "should not run"

    ok, msg = mod.initialize_registry(
        catalog="cat",
        schema="sch",
        volume="OntoBricksRegistry",
        lakebase_schema="reg_sc",
        lakebase_database="reg_db",
        store_factory=lambda **kwargs: FakeStore(),
    )
    assert ok is True
    assert "already initialized" in msg
    assert "initialized" not in calls
