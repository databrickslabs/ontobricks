"""Process-wide Lakebase store state: registry id + lazy-heal flags.

Stores are rebuilt on every request, so this state must be shared by every
instance bound to the same (database, schema) to avoid re-probing Postgres.
"""

from unittest.mock import patch

import pytest

from back.objects.registry.store.lakebase import LakebaseRegistryStore

pytestmark = pytest.mark.unit


def _store(schema="ontobricks_registry", database=""):
    with patch(
        "back.objects.registry.store.lakebase.store.get_lakebase_auth",
        return_value=object(),
    ):
        return LakebaseRegistryStore(registry_cfg=None, schema=schema, database=database)


def test_registry_id_is_shared_across_instances():
    _store()._registry_id = "rid-1"
    assert _store()._registry_id == "rid-1"


def test_lazy_heal_flag_is_shared_across_instances():
    _store()._status_column_ready = True
    assert _store()._status_column_ready is True


def test_new_store_does_not_reset_shared_flags():
    first = _store()
    first._edit_locks_ready = True
    _store()
    assert first._edit_locks_ready is True


def test_state_is_isolated_per_schema():
    _store(schema="reg_a")._registry_id = "rid-a"
    assert _store(schema="reg_b")._registry_id is None


def test_state_is_isolated_per_database():
    _store(database="db1")._registry_id = "rid-1"
    assert _store(database="db2")._registry_id is None


def test_registry_lookup_runs_once_for_many_instances():
    with patch.object(
        LakebaseRegistryStore, "_fetch_registry_id", return_value="rid-1"
    ) as fetch:
        for _ in range(3):
            assert _store()._registry() == "rid-1"
    assert fetch.call_count == 1


def test_reset_clears_state_seen_by_live_instances():
    live = _store()
    live._registry_id = "rid-1"
    LakebaseRegistryStore.reset_shared_state()
    assert live._registry_id is None


def test_targeted_reset_keeps_other_schemas():
    _store(schema="reg_a")._registry_id = "rid-a"
    _store(schema="reg_b")._registry_id = "rid-b"
    LakebaseRegistryStore.reset_shared_state(schema="reg_a")
    assert _store(schema="reg_b")._registry_id == "rid-b"


def test_initialize_forgets_cached_state():
    store = _store()
    store._registry_id = "stale"
    store._status_column_ready = True
    seen = {}

    def fake_ddl():
        seen["flag"] = store._status_column_ready
        raise RuntimeError("stop after reset")

    with patch.object(store, "_apply_ddl", side_effect=fake_ddl), patch.object(
        store, "_ensure_registry_row", side_effect=RuntimeError("stop")
    ):
        store.initialize()
    assert seen["flag"] is False


def test_instances_built_without_init_keep_per_instance_attributes():
    a = LakebaseRegistryStore.__new__(LakebaseRegistryStore)
    b = LakebaseRegistryStore.__new__(LakebaseRegistryStore)
    a._registry_id = "rid-a"
    assert b._registry_id is None
