"""``SyncedTableName``: the registration a synced table is in use under (ghost fallbacks)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.graphdb.lakebase import SyncedTableManager, SyncedTableName

pytestmark = pytest.mark.unit

_NAME = "main.ontobricks_graph.sales_v3_sync"


def _synced(state: str):
    return SimpleNamespace(data_synchronization_status=SimpleNamespace(detailed_state=state))


class _Manager:
    """``get`` / ``_extract_state`` of ``SyncedTableManager`` over a dict of registrations."""

    _extract_state = staticmethod(SyncedTableManager._extract_state)

    def __init__(self, registrations: dict):
        self.registrations = registrations

    def get(self, name):
        return self.registrations.get(name)


class TestRegistered:
    def test_a_usable_primary_registration_is_used(self):
        mgr = _Manager({_NAME: _synced("ONLINE"), f"{_NAME}_b": _synced("ONLINE")})

        assert SyncedTableName.registered(mgr, _NAME) == _NAME

    def test_a_failed_primary_falls_back_to_the_first_usable_suffix(self):
        mgr = _Manager({_NAME: _synced("SYNCED_TABLE_OFFLINE_FAILED"), f"{_NAME}_c": _synced("ONLINE")})

        assert SyncedTableName.registered(mgr, _NAME) == f"{_NAME}_c"

    def test_a_ghost_primary_with_a_fallback_uses_the_fallback(self):
        mgr = _Manager({f"{_NAME}_b": _synced("ONLINE_CONTINUOUS_UPDATE")})

        assert SyncedTableName.registered(mgr, _NAME) == f"{_NAME}_b"

    def test_no_usable_registration_keeps_the_primary_name(self):
        assert SyncedTableName.registered(_Manager({}), _NAME) == _NAME
