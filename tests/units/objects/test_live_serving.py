"""``TwinLiveServing``: the seed's pinned version, the replay start and the union view refresh."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.graphdb.lakebase import SyncedTableManager
from back.objects.digitaltwin.build import TwinLiveServing

pytestmark = pytest.mark.unit

_COMMIT_MS = 1793613600000  # 2026-11-02T10:00:00Z
_MARGIN_MS = 10 * 60 * 1000


class TestSeed:
    def test_the_seed_reads_the_slots_at_the_pinned_version(self):
        assert TwinLiveServing.seed_sql("main.kg.t_slots", 12) == (
            "SELECT map_id, source_key, predicate, subject, object FROM main.kg.t_slots VERSION AS OF 12"
        )

    @pytest.mark.parametrize(
        "committed_at",
        ["2026-11-02T10:00:00+00:00", "2026-11-02T10:00:00Z", "2026-11-02T10:00:00"],
        ids=["offset", "zulu", "naive-utc"],
    )
    def test_the_replay_starts_a_margin_before_the_snapshot_commit(self, committed_at):
        assert TwinLiveServing.replay_from_ms(committed_at, now_ms=0) == _COMMIT_MS - _MARGIN_MS

    @pytest.mark.parametrize("committed_at", [None, "", "yesterday"])
    def test_an_unknown_commit_time_replays_from_now(self, committed_at):
        now = _COMMIT_MS + 5_000

        assert TwinLiveServing.replay_from_ms(committed_at, now_ms=now) == now - _MARGIN_MS


class _SyncedStore:
    is_synced = True

    def __init__(self, registrations: dict):
        self.registrations = registrations
        self.views: list = []

    def synced_uc_name(self, graph, fallback_catalog=""):
        return f"{fallback_catalog}.ontobricks_graph.{graph.lower()}_sync"

    def synced_manager(self):
        store = self

        class _Manager:
            _extract_state = staticmethod(SyncedTableManager._extract_state)

            def get(self, name):
                return store.registrations.get(name)

        return _Manager()

    def ensure_synced_union_view(self, graph, synced_phy_override=""):
        self.views.append((graph, synced_phy_override))


def _online():
    return SimpleNamespace(data_synchronization_status=SimpleNamespace(detailed_state="ONLINE"))


class TestRefreshView:
    @pytest.fixture(autouse=True)
    def _catalog(self, monkeypatch):
        monkeypatch.setenv("ONTOBRICKS_SYNC_UC_CATALOG", "main")

    def test_a_fallback_registration_is_passed_as_the_synced_table(self):
        store = _SyncedStore({"main.ontobricks_graph.sales_v3_sync_b": _online()})

        TwinLiveServing.refresh_view(store, "sales_V3", domain=SimpleNamespace(delta={}), settings=None)

        assert store.views == [("sales_V3", "sales_v3_sync_b")]

    def test_the_primary_registration_needs_no_override(self):
        store = _SyncedStore({"main.ontobricks_graph.sales_v3_sync": _online()})

        TwinLiveServing.refresh_view(store, "sales_V3", domain=SimpleNamespace(delta={}), settings=None)

        assert store.views == [("sales_V3", "")]

    def test_an_unresolvable_registration_keeps_the_primary_name(self):
        store = _SyncedStore({})
        store.synced_manager = lambda: (_ for _ in ()).throw(RuntimeError("no workspace"))

        TwinLiveServing.refresh_view(store, "sales_V3")

        assert store.views == [("sales_V3", "")]
