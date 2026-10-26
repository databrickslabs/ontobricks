"""DigitalTwin Preview contracts for the entity-search fast path."""

from __future__ import annotations

import pytest

from back.objects.digitaltwin.DigitalTwin import DigitalTwin


pytestmark = pytest.mark.unit


def test_filter_preview_uses_index_rows_and_preserves_response_shape() -> None:
    class Store:
        kwargs: dict

        def find_preview_seeds(self, table_name: str, **kwargs):
            assert table_name == "g"
            self.kwargs = kwargs
            return [
                {
                    "uri": "http://ex/Ada",
                    "type": "http://ex/Person",
                    "label": "Ada",
                },
                {
                    "uri": "http://ex/Bob",
                    "type": "http://ex/Person",
                    "label": "Bob",
                },
            ]

    store = Store()
    result = DigitalTwin.filter_preview(
        store,
        "g",
        entity_type="",
        field="any",
        match_type="contains",
        value="a",
        max_preview=1,
    )

    assert store.kwargs["limit"] == 2
    assert result["capped"] is True
    assert result["total"] == 2
    assert result["seeds"] == [
        {
            "uri": "http://ex/Ada",
            "type": "Person",
            "type_uri": "http://ex/Person",
            "label": "Ada",
        }
    ]


def test_filter_preview_does_not_wrap_companion_probe_errors() -> None:
    """Explorer Search must fall back to SPO when cache existence probing fails."""
    from tests.units.graphdb.test_graphdb_adjacency_contract import FakeStore

    store = FakeStore()
    store.set_search_cache_mode("auto")

    def boom(_name: str) -> bool:
        raise RuntimeError("Lakehouse/RT is not supported for Thrift protocol")

    store.table_exists = boom  # type: ignore[method-assign]
    result = DigitalTwin.filter_preview(
        store,
        "g",
        entity_type="",
        field="any",
        match_type="contains",
        value="ada",
    )
    assert result["phase"] == "preview"
    assert result["seeds"] == []
    assert result["total"] == 0


def test_find_triples_bfs_does_not_fail_when_companion_probe_raises() -> None:
    """Graph Chat / MCP find uses BFS; probe errors must fall back to live SPO."""
    from tests.units.graphdb.test_graphdb_adjacency_contract import FakeStore

    store = FakeStore()
    store.set_search_cache_mode("auto")

    def boom(_name: str) -> bool:
        raise RuntimeError("Lakehouse/RT is not supported for Thrift protocol")

    store.table_exists = boom  # type: ignore[method-assign]
    result = DigitalTwin.find_triples_bfs(store, "g", search="ada", depth=1)
    assert result["seed_count"] == 0
    assert result["message"] == "No matching entities found"
