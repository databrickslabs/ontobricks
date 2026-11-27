"""Neo4j user overrides: journal of edits, journaled add/delete, replay after Build.

The Bolt connection is mocked; we assert the Cypher / parameters emitted and the
control flow (what is journaled, what is skipped, what is replayed).
"""

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

try:
    import neo4j  # noqa: F401

    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not NEO4J_AVAILABLE, reason="neo4j driver not installed (optional dep)"
)

T1 = {"subject": "http://x/a", "predicate": "http://x/p", "object": "http://x/b"}
T2 = {"subject": "http://x/c", "predicate": "http://x/p", "object": "http://x/d"}


def _store():
    from back.core.graphdb.neo4j.Store import Store

    s = Store(
        db_name="g",
        engine_config={
            "uri": "neo4j+s://h.databases.neo4j.io",
            "database": "neo4j",
            "auth_method": "basic",
            "username": "neo4j",
            "password": "pw",
        },
    )
    s._connection.run = MagicMock(return_value=[])  # type: ignore[assignment]
    s._writes = MagicMock()
    s._writes.insert_triples.return_value = 0
    s._writes.delete_triples.return_value = 0
    return s


def _cyphers(store) -> List[str]:
    return [c.args[0] for c in store._connection.run.call_args_list]


def _journal_calls(store) -> List[Any]:
    return [c for c in store._connection.run.call_args_list if "__Override" in c.args[0]]


class TestJournal:
    def test_supports_overrides(self):
        assert _store().supports_overrides is True

    def test_record_merges_one_node_per_triple(self):
        s = _store()
        s._journal.record("g", [T1, T2], "add", author="me", change_id="c1")
        merge = [c for c in _journal_calls(s) if "MERGE" in c.args[0]]
        assert len(merge) == 1
        kw = merge[0].kwargs
        assert kw["op"] == "add"
        assert kw["origin"] == "user"
        assert kw["author"] == "me"
        assert kw["change_id"] == "c1"
        assert len(kw["rows"]) == 2
        assert kw["rows"][0]["id"] != kw["rows"][1]["id"]

    def test_record_empty_is_noop(self):
        s = _store()
        assert s._journal.record("g", [], "add") == 0
        assert not _journal_calls(s)

    def test_record_rejects_bad_op(self):
        from back.core.errors import ValidationError

        with pytest.raises(ValidationError):
            _store()._journal.record("g", [T1], "bogus")

    def test_entry_id_is_stable_and_graph_scoped(self):
        from back.core.graphdb.neo4j.OverrideJournal import _entry_id

        assert _entry_id("g", T1) == _entry_id("g", dict(T1))
        assert _entry_id("g", T1) != _entry_id("h", T1)

    def test_counts(self):
        s = _store()
        s._connection.run.return_value = [
            {"op": "add", "cnt": 3},
            {"op": "del", "cnt": 1},
        ]
        assert s.get_override_stats("g") == {
            "user_adds": 3,
            "user_dels": 1,
            "dormant_dels": 0,
        }


class TestAddDelete:
    def test_add_triples_journals_then_applies(self):
        s = _store()
        s.add_triples("g", [T1, T1, T2], author="me", change_id="c1")
        assert _journal_calls(s)
        applied = s._writes.insert_triples.call_args.args[1]
        assert applied == [T1, T2]  # de-duplicated

    def test_delete_triples_journals_on_live_graph(self):
        s = _store()
        s.delete_triples("g", [T1], author="me", change_id="c2")
        merge = [c for c in _journal_calls(s) if "MERGE" in c.args[0]]
        assert merge and merge[0].kwargs["op"] == "del"
        s._writes.delete_triples.assert_called_once()

    def test_delete_triples_skips_journal_on_sandbox(self):
        s = _store()
        s.delete_triples("g__sim_abc", [T1])
        assert not _journal_calls(s)
        s._writes.delete_triples.assert_called_once()

    def test_insert_triples_ignores_origin_and_does_not_journal(self):
        s = _store()
        s.insert_triples("g", [T1], origin="community")
        assert not _journal_calls(s)
        s._writes.insert_triples.assert_called_once()


class TestRebuildAndReplay:
    def test_reset_for_rebuild_keeps_journal(self):
        s = _store()
        s.reset_for_rebuild("g")
        s._writes.drop_table.assert_called_once_with("g")
        s._writes.create_table.assert_called_once_with("g")
        assert not [c for c in _journal_calls(s) if "DELETE j" in c.args[0]]

    def test_drop_table_drops_journal(self):
        s = _store()
        s.drop_table("g")
        s._writes.drop_table.assert_called_once_with("g")
        assert [c for c in _journal_calls(s) if "DELETE j" in c.args[0]]

    def test_replay_applies_adds_and_dels(self):
        s = _store()
        entries: List[Dict[str, Any]] = [
            dict(T1, op="add"),
            dict(T2, op="del"),
        ]
        s._journal.entries = MagicMock(return_value=entries)  # type: ignore[assignment]
        assert s.replay_overrides("g") == 2
        assert s._writes.insert_triples.call_args.args[1] == [entries[0]]
        assert s._writes.delete_triples.call_args.args[1] == [entries[1]]

    def test_replay_empty_journal_writes_nothing(self):
        s = _store()
        s._journal.entries = MagicMock(return_value=[])  # type: ignore[assignment]
        assert s.replay_overrides("g") == 0
        s._writes.insert_triples.assert_not_called()
        s._writes.delete_triples.assert_not_called()

    def test_base_backend_replay_default_is_noop(self):
        from back.core.graphdb.interfaces.MaterializedGraphBackend import (
            MaterializedGraphBackend,
        )

        assert MaterializedGraphBackend.replay_overrides(MagicMock(), "g") == 0
