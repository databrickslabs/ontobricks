"""Origin-aware ``delete_triples`` on the three overlay-capable backends.

``origin='user'`` keeps the manual-hide behaviour. ``origin='reasoning'`` is the
removal-rule path: the ``del`` row is tagged ``reasoning``, a triple with a user
``add`` row is skipped (D2), and no journal entry is written (D5, Neo4j).
"""

from unittest.mock import MagicMock

import pytest

from back.core.errors import ValidationError
from back.core.graphdb import overrides
from back.core.graphdb.delta import materialize
from back.core.graphdb.lakebase.OverrideOpsMixin import OverrideOpsMixin
from back.core.graphdb.neo4j.Store import Store as Neo4jStore

OVR = "cat.sch.graph__overrides"
DATA = "cat.sch.data"
BATCH = [{"subject": "http://e/m1", "predicate": "http://e/p", "object": "http://e/X"}]
TRIPLE = BATCH[0]


# -- overrides contract ------------------------------------------------------


def test_validate_hide_origin_accepts_user_and_reasoning():
    assert overrides.validate_hide_origin("user") == "user"
    assert overrides.validate_hide_origin("reasoning") == "reasoning"


@pytest.mark.parametrize("bad", ["cohort", "community", "", "other"])
def test_validate_hide_origin_rejects_others(bad):
    with pytest.raises(ValidationError):
        overrides.validate_hide_origin(bad)


# -- Delta -------------------------------------------------------------------


def test_delta_user_statements_hide_then_drop_user_adds():
    stmts = materialize.build_hide_statements(OVR, DATA, BATCH, origin="user")
    assert len(stmts) == 2
    assert "WHEN MATCHED THEN DELETE" in stmts[1]


def test_delta_reasoning_statements_tag_rows_and_keep_user_adds():
    stmts = materialize.build_hide_statements(OVR, DATA, BATCH, origin="reasoning")
    assert len(stmts) == 1  # D2: the user-add drop is never issued
    assert "'reasoning'" in stmts[0]
    assert "WHEN MATCHED THEN DELETE" not in stmts[0]


def test_delta_reasoning_hide_skips_triples_with_user_adds():
    sql = materialize.build_hide_sql(OVR, DATA, BATCH, origin="reasoning")
    assert "u.origin = 'user'" in sql
    assert "NOT EXISTS" in sql


def test_delta_user_hide_sql_is_unchanged():
    sql = materialize.build_hide_sql(OVR, DATA, BATCH)
    assert "'user'" in sql
    assert "'reasoning'" not in sql
    assert "u.origin" not in sql


def test_delta_hide_rejects_unknown_origin():
    with pytest.raises(ValidationError):
        materialize.build_hide_statements(OVR, DATA, BATCH, origin="cohort")


# -- Lakebase ----------------------------------------------------------------


class _RecordingCursor:
    def __init__(self):
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append(sql)


class _FakeLakebase(OverrideOpsMixin):
    """Runs the real ``_hide_triples`` against a recording cursor."""

    def __init__(self):
        self.executed = []

    def _ensure_override_layout(self, name):
        pass

    def companion_phy(self, name):
        return "ov"

    def synced_phy(self, name):
        return "sy"

    def _synced_ref(self, name):
        return name

    def _run_override_batches(self, triples, batch_size, on_progress, statements):
        cur = _RecordingCursor()
        statements(cur)
        self.executed.extend(cur.sql)
        return len(triples)


def test_lakebase_user_hide_runs_drop_of_user_adds():
    store = _FakeLakebase()
    store._hide_triples("t", BATCH, 500, None, "me", "chg", "user")
    assert len(store.executed) == 2
    assert "'user'" in store.executed[0]
    assert "DELETE FROM ov" in store.executed[1]


def test_lakebase_reasoning_hide_keeps_user_adds():
    store = _FakeLakebase()
    store._hide_triples("t", BATCH, 500, None, "me", "chg", "reasoning")
    assert len(store.executed) == 1
    assert "'reasoning'" in store.executed[0]
    assert "NOT EXISTS" in store.executed[0]
    assert "u.origin = 'user'" in store.executed[0]


# -- Neo4j -------------------------------------------------------------------


def _neo4j_store(journal_entries=()):
    store = Neo4jStore.__new__(Neo4jStore)
    store._journal = MagicMock()
    store._journal.entries.return_value = list(journal_entries)
    store._writes = MagicMock()
    store._writes.delete_triples.return_value = 1
    return store


def test_neo4j_user_delete_is_journaled():
    store = _neo4j_store()
    store.delete_triples("graph", BATCH, origin="user")
    store._journal.record.assert_called_once()


def test_neo4j_reasoning_delete_is_not_journaled():
    store = _neo4j_store()
    store.delete_triples("graph", BATCH, origin="reasoning")
    store._journal.record.assert_not_called()
    store._writes.delete_triples.assert_called_once()


def test_neo4j_reasoning_delete_skips_journaled_user_adds():  # D2
    added = {"op": "add", **TRIPLE}
    store = _neo4j_store(journal_entries=[added])
    store.delete_triples("graph", BATCH, origin="reasoning")
    deleted = store._writes.delete_triples.call_args.args[1]
    assert deleted == []


def test_neo4j_rejects_unknown_origin():
    store = _neo4j_store()
    with pytest.raises(ValidationError):
        store.delete_triples("graph", BATCH, origin="cohort")
