"""SlotDiffProcessor: the stateful diff of one source row's slots, and its SlotChange record."""

from __future__ import annotations

import hashlib

import pytest

from back.core.errors import ValidationError
from back.core.streaming import SlotChange, SlotDiffProcessor, SlotRowProjector

pytestmark = pytest.mark.unit

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_AGE = "http://ex.org/age"
_CUSTOMER = "http://ex.org/Customer"
_MAPPING = {
    "table": "main.sales.customers",
    "sql_query": None,
    "uri_template": "http://ex.org/Customer/{id}",
    "id_column": "id",
    "label_column": "name",
    "class_uri": _CUSTOMER,
    "predicates": {
        _LABEL: {"type": "column", "column": "name"},
        _AGE: {"type": "column", "column": "age"},
    },
}
_PROJECTOR = SlotRowProjector.from_mapping(_MAPPING)
_MAP_ID = _PROJECTOR.map_id
_SUBJECT = "http://ex.org/Customer/1"


def _processor() -> SlotDiffProcessor:
    return SlotDiffProcessor([_PROJECTOR])


def _upsert(state, row, seq):
    return _processor().apply(
        state, map_id=_MAP_ID, source_key="1", op="upsert", row={"id": 1, **row}, seq=seq
    )


def _delete(state, seq):
    return _processor().apply(state, map_id=_MAP_ID, source_key="1", op="delete", row=None, seq=seq)


def _live(state) -> dict:
    return {p: (s, o) for p, (s, o, _seq) in state.items() if o is not None}


class TestSlotChange:
    def test_an_upsert_hashes_its_object_like_spark_sha2(self):
        change = SlotChange.upsert(
            map_id="m", source_key="1", predicate=_LABEL, subject=_SUBJECT, object="Ann", seq=4
        )

        assert change.object_hash == hashlib.sha256(b"Ann").hexdigest()
        assert change.deleted is False

    def test_a_tombstone_carries_no_subject_object_or_hash(self):
        change = SlotChange.tombstone(map_id="m", source_key="1", predicate=_LABEL, seq=5)

        assert (change.subject, change.object, change.object_hash, change.deleted) == (
            None,
            None,
            None,
            True,
        )

    def test_the_row_has_the_output_field_order(self):
        change = SlotChange.tombstone(map_id="m", source_key="1", predicate=_LABEL, seq=5)

        assert list(change.as_row()) == [
            "map_id",
            "source_key",
            "predicate",
            "subject",
            "object",
            "object_hash",
            "deleted",
            "seq",
        ]


class TestUpsert:
    def test_a_new_row_emits_an_upsert_per_non_blank_slot_sorted_by_predicate(self):
        state, changes = _upsert(None, {"name": "Ann", "age": 41}, seq=1)

        assert [(c.predicate, c.object, c.deleted) for c in changes] == [
            (_AGE, "41", False),
            (_RDF_TYPE, _CUSTOMER, False),
            (_LABEL, "Ann", False),
        ]
        assert all(c.subject == _SUBJECT and c.seq == 1 for c in changes)
        assert _live(state) == {
            _AGE: (_SUBJECT, "41"),
            _RDF_TYPE: (_SUBJECT, _CUSTOMER),
            _LABEL: (_SUBJECT, "Ann"),
        }

    def test_only_changed_slots_are_emitted(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)

        _state, changes = _upsert(state, {"name": "Ann", "age": 42}, seq=2)

        assert [(c.predicate, c.object) for c in changes] == [(_AGE, "42")]

    @pytest.mark.parametrize("blank", [None, "", "   "])
    def test_a_blanked_slot_is_tombstoned(self, blank):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)

        state, changes = _upsert(state, {"name": blank, "age": 41}, seq=2)

        assert [(c.predicate, c.deleted, c.seq) for c in changes] == [(_LABEL, True, 2)]
        assert _LABEL not in _live(state)

    def test_a_blank_slot_never_creates_a_slot(self):
        _state, changes = _upsert(None, {"name": "", "age": None}, seq=1)

        assert [c.predicate for c in changes if not c.deleted] == [_RDF_TYPE]

    def test_a_tab_is_not_blank_like_spark_trim(self):
        """``TRIM`` strips spaces only, so a tab-only object is a slot in ``_data`` too."""
        _state, changes = _upsert(None, {"name": "\t", "age": None}, seq=1)

        assert (_LABEL, "\t") in [(c.predicate, c.object) for c in changes if not c.deleted]

    def test_a_known_state_never_tombstones_a_slot_that_was_not_live(self):
        """A ``_slots`` snapshot without the age slot: a blank age stays silent."""
        snapshot = {_LABEL: (_SUBJECT, "Ann", SlotDiffProcessor.SNAPSHOT_SEQ)}

        _state, changes = _upsert(snapshot, {"name": "Ann", "age": ""}, seq=1)

        assert [(c.predicate, c.deleted) for c in changes] == [(_RDF_TYPE, False)]

    def test_an_unchanged_row_emits_nothing(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)

        _state, changes = _upsert(state, {"name": "Ann", "age": 41}, seq=2)

        assert changes == []

    def test_a_null_key_row_is_ignored(self):
        state, changes = _processor().apply(
            None, map_id=_MAP_ID, source_key="1", op="upsert", row={"id": None, "name": "x"}, seq=1
        )

        assert (state, changes) == ({}, [])


class TestDelete:
    def test_a_delete_tombstones_every_live_slot(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)

        state, changes = _delete(state, seq=2)

        assert [(c.predicate, c.deleted) for c in changes] == [
            (_AGE, True),
            (_RDF_TYPE, True),
            (_LABEL, True),
        ]
        assert _live(state) == {}

    def test_a_delete_of_an_unknown_row_tombstones_every_predicate_of_the_mapping(self):
        """No state: any slot may be live in ``_live``; the sink's seq guard absorbs extras."""
        _state, changes = _delete(None, seq=1)

        assert [(c.predicate, c.deleted, c.seq) for c in changes] == [
            (_AGE, True, 1),
            (_RDF_TYPE, True, 1),
            (_LABEL, True, 1),
        ]

    def test_a_delete_of_an_unknown_mapping_emits_nothing(self):
        _state, changes = _processor().apply(
            None, map_id="map_unknown", source_key="1", op="delete", row=None, seq=1
        )

        assert changes == []

    def test_a_reinsert_after_a_delete_upserts_every_slot_again(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)
        state, _ = _delete(state, seq=2)

        _state, changes = _upsert(state, {"name": "Ann", "age": 41}, seq=3)

        assert sorted(c.predicate for c in changes if not c.deleted) == sorted(
            [_AGE, _RDF_TYPE, _LABEL]
        )


class TestOrderingGuard:
    def test_a_stale_event_is_a_no_op(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=5)

        after, changes = _upsert(state, {"name": "Bob", "age": 1}, seq=4)

        assert (after, changes) == (state, [])

    def test_a_replayed_event_is_a_no_op(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=5)

        assert _upsert(state, {"name": "Ann", "age": 41}, seq=5) == (state, [])

    def test_a_stale_event_after_a_delete_cannot_resurrect_the_row(self):
        """The delete keeps a seq watermark so a late upsert stays stale."""
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)
        state, _ = _delete(state, seq=2)

        after, changes = _upsert(state, {"name": "Ann", "age": 41}, seq=1)

        assert (after, changes) == (state, [])

    def test_a_delete_of_an_unseen_row_still_records_its_watermark(self):
        state, _ = _delete(None, seq=7)

        assert _upsert(state, {"name": "Ann", "age": 41}, seq=6) == (state, [])


class TestEvictedState:
    """After the MapState TTL evicts a key, its next event must still reach ``_live``."""

    def test_evicted_state_and_a_delete_tombstone_every_known_predicate(self):
        state, _ = _upsert(None, {"name": "Ann", "age": 41}, seq=1)
        assert state  # the key was live, then the TTL evicted its state

        _state, changes = _delete(None, seq=2)

        assert {c.predicate for c in changes if c.deleted} == {_AGE, _RDF_TYPE, _LABEL}

    def test_evicted_state_and_an_upsert_with_a_blank_column_tombstone_it(self):
        _upsert(None, {"name": "Ann", "age": 41}, seq=1)

        _state, changes = _upsert(None, {"name": "Ann", "age": None}, seq=2)

        assert (_AGE, True) in [(c.predicate, c.deleted) for c in changes]
        assert (_LABEL, "Ann") in [(c.predicate, c.object) for c in changes if not c.deleted]

    def test_an_empty_state_counts_as_evicted(self):
        _state, changes = _delete({}, seq=2)

        assert len(changes) == 3


class TestInitialState:
    def test_the_slots_snapshot_becomes_per_key_state(self):
        rows = [
            {
                "map_id": _MAP_ID,
                "source_key": "1",
                "predicate": _LABEL,
                "subject": _SUBJECT,
                "object": "Ann",
            },
            {
                "map_id": _MAP_ID,
                "source_key": "1",
                "predicate": _AGE,
                "subject": _SUBJECT,
                "object": "",
            },
            {
                "map_id": _MAP_ID,
                "source_key": "2",
                "predicate": _LABEL,
                "subject": "s2",
                "object": "Bo",
                "seq": 9,
            },
        ]

        state = _processor().initial_state(rows)

        assert state == {
            (_MAP_ID, "1"): {_LABEL: (_SUBJECT, "Ann", SlotDiffProcessor.SNAPSHOT_SEQ)},
            (_MAP_ID, "2"): {_LABEL: ("s2", "Bo", 9)},
        }

    def test_the_first_event_after_the_snapshot_diffs_against_it(self):
        snapshot = _processor().initial_state(
            [
                {
                    "map_id": _MAP_ID,
                    "source_key": "1",
                    "predicate": _LABEL,
                    "subject": _SUBJECT,
                    "object": "Ann",
                },
                {
                    "map_id": _MAP_ID,
                    "source_key": "1",
                    "predicate": _RDF_TYPE,
                    "subject": _SUBJECT,
                    "object": _CUSTOMER,
                },
            ]
        )[(_MAP_ID, "1")]

        _state, changes = _upsert(snapshot, {"name": "Ann", "age": 3}, seq=0)

        assert [(c.predicate, c.object) for c in changes] == [(_AGE, "3")]


class TestValidation:
    def test_an_unknown_op_is_rejected(self):
        with pytest.raises(ValidationError, match="op"):
            _processor().apply(None, map_id=_MAP_ID, source_key="1", op="merge", row={}, seq=1)

    def test_an_upsert_needs_a_row(self):
        with pytest.raises(ValidationError, match="row"):
            _processor().apply(None, map_id=_MAP_ID, source_key="1", op="upsert", row=None, seq=1)

    def test_an_unknown_mapping_is_rejected(self):
        with pytest.raises(ValidationError, match="map_unknown"):
            _processor().apply(
                None, map_id="map_unknown", source_key="1", op="upsert", row={"id": 1}, seq=1
            )
