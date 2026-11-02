"""Property: AUTO CDC fold == SlotDiffProcessor fold == full-build projection.

The Real-Time-Mode stream turns every source-row change into
:class:`SlotChange` upserts and tombstones (:class:`SlotDiffProcessor`); the
``_live`` sink keeps, per ``(map_id, source_key, predicate)``, the change with
the highest ``seq`` (``ON CONFLICT ... WHERE EXCLUDED.seq > seq``) and readers
see the rows that are not tombstones. For any change history of a small
table this test checks that this fold equals

1. the non-blank slots of the final keyed rows, and
2. the AUTO CDC SCD TYPE 1 fold of the slot pipeline (the model of
   ``test_slot_fold.py``) with its blank-object slots removed — the
   ``_data`` relation's ``TRIM(object) != ''`` filter —

also when some events are delivered again after newer ones (late or replayed
deliveries must be no-ops), and when the stream starts from a ``_slots``
snapshot taken part-way through the history.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from back.core.streaming import SlotDiffProcessor, SlotRowProjector
from tests.property.test_slot_fold import (
    _COMMITS,
    _MAPPING,
    _MAP_ID,
    _folded,
    _non_blank,
    _replay,
    _slots_of,
)

pytestmark = pytest.mark.property

_PROJECTOR = SlotRowProjector.from_mapping(_MAPPING)


def _inputs(feed) -> list[tuple[str, str, dict, int]]:
    """The stream's input per change-feed row: ``(op, source_key, row, seq)``.

    The stream reads after-images: an update's post-image is an upsert and
    its pre-image carries nothing new. ``seq`` increases with every change,
    as a Kafka offset does. NULL-key rows have no source key and are skipped.
    """
    inputs = []
    for seq, (change_type, row, _version) in enumerate(feed):
        key = _PROJECTOR.source_key(row)
        if key is None or change_type == "update_preimage":
            continue
        op = SlotDiffProcessor.DELETE if change_type == "delete" else SlotDiffProcessor.UPSERT
        inputs.append((op, key, row, seq))
    return inputs


def _stream(inputs, state=None) -> tuple[dict, list]:
    """Run *inputs* through the processor; return the final per-key state and every change."""
    processor = SlotDiffProcessor([_PROJECTOR])
    state = dict(state or {})
    changes = []
    for op, key, row, seq in inputs:
        state[key], emitted = processor.apply(
            state.get(key), map_id=_MAP_ID, source_key=key, op=op, row=row, seq=seq
        )
        changes.extend(emitted)
    return state, changes


def _sink(changes, baseline=None) -> dict:
    """The ``_live`` fold: highest ``seq`` wins per slot, tombstones are hidden."""
    latest = dict(baseline or {})
    for change in changes:
        slot = (change.map_id, change.source_key, change.predicate)
        if slot not in latest or change.seq > latest[slot][0]:
            latest[slot] = (change.seq, change)
    return {
        slot: (change.subject, change.object)
        for slot, (_seq, change) in latest.items()
        if not change.deleted
    }


def _non_blank_slots(slots: dict) -> dict:
    return {slot: value for slot, value in slots.items() if _non_blank(value[1])}


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_COMMITS, st.randoms(use_true_random=False))
def test_the_streamed_slots_fold_to_the_final_rows_and_the_auto_cdc_fold(commits, rnd):
    table, _null_key_rows, feed = _replay(commits)

    _state, changes = _stream(_inputs(feed))

    streamed = _sink(changes)
    assert streamed == _non_blank_slots(_slots_of(table))
    assert streamed == _non_blank_slots(_folded(feed, rnd))


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_COMMITS, st.data())
def test_late_and_replayed_events_are_no_ops(commits, data):
    table, _null_key_rows, feed = _replay(commits)
    inputs = _inputs(feed)
    replays = data.draw(st.lists(st.sampled_from(inputs), max_size=6)) if inputs else []

    state, changes = _stream(inputs)
    replayed_state, replayed = _stream(replays, state)

    assert replayed == []
    assert replayed_state == state
    assert _sink(changes) == _non_blank_slots(_slots_of(table))


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(_COMMITS, _COMMITS)
def test_a_stream_started_from_a_slots_snapshot_reaches_the_final_rows(before, after):
    """``handleInitialState`` loads ``_slots`` (blank objects included); the stream goes on."""
    snapshot_table, _rows, snapshot_feed = _replay(before)
    table, _null_key_rows, feed = _replay(before + after)
    snapshot_rows = [
        {"map_id": map_id, "source_key": key, "predicate": predicate, "subject": s, "object": o}
        for (map_id, key, predicate), (s, o) in _slots_of(snapshot_table).items()
    ]
    initial = {
        key: slots
        for (_map, key), slots in SlotDiffProcessor([_PROJECTOR])
        .initial_state(snapshot_rows)
        .items()
    }
    baseline = {
        (_MAP_ID, key, predicate): (SlotDiffProcessor.SNAPSHOT_SEQ, _Live(s, o))
        for key, slots in initial.items()
        for predicate, (s, o, _seq) in slots.items()
    }

    _state, changes = _stream(_inputs(feed)[len(_inputs(snapshot_feed)) :], initial)

    assert _sink(changes, baseline) == _non_blank_slots(_slots_of(table))


class _Live:
    """A baseline slot loaded from the snapshot (what the reader sees before any change)."""

    deleted = False

    def __init__(self, subject, obj):
        self.subject, self.object = subject, obj
