"""Property: AUTO CDC fold == SlotDiffProcessor fold == full-build projection.

The Real-Time-Mode stream turns every source-row change into
:class:`SlotChange` upserts and tombstones (:class:`SlotDiffProcessor`); the
``_live`` sink keeps, per ``(map_id, source_key, predicate)``, the change with
the highest ``seq`` (``ON CONFLICT ... WHERE EXCLUDED.seq > seq``) and readers
see the rows that are not tombstones. For any change history of a small
table read by three mappings (two keyed on ``id``, one on the composite
``region|num``), with key-changing updates, NULL keys and blank or tab
objects, this checks that the sink fold equals

1. the non-blank slots of the final rows (the full build), and
2. the AUTO CDC SCD TYPE 1 fold of the slot pipeline with its blank slots
   removed (the ``_data`` relation's ``TRIM(object) != ''``; ``TRIM`` strips
   spaces only, so a tab is not blank) —

for in-order and out-of-order delivery, replayed events, a start from a
``_slots`` snapshot, and state evicted by the TTL. The AUTO CDC model is the
one of ``test_slot_fold.py`` generalised to several mappings: it evaluates the
compiled slot-event SQL (``_sql_oracle``), and one test checks it against that
module's single-mapping model.

The stream's input rows are what the job's ``explode`` emits from a
Debezium-style event: an insert or update is an ``upsert`` of the after-image,
a delete a ``delete`` of the before-image key, and an update that changes a
mapping's key also deletes the old key.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from back.core.streaming import SlotDiffProcessor, SlotRowProjector
from tests.property import test_slot_fold
from tests.units.streaming.test_slot_row_projector import _sql_oracle

pytestmark = pytest.mark.property

_TABLE = "main.sales.things"
_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_MAPPINGS = [
    {
        "table": _TABLE,
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": "http://ex.org/Customer",
        "predicates": {
            _LABEL: {"type": "column", "column": "name"},
            "http://ex.org/age": {"type": "column", "column": "age"},
        },
    },
    {
        "table": _TABLE,
        "sql_query": None,
        "uri_template": "http://ex.org/Person/{id}",
        "id_column": "id",
        "label_column": None,
        "class_uri": "http://ex.org/Person",
        "predicates": {"http://ex.org/kind": {"type": "column", "column": "kind"}},
    },
    {
        "table": _TABLE,
        "sql_query": None,
        "uri_template": "http://ex.org/Order/{region}/{num}",
        "id_column": "region",
        "label_column": "name",
        "class_uri": "http://ex.org/Order",
        "predicates": {"http://ex.org/kind": {"type": "column", "column": "kind"}},
    },
]
_PROJECTORS = [SlotRowProjector.from_mapping(mapping) for mapping in _MAPPINGS]
_ORACLES = [(p.map_id, _sql_oracle(m)) for p, m in zip(_PROJECTORS, _MAPPINGS, strict=True)]
_DELETES = ("delete", "update_preimage")
_NULL_KEY = "<null key>"


def _row(ident, name, age, kind) -> dict:
    """A source row; ``region|num`` is a second key, unique whenever ``id`` is."""
    keyed = ident is not None
    return {
        "id": str(ident) if keyed else None,
        "region": ("eu" if ident % 2 else "us") if keyed else None,
        "num": str(ident // 2) if keyed else None,
        "name": name,
        "age": age,
        "kind": kind,
    }


def _non_blank(obj) -> bool:
    """``TRIM(object) != ''``: Spark and Postgres ``TRIM`` strip spaces only."""
    return obj is not None and obj.strip(" ") != ""


_VALUES = st.sampled_from([None, "", " ", "\t", "a", "b"])
#: Per commit, distinct ids with what to do to each: an id not in the table is
#: inserted; one in it is deleted, updated, deleted and inserted again
#: (``replace``) or moved to the id + 5 (``rekey``: every mapping key changes).
#: ``None`` inserts a row whose keys are all NULL.
_COMMITS = st.lists(
    st.dictionaries(
        st.one_of(st.integers(0, 4), st.none()),
        st.tuples(
            st.sampled_from(["delete", "update", "replace", "rekey"]), _VALUES, _VALUES, _VALUES
        ),
        max_size=4,
    ),
    max_size=8,
)


def _replay(commits) -> tuple[dict, list, list]:
    """Apply *commits* to an empty table: ``(final rows, change feed, Debezium events)``."""
    table: dict = {}
    feed: list = []  # (change_type, row, commit version) — the Delta change feed
    events: list = []  # (op, before, after) — the Kafka topic
    for version, commit in enumerate(commits, start=1):
        for ident, (action, *values) in commit.items():
            new = _row(ident, *values)
            if ident is None or ident not in table:
                feed.append(("insert", new, version))
                events.append(("c", None, new))
                if ident is not None:
                    table[ident] = new
                continue
            old = table[ident]
            if action == "delete":
                del table[ident]
                feed.append(("delete", old, version))
                events.append(("d", old, None))
            elif action == "replace":
                table[ident] = new
                feed += [("delete", old, version), ("insert", new, version)]
                events += [("d", old, None), ("c", None, new)]
            else:
                if action == "rekey" and ident + 5 not in table:
                    del table[ident]
                    ident, new = ident + 5, _row(ident + 5, *values)
                table[ident] = new
                feed += [("update_preimage", old, version), ("update_postimage", new, version)]
                events.append(("u", old, new))
    return table, feed, events


def _inputs(events) -> list[tuple[str, str, str, dict, int]]:
    """The stream's input rows ``(map_id, source_key, op, row, seq)``; ``seq`` is the offset.

    NULL-key rows go through the processor path too (under a placeholder key):
    the processor must ignore them.
    """
    inputs = []
    for seq, (op, before, after) in enumerate(events):
        for projector in _PROJECTORS:
            if op == "d":
                inputs.append(
                    (projector.map_id, projector.source_key(before), "delete", before, seq)
                )
                continue
            if op == "u":
                old_key = projector.source_key(before)
                if old_key is not None and old_key != projector.source_key(after):
                    inputs.append((projector.map_id, old_key, "delete", before, seq))
            key = projector.source_key(after)
            inputs.append((projector.map_id, key or _NULL_KEY, "upsert", after, seq))
    return inputs


def _stream(inputs, state=None, evict=frozenset()) -> tuple[dict, list]:
    """Run *inputs* through the processor (dropping the state of the *evict* positions)."""
    processor = SlotDiffProcessor(_PROJECTORS)
    state = dict(state or {})
    changes = []
    for position, (map_id, key, op, row, seq) in enumerate(inputs):
        if position in evict:
            state.pop((map_id, key), None)
        state[(map_id, key)], emitted = processor.apply(
            state.get((map_id, key)), map_id=map_id, source_key=key, op=op, row=row, seq=seq
        )
        changes.extend(emitted)
    return state, changes


def _sink(changes, baseline=None) -> dict:
    """The ``_live`` fold: highest ``seq`` wins per slot, tombstones are hidden."""
    latest = dict(baseline or {})
    for change in changes:
        slot = (change.map_id, change.source_key, change.predicate)
        if slot not in latest or change.seq > latest[slot][0]:
            latest[slot] = (change.seq, None if change.deleted else (change.subject, change.object))
    return {slot: value for slot, (_seq, value) in latest.items() if value is not None}


def _final(table: dict) -> dict:
    """The full build: the non-blank slots of every final row, per mapping."""
    slots = {}
    for row in table.values():
        for map_id, oracle in _ORACLES:
            key, subject, row_slots = oracle(row)
            for predicate, obj in row_slots.items():
                if _non_blank(obj):
                    slots[(map_id, key, predicate)] = (subject, obj)
    return slots


def _auto_cdc(feed, rnd, oracles=_ORACLES) -> dict:
    """AUTO CDC SCD TYPE 1 over the slot events of *feed*, delivered in any order."""
    events = []
    for change_type, row, version in feed:
        rank = 0 if change_type in _DELETES else 1
        op = "DELETE" if change_type in _DELETES else "UPSERT"
        for map_id, oracle in oracles:
            projected = oracle(row)
            if projected is None:
                continue  # WHERE <key> IS NOT NULL
            key, subject, slots = projected
            for predicate, obj in slots.items():
                events.append(((map_id, key, predicate), (version, rank), op, (subject, obj)))
    rnd.shuffle(events)
    latest: dict = {}
    for slot, seq, op, value in events:
        if slot not in latest or seq > latest[slot][0]:
            latest[slot] = (seq, op, value)
    return {slot: value for slot, (_seq, op, value) in latest.items() if op == "UPSERT"}


def _non_blank_slots(slots: dict) -> dict:
    return {slot: value for slot, value in slots.items() if _non_blank(value[1])}


_SETTINGS = settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@_SETTINGS
@given(_COMMITS, st.randoms(use_true_random=False))
def test_the_streamed_slots_are_the_full_build_and_the_auto_cdc_fold(commits, rnd):
    table, feed, events = _replay(commits)

    _state, changes = _stream(_inputs(events))

    assert _sink(changes) == _final(table)
    assert _sink(changes) == _non_blank_slots(_auto_cdc(feed, rnd))


@_SETTINGS
@given(_COMMITS, st.randoms(use_true_random=False))
def test_out_of_order_delivery_reaches_the_same_slots(commits, rnd):
    """Events of different keys interleave and arrive late: stale ones are no-ops."""
    table, _feed, events = _replay(commits)
    inputs = _inputs(events)
    rnd.shuffle(inputs)

    _state, changes = _stream(inputs)

    assert _sink(changes) == _final(table)


@_SETTINGS
@given(_COMMITS, st.data())
def test_late_and_replayed_events_are_no_ops(commits, data):
    table, _feed, events = _replay(commits)
    inputs = _inputs(events)
    replays = data.draw(st.lists(st.sampled_from(inputs), max_size=6)) if inputs else []

    state, changes = _stream(inputs)
    replayed_state, replayed = _stream(replays, state)

    assert replayed == []
    assert replayed_state == state
    assert _sink(changes) == _final(table)


@_SETTINGS
@given(_COMMITS, st.data())
def test_state_evicted_by_the_ttl_still_reaches_the_final_rows(commits, data):
    """In-order delivery (the TTL exceeds the lateness): an evicted key's next event repairs it."""
    table, _feed, events = _replay(commits)
    inputs = _inputs(events)
    evict = data.draw(st.sets(st.integers(0, max(len(inputs) - 1, 0)), max_size=8))

    _state, changes = _stream(inputs, evict=evict)

    assert _sink(changes) == _final(table)


@_SETTINGS
@given(_COMMITS, _COMMITS, st.randoms(use_true_random=False))
def test_a_stream_started_from_a_slots_snapshot_reaches_the_final_rows(before, after, rnd):
    """``handleInitialState`` loads ``_slots`` (blank objects included); the stream goes on."""
    _snapshot_table, snapshot_feed, snapshot_events = _replay(before)
    table, _feed, events = _replay(before + after)
    snapshot = _auto_cdc(snapshot_feed, rnd)  # what ``_slots`` holds, blank slots included
    rows = [
        {"map_id": map_id, "source_key": key, "predicate": predicate, "subject": s, "object": o}
        for (map_id, key, predicate), (s, o) in snapshot.items()
    ]
    initial = SlotDiffProcessor(_PROJECTORS).initial_state(rows)
    baseline = {
        slot: (SlotDiffProcessor.SNAPSHOT_SEQ, value)
        for slot, value in _non_blank_slots(snapshot).items()
    }
    suffix = [entry for entry in _inputs(events) if entry[4] >= len(snapshot_events)]

    _state, changes = _stream(suffix, initial)

    assert _sink(changes, baseline) == _final(table)


@_SETTINGS
@given(test_slot_fold._COMMITS, st.randoms(use_true_random=False))
def test_the_generic_auto_cdc_model_is_the_slot_fold_model(commits, rnd):
    """This module's AUTO CDC model reproduces ``test_slot_fold``'s on its own mapping."""
    _table, _null_key_rows, feed = test_slot_fold._replay(commits)
    string_feed = [
        (change_type, {**row, "id": None if row["id"] is None else str(row["id"])}, version)
        for change_type, row, version in feed
    ]
    oracles = [(test_slot_fold._MAP_ID, _sql_oracle(test_slot_fold._MAPPING))]

    assert _auto_cdc(string_feed, rnd, oracles) == test_slot_fold._folded(feed, rnd)
