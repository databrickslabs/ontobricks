"""Property: the topic slot pipeline folds to the same ``_slots`` as the change-feed one.

In ``realtime`` mode the slot pipeline reads the Debezium topic instead of the
source tables' change data feed (``TopicSlotSource``). For any change history
of the table read by ``test_slot_diff_property``'s three mappings (one with a
composite key; key-changing updates; NULL keys; blank and tab objects), the
AUTO CDC fold of the topic variant's slot events equals the fold of the
change-feed variant's — blank slots included, since both keep them for
``_data`` to filter. The topic records come in Debezium's shapes: optionally
under ``payload``, with column names in any case, ``source.table`` as a full
or a bare table name, interleaved with records of an unmapped table.

The topic event model encodes the rules of the compiled view; the last class
checks those rules against the SQL the compiler emits.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from back.core.streaming import SlotProjectionCompiler, TopicSlotSource, TopicSource
from tests.property.test_slot_diff_property import (
    _COMMITS,
    _MAPPINGS,
    _PROJECTORS,
    _TABLE,
    _auto_cdc,
    _replay,
)

pytestmark = pytest.mark.property

_TOPIC = TopicSource("b:9093", "cdc.things", "kafka-scope", "jaas")
_UNMAPPED = "main.sales.other"
_TABLE_NAMES = [_TABLE, _TABLE.upper(), "things", "inventory.things"]


def _key(image, columns) -> str | None:
    """``source_key`` of an image: ``concat_ws('|', ...)`` skips NULLs (a NULL map gives '')."""
    values = [None if image is None else image.get(column) for column in columns]
    if len(values) == 1:
        return values[0]
    return "|".join(value for value in values if value is not None)


def _field(envelope: dict, name: str):
    """``coalesce(e.payload.<name>, e.<name>)``."""
    value = (envelope.get("payload") or {}).get(name)
    return value if value is not None else envelope.get(name)


def _lowered(image: dict) -> dict:
    """``transform_keys(<image>, (k, v) -> lower(k))``."""
    return {column.lower(): value for column, value in image.items()}


def _topic_events(records, mapping, map_id):
    """The slot events of *mapping*'s topic view for Kafka *records* (``(offset, value)``)."""
    compiler = SlotProjectionCompiler
    prefix, id_column = compiler.subject_parts(mapping)
    keys = [column.lower() for column in compiler.key_columns(mapping)]
    sources = compiler.slot_sources(mapping)
    full, leaf = mapping["table"].lower(), mapping["table"].rsplit(".", 1)[-1].lower()
    for offset, envelope in records:
        op = (_field(envelope, "op") or "").lower()
        before, after = (
            None if _field(envelope, n) is None else _lowered(_field(envelope, n))
            for n in ("before", "after")
        )
        table = ((_field(envelope, "source") or {}).get("table") or "").lower()
        if table != full and table.rsplit(".", 1)[-1] != leaf:
            continue
        current = before if op == "d" else after if op in ("c", "u", "r") else None
        changed = op == "u" and _key(before, keys) != _key(after, keys)
        images = [
            (current, op == "d", 0 if op == "d" else 1),
            (before if changed else None, True, 0),
        ]
        for image, is_delete, rank in images:
            if image is None or any(image.get(column) is None for column in keys):
                continue
            key = _key(image, keys)
            subject = (
                None if image.get(id_column.lower()) is None else prefix + image[id_column.lower()]
            )
            for predicate, literal, column in sources:
                obj = literal if column is None else (image.get(column.lower()) or "")
                op_out = "DELETE" if is_delete or obj is None else "UPSERT"
                yield (map_id, key, predicate), (offset, rank), op_out, (subject, obj)


def _fold(events, rnd) -> dict:
    events = list(events)
    rnd.shuffle(events)
    latest: dict = {}
    for slot, seq, op, value in events:
        if slot not in latest or seq > latest[slot][0]:
            latest[slot] = (seq, op, value)
    return {slot: value for slot, (_seq, op, value) in latest.items() if op == "UPSERT"}


def _recase(image, upper: bool):
    if image is None or not upper:
        return image
    return {column.upper(): value for column, value in image.items()}


@st.composite
def _records(draw, events):
    """Kafka records of *events* in Debezium shapes, with an unmapped table's records mixed in."""
    records = []
    for op, before, after in events:
        if draw(st.booleans()) and draw(st.booleans()):
            noise = {"id": "1", "name": "x", "age": "1", "kind": "k", "region": "eu", "num": "0"}
            records.append(
                {"op": "c", "before": None, "after": noise, "source": {"table": _UNMAPPED}}
            )
        upper = draw(st.booleans())
        record = {
            "op": op,
            "before": _recase(before, upper),
            "after": _recase(after, upper),
            "source": {"table": draw(st.sampled_from(_TABLE_NAMES))},
        }
        records.append({"payload": record} if draw(st.booleans()) else record)
    return list(enumerate(records))


_SETTINGS = settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@_SETTINGS
@given(_COMMITS, st.data(), st.randoms(use_true_random=False))
def test_the_topic_variant_folds_to_the_change_feed_slots(commits, data, rnd):
    _table, feed, events = _replay(commits)
    records = data.draw(_records(events))

    topic = _fold(
        (
            event
            for mapping, projector in zip(_MAPPINGS, _PROJECTORS, strict=True)
            for event in _topic_events(records, mapping, projector.map_id)
        ),
        rnd,
    )

    assert topic == _auto_cdc(feed, rnd)


class TestTheModelMatchesTheSQL:
    """The rules the topic event model encodes, read off the view the compiler emits."""

    @staticmethod
    def _view(mapping=None) -> str:
        return TopicSlotSource.slot_event_view_sql(
            mapping or _MAPPINGS[2], view_name="v", topic=_TOPIC
        )

    def test_envelopes_are_read_under_payload_with_lower_cased_columns(self):
        view = self._view()

        assert "lower(coalesce(e.payload.op, e.op)) AS _op" in view
        assert "transform_keys(coalesce(e.payload.after, e.after), (k, v) -> lower(k))" in view
        assert "lower(coalesce(e.payload.source.table, e.source.table)) AS _table" in view

    def test_images_ops_and_ranks_are_derived_as_modelled(self):
        view = self._view()

        assert (
            "named_struct('img', CASE WHEN c._op = 'd' THEN c._before "
            "WHEN c._op IN ('c', 'u', 'r') THEN c._after END,\n"
            "               'is_delete', c._op = 'd', 'seq_rank', "
            "CASE WHEN c._op = 'd' THEN 0 ELSE 1 END)"
        ) in view
        assert (
            "CASE WHEN c._op = 'u' AND NOT (concat_ws('|', c._before['region'], c._before['num']) "
            "<=> concat_ws('|', c._after['region'], c._after['num'])) THEN c._before END,\n"
            "               'is_delete', true, 'seq_rank', 0"
        ) in view
        assert (
            "CASE WHEN image.is_delete OR s.object IS NULL THEN 'DELETE' ELSE 'UPSERT' END" in view
        )

    def test_keys_tables_and_objects_are_filtered_and_projected_as_modelled(self):
        view = self._view()

        assert "concat_ws('|', image.img['region'], image.img['num']) AS source_key" in view
        assert (
            "WHERE (c._table = 'main.sales.things' OR substring_index(c._table, '.', -1) = "
            "'things') AND image.img['region'] IS NOT NULL AND image.img['num'] IS NOT NULL"
        ) in view
        assert "COALESCE(image.img['kind'], '')" in view
        assert "CONCAT('http://ex.org/Order/', image.img['region']) AS subject" in view

    def test_the_offset_sequences_the_events(self):
        view = self._view()

        assert "SELECT offset AS _commit_version" in view
        assert "image.seq_rank AS seq_rank" in view
