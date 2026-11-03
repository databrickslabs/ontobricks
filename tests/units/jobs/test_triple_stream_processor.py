"""The transformWithState adapter decodes envelopes, keeps MapState and encodes SlotChanges.

PySpark is not installed in the unit-test environment: the adapter's module
must import without it, and a fake ``StatefulProcessorHandle`` / ``MapState``
stand in for the Spark state store.
"""

from __future__ import annotations

import json

import pytest

from back.core.streaming import SlotChange, SlotRowProjector
from jobs.triple_stream import processor as adapter
from jobs.triple_stream.processor import SlotDiffStatefulProcessor

pytestmark = pytest.mark.unit

_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_CUSTOMER = "http://ex.org/Customer"
_PROJECTOR = SlotRowProjector.from_mapping(
    {
        "table": "main.sales.customers",
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": _CUSTOMER,
        "predicates": {_LABEL: {"type": "column", "column": "name"}},
    }
)
_KEY = (_PROJECTOR.map_id, "1")


class _FakeMapState:
    """The subset of ``pyspark`` ``MapState`` the adapter uses (tuple keys and values)."""

    def __init__(self) -> None:
        self.data: dict = {}
        self.updates = 0
        self.removals = 0

    def iterator(self):
        return iter(list(self.data.items()))

    def updateValue(self, key, value):
        self.updates += 1
        self.data[tuple(key)] = tuple(value)

    def removeKey(self, key):
        self.removals += 1
        self.data.pop(tuple(key), None)


class _FakeHandle:
    def __init__(self) -> None:
        self.calls: list = []
        self.state = _FakeMapState()

    def getMapState(self, name, key_schema, value_schema, **kwargs):
        self.calls.append((name, key_schema, value_schema, kwargs))
        return self.state


def _processor(**kwargs):
    proc = SlotDiffStatefulProcessor([_PROJECTOR], **kwargs)
    handle = _FakeHandle()
    proc.init(handle)
    return proc, handle


def _envelope(op, seq, row=None):
    return {
        "map_id": _KEY[0],
        "source_key": _KEY[1],
        "op": op,
        "seq": seq,
        "row_json": None if row is None else json.dumps(row),
    }


class TestSchemas:
    def test_the_envelope_fields_are_the_contract(self):
        assert adapter.ENVELOPE_FIELDS == ("map_id", "source_key", "op", "seq", "row_json")

    def test_the_output_schema_follows_the_slot_change_fields(self):
        fields = [name for name, _type in adapter.OUTPUT_FIELDS]

        assert fields == list(SlotChange.__dataclass_fields__)

    def test_without_pyspark_the_schemas_are_ddl_strings(self):
        assert adapter.OUTPUT_SCHEMA == (
            "map_id STRING, source_key STRING, predicate STRING, subject STRING, "
            "object STRING, object_hash STRING, deleted BOOLEAN, seq BIGINT"
        )
        assert adapter.STATE_KEY_SCHEMA == "predicate STRING"
        assert adapter.STATE_VALUE_SCHEMA == "subject STRING, object STRING, seq BIGINT"


class TestInit:
    def test_init_declares_the_slot_map_state(self):
        _proc, handle = _processor()

        assert handle.calls == [("slots", adapter.STATE_KEY_SCHEMA, adapter.STATE_VALUE_SCHEMA, {})]

    def test_a_state_ttl_is_passed_to_the_map_state(self):
        _proc, handle = _processor(state_ttl_ms=3_600_000)

        assert handle.calls[0][3] == {"ttlDurationMs": 3_600_000}


class TestHandleInputRows:
    def test_an_insert_yields_one_output_row_per_slot_change(self):
        proc, handle = _processor()

        rows = list(
            proc.handleInputRows(
                _KEY, iter([_envelope("upsert", 3, {"id": 1, "name": "Ann"})]), None
            )
        )

        assert [(r["predicate"], r["object"], r["deleted"], r["seq"]) for r in rows] == [
            (_RDF_TYPE, _CUSTOMER, False, 3),
            (_LABEL, "Ann", False, 3),
        ]
        assert list(rows[0]) == [name for name, _type in adapter.OUTPUT_FIELDS]
        assert handle.state.data[(_LABEL,)] == ("http://ex.org/Customer/1", "Ann", 3)

    def test_a_real_time_mode_row_is_a_plain_tuple_in_envelope_order(self):
        """Real-Time Mode hands ``handleInputRows`` tuples, not ``Row`` objects (measured live)."""
        proc, _handle = _processor()
        envelope = _envelope("upsert", 3, {"id": 1, "name": "Ann"})
        as_tuple = tuple(envelope[name] for name in adapter.ENVELOPE_FIELDS)

        rows = list(proc.handleInputRows(_KEY, iter([as_tuple]), None))

        assert [(r["predicate"], r["object"], r["seq"]) for r in rows] == [
            (_RDF_TYPE, _CUSTOMER, 3),
            (_LABEL, "Ann", 3),
        ]

    def test_an_unreadable_row_type_is_refused(self):
        proc, _handle = _processor()

        with pytest.raises(TypeError, match="Cannot read a stream row"):
            list(proc.handleInputRows(_KEY, iter([42]), None))

    def test_the_rows_of_a_batch_are_applied_in_seq_order(self):
        proc, _handle = _processor()
        batch = [
            _envelope("upsert", 5, {"id": 1, "name": "Bob"}),
            _envelope("upsert", 4, {"id": 1, "name": "Ann"}),
        ]

        rows = list(proc.handleInputRows(_KEY, iter(batch), None))

        assert [
            (r["predicate"], r["object"], r["seq"]) for r in rows if r["predicate"] == _LABEL
        ] == [
            (_LABEL, "Ann", 4),
            (_LABEL, "Bob", 5),
        ]

    def test_a_delete_tombstones_the_stored_slots_and_keeps_a_watermark(self):
        proc, handle = _processor()
        list(
            proc.handleInputRows(
                _KEY, iter([_envelope("upsert", 3, {"id": 1, "name": "Ann"})]), None
            )
        )

        rows = list(proc.handleInputRows(_KEY, iter([_envelope("delete", 4)]), None))

        assert {(r["predicate"], r["deleted"]) for r in rows} == {(_LABEL, True), (_RDF_TYPE, True)}
        assert handle.state.data[(_LABEL,)] == (None, None, 4)

    def test_an_evicted_key_still_tombstones_its_slots_on_delete(self):
        """The TTL emptied the MapState: the delete must still reach ``_live``."""
        proc, handle = _processor()
        list(
            proc.handleInputRows(
                _KEY, iter([_envelope("upsert", 3, {"id": 1, "name": "Ann"})]), None
            )
        )
        handle.state.data.clear()  # TTL eviction

        rows = list(proc.handleInputRows(_KEY, iter([_envelope("delete", 4)]), None))

        assert {(r["predicate"], r["deleted"]) for r in rows} == {
            (_LABEL, True),
            (_RDF_TYPE, True),
        }

    def test_a_stale_row_writes_no_state_and_yields_nothing(self):
        proc, handle = _processor()
        list(
            proc.handleInputRows(
                _KEY, iter([_envelope("upsert", 3, {"id": 1, "name": "Ann"})]), None
            )
        )
        writes = handle.state.updates

        rows = list(
            proc.handleInputRows(_KEY, iter([_envelope("upsert", 2, {"id": 1, "name": "X"})]), None)
        )

        assert rows == []
        assert handle.state.updates == writes

    def test_spark_rows_are_read_by_field_name(self):
        proc, _handle = _processor()
        envelope = _RowLike(_envelope("upsert", 3, {"id": 1, "name": "Ann"}))

        rows = list(proc.handleInputRows(_KEY, iter([envelope]), None))

        assert len(rows) == 2


class _RowLike(tuple):
    """A ``pyspark.sql.Row`` stand-in: a tuple with ``asDict`` and name lookups."""

    def __new__(cls, fields: dict):
        obj = super().__new__(cls, tuple(fields.values()))
        obj._fields = dict(fields)
        return obj

    def asDict(self):
        return dict(self._fields)

    def __getitem__(self, item):
        if isinstance(item, str):
            return self._fields[item]
        return super().__getitem__(item)


class TestHandleInitialState:
    def test_the_slots_snapshot_seeds_the_map_state(self):
        proc, handle = _processor()
        snapshot = {
            "map_id": _KEY[0],
            "source_key": "1",
            "predicate": _LABEL,
            "subject": "http://ex.org/Customer/1",
            "object": "Ann",
        }

        proc.handleInitialState(_KEY, _RowLike(snapshot), None)

        assert handle.state.data == {(_LABEL,): ("http://ex.org/Customer/1", "Ann", -1)}

    def test_an_initial_state_frame_seeds_every_row(self):
        proc, handle = _processor()
        frame = _Frame(
            [
                {
                    "map_id": _KEY[0],
                    "source_key": "1",
                    "predicate": _LABEL,
                    "subject": "s",
                    "object": "Ann",
                },
                {
                    "map_id": _KEY[0],
                    "source_key": "1",
                    "predicate": _RDF_TYPE,
                    "subject": "s",
                    "object": _CUSTOMER,
                },
            ]
        )

        proc.handleInitialState(_KEY, frame, None)

        assert set(handle.state.data) == {(_LABEL,), (_RDF_TYPE,)}

    def test_the_first_event_diffs_against_the_snapshot(self):
        proc, _handle = _processor()
        proc.handleInitialState(
            _KEY,
            [
                {
                    "map_id": _KEY[0],
                    "source_key": "1",
                    "predicate": _LABEL,
                    "subject": "http://ex.org/Customer/1",
                    "object": "Ann",
                },
                {
                    "map_id": _KEY[0],
                    "source_key": "1",
                    "predicate": _RDF_TYPE,
                    "subject": "http://ex.org/Customer/1",
                    "object": _CUSTOMER,
                },
            ],
            None,
        )

        rows = list(
            proc.handleInputRows(
                _KEY, iter([_envelope("upsert", 0, {"id": 1, "name": "Ann"})]), None
            )
        )

        assert rows == []


class _Frame:
    """A pandas-DataFrame stand-in (``transformWithStateInPandas`` passes frames)."""

    def __init__(self, records):
        self._records = records

    def to_dict(self, orient):
        assert orient == "records"
        return list(self._records)


def test_close_is_a_no_op():
    proc, _handle = _processor()

    assert proc.close() is None
