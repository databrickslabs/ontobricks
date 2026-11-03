"""``transformWithState`` adapter of :class:`SlotDiffProcessor` for the Real-Time-Mode stream.

The stream is grouped by ``(map_id, source_key)``. Each input row is an
envelope (:data:`ENVELOPE_FIELDS`): the op (``upsert`` / ``delete``), the
event ``seq`` (Kafka offset) and the source row as JSON. The adapter keeps the
key's slots in a ``MapState`` (predicate -> ``subject, object, seq``), applies
the pure :class:`SlotDiffProcessor` and yields one output row per
:class:`SlotChange` (:data:`OUTPUT_SCHEMA`). ``handleInitialState`` seeds the
state from the ``…_slots`` snapshot; state eviction is the caller's
``state_ttl_ms`` (``ttlDurationMs`` on the ``MapState``). The TTL must exceed
the maximum event lateness; without one the state keeps a watermark per
predicate of every key it saw (see :class:`SlotDiffProcessor`). The envelope's
row must be string-valued: the job casts every source column to STRING in
Spark before ``to_json`` (see :class:`SlotRowProjector`).

The module stays importable without ``pyspark``: the Spark types are imported
in a guarded block and the schemas fall back to DDL strings, which
``transformWithState`` and ``getMapState`` accept as well. The logic lives in
``back.core.streaming``, which imports the application package (the slot
compiler pulls in the SPARQL translator and the domain objects), so the job
needs the OntoBricks app package installed on the cluster.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from back.core.streaming import SlotChange, SlotDiffProcessor, SlotRowProjector

try:  # pragma: no cover - the cluster path; unit tests run without pyspark
    from pyspark.sql import Row
    from pyspark.sql.streaming.stateful_processor import StatefulProcessor
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    _SPARK_TYPES = {"STRING": StringType, "BOOLEAN": BooleanType, "BIGINT": LongType}
except ImportError:
    Row = None
    StatefulProcessor = object
    StructField = StructType = None
    _SPARK_TYPES = {}

#: Fields of an input (envelope) row, in order.
ENVELOPE_FIELDS = ("map_id", "source_key", "op", "seq", "row_json")

#: ``(name, SQL type)`` of an output row: the :class:`SlotChange` fields.
OUTPUT_FIELDS = (
    ("map_id", "STRING"),
    ("source_key", "STRING"),
    ("predicate", "STRING"),
    ("subject", "STRING"),
    ("object", "STRING"),
    ("object_hash", "STRING"),
    ("deleted", "BOOLEAN"),
    ("seq", "BIGINT"),
)
STATE_KEY_FIELDS = (("predicate", "STRING"),)
STATE_VALUE_FIELDS = (("subject", "STRING"), ("object", "STRING"), ("seq", "BIGINT"))


def _schema(fields: tuple[tuple[str, str], ...]) -> Any:
    """A ``StructType`` on the cluster, the equivalent DDL string without pyspark."""
    if StructType is None:
        return ", ".join(f"{name} {sql_type}" for name, sql_type in fields)
    return StructType(
        [StructField(name, _SPARK_TYPES[sql_type](), True) for name, sql_type in fields]
    )


OUTPUT_SCHEMA = _schema(OUTPUT_FIELDS)
STATE_KEY_SCHEMA = _schema(STATE_KEY_FIELDS)
STATE_VALUE_SCHEMA = _schema(STATE_VALUE_FIELDS)


class SlotDiffStatefulProcessor(StatefulProcessor):
    """Per ``(map_id, source_key)``: envelope rows in, slot-change rows out, slots in MapState."""

    STATE_NAME = "slots"

    def __init__(
        self, projectors: list[SlotRowProjector], *, state_ttl_ms: int | None = None
    ) -> None:
        self._diff = SlotDiffProcessor(projectors)
        self._ttl_ms = state_ttl_ms
        self._slots: Any = None

    def init(self, handle: Any) -> None:
        """Declare the slot ``MapState`` (with its TTL when the caller set one)."""
        ttl = {"ttlDurationMs": self._ttl_ms} if self._ttl_ms else {}
        self._slots = handle.getMapState(
            self.STATE_NAME, STATE_KEY_SCHEMA, STATE_VALUE_SCHEMA, **ttl
        )

    def handleInitialState(self, key: Any, initialState: Any, timerValues: Any = None) -> None:
        """Seed the key's state from ``…_slots`` snapshot rows (a Row, rows, or a frame)."""
        map_id, source_key = tuple(key)
        records = [
            {"map_id": map_id, "source_key": source_key, **record}
            for record in self._records(initialState)
        ]
        seeded = self._diff.initial_state(records).get((map_id, source_key), {})
        for predicate, value in seeded.items():
            self._slots.updateValue((predicate,), value)

    def handleInputRows(
        self, key: Any, rows: Iterable[Any], timerValues: Any = None
    ) -> Iterator[Any]:
        """Apply the key's envelope rows in ``seq`` order; yield one row per slot change."""
        map_id, source_key = tuple(key)
        before = self._load()
        state = dict(before)
        changes: list[SlotChange] = []
        for envelope in sorted((self._decode(row) for row in rows), key=lambda e: e["seq"]):
            state, emitted = self._diff.apply(
                state, map_id=map_id, source_key=source_key, **envelope
            )
            changes.extend(emitted)
        self._store(before, state)
        for change in changes:
            yield self._output_row(change)

    def close(self) -> None:
        """Nothing to release: the state store owns the state."""

    # ------------------------------------------------------------------
    # State and row codecs
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, tuple[str | None, str | None, int]]:
        """The key's state from ``MapState`` (``(predicate,) -> (subject, object, seq)``)."""
        return {
            state_key[0]: (value[0], value[1], int(value[2]))
            for state_key, value in self._slots.iterator()
        }

    def _store(self, before: Mapping, after: Mapping) -> None:
        """Write the new state back: remove dropped predicates, update the changed entries.

        An applied event stamps its ``seq`` on every entry (that is the
        ordering guard), so after one every entry is rewritten; a stale batch
        writes nothing.
        """
        for predicate in before:
            if predicate not in after:
                self._slots.removeKey((predicate,))
        for predicate, value in after.items():
            if before.get(predicate) != value:
                self._slots.updateValue((predicate,), value)

    @staticmethod
    def _decode(row: Any) -> dict:
        """``op`` / ``seq`` / ``row`` keyword arguments of :meth:`SlotDiffProcessor.apply`."""
        record = SlotDiffStatefulProcessor._as_dict(row)
        payload = record.get("row_json")
        return {
            "op": str(record["op"]),
            "seq": int(record["seq"]),
            "row": json.loads(payload) if payload else None,
        }

    @staticmethod
    def _output_row(change: SlotChange) -> Any:
        fields = change.as_row()
        return Row(**fields) if Row is not None else fields

    @staticmethod
    def _records(initial_state: Any) -> list[dict]:
        """Snapshot rows as dicts, from a Row / mapping, an iterable of them, or a frame."""
        if initial_state is None:
            return []
        if hasattr(initial_state, "to_dict"):  # a pandas frame (transformWithStateInPandas)
            return list(initial_state.to_dict("records"))
        if hasattr(initial_state, "asDict") or isinstance(initial_state, Mapping):
            return [SlotDiffStatefulProcessor._as_dict(initial_state)]
        return [SlotDiffStatefulProcessor._as_dict(row) for row in initial_state]

    @staticmethod
    def _as_dict(row: Any) -> dict:
        """A ``Row``, a mapping, or — as Real-Time Mode hands rows over — a plain tuple in :data:`ENVELOPE_FIELDS` order."""
        if hasattr(row, "asDict"):
            return row.asDict()
        if isinstance(row, Mapping):
            return dict(row)
        if isinstance(row, (tuple, list)):
            return dict(zip(ENVELOPE_FIELDS, row, strict=False))
        raise TypeError(f"Cannot read a stream row of type {type(row).__name__}")
