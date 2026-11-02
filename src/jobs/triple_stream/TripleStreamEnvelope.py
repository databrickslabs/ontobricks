"""Debezium-style change envelopes of the realtime stream, exploded into adapter input rows."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from back.core.errors import ValidationError
from back.core.logging import get_logger
from jobs.triple_stream.SparkColumnText import SparkColumnText

logger = get_logger(__name__)

#: Debezium operation codes: create, update, delete, read (snapshot).
_DELETE_OPS = ("d",)
_UPSERT_OPS = ("c", "u", "r")


class TripleStreamEnvelope:
    """Parse one change envelope and explode it into ``(map_id, source_key, op, seq, row_json)`` rows.

    Pure Python (it runs inside ``mapInPandas`` on the executors): the
    envelope is ``{"op", "after", "before", "source": {"table"}, "ts_ms"}``,
    optionally under ``"payload"``. ``source.table`` is matched against the
    mappings' tables — a ``catalog.schema.table`` exactly, a bare table name
    (stock Debezium) by its leaf name, case-insensitively.
    """

    @staticmethod
    def parse(value: Any) -> dict | None:
        """``{"op", "row", "before", "table"}`` of a change envelope; ``None`` without a change.

        A delete's ``row`` is its ``before`` image, any other operation's its
        ``after`` image (``before`` then is the pre-image of an update, or
        ``None``). Unparsable JSON, a Kafka tombstone (``null`` value), an
        unknown operation or a missing image is skipped.
        """
        if value is None:
            return None
        try:
            record = json.loads(value if isinstance(value, str) else bytes(value).decode("utf-8"))
        except (ValueError, TypeError, UnicodeDecodeError):
            return None
        if isinstance(record, Mapping) and isinstance(record.get("payload"), Mapping):
            record = record["payload"]
        if not isinstance(record, Mapping):
            return None
        op = str(record.get("op") or "").lower()
        source = record.get("source") if isinstance(record.get("source"), Mapping) else {}
        table = str(source.get("table") or "").strip().lower()
        before = record.get("before") if isinstance(record.get("before"), Mapping) else None
        if op in _DELETE_OPS:
            row, kind, before = before, "delete", None
        elif op in _UPSERT_OPS:
            row, kind = record.get("after"), "upsert"
        else:
            return None
        if not table or not isinstance(row, Mapping):
            return None
        return {"op": kind, "row": dict(row), "before": before, "table": table}

    @staticmethod
    def projectors_by_table(projectors: Iterable[Any]) -> dict[str, list]:
        """The projectors of each source table (lower-cased ``catalog.schema.table``)."""
        by_table: dict[str, list] = {}
        for projector in projectors:
            by_table.setdefault(str(projector.source_table).lower(), []).append(projector)
        return by_table

    @staticmethod
    def table_of(name: str, projectors_by_table: Mapping) -> str | None:
        """The mapped table an envelope's ``source.table`` names, ``None`` when none / ambiguous."""
        if name in projectors_by_table:
            return name
        leaf = name.rsplit(".", 1)[-1]
        matches = [table for table in projectors_by_table if table.rsplit(".", 1)[-1] == leaf]
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def explode(
        envelope: Mapping,
        seq: int,
        projectors_by_table: Mapping,
        column_types: Mapping | None = None,
    ) -> list[dict]:
        """One adapter input row per mapping of the envelope's table (``ENVELOPE_FIELDS``).

        Every value is first turned into the text ``CAST(… AS STRING)`` gives
        it (:class:`SparkColumnText`, typed by *column_types* — the source
        table's ``{column: spark type}``), so the adapter only sees strings,
        exactly like the slot SQL. An update that changes a mapping's key also
        tombstones the old key (``delete`` from the pre-image), as the AUTO CDC
        fold does through the pre-image. A key that is NULL for a mapping
        yields nothing for it, like the slot-event view's ``WHERE <key> IS NOT
        NULL``. An unmapped table yields nothing.
        """
        table = TripleStreamEnvelope.table_of(envelope["table"], projectors_by_table)
        if table is None:
            return []
        types = (column_types or {}).get(table, {})
        row = SparkColumnText.row(envelope["row"], types)
        before = SparkColumnText.row(envelope["before"], types) if envelope.get("before") else None
        rows = []
        for projector in projectors_by_table[table]:
            key = projector.source_key(row)
            old = projector.source_key(before) if before is not None else None
            if envelope["op"] == "upsert" and old is not None and old != key:
                rows.append(TripleStreamEnvelope._input_row(projector, old, "delete", seq, before))
            if key is not None:
                rows.append(TripleStreamEnvelope._input_row(projector, key, envelope["op"], seq, row))
        return rows

    @staticmethod
    def explode_records(
        records: Iterable[tuple[Any, Any]],
        projectors_by_table: Mapping,
        column_types: Mapping | None = None,
        *,
        on_skip: Callable[[str], None] | None = None,
    ) -> list[dict]:
        """:meth:`explode` every ``(value, seq)`` record; a record that cannot be used is skipped.

        Skipped — and reported to *on_skip* with the reason, never raised, so
        one bad record cannot stop the stream: unparsable or change-less
        envelopes (``unparsable``), records without a seq (``no_seq``), an
        unmapped or ambiguous table (``unmapped_table``), and a value its
        column type cannot read (``bad_value``).
        """
        skip = on_skip or (lambda reason: None)
        rows: list[dict] = []
        for value, seq in records:
            envelope = TripleStreamEnvelope.parse(value)
            if envelope is None:
                skip("unparsable")
                continue
            if seq is None:
                skip("no_seq")
                continue
            try:
                exploded = TripleStreamEnvelope.explode(
                    envelope, int(seq), projectors_by_table, column_types
                )
            except (ValueError, ValidationError) as exc:
                logger.warning("Skipped a change of %s: %s", envelope["table"], exc)
                skip("bad_value")
                continue
            if not exploded and TripleStreamEnvelope.table_of(
                envelope["table"], projectors_by_table
            ) is None:
                skip("unmapped_table")
            rows.extend(exploded)
        return rows

    @staticmethod
    def _input_row(projector: Any, key: str, op: str, seq: int, row: Mapping) -> dict:
        return {
            "map_id": projector.map_id,
            "source_key": key,
            "op": op,
            "seq": int(seq),
            "row_json": json.dumps(row, sort_keys=True),
        }
