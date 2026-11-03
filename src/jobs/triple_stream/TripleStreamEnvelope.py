"""Debezium-style change envelopes of the realtime stream, exploded into adapter input rows."""

from __future__ import annotations

import json
from collections import Counter
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
        column type cannot read (``bad_value``: a wrong shape — ``TypeError`` —,
        an out-of-range number, date or decimal — ``ArithmeticError`` — or an
        unreadable one — ``ValueError``). A raise here would fail every retry on
        the same offset.
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
            except (ValueError, TypeError, ArithmeticError, ValidationError) as exc:
                logger.warning(
                    "Skipped a change of %s (%s): %s", envelope["table"], type(exc).__name__, exc
                )
                skip("bad_value")
                continue
            if not exploded and TripleStreamEnvelope.table_of(
                envelope["table"], projectors_by_table
            ) is None:
                skip("unmapped_table")
            rows.extend(exploded)
        return rows

    @staticmethod
    def explode_sql(projectors_by_table: Mapping, column_types: Mapping, records_view: str) -> str:
        """Spark SQL over *records_view* (``value STRING, seq BIGINT``) yielding ``ENVELOPE_FIELDS`` rows.

        The Spark-native twin of :meth:`explode`, for Real-Time Mode, which refuses
        the ``mapInPandas`` operator (``OPERATOR_OR_SINK_NOT_IN_ALLOWLIST``) and a
        ``UNION`` of several reads of the same stream
        (``IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED``) — both measured live — so
        this is one pass: every candidate row of every mapping is a
        ``named_struct`` in one ``array``, ``filter``ed on its ``keep`` flag and
        ``explode``d. Same rules as :meth:`explode`: the envelope's ``payload``
        wrapper is optional; ``c`` / ``u`` / ``r`` upsert the ``after`` image, ``d``
        deletes the ``before`` image; the table is matched by its full name, or by
        its leaf name when that leaf is unique among the mapped tables; every value
        is the text ``CAST(… AS STRING)`` gives it (``from_json`` typed by
        *column_types*, ``STRING`` for a column the types do not list); the key is
        the compiler's cast (``concat_ws('|', …)`` for a composite key) and a NULL
        key yields nothing; an update whose key changed also tombstones the old key
        from the pre-image.
        """
        env = (
            "WITH env AS (SELECT seq, "
            "lower(coalesce(get_json_object(value, '$.payload.op'), get_json_object(value, '$.op'))) AS op, "
            "coalesce(get_json_object(value, '$.payload.after'), get_json_object(value, '$.after')) AS after, "
            "coalesce(get_json_object(value, '$.payload.before'), get_json_object(value, '$.before')) AS before, "
            "lower(trim(coalesce(get_json_object(value, '$.payload.source.table'), "
            f"get_json_object(value, '$.source.table')))) AS src FROM {records_view})"
        )
        leaves = Counter(table.rsplit(".", 1)[-1] for table in projectors_by_table)
        images, candidates = [], []
        upserts = ", ".join(f"'{op}'" for op in _UPSERT_OPS)
        deletes = ", ".join(f"'{op}'" for op in _DELETE_OPS)
        for index, (table, projectors) in enumerate(projectors_by_table.items()):
            leaf = table.rsplit(".", 1)[-1]
            match = f"src = '{table}'"
            if leaves[leaf] == 1:
                match = f"({match} OR element_at(split(src, '[.]'), -1) = '{leaf}')"
            columns = TripleStreamEnvelope._typed_columns(projectors, (column_types or {}).get(table))
            ddl = ", ".join(f"`{column}` {spark_type}" for column, spark_type in columns.items())
            a, b = f"a{index}", f"b{index}"
            images.append(f"from_json(after, '{ddl}') AS {a}, from_json(before, '{ddl}') AS {b}")
            for projector in projectors:
                key_a = TripleStreamEnvelope._key_sql(a, projector.key_columns)
                key_b = TripleStreamEnvelope._key_sql(b, projector.key_columns)
                row_a = f"to_json({TripleStreamEnvelope._text_struct_sql(a, columns)})"
                row_b = f"to_json({TripleStreamEnvelope._text_struct_sql(b, columns)})"
                candidates.append(TripleStreamEnvelope._candidate(
                    projector.map_id, key_a, "upsert", row_a,
                    f"{match} AND op IN ({upserts}) AND {a} IS NOT NULL AND {key_a} IS NOT NULL",
                ))
                candidates.append(TripleStreamEnvelope._candidate(
                    projector.map_id, key_b, "delete", row_b,
                    f"{match} AND op IN ({deletes}) AND {b} IS NOT NULL AND {key_b} IS NOT NULL",
                ))
                candidates.append(TripleStreamEnvelope._candidate(
                    projector.map_id, key_b, "delete", row_b,
                    f"{match} AND op IN ({upserts}) AND {a} IS NOT NULL AND {b} IS NOT NULL "
                    f"AND {key_b} IS NOT NULL AND {key_b} <> coalesce({key_a}, '')",
                ))
        if not candidates:
            return (
                f"{env} SELECT CAST(NULL AS STRING) AS map_id, CAST(NULL AS STRING) AS source_key, "
                "CAST(NULL AS STRING) AS op, seq, CAST(NULL AS STRING) AS row_json FROM env WHERE false"
            )
        typed = f"typed AS (SELECT seq, op, src, {', '.join(images)} FROM env)"
        return (
            f"{env}, {typed}\n"
            "SELECT r.map_id, r.source_key, r.op, seq, r.row_json FROM typed\n"
            "LATERAL VIEW explode(filter(array(\n  " + ",\n  ".join(candidates) + "\n), x -> x.keep)) rows AS r"
        )

    @staticmethod
    def _candidate(map_id: str, key: str, op: str, row_json: str, keep: str) -> str:
        """One ``named_struct`` of the explode array: a candidate row and the condition that keeps it."""
        return (
            f"named_struct('map_id', '{map_id}', 'source_key', {key}, 'op', '{op}', "
            f"'row_json', {row_json}, 'keep', ({keep}))"
        )

    @staticmethod
    def _typed_columns(projectors: Iterable[Any], types: Mapping | None) -> dict[str, str]:
        """``{column: spark type}`` of a table: its described types, plus any projector column as STRING."""
        columns = {str(column): str(spark_type) for column, spark_type in (types or {}).items()}
        for projector in projectors:
            for column in (*projector.key_columns, projector.id_column):
                if column:
                    columns.setdefault(str(column), "STRING")
            for _predicate, _literal, column in projector.sources:
                if column:
                    columns.setdefault(str(column), "STRING")
        return columns

    @staticmethod
    def _key_sql(image: str, key_columns: Iterable[str]) -> str:
        """The compiler's source-key cast over the typed *image* struct."""
        casts = [f"CAST({image}.`{column}` AS STRING)" for column in key_columns if column]
        if len(casts) == 1:
            return casts[0]
        return f"concat_ws('|', {', '.join(casts)})"

    @staticmethod
    def _text_struct_sql(image: str, columns: Mapping[str, str]) -> str:
        """``struct(…)`` of every column of *image* as the text ``CAST(… AS STRING)`` gives it."""
        return "struct(" + ", ".join(f"CAST({image}.`{c}` AS STRING) AS `{c}`" for c in columns) + ")"

    @staticmethod
    def _input_row(projector: Any, key: str, op: str, seq: int, row: Mapping) -> dict:
        return {
            "map_id": projector.map_id,
            "source_key": key,
            "op": op,
            "seq": int(seq),
            "row_json": json.dumps(row, sort_keys=True),
        }
