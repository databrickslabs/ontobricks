"""Slot-event views over a Debezium topic: the realtime variant of the slot pipeline's source.

In ``realtime`` mode the slot pipeline reads the same topic as the stream job,
so ``_slots`` / ``_data`` (Delta) follow the same changes as ``_live``. Each
row-local mapping's temporary view reads ``STREAM read_kafka(...)``
(:class:`TopicSource`), parses the Debezium envelope and emits exactly the
columns of :meth:`SlotProjectionCompiler.slot_event_view_sql` — ``map_id,
source_key, predicate, object, subject, op, _commit_version, seq_rank`` — so
the AUTO CDC flow, the ``_slots`` table and the ``_data`` MV are unchanged.
``_commit_version`` holds the Kafka ``offset``.

The envelope rules are the stream job's (``TripleStreamEnvelope``):

* the record is ``{op, before, after, source: {table}}``, optionally under
  ``payload``; images are read as ``MAP<STRING, STRING>`` with lower-cased
  column names (Spark resolves columns case-insensitively);
* ``source.table`` matches the mapping's table by full name, or by leaf name
  when no other mapped table shares it, case-insensitively;
* ``d`` deletes the key of ``before``; ``c`` / ``u`` / ``r`` upsert ``after``;
  an ``u`` whose ``before`` has another key also deletes that old key — the
  pre-image's rank-0 ``DELETE`` of the change-feed variant;
* deletes rank 0 and upserts 1 (``seq_rank``), as in the change-feed variant;
* objects follow the change-feed view: ``COALESCE(<value>, '')``, a NULL
  object is a ``DELETE``, a blank one stays a slot (``_data`` filters it).
"""

from __future__ import annotations

from back.core.errors import ValidationError
from back.core.helpers import sql_escape
from back.core.streaming.SlotProjectionCompiler import SlotProjectionCompiler
from back.core.streaming.TopicSource import TopicSource


class TopicSlotSource:
    """Emit the topic-reading slot-event view of a row-local mapping."""

    #: ``from_json`` schema of a Debezium change envelope, optionally under ``payload``.
    ENVELOPE_SCHEMA = (
        "op STRING, before MAP<STRING, STRING>, after MAP<STRING, STRING>, "
        "source STRUCT<table: STRING>, "
        "payload STRUCT<op: STRING, before: MAP<STRING, STRING>, "
        "after: MAP<STRING, STRING>, source: STRUCT<table: STRING>>"
    )

    @staticmethod
    def slot_event_view_sql(
        mapping: dict, *, view_name: str, topic: TopicSource, match_leaf: bool = True
    ) -> str:
        """Return the temporary view of *mapping*'s slot events over *topic*.

        *match_leaf* is ``False`` when another mapped table has the same leaf
        name: the envelope's ``source.table`` must then name the table in full.
        """
        compiler = SlotProjectionCompiler
        if compiler.classify(mapping) != compiler.ROW_LOCAL:
            raise ValidationError(
                f"Mapping {mapping.get('class_uri') or mapping.get('table')!r} is not row-local"
            )
        compiler._require_ident(view_name, "view name")
        table = mapping["table"]
        compiler._require_fqn(table, "source table")
        prefix, id_column = compiler.subject_parts(mapping)
        key_columns = compiler.key_columns(mapping)
        image = "image.img"
        subject = TopicSlotSource._value(image, id_column)
        if prefix:
            subject = f"CONCAT('{sql_escape(prefix)}', {subject})"
        pairs = [
            f"'{sql_escape(predicate)}', "
            + (
                f"'{sql_escape(literal)}'"
                if column is None
                else f"COALESCE({TopicSlotSource._value(image, column)}, '')"
            )
            for predicate, literal, column in compiler.slot_sources(mapping)
        ]
        not_null = " AND ".join(
            f"{TopicSlotSource._value(image, column)} IS NOT NULL" for column in key_columns
        )
        key_changed = (
            f"NOT ({TopicSlotSource._key('c._before', key_columns)} <=> "
            f"{TopicSlotSource._key('c._after', key_columns)})"
        )
        full_name, leaf = table.lower(), table.rsplit(".", 1)[-1].lower()
        table_match = (
            f"(c._table = '{full_name}' OR substring_index(c._table, '.', -1) = '{leaf}')"
            if match_leaf
            else f"c._table = '{full_name}'"
        )
        return (
            f"CREATE TEMPORARY VIEW {view_name} AS\n"
            f"{TopicSlotSource._changes_cte(topic)}"
            f"SELECT '{compiler.map_id(mapping)}' AS map_id, "
            f"{TopicSlotSource._key(image, key_columns)} AS source_key,\n"
            f"       s.predicate, s.object, {subject} AS subject,\n"
            "       CASE WHEN image.is_delete OR s.object IS NULL THEN 'DELETE' ELSE 'UPSERT' END"
            " AS op,\n"
            "       c._commit_version,\n"
            "       image.seq_rank AS seq_rank\n"
            "FROM changes c\n"
            f"{TopicSlotSource._images_sql(key_changed)}"
            f"LATERAL VIEW stack({len(pairs)}, {', '.join(pairs)}) s AS predicate, object\n"
            f"WHERE {table_match} AND {not_null}"
        )

    @staticmethod
    def _changes_cte(topic: TopicSource) -> str:
        """``WITH changes AS (...)``: one decoded envelope per Kafka record, ``offset`` first."""
        return (
            "WITH changes AS (\n"
            "  SELECT offset AS _commit_version,\n"
            "         lower(coalesce(e.payload.op, e.op)) AS _op,\n"
            "         transform_keys(coalesce(e.payload.before, e.before), (k, v) -> lower(k))"
            " AS _before,\n"
            "         transform_keys(coalesce(e.payload.after, e.after), (k, v) -> lower(k))"
            " AS _after,\n"
            "         lower(coalesce(e.payload.source.table, e.source.table)) AS _table\n"
            "  FROM (\n"
            "    SELECT offset, from_json(CAST(value AS STRING), "
            f"'{TopicSlotSource.ENVELOPE_SCHEMA}') AS e\n"
            f"    FROM {topic.read_kafka_sql()}\n"
            "  )\n"
            ")\n"
        )

    @staticmethod
    def _images_sql(key_changed: str) -> str:
        """A change's images: ``after`` (``before`` for ``d``), and the old key's on a re-key."""
        return (
            "LATERAL VIEW explode(array(\n"
            "  named_struct('img', CASE WHEN c._op = 'd' THEN c._before "
            "WHEN c._op IN ('c', 'u', 'r') THEN c._after END,\n"
            "               'is_delete', c._op = 'd', 'seq_rank', "
            "CASE WHEN c._op = 'd' THEN 0 ELSE 1 END),\n"
            f"  named_struct('img', CASE WHEN c._op = 'u' AND {key_changed} THEN c._before END,\n"
            "               'is_delete', true, 'seq_rank', 0)\n"
            ")) i AS image\n"
        )

    @staticmethod
    def _value(image: str, column: str) -> str:
        """*column* of a lower-cased image map (the column was validated by the compiler)."""
        return f"{image}['{column.lower()}']"

    @staticmethod
    def _key(image: str, columns: list[str]) -> str:
        """``source_key`` of an image: ``concat_ws('|', ...)`` for a composite template."""
        values = [TopicSlotSource._value(image, column) for column in columns]
        return values[0] if len(values) == 1 else f"concat_ws('|', {', '.join(values)})"
