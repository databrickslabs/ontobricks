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
  column names (Spark resolves columns case-insensitively; of two columns
  differing only in case the last one wins, as in the stream job);
* values are typed from the source table's column types
  (:class:`DebeziumValueText`), so a date, timestamp or decimal reads as
  ``CAST(<column> AS STRING)`` prints it; an untyped column keeps its text;
* ``source.table`` matches the mapping's table by full name, or by leaf name
  when no other mapped table shares it, case-insensitively;
* ``d`` deletes the key of ``before``; ``c`` / ``u`` / ``r`` upsert ``after``;
  an ``u`` whose ``before`` has another key also deletes that old key — the
  pre-image's rank-0 ``DELETE`` of the change-feed variant;
* deletes rank 0 and upserts 1 (``seq_rank``), as in the change-feed variant;
* objects follow the change-feed view: ``COALESCE(<value>, '')``, a NULL
  object is a ``DELETE``, a blank one stays a slot (``_data`` filters it).

Preconditions on the topic (neither the mappings nor the realtime config can
tell, so they are not checked):

* **keyed by the source row's primary key.** Offsets order records only
  within a partition; the fold relies on every change of a slot key landing
  in one partition. Debezium keys records by the primary key, which is right
  when the mapping's key (its subject template columns) is that key or is
  functionally fixed per row. A mapping keyed by another, mutable column can
  see a re-key's delete and upsert in different partitions and lose one.
* **full pre-images** (Postgres ``REPLICA IDENTITY FULL``, MySQL
  ``binlog_row_image=FULL``). A delete or a re-key reads the mapping's key
  from ``before``; with key-only pre-images a mapping keyed by a non-PK
  column never deletes and a re-key leaves the old key's slots behind.
* **retention.** A full refresh of ``_slots`` (any change to the pipeline
  SQL, a new topic or broker list included) replays the topic from
  ``earliest``: the topic must keep each row's latest change (log
  compaction, or a fresh Debezium snapshot), else rows not changed within
  the retention are missing from the rebuilt ``_slots``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from back.core.errors import ValidationError
from back.core.helpers import sql_escape
from back.core.logging import get_logger
from back.core.streaming.DebeziumValueText import DebeziumValueText
from back.core.streaming.SlotProjectionCompiler import SlotProjectionCompiler
from back.core.streaming.TopicSource import TopicSource

logger = get_logger(__name__)


class TopicSlotSource:
    """Emit the topic-reading slot-event views of row-local mappings."""

    #: ``from_json`` schema of a Debezium change envelope, optionally under ``payload``.
    ENVELOPE_SCHEMA = (
        "op STRING, before MAP<STRING, STRING>, after MAP<STRING, STRING>, "
        "source STRUCT<`table`: STRING>, "
        "payload STRUCT<op: STRING, before: MAP<STRING, STRING>, "
        "after: MAP<STRING, STRING>, source: STRUCT<`table`: STRING>>"
    )

    @staticmethod
    def view_emitter(
        mappings: list[dict],
        *,
        topic: TopicSource | None,
        column_types: Mapping[str, Mapping[str, str]] | None,
    ) -> Callable[[dict, str], str]:
        """``(mapping, view_name) -> view SQL`` over *topic* for the row-local *mappings*.

        *column_types* is ``{table: {column: UC type}}`` of the source tables
        (case-insensitive); ``None`` types nothing on purpose (a caller that
        renders no SQL, e.g. the DAB renderer). Warns, once per table, about
        mapped columns a given *column_types* has no type for (their Debezium
        text is kept) and about a leaf name two mapped tables share (only full
        ``source.table`` names match it, and stock Debezium sends bare names).
        """
        if not isinstance(topic, TopicSource):
            raise ValidationError("A topic slot pipeline needs its topic")
        types = {
            str(table).lower(): {str(c).lower(): str(t) for c, t in columns.items()}
            for table, columns in (column_types or {}).items()
        }
        by_table: dict[str, set[str]] = {}
        for mapping in mappings:
            columns = by_table.setdefault(mapping["table"].lower(), set())
            columns.update(TopicSlotSource._mapped_columns(mapping))
        leaves = [table.rsplit(".", 1)[-1] for table in by_table]
        for table, columns in sorted(by_table.items()):
            typed = None if column_types is None else types.get(table, {})
            TopicSlotSource._warn(table, columns, typed, leaves)

        def emit(mapping: dict, view_name: str) -> str:
            table = mapping["table"].lower()
            return TopicSlotSource.slot_event_view_sql(
                mapping,
                view_name=view_name,
                topic=topic,
                match_leaf=leaves.count(table.rsplit(".", 1)[-1]) == 1,
                types=types.get(table),
            )

        return emit

    @staticmethod
    def slot_event_view_sql(
        mapping: dict,
        *,
        view_name: str,
        topic: TopicSource,
        match_leaf: bool = False,
        types: Mapping[str, str] | None = None,
    ) -> str:
        """Return the temporary view of *mapping*'s slot events over *topic*.

        *match_leaf* also matches a bare ``source.table`` by the table's leaf
        name — safe only when no other mapped table shares it. *types* is
        ``{column: UC type}`` of the mapping's table: its mapped columns are
        read as typed values (:class:`DebeziumValueText`).
        """
        compiler = SlotProjectionCompiler
        if compiler.classify(mapping) != compiler.ROW_LOCAL:
            raise ValidationError(
                f"Mapping {mapping.get('class_uri') or mapping.get('table')!r} is not row-local"
            )
        compiler.require_ident(view_name, "view name")
        table = mapping["table"]
        compiler.require_fqn(table, "source table")
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
            f"{TopicSlotSource._changes_ctes(topic, TopicSlotSource._typed(mapping, types))}"
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
    def _changes_ctes(topic: TopicSource, typed: dict[str, str]) -> str:
        """``records`` (one decoded envelope per Kafka record) and ``changes`` (its typed images)."""
        return (
            "WITH records AS (\n"
            "  SELECT _commit_version,\n"
            "         lower(coalesce(e.payload.op, e.op)) AS _op,\n"
            "         coalesce(e.payload.before, e.before) AS _before,\n"
            "         coalesce(e.payload.after, e.after) AS _after,\n"
            "         lower(coalesce(e.payload.source.`table`, e.source.`table`)) AS _table\n"
            "  FROM (\n"
            "    SELECT `offset` AS _commit_version, from_json(CAST(value AS STRING), "
            f"'{TopicSlotSource.ENVELOPE_SCHEMA}') AS e\n"
            f"    FROM {topic.read_kafka_sql()}\n"
            "  )\n"
            "),\n"
            "changes AS (\n"
            "  SELECT _commit_version, _op, _table,\n"
            f"         {TopicSlotSource._image('_before', typed)} AS _before,\n"
            f"         {TopicSlotSource._image('_after', typed)} AS _after\n"
            "  FROM records\n"
            ")\n"
        )

    @staticmethod
    def _image(image: str, typed: dict[str, str]) -> str:
        """*image* with lower-cased columns (the last of case-duplicates wins), typed values."""
        lowered = (
            f"CASE WHEN size(array_distinct(transform(map_keys({image}), k -> lower(k)))) = "
            f"size({image}) THEN transform_keys({image}, (k, v) -> lower(k)) "
            f"ELSE aggregate(map_entries({image}), CAST(map() AS MAP<STRING, STRING>), "
            "(acc, x) -> map_concat(map_filter(acc, (k, v) -> k != lower(x.key)), "
            "map(lower(x.key), x.value))) END"
        )
        if not typed:
            return lowered
        cases = " ".join(f"WHEN '{column}' THEN {sql}" for column, sql in typed.items())
        return f"transform_values({lowered}, (k, v) -> CASE k {cases} ELSE v END)"

    @staticmethod
    def _typed(mapping: dict, types: Mapping[str, str] | None) -> dict[str, str]:
        """``{column: SQL of v}`` for the mapping's columns whose type is not text, by name."""
        lowered = {str(column).lower(): uc_type for column, uc_type in (types or {}).items()}
        typed = {
            column: DebeziumValueText.sql("v", lowered.get(column))
            for column in sorted(TopicSlotSource._mapped_columns(mapping))
        }
        return {column: sql for column, sql in typed.items() if sql is not None}

    @staticmethod
    def _mapped_columns(mapping: dict) -> set[str]:
        """The lower-cased columns *mapping*'s view reads: its key, subject and objects."""
        compiler = SlotProjectionCompiler
        columns = {*compiler.key_columns(mapping), compiler.subject_parts(mapping)[1]}
        columns.update(column for _p, _l, column in compiler.slot_sources(mapping) if column)
        return {column.lower() for column in columns}

    @staticmethod
    def _warn(
        table: str, columns: set[str], types: Mapping[str, str] | None, leaves: list[str]
    ) -> None:
        untyped = [] if types is None else sorted(columns - set(types))
        if untyped:
            logger.warning(
                "Topic slot views of %s: no column type for %s; their Debezium text is kept, "
                "which differs from CAST(<column> AS STRING) for dates, timestamps and decimals",
                table,
                ", ".join(untyped),
            )
        leaf = table.rsplit(".", 1)[-1]
        if leaves.count(leaf) > 1:
            logger.warning(
                "Topic slot views of %s match source.table by its full name only: another "
                "mapped table is also named %s, and stock Debezium sends bare table names, "
                "so such changes are never read",
                table,
                leaf,
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
