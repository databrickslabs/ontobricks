"""TopicSlotSource: the slot-event view of a row-local mapping over a Debezium topic."""

from __future__ import annotations

import logging

import pytest

from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.core.streaming import (
    DebeziumValueText,
    SlotProjectionCompiler,
    TopicSlotSource,
    TopicSource,
)

pytestmark = pytest.mark.unit

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_TOPIC = TopicSource("b:9093", "cdc.customers", "kafka-scope", "jaas")


def _customer(**overrides) -> dict:
    mapping = {
        "table": "main.sales.customers",
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": "http://ex.org/Customer",
        "predicates": {
            _LABEL: {"type": "column", "column": "name"},
            "http://ex.org/age": {"type": "column", "column": "Age"},
        },
    }
    mapping.update(overrides)
    return mapping


def _view(mapping=None, **kwargs) -> str:
    return TopicSlotSource.slot_event_view_sql(
        mapping or _customer(), view_name="v", topic=_TOPIC, **kwargs
    )


def _lowered(image: str) -> str:
    """The image with lower-cased column names; of case-duplicates the last one wins."""
    return (
        f"CASE WHEN size(array_distinct(transform(map_keys({image}), k -> lower(k)))) = "
        f"size({image}) THEN transform_keys({image}, (k, v) -> lower(k)) "
        f"ELSE aggregate(map_entries({image}), CAST(map() AS MAP<STRING, STRING>), "
        "(acc, x) -> map_concat(map_filter(acc, (k, v) -> k != lower(x.key)), "
        "map(lower(x.key), x.value))) END"
    )


class TestPinnedSQL:
    def test_the_topic_view_of_a_mapping(self):
        mapping = _customer()
        map_id = SlotProjectionCompiler.map_id(mapping)

        assert _view(mapping) == (
            "CREATE TEMPORARY VIEW v AS\n"
            "WITH records AS (\n"
            "  SELECT _commit_version,\n"
            "         lower(coalesce(e.payload.op, e.op)) AS _op,\n"
            "         coalesce(e.payload.before, e.before) AS _before,\n"
            "         coalesce(e.payload.after, e.after) AS _after,\n"
            "         lower(coalesce(e.payload.source.`table`, e.source.`table`)) AS _table\n"
            "  FROM (\n"
            "    SELECT `offset` AS _commit_version, from_json(CAST(value AS STRING), "
            f"'{TopicSlotSource.ENVELOPE_SCHEMA}') AS e\n"
            f"    FROM {_TOPIC.read_kafka_sql()}\n"
            "  )\n"
            "),\n"
            "changes AS (\n"
            "  SELECT _commit_version, _op, _table,\n"
            f"         {_lowered('_before')} AS _before,\n"
            f"         {_lowered('_after')} AS _after\n"
            "  FROM records\n"
            ")\n"
            f"SELECT '{map_id}' AS map_id, image.img['id'] AS source_key,\n"
            "       s.predicate, s.object, "
            "CONCAT('http://ex.org/Customer/', image.img['id']) AS subject,\n"
            "       CASE WHEN image.is_delete OR s.object IS NULL THEN 'DELETE' ELSE 'UPSERT' END"
            " AS op,\n"
            "       c._commit_version,\n"
            "       image.seq_rank AS seq_rank\n"
            "FROM changes c\n"
            "LATERAL VIEW explode(array(\n"
            "  named_struct('img', CASE WHEN c._op = 'd' THEN c._before "
            "WHEN c._op IN ('c', 'u', 'r') THEN c._after END,\n"
            "               'is_delete', c._op = 'd', 'seq_rank', "
            "CASE WHEN c._op = 'd' THEN 0 ELSE 1 END),\n"
            "  named_struct('img', CASE WHEN c._op = 'u' "
            "AND NOT (c._before['id'] <=> c._after['id']) "
            "THEN c._before END,\n"
            "               'is_delete', true, 'seq_rank', 0)\n"
            ")) i AS image\n"
            f"LATERAL VIEW stack(3, '{_RDF_TYPE}', 'http://ex.org/Customer', "
            f"'{_LABEL}', COALESCE(image.img['name'], ''), "
            "'http://ex.org/age', COALESCE(image.img['age'], '')) s AS predicate, object\n"
            "WHERE c._table = 'main.sales.customers' AND image.img['id'] IS NOT NULL"
        )

    def test_the_envelope_schema_reads_images_as_string_maps(self):
        assert TopicSlotSource.ENVELOPE_SCHEMA == (
            "op STRING, before MAP<STRING, STRING>, after MAP<STRING, STRING>, "
            "source STRUCT<`table`: STRING>, "
            "payload STRUCT<op: STRING, before: MAP<STRING, STRING>, "
            "after: MAP<STRING, STRING>, source: STRUCT<`table`: STRING>>"
        )

    def test_reserved_words_are_backticked(self):
        """``offset`` and ``table`` are reserved under ``enforceReservedKeywords``."""
        sql = _view()

        assert "`offset`" in sql and " offset " not in sql
        assert ".table" not in sql and "<table" not in sql


class TestRules:
    def test_the_view_columns_are_the_cdf_view_columns(self):
        """The AUTO CDC flow and ``COLUMNS * EXCEPT (op, _commit_version, seq_rank)`` are reused."""
        cdf = SlotProjectionCompiler.slot_event_view_sql(
            _customer(), base_uri="", view_name="v", source_fqn="main.sales.customers"
        )
        columns = [" AS map_id", " AS source_key", "s.predicate, s.object", " AS subject", " AS op"]

        for column in columns:
            assert column in cdf and column in _view()
        assert "_commit_version" in _view() and " AS seq_rank" in _view()

    def test_the_stack_has_the_cdf_predicates_in_the_same_order(self):
        topic = _view()
        predicates = [p for p, _expr in SlotProjectionCompiler._stack_pairs(_customer())]

        positions = [topic.index(f"'{p}', ") for p in predicates]
        assert positions == sorted(positions)

    def test_a_composite_key_joins_and_compares_every_column(self):
        sql = _view(_customer(uri_template="http://ex.org/O/{region}/{num}", id_column="region"))

        assert "concat_ws('|', image.img['region'], image.img['num']) AS source_key" in sql
        assert "image.img['region'] IS NOT NULL AND image.img['num'] IS NOT NULL" in sql
        assert (
            "NOT (concat_ws('|', c._before['region'], c._before['num']) <=> "
            "concat_ws('|', c._after['region'], c._after['num']))"
        ) in sql

    def test_the_full_table_name_is_matched_by_default(self):
        sql = _view()

        assert "WHERE c._table = 'main.sales.customers' AND" in sql
        assert "substring_index" not in sql

    def test_a_unique_leaf_name_may_match_a_bare_debezium_table_name(self):
        sql = _view(match_leaf=True)

        assert (
            "WHERE (c._table = 'main.sales.customers' OR substring_index(c._table, '.', -1) = "
            "'customers') AND"
        ) in sql

    def test_the_table_name_is_lower_cased(self):
        sql = _view(_customer(table="Main.Sales.Customers"), match_leaf=True)

        assert "c._table = 'main.sales.customers' OR" in sql
        assert "substring_index(c._table, '.', -1) = 'customers'" in sql

    def test_literals_are_escaped(self):
        sql = _view(_customer(class_uri="http://ex.org/O'Brien"))

        assert "'http://ex.org/O''Brien'" in sql

    def test_an_unsafe_table_is_rejected(self):
        with pytest.raises(ValidationError):
            _view(_customer(table="main.sales.t; DROP"))

    def test_a_batch_mapping_is_rejected(self):
        with pytest.raises(ValidationError, match="row-local"):
            _view(_customer(sql_query="SELECT 1"))


class TestTypedValues:
    def test_typed_columns_are_converted_once_per_image(self):
        sql = _view(types={"age": "int", "id": "decimal(10,2)"})
        cases = (
            f"CASE k WHEN 'age' THEN {DebeziumValueText.sql('v', 'int')} "
            f"WHEN 'id' THEN {DebeziumValueText.sql('v', 'decimal(10,2)')} ELSE v END"
        )

        assert f"transform_values({_lowered('_before')}, (k, v) -> {cases}) AS _before" in sql
        assert f"transform_values({_lowered('_after')}, (k, v) -> {cases}) AS _after" in sql

    def test_type_names_and_columns_match_case_insensitively(self):
        assert _view(types={"AGE": "INT"}) == _view(types={"age": "int"})

    def test_text_typed_and_unmapped_columns_are_not_converted(self):
        sql = _view(types={"name": "string", "unused": "date"})

        assert "transform_values" not in sql
        assert sql == _view()


class TestViewEmitter:
    @pytest.fixture
    def logs(self, caplog):
        """Capture the module logger even when ``LogManager.setup()`` stopped propagation."""
        target = get_logger("back.core.streaming.TopicSlotSource")
        previous_level = target.level
        target.addHandler(caplog.handler)
        target.setLevel(logging.DEBUG)
        try:
            yield caplog
        finally:
            target.removeHandler(caplog.handler)
            target.setLevel(previous_level)

    @staticmethod
    def _warnings(logs) -> list[str]:
        """Distinct warnings (a record can reach caplog both directly and by propagation)."""
        return sorted({r.getMessage() for r in logs.records if r.levelno == logging.WARNING})

    def test_a_topic_is_required(self):
        with pytest.raises(ValidationError, match="topic"):
            TopicSlotSource.view_emitter([_customer()], topic=None, column_types=None)

    def test_the_view_is_the_mappings_with_its_tables_types(self):
        emit = TopicSlotSource.view_emitter(
            [_customer()],
            topic=_TOPIC,
            column_types={"MAIN.SALES.CUSTOMERS": {"Age": "int", "ID": "int", "name": "string"}},
        )

        assert emit(_customer(), "v") == _view(
            match_leaf=True, types={"age": "int", "id": "int", "name": "string"}
        )

    def test_a_mapped_column_without_a_type_is_warned_about_once_per_table(self, logs):
        other = _customer(class_uri="http://ex.org/VIP", predicates={})
        TopicSlotSource.view_emitter(
            [_customer(), other], topic=_TOPIC, column_types={"main.sales.customers": {"id": "int"}}
        )

        warnings = self._warnings(logs)
        assert len(warnings) == 1
        assert "main.sales.customers" in warnings[0] and "age, name" in warnings[0]

    def test_no_column_types_at_all_warns_for_every_mapped_column(self, logs):
        TopicSlotSource.view_emitter([_customer()], topic=_TOPIC, column_types=None)

        assert "age, id, name" in self._warnings(logs)[0]

    def test_fully_typed_mappings_compile_silently(self, logs):
        TopicSlotSource.view_emitter(
            [_customer()],
            topic=_TOPIC,
            column_types={"main.sales.customers": {"id": "int", "name": "string", "age": "int"}},
        )

        assert self._warnings(logs) == []

    def test_a_shared_leaf_name_matches_full_names_only_and_is_warned_about(self, logs):
        eu, us = _customer(table="main.eu.customers"), _customer(table="main.us.Customers")
        types = {
            table: {"id": "int", "name": "string", "age": "int"}
            for table in ("main.eu.customers", "main.us.customers")
        }
        emit = TopicSlotSource.view_emitter([eu, us], topic=_TOPIC, column_types=types)

        assert "WHERE c._table = 'main.eu.customers' AND" in emit(eu, "v")
        assert "WHERE c._table = 'main.us.customers' AND" in emit(us, "v")
        warnings = self._warnings(logs)
        assert len(warnings) == 2
        assert all("customers" in w and "bare" in w for w in warnings)

    def test_one_table_named_in_two_cases_is_one_table(self, logs):
        upper = _customer(table="MAIN.SALES.CUSTOMERS", class_uri="http://ex.org/VIP")
        types = {"main.sales.customers": {"id": "int", "name": "string", "age": "int"}}
        emit = TopicSlotSource.view_emitter([_customer(), upper], topic=_TOPIC, column_types=types)

        assert "substring_index(c._table, '.', -1) = 'customers'" in emit(upper, "v")
        assert self._warnings(logs) == []
