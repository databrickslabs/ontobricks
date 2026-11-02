"""TopicSlotSource: the slot-event view of a row-local mapping over a Debezium topic."""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.core.streaming import SlotProjectionCompiler, TopicSlotSource, TopicSource

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


class TestPinnedSQL:
    def test_the_topic_view_of_a_mapping(self):
        mapping = _customer()
        map_id = SlotProjectionCompiler.map_id(mapping)

        assert _view(mapping) == (
            "CREATE TEMPORARY VIEW v AS\n"
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
            f"    FROM {_TOPIC.read_kafka_sql()}\n"
            "  )\n"
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
            "WHERE (c._table = 'main.sales.customers' OR substring_index(c._table, '.', -1) = "
            "'customers') AND image.img['id'] IS NOT NULL"
        )

    def test_the_envelope_schema_reads_images_as_string_maps(self):
        assert TopicSlotSource.ENVELOPE_SCHEMA == (
            "op STRING, before MAP<STRING, STRING>, after MAP<STRING, STRING>, "
            "source STRUCT<table: STRING>, "
            "payload STRUCT<op: STRING, before: MAP<STRING, STRING>, "
            "after: MAP<STRING, STRING>, source: STRUCT<table: STRING>>"
        )


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

    def test_an_ambiguous_leaf_name_matches_the_full_name_only(self):
        sql = _view(match_leaf=False)

        assert "WHERE c._table = 'main.sales.customers' AND" in sql
        assert "substring_index" not in sql

    def test_literals_are_escaped(self):
        sql = _view(_customer(class_uri="http://ex.org/O'Brien"))

        assert "'http://ex.org/O''Brien'" in sql

    def test_an_unsafe_table_is_rejected(self):
        with pytest.raises(ValidationError):
            _view(_customer(table="main.sales.t; DROP"))

    def test_a_batch_mapping_is_rejected(self):
        with pytest.raises(ValidationError, match="row-local"):
            _view(_customer(sql_query="SELECT 1"))
