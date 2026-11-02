"""SlotProjectionCompiler.compile with a topic source: the realtime slot pipeline."""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.core.streaming import SlotProjectionCompiler, TopicSlotSource, TopicSource

pytestmark = pytest.mark.unit

_TOPIC = TopicSource("b:9093", "cdc.sales", "kafka-scope", "jaas")
_SLOTS = "main.sales.t_slots"


def _entity(table: str, cls: str, **overrides) -> dict:
    mapping = {
        "table": table,
        "sql_query": None,
        "uri_template": f"http://ex.org/{cls}/{{id}}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": f"http://ex.org/{cls}",
        "predicates": {},
    }
    mapping.update(overrides)
    return mapping


def _mappings() -> dict:
    return {
        "http://ex.org/Customer": _entity("main.sales.customers", "Customer"),
        "http://ex.org/Order": _entity("main.sales.orders", "Order", sql_query="SELECT 1"),
    }


def _compile(entities=None, **kwargs):
    return SlotProjectionCompiler.compile(
        entities if entities is not None else _mappings(),
        [],
        base_uri="",
        slots_fqn=_SLOTS,
        **kwargs,
    )


class TestTopicSource:
    def test_the_change_feed_is_the_default_source(self):
        spec = _compile()

        assert (spec.source, spec.topic) == ("cdf", None)
        assert "readChangeFeed" in spec.statements[1]

    def test_a_topic_source_compiles_the_topic_views(self):
        spec = _compile(source="topic", topic=_TOPIC)
        customer = {**_mappings()["http://ex.org/Customer"], "class_uri": "http://ex.org/Customer"}
        map_id = SlotProjectionCompiler.map_id(customer)

        assert spec.statements[1] == TopicSlotSource.slot_event_view_sql(
            customer, view_name=f"slot_events_{map_id}", topic=_TOPIC
        )
        assert "readChangeFeed" not in spec.to_sql()

    def test_the_slots_table_and_the_auto_cdc_flows_are_unchanged(self):
        cdf, topic = _compile(), _compile(source="topic", topic=_TOPIC)

        assert topic.statements[0] == cdf.statements[0]
        assert topic.statements[2::2] == cdf.statements[2::2]
        assert len(topic.statements) == len(cdf.statements)

    def test_the_spec_records_the_source_the_topic_and_the_mapped_tables(self):
        spec = _compile(source="topic", topic=_TOPIC)

        assert (spec.source, spec.topic) == ("topic", _TOPIC)
        assert spec.source_tables == ["main.sales.customers"]
        assert spec.row_local_map_ids == _compile().row_local_map_ids
        assert [m["table"] for m in spec.batch_mappings] == ["main.sales.orders"]

    def test_a_topic_source_needs_no_row_tracking_probe(self):
        assert _compile(source="topic", topic=_TOPIC).needs_row_tracking is False
        assert _compile().needs_row_tracking is True

    def test_tables_sharing_a_leaf_name_match_by_full_name_only(self):
        entities = {
            "http://ex.org/A": _entity("main.eu.customers", "A"),
            "http://ex.org/B": _entity("main.us.customers", "B"),
            "http://ex.org/C": _entity("main.us.orders", "C"),
        }

        sql = _compile(entities, source="topic", topic=_TOPIC).statements

        assert "WHERE c._table = 'main.eu.customers' AND" in sql[1]
        assert "WHERE c._table = 'main.us.customers' AND" in sql[3]
        assert "substring_index(c._table, '.', -1) = 'orders'" in sql[5]


class TestArguments:
    def test_a_topic_source_needs_a_topic(self):
        with pytest.raises(ValidationError, match="topic"):
            _compile(source="topic")

    def test_a_change_feed_source_takes_no_topic(self):
        with pytest.raises(ValidationError, match="topic"):
            _compile(source="cdf", topic=_TOPIC)

    def test_an_unknown_source_is_rejected(self):
        with pytest.raises(ValidationError, match="kafka"):
            _compile(source="kafka", topic=_TOPIC)
