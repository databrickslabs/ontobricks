"""TopicSource: the Kafka-compatible topic a realtime slot pipeline reads, as SDP SQL."""

from __future__ import annotations

import re

import pytest

from back.core.errors import ValidationError
from back.core.streaming import TopicSource

pytestmark = pytest.mark.unit


def _topic(**overrides) -> TopicSource:
    fields = {
        "bootstrap_servers": "broker1:9093,broker2:9093",
        "topic": "dbserver1.sales.customers",
        "secret_scope": "kafka-scope",
        "secret_key": "jaas",
    }
    fields.update(overrides)
    return TopicSource(**fields)


class TestValidation:
    def test_a_valid_topic_is_kafka_by_default(self):
        assert _topic().kind == "kafka"

    def test_the_stream_jobs_fixed_security_settings_are_not_options(self):
        """The stream job always logs in with SASL_SSL / PLAIN; the slot pipeline matches it."""
        with pytest.raises(TypeError):
            _topic(security_protocol="PLAINTEXT")

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("bootstrap_servers", "broker:9092'; DROP"),
            ("topic", "a topic"),
            ("secret_scope", "scope'"),
            ("secret_key", ""),
            ("topic", " padded"),
            ("bootstrap_servers", None),
            ("kind", "kinesis"),
            ("kind", "Kafka"),
        ],
    )
    def test_an_unsafe_or_unknown_value_is_rejected(self, field, value):
        with pytest.raises(ValidationError, match=field):
            _topic(**{field: value})

    def test_the_field_rules_are_the_realtime_sources(self):
        """One home for the patterns: what ``RealtimeSource.parse`` refuses, a topic refuses."""
        from back.core.graphdb import RealtimeSource

        bad = "x" * 250
        with pytest.raises(ValidationError):
            RealtimeSource.parse({"kind": "kafka", "topic": bad})
        with pytest.raises(ValidationError, match="topic"):
            _topic(topic=bad)


class TestFromRealtimeSource:
    def test_a_kafka_realtime_source_maps_to_a_topic(self):
        topic = TopicSource.from_realtime_source(
            {
                "kind": "Kafka",
                "bootstrap_servers": "b:9093",
                "topic": "t",
                "secret_scope": "s",
                "secret_key": "k",
                "key_format": "json",
                "state_ttl_hours": 72,
            }
        )

        assert topic == TopicSource("b:9093", "t", "s", "k", kind="kafka")

    def test_an_event_hubs_source_keeps_its_kind(self):
        topic = TopicSource.from_realtime_source(
            {
                "kind": "eventhubs",
                "bootstrap_servers": "ns.servicebus.windows.net:9093",
                "topic": "t",
                "secret_scope": "s",
                "secret_key": "k",
            }
        )

        assert topic.kind == "eventhubs"

    def test_kinesis_cannot_feed_a_slot_pipeline(self):
        with pytest.raises(ValidationError, match="kinesis"):
            TopicSource.from_realtime_source(
                {
                    "kind": "kinesis",
                    "bootstrap_servers": "us-west-2",
                    "topic": "t",
                    "secret_scope": "s",
                    "secret_key": "k",
                }
            )


class TestReadKafkaSQL:
    def test_the_stream_reads_the_topic_from_the_earliest_offset(self):
        assert _topic().read_kafka_sql() == (
            "STREAM read_kafka(\n"
            "    bootstrapServers => 'broker1:9093,broker2:9093',\n"
            "    subscribe => 'dbserver1.sales.customers',\n"
            "    startingOffsets => 'earliest',\n"
            "    `kafka.security.protocol` => 'SASL_SSL',\n"
            "    `kafka.sasl.mechanism` => 'PLAIN',\n"
            "    `kafka.sasl.jaas.config` => secret('kafka-scope', 'jaas')\n"
            "  )"
        )

    @pytest.mark.parametrize("kind", ["kafka", "eventhubs"])
    def test_every_kafka_consumer_option_is_backticked(self, kind):
        """``read_kafka``: ``kafka.``-prefixed option names need backticks, else a parser error."""
        sql = _topic(kind=kind).read_kafka_sql()
        options = re.findall(r"^\s*(\S+) =>", sql, flags=re.MULTILINE)

        assert [o for o in options if "kafka." in o] == [
            "`kafka.security.protocol`",
            "`kafka.sasl.mechanism`",
            "`kafka.sasl.jaas.config`",
        ]

    def test_event_hubs_logs_in_with_the_connection_string(self):
        sql = _topic(kind="eventhubs").read_kafka_sql()

        assert (
            "`kafka.sasl.jaas.config` => concat('kafkashaded.org.apache.kafka.common.security"
            '.plain.PlainLoginModule required username="$ConnectionString" password="\', '
            "replace(secret('kafka-scope', 'jaas'), '\"', '\\\\\"'), '\";')"
        ) in sql

    def test_the_secret_value_never_appears_in_the_sql(self):
        """Only the scope and key names do: Databricks resolves ``secret()`` at run time."""
        sql = _topic().read_kafka_sql()

        assert "secret('kafka-scope', 'jaas')" in sql
        assert "password" not in sql

    def test_no_pipeline_configuration_reference_is_emitted(self):
        """The connection stays in the SQL: a new cluster or topic is a new offset space.

        Rotating the credential under the same secret scope / key changes no SQL;
        only renaming the secret, or a new broker list or topic, does.
        """
        assert "${" not in _topic(kind="eventhubs").read_kafka_sql()
