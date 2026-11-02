"""``RealtimeSource``: the domain's realtime topic config — names only, never secrets."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.core.graphdb import RealtimeSource

pytestmark = pytest.mark.unit

_COMPLETE = {
    "kind": "kafka",
    "bootstrap_servers": "broker-1:9092,broker-2:9092",
    "topic": "sales.customers",
    "secret_scope": "ontobricks",
    "secret_key": "kafka-sasl",
    "key_format": "json",
    "state_ttl_hours": 72,
}


class TestParse:
    def test_a_complete_config_is_kept(self):
        assert RealtimeSource.parse(dict(_COMPLETE)) == _COMPLETE

    @pytest.mark.parametrize("raw", [None, {}, {"kind": "", "topic": "  "}])
    def test_nothing_set_is_no_config(self, raw):
        assert RealtimeSource.parse(raw) == {}

    def test_values_are_trimmed_and_the_kind_lower_cased(self):
        config = RealtimeSource.parse({**_COMPLETE, "kind": " Kafka ", "topic": " t1 "})

        assert config["kind"] == "kafka" and config["topic"] == "t1"

    def test_the_state_ttl_defaults_to_three_days(self):
        config = {k: v for k, v in _COMPLETE.items() if k != "state_ttl_hours"}

        assert RealtimeSource.parse(config)["state_ttl_hours"] == 72

    @pytest.mark.parametrize("hours", [-1, "soon", True])
    def test_the_state_ttl_must_be_whole_hours(self, hours):
        with pytest.raises(ValidationError, match="state_ttl_hours"):
            RealtimeSource.parse({**_COMPLETE, "state_ttl_hours": hours})

    def test_the_key_format_defaults_to_json(self):
        config = {k: v for k, v in _COMPLETE.items() if k != "key_format"}

        assert RealtimeSource.parse(config)["key_format"] == "json"

    @pytest.mark.parametrize(
        "field", ["password", "sasl_jaas_config", "connection_string", "secret_value"]
    )
    def test_a_secret_value_is_refused_not_stored(self, field):
        with pytest.raises(ValidationError, match="secret_scope"):
            RealtimeSource.parse({**_COMPLETE, field: "hunter2"})

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("kind", "rabbitmq"),
            ("key_format", "avro"),
            ("topic", "bad topic"),
            ("secret_scope", "scope/with/slash"),
            ("secret_key", "x" * 200),
            ("bootstrap_servers", "broker;drop table"),
        ],
    )
    def test_an_invalid_value_is_refused(self, field, value):
        with pytest.raises(ValidationError, match=field):
            RealtimeSource.parse({**_COMPLETE, field: value})

    def test_a_non_mapping_is_refused(self):
        with pytest.raises(ValidationError):
            RealtimeSource.parse("kafka://broker")


class TestCoerce:
    def test_a_stored_invalid_config_reads_as_none(self):
        assert RealtimeSource.coerce({**_COMPLETE, "kind": "rabbitmq"}) == {}

    def test_a_stored_valid_config_reads_back(self):
        assert RealtimeSource.coerce(_COMPLETE) == _COMPLETE


class TestTopicAvailable:
    @pytest.mark.parametrize("missing", ["bootstrap_servers", "topic", "secret_scope", "secret_key"])
    def test_an_incomplete_config_is_not_available(self, missing):
        config = {**_COMPLETE, missing: ""}

        assert RealtimeSource.is_complete(RealtimeSource.parse(config)) is False

    def test_a_complete_config_needs_the_setting_on(self):
        domain = SimpleNamespace(info={"realtime_source": _COMPLETE})

        assert RealtimeSource.topic_available(domain, SimpleNamespace(realtime_enabled=True))
        assert not RealtimeSource.topic_available(domain, SimpleNamespace(realtime_enabled=False))
        assert not RealtimeSource.topic_available(domain, SimpleNamespace())

    @pytest.mark.parametrize("backend", ["databricks", "neo4j"])
    def test_only_a_lakebase_domain_can_serve_the_stream(self, backend):
        domain = SimpleNamespace(info={"realtime_source": _COMPLETE, "graph_backend": backend})

        assert not RealtimeSource.topic_available(domain, SimpleNamespace(realtime_enabled=True))

    def test_a_domain_without_info(self):
        assert not RealtimeSource.topic_available(SimpleNamespace(), SimpleNamespace(realtime_enabled=True))


class TestKindOptions:
    def test_every_kind_is_offered_and_the_stored_one_is_selected(self):
        options = RealtimeSource.kind_options("eventhubs")

        assert [o["kind"] for o in options] == ["kafka", "eventhubs", "kinesis"]
        assert [o["kind"] for o in options if o["selected"]] == ["eventhubs"]

    def test_kafka_is_the_default_and_kinesis_says_it_is_experimental(self):
        options = {o["kind"]: o for o in RealtimeSource.kind_options()}

        assert options["kafka"]["selected"] is True
        assert options["kinesis"]["label"] == "Kinesis (experimental)"
        assert options["eventhubs"]["label"] == "Event Hubs"
