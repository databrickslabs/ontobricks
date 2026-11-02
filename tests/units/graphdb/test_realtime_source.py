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

    def test_a_domain_without_info(self):
        assert not RealtimeSource.topic_available(SimpleNamespace(), SimpleNamespace(realtime_enabled=True))
