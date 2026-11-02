"""The Kafka-compatible topic a realtime domain's slot pipeline reads instead of the change feed."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from back.core.errors import ValidationError

#: ``kafka``: the secret holds the SASL JAAS line; ``eventhubs``: the namespace
#: connection string (its Kafka endpoint, ``$ConnectionString`` login).
_KINDS = ("kafka", "eventhubs")
_FIELDS = ("bootstrap_servers", "topic", "secret_scope", "secret_key")
_PLAIN_LOGIN = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required"


@dataclass(frozen=True)
class TopicSource:
    """Where the realtime slot pipeline reads source-row changes: a topic and its credentials.

    Only the *names* of the Databricks secret are kept: the SQL calls
    ``secret('<scope>', '<key>')`` and Databricks resolves it when the
    pipeline runs, so no credential is stored or written into the pipeline.
    Every field is validated on construction, because each is interpolated
    into the pipeline SQL; the field rules are ``RealtimeSource``'s. The
    login is the stream job's: ``SASL_SSL`` with the ``PLAIN`` mechanism.

    The connection is part of the pipeline SQL, hence of its fingerprint: a
    new broker list or topic is a new offset space, so it must replay the
    topic (a full refresh). Rotate a credential by writing the new value
    under the same secret scope and key: that changes no SQL.
    """

    bootstrap_servers: str
    topic: str
    secret_scope: str
    secret_key: str
    kind: str = "kafka"

    def __post_init__(self) -> None:
        # Imported here: ``back.core.graphdb`` is heavy, and this module is
        # imported (through ``SlotPipelineSpec``) by the stream job's executors.
        from back.core.graphdb.RealtimeSource import RealtimeSource

        if self.kind not in _KINDS:
            raise ValidationError(
                f"Invalid topic source kind: {self.kind!r} (expected one of {', '.join(_KINDS)})"
            )
        values = {field: getattr(self, field) for field in _FIELDS}
        for field, value in values.items():
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValidationError(f"Invalid topic source {field}: {value!r}")
        RealtimeSource.parse({"kind": self.kind, **values})

    @classmethod
    def from_realtime_source(cls, config: Mapping) -> TopicSource:
        """The topic of a domain's ``realtime_source`` (``RealtimeSource.parse`` output).

        Kinesis has no Kafka endpoint, so it cannot feed a slot pipeline:
        :class:`ValidationError`.
        """
        kind = str(config.get("kind") or "kafka").strip().lower()
        if kind not in _KINDS:
            raise ValidationError(
                f"A {kind} realtime source cannot feed the slot pipeline (read_kafka reads "
                "Kafka and the Event Hubs Kafka endpoint only)"
            )
        return cls(**{field: str(config.get(field) or "").strip() for field in _FIELDS}, kind=kind)

    def read_kafka_sql(self) -> str:
        """``STREAM read_kafka(...)`` reading the topic from its earliest offset.

        ``read_kafka`` needs named arguments; consumer options take the
        ``kafka.`` prefix and backticks (a dotted name is a parser error
        otherwise). The JAAS config is ``secret()`` itself for ``kafka`` and
        a ``$ConnectionString`` PLAIN login for ``eventhubs``.
        """
        secret = f"secret('{self.secret_scope}', '{self.secret_key}')"
        if self.kind == "eventhubs":
            jaas = (
                f'concat(\'{_PLAIN_LOGIN} username="$ConnectionString" password="\', '
                f"replace({secret}, '\"', '\\\\\"'), '\";')"
            )
        else:
            jaas = secret
        return (
            "STREAM read_kafka(\n"
            f"    bootstrapServers => '{self.bootstrap_servers}',\n"
            f"    subscribe => '{self.topic}',\n"
            "    startingOffsets => 'earliest',\n"
            "    `kafka.security.protocol` => 'SASL_SSL',\n"
            "    `kafka.sasl.mechanism` => 'PLAIN',\n"
            f"    `kafka.sasl.jaas.config` => {jaas}\n"
            "  )"
        )
