"""The Kafka-compatible topic a realtime domain's slot pipeline reads instead of the change feed."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from back.core.errors import ValidationError

_PATTERNS = {
    # host:port[,host:port…] (IPv6 hosts in brackets).
    "bootstrap_servers": re.compile(r"[A-Za-z0-9_.\-:,\[\]]{1,1024}"),
    "topic": re.compile(r"[A-Za-z0-9_.\-]{1,249}"),
    # Databricks secret scope / key names.
    "secret_scope": re.compile(r"[A-Za-z0-9_.\-]{1,128}"),
    "secret_key": re.compile(r"[A-Za-z0-9_.\-]{1,128}"),
}
_CHOICES = {
    "security_protocol": ("SASL_SSL", "SASL_PLAINTEXT", "SSL", "PLAINTEXT"),
    "sasl_mechanism": ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"),
    # ``kafka``: the secret holds the SASL JAAS line; ``eventhubs``: the
    # namespace connection string (its Kafka endpoint, ``$ConnectionString`` login).
    "kind": ("kafka", "eventhubs"),
}
_PLAIN_LOGIN = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required"


@dataclass(frozen=True)
class TopicSource:
    """Where the realtime slot pipeline reads source-row changes: a topic and its credentials.

    Only the *names* of the Databricks secret are kept: the SQL calls
    ``secret('<scope>', '<key>')`` and Databricks resolves it when the
    pipeline runs, so no credential is stored or written into the pipeline.
    Every field is validated on construction, because each is interpolated
    into the pipeline SQL.
    """

    bootstrap_servers: str
    topic: str
    secret_scope: str
    secret_key: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    kind: str = "kafka"

    def __post_init__(self) -> None:
        for field, pattern in _PATTERNS.items():
            value = getattr(self, field)
            if not isinstance(value, str) or pattern.fullmatch(value) is None:
                raise ValidationError(f"Invalid topic source {field}: {value!r}")
        for field, allowed in _CHOICES.items():
            if getattr(self, field) not in allowed:
                raise ValidationError(
                    f"Invalid topic source {field}: {getattr(self, field)!r} "
                    f"(expected one of {', '.join(allowed)})"
                )

    @classmethod
    def from_realtime_source(cls, config: Mapping) -> TopicSource:
        """The topic of a domain's ``realtime_source`` (``RealtimeSource.parse`` output).

        Kinesis has no Kafka endpoint, so it cannot feed a slot pipeline:
        :class:`ValidationError`.
        """
        kind = str(config.get("kind") or "kafka").strip().lower()
        if kind not in _CHOICES["kind"]:
            raise ValidationError(
                f"A {kind} realtime source cannot feed the slot pipeline (read_kafka reads "
                "Kafka and the Event Hubs Kafka endpoint only)"
            )
        return cls(
            bootstrap_servers=str(config.get("bootstrap_servers") or "").strip(),
            topic=str(config.get("topic") or "").strip(),
            secret_scope=str(config.get("secret_scope") or "").strip(),
            secret_key=str(config.get("secret_key") or "").strip(),
            kind=kind,
        )

    def read_kafka_sql(self) -> str:
        """``STREAM read_kafka(...)`` reading the topic from its earliest offset.

        ``read_kafka`` needs named arguments; consumer options take the
        ``kafka.`` prefix. The JAAS config is ``secret()`` itself for ``kafka``
        and a ``$ConnectionString`` PLAIN login for ``eventhubs``.
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
            f"    kafka.security.protocol => '{self.security_protocol}',\n"
            f"    kafka.sasl.mechanism => '{self.sasl_mechanism}',\n"
            f"    kafka.sasl.jaas.config => {jaas}\n"
            "  )"
        )
