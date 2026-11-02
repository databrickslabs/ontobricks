"""A domain's realtime source: the Kafka-compatible topic a ``realtime`` domain streams."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from back.core.errors import ValidationError

#: Connector families the stream job can read (``TripleStreamJob``).
REALTIME_SOURCE_KINDS = ("kafka", "eventhubs", "kinesis")
_KEY_FORMATS = ("json",)
_FIELDS = ("kind", "bootstrap_servers", "topic", "secret_scope", "secret_key", "key_format")
#: Fields a complete config needs (``key_format`` has a default).
_REQUIRED = ("kind", "bootstrap_servers", "topic", "secret_scope", "secret_key")
_PATTERNS = {
    # Databricks secret scope / key names.
    "secret_scope": re.compile(r"[A-Za-z0-9_.\-]{1,128}"),
    "secret_key": re.compile(r"[A-Za-z0-9_.\-]{1,128}"),
    # Kafka topic names (Event Hubs entity / Kinesis stream names fit too).
    "topic": re.compile(r"[A-Za-z0-9_.\-]{1,249}"),
    # host:port[,host:port…] (Kinesis: the region, e.g. us-west-2).
    "bootstrap_servers": re.compile(r"[A-Za-z0-9_.\-:,\[\]]{1,1024}"),
}


class RealtimeSource:
    """Validation and readiness of a domain's ``info["realtime_source"]``.

    ``{kind, bootstrap_servers, topic, secret_scope, secret_key, key_format}``:
    the topic the ``realtime`` stream job reads and the *names* of the
    Databricks secret holding its credentials. A secret value is never stored —
    any other field is refused.
    """

    @staticmethod
    def parse(raw: Any) -> dict:
        """The config to store for *raw*: ``{}`` when nothing is set, else validated fields.

        Raises :class:`ValidationError` for a non-mapping, an unknown field (a
        password or connection string belongs in the secret scope), or a value
        outside its allowed shape.
        """
        if raw is None:
            return {}
        if not isinstance(raw, Mapping):
            raise ValidationError("realtime_source must be an object")
        unknown = sorted(str(key) for key in raw if key not in _FIELDS)
        if unknown:
            raise ValidationError(
                f"Unknown realtime_source field(s): {', '.join(unknown)} — credentials are "
                "referenced by secret_scope / secret_key, never stored"
            )
        values = {field: str(raw.get(field) or "").strip() for field in _FIELDS}
        if not any(values[field] for field in _REQUIRED):
            return {}
        values["kind"] = values["kind"].lower()
        values["key_format"] = (values["key_format"] or "json").lower()
        if values["kind"] and values["kind"] not in REALTIME_SOURCE_KINDS:
            raise ValidationError(
                f"realtime_source kind must be one of {', '.join(REALTIME_SOURCE_KINDS)}"
            )
        if values["key_format"] not in _KEY_FORMATS:
            raise ValidationError("realtime_source key_format must be json")
        for field, pattern in _PATTERNS.items():
            if values[field] and pattern.fullmatch(values[field]) is None:
                raise ValidationError(f"Invalid realtime_source {field}: {values[field]!r}")
        return values

    @staticmethod
    def coerce(raw: Any) -> dict:
        """:meth:`parse` for stored data: an invalid config reads as ``{}``."""
        try:
            return RealtimeSource.parse(raw)
        except ValidationError:
            return {}

    @staticmethod
    def is_complete(config: Mapping) -> bool:
        """True when every field the stream job needs is set."""
        return all(str(config.get(field) or "").strip() for field in _REQUIRED)

    @staticmethod
    def of(domain: Any) -> dict:
        """The domain's stored config (``{}`` when none or invalid)."""
        return RealtimeSource.coerce((getattr(domain, "info", None) or {}).get("realtime_source"))

    @staticmethod
    def topic_available(domain: Any, settings: Any) -> bool:
        """The resolver's ``realtime_topic_available``.

        A complete config, ``settings.realtime_enabled`` on, and a Lakebase
        graph backend: the stream serves through the Lakebase ``_live`` table.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        enabled = getattr(settings, "realtime_enabled", False) is True
        if not enabled or GraphDBFactory._resolve_graph_backend(domain) != "lakebase":
            return False
        return RealtimeSource.is_complete(RealtimeSource.of(domain))
