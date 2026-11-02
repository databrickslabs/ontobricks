"""A domain's realtime source: the Kafka-compatible topic a ``realtime`` domain streams."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from back.core.errors import ValidationError

#: Connector families the stream job can read (``TripleStreamJob``); ``kinesis``
#: is experimental (opaque sequence numbers, ordering within ~1 ms arbitrary).
REALTIME_SOURCE_KINDS = ("kafka", "eventhubs", "kinesis")
EXPERIMENTAL_SOURCE_KINDS = ("kinesis",)
_KIND_LABELS = {"kafka": "Kafka", "eventhubs": "Event Hubs", "kinesis": "Kinesis"}
#: The stream's state TTL when the domain sets none (longer than any expected lateness).
DEFAULT_STATE_TTL_HOURS = 72
_KEY_FORMATS = ("json",)
_TEXT_FIELDS = ("kind", "bootstrap_servers", "topic", "secret_scope", "secret_key", "key_format")
_FIELDS = (*_TEXT_FIELDS, "state_ttl_hours")
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

    ``{kind, bootstrap_servers, topic, secret_scope, secret_key, key_format,
    state_ttl_hours}``: the topic the ``realtime`` stream job reads, the
    *names* of the Databricks secret holding its credentials, and how long the
    stream keeps a row's state (hours; it must exceed the event lateness). A
    secret value is never stored — any other field is refused.
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
        values: dict = {field: str(raw.get(field) or "").strip() for field in _TEXT_FIELDS}
        if not any(values[field] for field in _REQUIRED):
            return {}
        values["state_ttl_hours"] = RealtimeSource._ttl_hours(raw.get("state_ttl_hours"))
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
    def _ttl_hours(raw: Any) -> int:
        if raw in (None, ""):
            return DEFAULT_STATE_TTL_HOURS
        try:
            hours = int(raw)
        except (TypeError, ValueError):
            hours = -1
        if isinstance(raw, bool) or hours < 0:
            raise ValidationError("realtime_source state_ttl_hours must be a whole number of hours")
        return hours

    @staticmethod
    def coerce(raw: Any) -> dict:
        """:meth:`parse` for stored data: an invalid config reads as ``{}``."""
        try:
            return RealtimeSource.parse(raw)
        except ValidationError:
            return {}

    @staticmethod
    def kind_options(current: str = "") -> list[dict]:
        """``[{kind, label, selected}]`` for the source kind picker (experimental kinds say so)."""
        selected = current or REALTIME_SOURCE_KINDS[0]
        return [
            {
                "kind": kind,
                "label": _KIND_LABELS[kind] + (" (experimental)" if kind in EXPERIMENTAL_SOURCE_KINDS else ""),
                "selected": kind == selected,
            }
            for kind in REALTIME_SOURCE_KINDS
        ]

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
