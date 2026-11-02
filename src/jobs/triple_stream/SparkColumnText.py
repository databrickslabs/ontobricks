"""``CAST(<column> AS STRING)`` of a decoded change-envelope value, from the column's Spark type.

The slot SQL projects every source column through ``CAST(… AS STRING)`` (via
``TRY_CAST``), and :class:`back.core.streaming.SlotRowProjector` expects the
same text. A Debezium-style envelope carries the *encoded* values instead —
dates as epoch days, timestamps as epoch milli/micro/nanoseconds, decimals as
base64 unscaled bytes (``decimal.handling.mode=precise``) or text — so the
stream job turns each value into the string Spark would print for it, given
the column's type in the Unity Catalog source table (``StructField.dataType
.simpleString()``). Timestamps print in UTC (the job's session time zone).
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

_EPOCH_DAY = date(1970, 1, 1)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_INTEGRAL = ("tinyint", "smallint", "int", "bigint", "integer", "long", "short", "byte")


class SparkColumnText:
    """The Spark ``STRING`` text of envelope values, typed by the source table's schema."""

    @staticmethod
    def row(row: Mapping, types: Mapping[str, str]) -> dict:
        """*row* with every value as Spark text (``None`` stays ``None``); untyped columns as strings."""
        return {
            column: SparkColumnText.text(value, types.get(str(column).lower(), "string"))
            for column, value in row.items()
        }

    @staticmethod
    def text(value: Any, spark_type: str) -> str | None:
        """``CAST(value AS STRING)`` for a column of *spark_type* (``int``, ``decimal(10,2)``…)."""
        if value is None:
            return None
        kind = (spark_type or "string").strip().lower()
        if kind == "boolean":
            return SparkColumnText._boolean(value)
        if kind in _INTEGRAL:
            return str(int(value))
        if kind in ("double", "float", "real"):
            return SparkColumnText.double(float(value))
        if kind.startswith("decimal"):
            return SparkColumnText.decimal(value, SparkColumnText._scale(kind))
        if kind == "date":
            return SparkColumnText.date(value)
        if kind.startswith("timestamp"):
            return SparkColumnText.timestamp(value)
        if kind == "binary":
            return SparkColumnText._binary(value)
        if kind.startswith(("array", "map", "struct")):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        return value if isinstance(value, str) else str(value)

    @staticmethod
    def double(value: float) -> str:
        """Java ``Double.toString``: plain in ``[1e-3, 1e7)``, else ``1.0E20`` scientific."""
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0:
            return "-0.0" if str(value).startswith("-") else "0.0"
        if 1e-3 <= abs(value) < 1e7:
            text = repr(value)
            return text if "." in text or "e" in text else f"{text}.0"
        sign, digits, exponent = Decimal(repr(value)).as_tuple()
        mantissa = "".join(map(str, digits)).rstrip("0") or "0"
        power = len(digits) + exponent - 1
        return f"{'-' if sign else ''}{mantissa[0]}.{mantissa[1:] or '0'}E{power}"

    @staticmethod
    def decimal(value: Any, scale: int) -> str:
        """A ``decimal(p, scale)`` as Spark prints it (exactly *scale* fraction digits).

        A number or numeric text is taken as is; other text is Debezium's
        base64 of the big-endian two's-complement unscaled value.
        """
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            try:
                unscaled = int.from_bytes(base64.b64decode(str(value), validate=True), "big", signed=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError(f"Not a decimal value: {value!r}") from exc
            number = Decimal(unscaled).scaleb(-scale)
        return f"{number.quantize(Decimal(1).scaleb(-scale)):.{scale}f}" if scale else str(int(number))

    @staticmethod
    def date(value: Any) -> str:
        """``yyyy-MM-dd``: Debezium's epoch days, or a text date kept as is."""
        if isinstance(value, str):
            return value
        return (_EPOCH_DAY + timedelta(days=int(value))).isoformat()

    @staticmethod
    def timestamp(value: Any) -> str:
        """``yyyy-MM-dd HH:mm:ss[.ffffff]`` in UTC, trailing fraction zeros dropped.

        An integer is epoch nanoseconds (≥ 1e17), microseconds (≥ 1e14) or
        milliseconds; text is kept as is.
        """
        if isinstance(value, str):
            return value
        number = int(value)
        size = abs(number)
        micros = number // 1000 if size >= 10**17 else number if size >= 10**14 else number * 1000
        stamp = _EPOCH + timedelta(microseconds=micros)
        text = stamp.strftime("%Y-%m-%d %H:%M:%S")
        fraction = f"{stamp.microsecond:06d}".rstrip("0")
        return f"{text}.{fraction}" if fraction else text

    @staticmethod
    def _boolean(value: Any) -> str:
        if isinstance(value, str):
            return "true" if value.strip().lower() in ("true", "1", "t", "yes") else "false"
        return "true" if value else "false"

    @staticmethod
    def _binary(value: Any) -> str:
        """``CAST(binary AS STRING)`` decodes UTF-8; Debezium sends bytes as base64."""
        try:
            return base64.b64decode(str(value), validate=True).decode("utf-8", "replace")
        except (binascii.Error, ValueError):
            return str(value)

    @staticmethod
    def _scale(spark_type: str) -> int:
        """The scale of ``decimal(p,s)`` (``decimal`` alone: Spark's default ``(10,0)``)."""
        inside = spark_type.partition("(")[2].rstrip(")")
        parts = [p.strip() for p in inside.split(",") if p.strip()]
        return int(parts[1]) if len(parts) == 2 else 0
