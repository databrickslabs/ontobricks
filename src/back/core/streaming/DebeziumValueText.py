"""SQL turning a Debezium value's JSON text into the text ``CAST(<typed column> AS STRING)`` gives.

The topic slot view reads change images as ``MAP<STRING, STRING>``: a value is
the text ``from_json`` gives its JSON value (a JSON string as is, a number or
boolean as its JSON text). Stock Debezium encodes a value by its column type,
so that text is not what the change-feed view's ``CAST(<column> AS STRING)``
prints: a date is epoch days, a timestamp epoch milli/micro/nanoseconds, a
decimal the base64 of its unscaled bytes (``decimal.handling.mode=precise``,
or its text with ``string``). These SQL expressions apply the stream job's
rules (``jobs.triple_stream.SparkColumnText``) from the column's Unity Catalog
type, so the topic-fed slots equal the stream job's and the change feed's.

Residuals (shared with the stream job unless noted):

* ``from_json`` cannot tell a JSON number from a numeric JSON string: a date
  or timestamp sent as a string of digits is read as an epoch count here (the
  stream job keeps the text), a boolean column's ``"2"`` reads ``true`` here
  (``false`` there); a base64 decimal whose text happens to be numeric
  (``1234``) is read as that number in both;
* decimal text with more fraction digits than the column's scale rounds
  HALF_UP here (as ``CAST`` does) and half-even / truncated in the stream job;
  Debezium sends exactly the column's scale;
* a timestamp's unit is guessed from its magnitude (as the stream job does):
  micro- and nanosecond values within about 3 years of 1970 read as milli /
  microseconds; timestamps print in the pipeline's session time zone (UTC by
  default on Databricks), the stream job's in UTC;
* nested types (``array``, ``map``, ``struct``) keep Debezium's JSON text;
* a value its type cannot read is NULL here (a blank object, or no slots when
  it is a key column) where the stream job skips the whole record.
"""

from __future__ import annotations

_INTEGRAL = ("tinyint", "smallint", "int", "bigint", "integer", "long", "short", "byte")
_FLOATING = ("double", "float", "real")
_TIMESTAMP_UNITS = (
    # (minimum magnitude, micros from n): nanoseconds floor-divided, microseconds, else milliseconds.
    (10**17, "timestamp_micros(({n} - pmod({n}, 1000)) DIV 1000)"),
    (10**14, "timestamp_micros({n})"),
)


class DebeziumValueText:
    """SQL expressions reading a Debezium-encoded value's text as its typed column's text."""

    #: An epoch count (days, or a timestamp unit), as ``RLIKE`` matches it.
    INTEGER_TEXT = "^-?[0-9]+$"
    #: Plain decimal text (``string`` mode, or a JSON number), as ``RLIKE`` matches it.
    NUMERIC_TEXT = "^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$"

    @staticmethod
    def sql(value: str, uc_type: str | None) -> str | None:
        """SQL of *value* (a STRING expression) as a *uc_type* column prints; ``None`` keeps it.

        *uc_type* is the column's Unity Catalog / Spark type (``int``,
        ``decimal(10,2)``, ``timestamp`` …), case-insensitive. Text, nested
        and unknown types return ``None``: the text is already the value.
        """
        kind = (uc_type or "").strip().lower()
        if kind in _INTEGRAL:
            return f"CAST(try_cast({value} AS BIGINT) AS STRING)"
        if kind in _FLOATING:
            return f"CAST(try_cast({value} AS DOUBLE) AS STRING)"
        if kind == "boolean":
            return (
                f"CASE WHEN {value} IS NULL THEN NULL "
                f"WHEN lower(trim({value})) IN ('true', 't', 'yes') "
                f"OR try_cast({value} AS DOUBLE) != 0 THEN 'true' ELSE 'false' END"
            )
        if kind == "date":
            return DebeziumValueText._epoch(
                value, f"date_add(DATE'1970-01-01', try_cast({value} AS INT))"
            )
        if kind.startswith("timestamp"):
            return DebeziumValueText._epoch(value, DebeziumValueText._timestamp(value))
        if kind.startswith("decimal"):
            return DebeziumValueText._decimal(value, DebeziumValueText.scale(kind))
        if kind == "binary":
            return f"coalesce(CAST(try_to_binary({value}, 'base64') AS STRING), {value})"
        return None

    @staticmethod
    def scale(uc_type: str) -> int:
        """The scale of ``decimal(p,s)``; ``decimal`` / ``decimal(p)`` have scale 0."""
        inside = uc_type.partition("(")[2].rstrip(")")
        parts = [part.strip() for part in inside.split(",") if part.strip()]
        return int(parts[1]) if len(parts) == 2 else 0

    @staticmethod
    def _epoch(value: str, typed: str) -> str:
        """*typed* (a DATE / TIMESTAMP of the integer *value*) as text; other text is kept."""
        return (
            f"CASE WHEN {value} RLIKE '{DebeziumValueText.INTEGER_TEXT}' "
            f"THEN CAST({typed} AS STRING) ELSE {value} END"
        )

    @staticmethod
    def _timestamp(value: str) -> str:
        """The TIMESTAMP of an epoch count, its unit guessed from its magnitude (the stream job's)."""
        n = f"try_cast({value} AS BIGINT)"
        branches = " ".join(
            f"WHEN abs({n}) >= {minimum} THEN {micros.format(n=n)}"
            for minimum, micros in _TIMESTAMP_UNITS
        )
        return f"CASE {branches} ELSE timestamp_millis({n}) END"

    @staticmethod
    def _decimal(value: str, scale: int) -> str:
        """Numeric text cast to the scale, else base64 unscaled bytes shifted by it."""
        unscaled = f"CAST({DebeziumValueText._unscaled(value)} AS STRING)"
        if scale:
            unscaled = (
                f"CAST(try_cast(concat({unscaled}, 'E-{scale}') AS DECIMAL(38, {scale})) AS STRING)"
            )
        return (
            f"CASE WHEN {value} RLIKE '{DebeziumValueText.NUMERIC_TEXT}' "
            f"THEN CAST(try_cast({value} AS DECIMAL(38, {scale})) AS STRING) "
            f"ELSE {unscaled} END"
        )

    @staticmethod
    def _unscaled(value: str) -> str:
        """The big-endian two's-complement integer of base64 *value*, as ``DECIMAL(38, 0)``.

        The first byte is signed, each next one shifts in unsigned; an
        overflow (more than 38 digits) or invalid base64 is NULL.
        """
        hex_text = f"hex(try_to_binary({value}, 'base64'))"
        first = (
            f"CAST(coalesce(CAST(conv(substr({hex_text}, 1, 2), 16, 10) AS INT), 0) "
            f"- IF({hex_text} >= '8', 256, 0) AS DECIMAL(38, 0))"
        )
        return (
            f"aggregate(regexp_extract_all(substr({hex_text}, 3), '..', 0), {first}, "
            "(acc, b) -> CAST(try_add(try_multiply(acc, 256), CAST(conv(b, 16, 10) AS INT)) "
            "AS DECIMAL(38, 0)))"
        )
