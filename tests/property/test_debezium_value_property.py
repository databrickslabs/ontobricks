"""Property: the topic view's typed values equal the stream job's ``SparkColumnText`` text.

The topic slot view reads Debezium images as ``MAP<STRING, STRING>``, so a
value arrives as ``from_json``'s text of its JSON value; ``DebeziumValueText``
turns that text into ``CAST(<typed column> AS STRING)`` in SQL. The stream
job (``SparkColumnText``) does the same in Python from the decoded JSON value.
For Debezium-shaped values — epoch-day dates, epoch milli / micro /
nanosecond timestamps, base64 unscaled decimals (``precise``) or plain
decimal text (``string``), booleans as JSON booleans or 0 / 1 — the model
of the SQL rule below gives the stream job's text.

The model encodes each SQL expression step by step with Spark's semantics
(``CAST(DECIMAL AS STRING)`` prints plain, rounding is HALF_UP, ``DIV``
truncates, ``pmod`` is non-negative); the last class checks the model's
constants against the SQL ``DebeziumValueText`` emits. Values the stream job
cannot read (it skips their record) are outside the domain.
"""

from __future__ import annotations

import base64
import json
import re
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Context, Decimal, localcontext

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from back.core.streaming import DebeziumValueText
from jobs.triple_stream.SparkColumnText import SparkColumnText

pytestmark = pytest.mark.property

_SETTINGS = settings(max_examples=300, deadline=None)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_INTEGER = r"-?[0-9]+"
_NUMERIC = r"[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?"


def _json_text(value) -> str | None:
    """``from_json``'s ``MAP<STRING, STRING>`` value: a JSON string as is, else its JSON text."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)


def _spark_timestamp(micros: int) -> str:
    """``CAST(<timestamp> AS STRING)`` in UTC: trailing fraction zeros dropped."""
    stamp = _EPOCH + timedelta(microseconds=micros)
    fraction = f"{stamp.microsecond:06d}".rstrip("0")
    return stamp.strftime("%Y-%m-%d %H:%M:%S") + (f".{fraction}" if fraction else "")


def _spark_decimal(number: Decimal, scale: int) -> str:
    """``CAST(CAST(<number> AS DECIMAL(38, scale)) AS STRING)``: HALF_UP, plain."""
    with localcontext(Context(prec=80)):
        return f"{number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP):.{scale}f}"


class SQLModel:
    """What the SQL of ``DebeziumValueText.sql`` computes from a value's text."""

    @staticmethod
    def date(text: str) -> str:
        if re.fullmatch(_INTEGER, text):
            return (date(1970, 1, 1) + timedelta(days=int(text))).isoformat()
        return text

    @staticmethod
    def timestamp(text: str) -> str:
        if not re.fullmatch(_INTEGER, text):
            return text
        n = int(text)
        if abs(n) >= 10**17:
            micros = (n - n % 1000) // 1000  # (n - pmod(n, 1000)) DIV 1000, exact
        elif abs(n) >= 10**14:
            micros = n  # timestamp_micros
        else:
            micros = n * 1000  # timestamp_millis
        return _spark_timestamp(micros)

    @staticmethod
    def decimal(text: str, scale: int) -> str:
        if re.fullmatch(_NUMERIC, text):
            return _spark_decimal(Decimal(text), scale)
        hex_text = base64.b64decode(text).hex().upper()
        unscaled = int(hex_text[:2] or "0", 16) - (256 if hex_text >= "8" else 0)
        for pair in re.findall("..", hex_text[2:]):
            unscaled = unscaled * 256 + int(pair, 16)
        if not scale:
            return str(unscaled)
        return _spark_decimal(Decimal(f"{unscaled}E-{scale}"), scale)

    @staticmethod
    def boolean(text: str) -> str:
        try:
            number = float(text)
        except ValueError:
            number = 0.0
        return "true" if text.strip(" ").lower() in ("true", "t", "yes") or number != 0 else "false"


def _java_bytes(unscaled: int, extra: int) -> bytes:
    """``BigInteger.toByteArray()`` (minimal two's complement), sign-extended by *extra* bytes."""
    bits = (unscaled if unscaled >= 0 else ~unscaled).bit_length()
    return unscaled.to_bytes(bits // 8 + 1 + extra, "big", signed=True)


def _stream_text(value, spark_type: str) -> str:
    try:
        return SparkColumnText.text(value, spark_type)
    except (ValueError, ArithmeticError, TypeError):
        assume(False)  # the stream job skips the record: outside the domain
        raise


@_SETTINGS
@given(st.dates(min_value=date(1, 1, 1), max_value=date(9999, 12, 31)), st.booleans())
def test_dates_match_the_stream_job(day, as_text):
    value = day.isoformat() if as_text else (day - date(1970, 1, 1)).days

    stream = _stream_text(value, "date")

    assert SQLModel.date(_json_text(value)) == stream


@_SETTINGS
@given(
    st.datetimes(min_value=datetime(1900, 1, 1), max_value=datetime(2200, 12, 31)),
    st.sampled_from(["millis", "micros", "nanos", "text"]),
    st.integers(min_value=0, max_value=999),
)
def test_timestamps_match_the_stream_job(moment, unit, nanos):
    micros = (moment.replace(tzinfo=timezone.utc) - _EPOCH) // timedelta(microseconds=1)
    value = {
        "millis": micros // 1000,
        "micros": micros,
        "nanos": micros * 1000 + nanos,
        "text": moment.isoformat(),
    }[unit]

    # Micro / nanoseconds within ~3 years of 1970 are read as milli / microseconds by
    # both (the magnitude rule); the stream job skips those that then overflow.
    stream = _stream_text(value, "timestamp")

    assert SQLModel.timestamp(_json_text(value)) == stream


@_SETTINGS
@given(
    st.integers(min_value=-(10**28) + 1, max_value=10**28 - 1),
    st.integers(min_value=0, max_value=10),
    st.sampled_from(["precise", "string"]),
    st.integers(min_value=0, max_value=2),
)
def test_decimals_match_the_stream_job(unscaled, scale, mode, extra):
    if mode == "precise":
        value = base64.b64encode(_java_bytes(unscaled, extra)).decode()
    else:
        value = format(Decimal(unscaled).scaleb(-scale), "f")
    spark_type = f"decimal(38,{scale})"

    stream = _stream_text(value, spark_type)

    assert SQLModel.decimal(_json_text(value), scale) == stream


@_SETTINGS
@given(
    st.one_of(
        st.booleans(),
        st.integers(min_value=-5, max_value=5),
        st.sampled_from(["true", "false", "TRUE", "t", "yes", "no", "1", "0"]),
    )
)
def test_booleans_match_the_stream_job(value):
    stream = _stream_text(value, "boolean")

    assert SQLModel.boolean(_json_text(value)) == stream


class TestTheModelMatchesTheSQL:
    """The constants and branches the model encodes, read off the emitted SQL."""

    def test_dates_and_timestamps_test_the_integer_pattern(self):
        for spark_type in ("date", "timestamp"):
            assert f"v RLIKE '^{_INTEGER}$'" in DebeziumValueText.sql("v", spark_type)

    def test_timestamps_split_by_magnitude_as_modelled(self):
        sql = DebeziumValueText.sql("v", "timestamp")

        assert f">= {10**17} THEN timestamp_micros((" in sql and "pmod(" in sql
        assert f">= {10**14} THEN timestamp_micros(" in sql
        assert "ELSE timestamp_millis(" in sql

    def test_decimals_test_the_numeric_pattern_then_fold_the_bytes(self):
        sql = DebeziumValueText.sql("v", "decimal(38,2)")

        assert f"v RLIKE '^{_NUMERIC}$'" in sql
        assert "- IF(hex(try_to_binary(v, 'base64')) >= '8', 256, 0)" in sql
        assert "try_add(try_multiply(acc, 256), CAST(conv(b, 16, 10) AS INT))" in sql
        assert "'E-2') AS DECIMAL(38, 2))" in sql

    def test_booleans_read_true_words_and_non_zero_numbers(self):
        sql = DebeziumValueText.sql("v", "boolean")

        assert "lower(trim(v)) IN ('true', 't', 'yes') OR try_cast(v AS DOUBLE) != 0" in sql
