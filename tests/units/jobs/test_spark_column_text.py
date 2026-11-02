"""``SparkColumnText``: envelope values as ``CAST(… AS STRING)`` prints them."""

from __future__ import annotations

import base64

import pytest

from jobs.triple_stream.SparkColumnText import SparkColumnText

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("value", "spark_type", "text"),
    [
        (None, "int", None),
        (7, "int", "7"),
        ("7", "bigint", "7"),
        (True, "boolean", "true"),
        (0, "boolean", "false"),
        ("hello", "string", "hello"),
        (12, "string", "12"),
        (3.5, "double", "3.5"),
        (100.0, "double", "100.0"),
        (1e20, "double", "1.0E20"),
        (1.234e-05, "double", "1.234E-5"),
        (12345678.9, "double", "1.23456789E7"),
        (0.0, "float", "0.0"),
        (float("nan"), "double", "NaN"),
        ("12.5", "decimal(10,2)", "12.50"),
        (7, "decimal(10,0)", "7"),
        (19723, "date", "2024-01-01"),
        ("2024-01-01", "date", "2024-01-01"),
        (1704067200000, "timestamp", "2024-01-01 00:00:00"),
        (1704067200123456, "timestamp", "2024-01-01 00:00:00.123456"),
        (1704067200500000000, "timestamp_ntz", "2024-01-01 00:00:00.5"),
        ({"b": 1, "a": [2]}, "struct<a:array<int>,b:int>", '{"a":[2],"b":1}'),
    ],
)
def test_each_type_prints_like_spark(value, spark_type, text):
    assert SparkColumnText.text(value, spark_type) == text


def test_debezium_precise_decimals_are_base64_unscaled_bytes():
    encoded = base64.b64encode((-1250).to_bytes(2, "big", signed=True)).decode()

    assert SparkColumnText.text(encoded, "decimal(10,2)") == "-12.50"


def test_binary_is_decoded_as_utf8():
    assert SparkColumnText.text(base64.b64encode("héllo".encode()).decode(), "binary") == "héllo"


def test_a_row_is_typed_by_its_columns_case_insensitively():
    row = SparkColumnText.row({"ID": 7, "Born": 19723, "note": None}, {"id": "int", "born": "date"})

    assert row == {"ID": "7", "Born": "2024-01-01", "note": None}


def test_an_unknown_column_is_text():
    assert SparkColumnText.row({"x": 5}, {}) == {"x": "5"}
