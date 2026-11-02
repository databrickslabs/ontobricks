"""DebeziumValueText: SQL turning a Debezium value's JSON text into ``CAST(<column> AS STRING)``."""

from __future__ import annotations

import pytest

from back.core.streaming import DebeziumValueText

pytestmark = pytest.mark.unit

_UNSCALED = (
    "aggregate(regexp_extract_all(substr(hex(try_to_binary(v, 'base64')), 3), '..', 0), "
    "CAST(coalesce(CAST(conv(substr(hex(try_to_binary(v, 'base64')), 1, 2), 16, 10) AS INT), 0) "
    "- IF(hex(try_to_binary(v, 'base64')) >= '8', 256, 0) AS DECIMAL(38, 0)), "
    "(acc, b) -> CAST(try_add(try_multiply(acc, 256), CAST(conv(b, 16, 10) AS INT)) "
    "AS DECIMAL(38, 0)))"
)


class TestTextTypes:
    @pytest.mark.parametrize(
        "uc_type", ["string", "STRING", "varchar(10)", "char(3)", "array<int>"]
    )
    def test_a_text_or_nested_type_keeps_the_text(self, uc_type):
        assert DebeziumValueText.sql("v", uc_type) is None

    def test_an_unknown_type_keeps_the_text(self):
        assert DebeziumValueText.sql("v", "interval day") is None


class TestConversions:
    def test_a_date_is_epoch_days_or_text(self):
        assert DebeziumValueText.sql("v", "date") == (
            "CASE WHEN v RLIKE '^-?[0-9]+$' "
            "THEN CAST(date_add(DATE'1970-01-01', try_cast(v AS INT)) AS STRING) ELSE v END"
        )

    @pytest.mark.parametrize("uc_type", ["timestamp", "timestamp_ntz", "TIMESTAMP"])
    def test_a_timestamp_is_epoch_milli_micro_or_nanoseconds_by_magnitude(self, uc_type):
        n = "try_cast(v AS BIGINT)"
        assert DebeziumValueText.sql("v", uc_type) == (
            "CASE WHEN v RLIKE '^-?[0-9]+$' THEN CAST(CASE "
            f"WHEN abs({n}) >= 100000000000000000 THEN timestamp_micros(({n} - pmod({n}, 1000)) DIV 1000) "
            f"WHEN abs({n}) >= 100000000000000 THEN timestamp_micros({n}) "
            f"ELSE timestamp_millis({n}) END AS STRING) ELSE v END"
        )

    def test_a_decimal_is_numeric_text_or_base64_unscaled_bytes(self):
        assert DebeziumValueText.sql("v", "decimal(10,2)") == (
            "CASE WHEN v RLIKE '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$' "
            "THEN CAST(try_cast(v AS DECIMAL(38, 2)) AS STRING) "
            f"ELSE CAST(try_cast(concat(CAST({_UNSCALED} AS STRING), 'E-2') AS DECIMAL(38, 2)) "
            "AS STRING) END"
        )

    @pytest.mark.parametrize("uc_type", ["decimal", "DECIMAL(38, 0)", "decimal(5)"])
    def test_a_decimal_without_scale_prints_the_unscaled_value(self, uc_type):
        assert DebeziumValueText.sql("v", uc_type) == (
            "CASE WHEN v RLIKE '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$' "
            "THEN CAST(try_cast(v AS DECIMAL(38, 0)) AS STRING) "
            f"ELSE CAST({_UNSCALED} AS STRING) END"
        )

    def test_a_boolean_is_true_for_true_words_and_non_zero_numbers(self):
        assert DebeziumValueText.sql("v", "boolean") == (
            "CASE WHEN v IS NULL THEN NULL "
            "WHEN lower(trim(v)) IN ('true', 't', 'yes') OR try_cast(v AS DOUBLE) != 0 "
            "THEN 'true' ELSE 'false' END"
        )

    @pytest.mark.parametrize("uc_type", ["int", "bigint", "smallint", "tinyint", "long"])
    def test_an_integral_number_is_read_as_a_bigint(self, uc_type):
        assert DebeziumValueText.sql("v", uc_type) == "CAST(try_cast(v AS BIGINT) AS STRING)"

    @pytest.mark.parametrize("uc_type", ["double", "float", "real"])
    def test_a_floating_number_is_read_as_a_double(self, uc_type):
        assert DebeziumValueText.sql("v", uc_type) == "CAST(try_cast(v AS DOUBLE) AS STRING)"

    def test_binary_is_base64_decoded_as_utf8_else_kept(self):
        assert DebeziumValueText.sql("v", "binary") == (
            "coalesce(CAST(try_to_binary(v, 'base64') AS STRING), v)"
        )

    def test_the_value_expression_is_substituted(self):
        assert (
            DebeziumValueText.sql("x['c']", "int") == "CAST(try_cast(x['c'] AS BIGINT) AS STRING)"
        )


class TestScale:
    @pytest.mark.parametrize(
        ("uc_type", "scale"),
        [("decimal(10,2)", 2), ("DECIMAL(38, 6)", 6), ("decimal", 0), ("decimal(5)", 0)],
    )
    def test_the_scale_of_a_decimal_type(self, uc_type, scale):
        assert DebeziumValueText.scale(uc_type) == scale
