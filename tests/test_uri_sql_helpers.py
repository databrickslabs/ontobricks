"""Tests for back.core.helpers.URIHelpers and SQLHelpers."""

import pytest

from back.core.helpers.URIHelpers import URIHelpers
from back.core.helpers.SQLHelpers import SQLHelpers


class TestURIHelpers:
    def test_is_uri_http(self):
        assert URIHelpers.is_uri("http://example.org/thing") is True

    def test_is_uri_https(self):
        assert URIHelpers.is_uri("https://example.org/thing") is True

    def test_is_uri_not_uri(self):
        assert URIHelpers.is_uri("Customer") is False

    def test_is_uri_empty(self):
        assert URIHelpers.is_uri("") is False

    def test_extract_local_name_fragment(self):
        assert URIHelpers.extract_local_name("http://example.org/ont#Customer") == "Customer"

    def test_extract_local_name_path(self):
        assert URIHelpers.extract_local_name("http://example.org/ont/Customer") == "Customer"

    def test_extract_local_name_empty(self):
        assert URIHelpers.extract_local_name("") == ""

    def test_safe_identifier_basic(self):
        assert URIHelpers.safe_identifier("hello_world") == "hello_world"

    def test_safe_identifier_special_chars(self):
        assert URIHelpers.safe_identifier("hello-world!") == "hello_world_"

    def test_safe_identifier_starts_with_digit(self):
        result = URIHelpers.safe_identifier("123abc")
        assert not result[0].isdigit()
        assert result == "_123abc"

    def test_safe_identifier_custom_prefix(self):
        result = URIHelpers.safe_identifier("123", prefix="cls_")
        assert result == "cls_123"

    def test_safe_identifier_empty(self):
        assert URIHelpers.safe_identifier("") == "_unnamed"


class TestSQLHelpers:
    def test_sql_escape_quotes(self):
        assert SQLHelpers.sql_escape("it's a test") == "it''s a test"

    def test_sql_escape_backslash(self):
        assert SQLHelpers.sql_escape("a\\b") == "a\\\\b"

    def test_sql_escape_none(self):
        assert SQLHelpers.sql_escape(None) == ""

    def test_sql_escape_normal(self):
        assert SQLHelpers.sql_escape("hello") == "hello"

    def test_validate_table_name_valid(self):
        SQLHelpers.validate_table_name("catalog.schema.table")

    def test_validate_table_name_empty(self):
        from back.core.errors import ValidationError
        with pytest.raises(ValidationError):
            SQLHelpers.validate_table_name("")

    def test_validate_table_name_whitespace(self):
        from back.core.errors import ValidationError
        with pytest.raises(ValidationError):
            SQLHelpers.validate_table_name("   ")

    def test_effective_view_table_from_domain(self):
        class FakeDomain:
            delta = {"catalog": "cat", "schema": "sch"}
            info = {"name": "MyDomain"}
            current_version = "2"

        result = SQLHelpers.effective_view_table(FakeDomain())
        assert result == "cat.sch.triplestore_mydomain_V2"

    def test_effective_view_table_no_name_raises(self):
        from back.core.errors import ValidationError

        class FakeDomain:
            delta = {"catalog": "cat", "schema": "sch"}
            info = {}
            current_version = "1"

        with pytest.raises(ValidationError):
            SQLHelpers.effective_view_table(FakeDomain())

    def test_effective_view_table_without_registry(self):
        """With a domain name but no registry, return the bare derived view name."""

        class FakeDomain:
            delta = {"catalog": "", "schema": ""}
            info = {"name": "MyDomain"}
            current_version = "1"

        result = SQLHelpers.effective_view_table(FakeDomain())
        assert result == "triplestore_mydomain_V1"

    def test_effective_graph_name(self):
        class FakeDomain:
            info = {"name": "TestGraph"}
            current_version = "3"

        assert SQLHelpers.effective_graph_name(FakeDomain()) == "TestGraph_V3"

    def test_effective_graph_name_defaults(self):
        result = SQLHelpers.effective_graph_name(object())
        assert "_V" in result
