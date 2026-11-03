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

    def test_sql_cast_emits_try_cast(self):
        assert SQLHelpers.sql_cast("t2.object", "DOUBLE") == "TRY_CAST(t2.object AS DOUBLE)"

    def test_sql_numeric_delegates_to_sql_cast(self):
        assert SQLHelpers.sql_numeric("t2.object") == SQLHelpers.sql_cast("t2.object", "DOUBLE")

    def test_sql_numeric_custom_type_delegates_to_sql_cast(self):
        assert SQLHelpers.sql_numeric("t2.object", "BIGINT") == SQLHelpers.sql_cast(
            "t2.object", "BIGINT"
        )

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

    def test_validate_uc_identifier_accepts_hyphen(self):
        assert SQLHelpers.validate_uc_identifier("my-catalog", role="catalog") == "my-catalog"

    def test_validate_uc_identifier_accepts_leading_digit(self):
        """UC quoted identifiers may start with a digit (e.g. 5_g_subscribers)."""
        assert (
            SQLHelpers.validate_uc_identifier("5_g_subscribers", role="table")
            == "5_g_subscribers"
        )

    def test_validate_uc_identifier_rejects_injection(self):
        from back.core.errors import ValidationError
        with pytest.raises(ValidationError, match="Invalid UC catalog"):
            SQLHelpers.validate_uc_identifier("cat; DROP", role="catalog")

    def test_quote_uc_fqn(self):
        assert SQLHelpers.quote_uc_fqn("main", "default", "tbl") == "`main`.`default`.`tbl`"

    def test_quote_uc_fqn_leading_digit_table(self):
        assert (
            SQLHelpers.quote_uc_fqn("main", "default", "5_g_subscribers")
            == "`main`.`default`.`5_g_subscribers`"
        )

    def test_rewrite_collapsed_uc_fqn_from_clause(self):
        sql = (
            "SELECT id AS ID, name AS Label FROM "
            "`catalog_poc_dard.anonym_bcp_ddv_lyt_modeloloyalty.hd_activity_participant` "
            "WHERE id IS NOT NULL"
        )
        rewritten = SQLHelpers.rewrite_collapsed_uc_fqn(sql)
        assert (
            "`catalog_poc_dard`.`anonym_bcp_ddv_lyt_modeloloyalty`"
            ".`hd_activity_participant`"
        ) in rewritten
        assert (
            "`catalog_poc_dard.anonym_bcp_ddv_lyt_modeloloyalty."
            "hd_activity_participant`"
        ) not in rewritten

    def test_rewrite_collapsed_uc_fqn_join_clause(self):
        sql = (
            "SELECT a.id FROM `cat.sch.left` a "
            "JOIN `cat.sch.right` b ON a.id = b.id"
        )
        rewritten = SQLHelpers.rewrite_collapsed_uc_fqn(sql)
        assert "`cat`.`sch`.`left`" in rewritten
        assert "`cat`.`sch`.`right`" in rewritten
        assert "`cat.sch.left`" not in rewritten
        assert "`cat.sch.right`" not in rewritten

    def test_rewrite_leaves_per_segment_quotes_and_bare_fqn(self):
        already_split = "SELECT * FROM `cat`.`sch`.`tbl`"
        bare = "SELECT * FROM cat.sch.tbl"
        assert SQLHelpers.rewrite_collapsed_uc_fqn(already_split) == already_split
        assert SQLHelpers.rewrite_collapsed_uc_fqn(bare) == bare

    def test_rewrite_leaves_dotted_column_quotes(self):
        sql = "SELECT `customer.id` AS ID FROM cat.sch.tbl"
        assert SQLHelpers.rewrite_collapsed_uc_fqn(sql) == sql

    def test_find_collapsed_uc_fqn(self):
        sql = "SELECT 1 FROM `a.b.c` JOIN `x`.`y`.`z`"
        hits = SQLHelpers.find_collapsed_uc_fqn(sql)
        assert hits == ["`a.b.c`"]

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

    def test_effective_view_table_stringifies_integer_version(self):
        class FakeDomain:
            delta = {"catalog": "cat", "schema": "sch"}
            info = {"name": "BigCustomers100m"}
            current_version = 1

        assert (
            SQLHelpers.effective_view_table(FakeDomain())
            == "cat.sch.triplestore_bigcustomers100m_V1"
        )

    def test_effective_graph_name(self):
        class FakeDomain:
            info = {"name": "TestGraph"}
            current_version = "3"

        assert SQLHelpers.effective_graph_name(FakeDomain()) == "TestGraph_V3"

    def test_effective_graph_name_defaults(self):
        result = SQLHelpers.effective_graph_name(object())
        assert "_V" in result
