"""The condition translators behind the Data Quality IF block.

``test_dataquality.py`` covers conditions through ``SHACLService`` — a shape in,
a query or a violation list out. These tests exercise
``ShapeConditions`` directly, where the operator vocabulary, the value quoting
and the three back ends (Spark SQL, in-memory, SPARQL) are decided.

The three back ends must agree: a condition that selects a subject in one must
select it in the others, otherwise a rule previewed in the UI, executed on the
warehouse and exported to Turtle would mean three different things.
"""

import pytest

from back.core.w3c.shacl import ShapeConditions

CLS = "http://test.org/ontology/Customer"
STATUS = "http://test.org/ontology/status"
AMOUNT = "http://test.org/ontology/amount"
ORDER = "http://test.org/ontology/hasOrder"
TABLE = "cat.sch.triples"


def _cond(op, value="", prop_uri=STATUS, name="status"):
    return {"property": name, "property_uri": prop_uri, "op": op, "value": value}


def _esc(value: str) -> str:
    return value.replace("'", "''")


def _sql(conditions, logic="and"):
    return ShapeConditions.subject_sql(conditions, logic, CLS, TABLE, _esc)


# ---------------------------------------------------------------------------
# The operator vocabulary
# ---------------------------------------------------------------------------


class TestOperators:
    def test_conditions_share_the_decision_table_vocabulary(self):
        """One vocabulary, so a decision table and a rule guard read alike."""
        from back.core.reasoning.constants import DT_OP_SQL

        assert ShapeConditions.CONDITION_OPS == (set(DT_OP_SQL) - {"any"}) | {
            "exists",
            "notExists",
        }

    def test_the_wildcard_operator_is_not_a_guard(self):
        """`any` means "don't care" in a decision table — a guard would be a no-op."""
        assert "any" not in ShapeConditions.CONDITION_OPS

    def test_only_conformance_and_consistency_accept_conditions(self):
        assert ShapeConditions.CONDITION_CATEGORIES == ("conformance", "consistency")


class TestGetConditions:
    def test_a_row_without_a_property_uri_is_dropped(self):
        shape = {"conditions": [{"property": "status", "op": "eq", "value": "active"}]}
        assert ShapeConditions.get_conditions(shape) == []

    def test_a_row_with_an_unknown_operator_is_dropped(self):
        assert ShapeConditions.get_conditions({"conditions": [_cond("matches", "x")]}) == []

    def test_a_usable_row_survives_beside_a_broken_one(self):
        shape = {"conditions": [_cond("eq", "active"), {"op": "eq", "value": "x"}]}
        assert ShapeConditions.get_conditions(shape) == [_cond("eq", "active")]

    @pytest.mark.parametrize("raw", [None, "", {}, 42, [None, "row"]])
    def test_a_malformed_condition_list_yields_nothing(self, raw):
        assert ShapeConditions.get_conditions({"conditions": raw}) == []

    def test_logic_defaults_to_and(self):
        assert ShapeConditions.get_logic({}) == "and"
        assert ShapeConditions.get_logic({"condition_logic": "nonsense"}) == "and"
        assert ShapeConditions.get_logic({"condition_logic": "or"}) == "or"


# ---------------------------------------------------------------------------
# Spark SQL
# ---------------------------------------------------------------------------


class TestSubjectSQL:
    def test_the_guard_selects_subjects_of_the_target_entity(self):
        sql = _sql([_cond("eq", "active")])
        assert "SELECT DISTINCT t0.subject AS s" in sql
        assert f"t0.object = '{CLS}'" in sql

    def test_no_conditions_yields_no_guard(self):
        assert _sql([]) is None

    def test_no_target_entity_yields_no_guard(self):
        """Without a class the guard would range over the whole triple store."""
        assert ShapeConditions.subject_sql([_cond("eq", "a")], "and", "", TABLE, _esc) is None

    def test_a_list_of_unusable_rows_yields_no_guard(self):
        assert _sql([_cond("eq", "a", prop_uri="")]) is None

    @pytest.mark.parametrize(
        "op,expected",
        [
            ("eq", "LOWER(c0.object) = 'active'"),
            ("neq", "LOWER(c0.object) <> 'active'"),
            # The doubled % comes from DT_OP_SQL, whose templates are shared with
            # the decision table. Two adjacent wildcards match what one matches,
            # so the pattern is correct.
            ("startsWith", "LOWER(c0.object) LIKE CONCAT('active', '%%')"),
            ("endsWith", "LOWER(c0.object) LIKE CONCAT('%%', 'active')"),
            ("contains", "LOWER(c0.object) LIKE CONCAT('%%', 'active', '%%')"),
        ],
    )
    def test_string_operators_compare_case_insensitively(self, op, expected):
        assert expected in _sql([_cond(op, "Active")])

    @pytest.mark.parametrize(
        "op,symbol", [("gt", ">"), ("gte", ">="), ("lt", "<"), ("lte", "<=")]
    )
    def test_numeric_operators_cast_the_object(self, op, symbol):
        # TRY_CAST, not CAST: warehouses run with ANSI mode on, where one
        # non-numeric object would abort the query instead of failing to match.
        sql = _sql([_cond(op, "1000", prop_uri=AMOUNT, name="amount")])
        assert f"TRY_CAST(c0.object AS DOUBLE) {symbol} 1000" in sql

    def test_a_numeric_equality_is_not_cast(self):
        """`eq` compares text; casting would fail on a non-numeric object."""
        sql = _sql([_cond("eq", "1000", prop_uri=AMOUNT, name="amount")])
        assert "c0.object = 1000" in sql
        assert "CAST" not in sql

    def test_a_quote_in_a_value_is_escaped(self):
        sql = _sql([_cond("eq", "O'Brien")])
        assert "'o''brien'" in sql
        assert "'o'brien'" not in sql

    def test_a_quote_in_a_property_uri_is_escaped(self):
        sql = _sql([_cond("eq", "active", prop_uri="http://x/it's")])
        assert "http://x/it''s" in sql

    def test_attribute_conditions_left_join(self):
        """An inner join would drop subjects the OR branch should still match."""
        sql = _sql([_cond("eq", "active")])
        assert f"LEFT JOIN {TABLE} c0" in sql
        assert "INNER JOIN" not in sql

    def test_existence_uses_a_correlated_subquery(self):
        sql = _sql([_cond("exists", prop_uri=ORDER, name="hasOrder")])
        assert "EXISTS (SELECT 1" in sql
        assert "NOT EXISTS" not in sql
        assert "LEFT JOIN" not in sql

    def test_non_existence_negates_the_subquery(self):
        sql = _sql([_cond("notExists", prop_uri=ORDER, name="hasOrder")])
        assert "NOT EXISTS (SELECT 1" in sql

    def test_and_joins_every_predicate(self):
        sql = _sql([_cond("eq", "active"), _cond("gt", "10", prop_uri=AMOUNT, name="amount")])
        assert " AND (" in sql
        assert " OR " not in sql

    def test_or_joins_the_predicates_with_or(self):
        sql = _sql(
            [_cond("eq", "active"), _cond("gt", "10", prop_uri=AMOUNT, name="amount")],
            logic="or",
        )
        assert " OR " in sql

    def test_each_condition_gets_its_own_alias(self):
        sql = _sql([_cond("eq", "active"), _cond("gt", "10", prop_uri=AMOUNT, name="amount")])
        assert "c0" in sql and "c1" in sql

    def test_a_property_uri_may_be_normalized(self):
        """The service reconciles hash and slash URIs before matching predicates."""
        hashed = "http://test.org/ontology#status"
        sql = ShapeConditions.subject_sql(
            [_cond("eq", "active", prop_uri=hashed)],
            "and",
            CLS,
            TABLE,
            _esc,
            lambda u: u.replace("#", "/"),
        )
        assert f"c0.predicate = '{STATUS}'" in sql
        assert hashed not in sql

    def test_wrap_restricts_an_existing_query(self):
        wrapped = ShapeConditions.wrap_sql("SELECT s FROM t", "SELECT s FROM guard")
        assert wrapped.startswith("SELECT v.* FROM (")
        assert "WHERE v.s IN (" in wrapped
        assert "SELECT s FROM t" in wrapped


# ---------------------------------------------------------------------------
# In-memory
# ---------------------------------------------------------------------------

INSTANCES = {"c1", "c2", "c3"}
BY_PRED = {
    STATUS: {"c1": ["active"], "c2": ["Closed"], "c3": []},
    AMOUNT: {"c1": ["1500"], "c2": ["10"], "c3": ["not-a-number"]},
    ORDER: {"c1": ["o1"]},
}


def _match(conditions, logic="and"):
    return ShapeConditions.matching_subjects(conditions, logic, INSTANCES, BY_PRED)


class TestMatchingSubjects:
    def test_no_conditions_matches_everything(self):
        assert _match([]) == INSTANCES

    def test_unusable_rows_match_everything(self):
        """A half-filled row must not silently narrow the rule."""
        assert _match([_cond("eq", "active", prop_uri="")]) == INSTANCES

    @pytest.mark.parametrize(
        "op,value,expected",
        [
            ("eq", "active", {"c1"}),
            ("eq", "ACTIVE", {"c1"}),
            ("neq", "active", {"c2"}),
            ("startsWith", "clo", {"c2"}),
            ("endsWith", "sed", {"c2"}),
            ("contains", "los", {"c2"}),
        ],
    )
    def test_string_operators_ignore_case(self, op, value, expected):
        assert _match([_cond(op, value)]) == expected

    @pytest.mark.parametrize(
        "op,value,expected",
        [
            ("gt", "100", {"c1"}),
            ("gte", "10", {"c1", "c2"}),
            ("lt", "100", {"c2"}),
            ("lte", "10", {"c2"}),
        ],
    )
    def test_numeric_operators(self, op, value, expected):
        assert _match([_cond(op, value, prop_uri=AMOUNT, name="amount")]) == expected

    def test_a_non_numeric_value_never_satisfies_a_numeric_comparison(self):
        """c3 holds "not-a-number"; comparing it must not raise or match."""
        assert "c3" not in _match([_cond("gt", "0", prop_uri=AMOUNT, name="amount")])

    def test_a_missing_property_matches_nothing_but_not_exists(self):
        assert _match([_cond("eq", "active", prop_uri="http://unknown")]) == set()

    def test_existence_reads_the_relationship(self):
        assert _match([_cond("exists", prop_uri=ORDER, name="hasOrder")]) == {"c1"}

    def test_non_existence_includes_subjects_with_no_value_at_all(self):
        assert _match([_cond("notExists", prop_uri=ORDER, name="hasOrder")]) == {"c2", "c3"}

    def test_and_requires_every_condition(self):
        assert _match([_cond("eq", "active"), _cond("notExists", prop_uri=ORDER)]) == set()

    def test_or_requires_one_condition(self):
        matched = _match(
            [_cond("eq", "active"), _cond("notExists", prop_uri=ORDER)], logic="or"
        )
        assert matched == {"c1", "c2", "c3"}

    def test_a_multi_valued_property_matches_existentially(self):
        by_pred = {STATUS: {"c1": ["closed", "active"]}}
        assert ShapeConditions.matching_subjects(
            [_cond("eq", "active")], "and", {"c1"}, by_pred
        ) == {"c1"}

    def test_a_property_uri_may_be_resolved(self):
        assert ShapeConditions.matching_subjects(
            [_cond("eq", "active", prop_uri=STATUS.replace("/", "#"))],
            "and",
            INSTANCES,
            BY_PRED,
            lambda u: u.replace("#", "/"),
        ) == {"c1"}


class TestBackEndsAgree:
    """The same condition must select the same subject everywhere."""

    @pytest.mark.parametrize(
        "op,value,prop",
        [
            ("eq", "active", STATUS),
            ("neq", "active", STATUS),
            ("contains", "act", STATUS),
            ("gt", "100", AMOUNT),
            ("lte", "10", AMOUNT),
            ("exists", "", ORDER),
            ("notExists", "", ORDER),
        ],
    )
    def test_every_operator_is_translatable_by_all_three(self, op, value, prop):
        conditions = [_cond(op, value, prop_uri=prop)]
        assert _sql(conditions) is not None
        assert ShapeConditions.sparql_target(conditions, "and", CLS) is not None
        ShapeConditions.matching_subjects(conditions, "and", INSTANCES, BY_PRED)


# ---------------------------------------------------------------------------
# SPARQL target
# ---------------------------------------------------------------------------


class TestSparqlTarget:
    def test_the_query_selects_focus_nodes_of_the_target_entity(self):
        query = ShapeConditions.sparql_target([_cond("eq", "active")], "and", CLS)
        assert query.startswith("SELECT $this WHERE {")
        assert f"$this a <{CLS}> ." in query

    def test_no_conditions_yields_no_target(self):
        assert ShapeConditions.sparql_target([], "and", CLS) is None

    def test_no_target_entity_yields_no_target(self):
        assert ShapeConditions.sparql_target([_cond("eq", "a")], "and", "") is None

    def test_full_iris_are_used(self):
        """Prefixes in a sh:select resolve from sh:prefixes, which we don't emit."""
        query = ShapeConditions.sparql_target([_cond("eq", "active")], "and", CLS)
        assert f"<{STATUS}>" in query
        assert "xsd:" not in query

    def test_string_comparison_is_case_insensitive(self):
        query = ShapeConditions.sparql_target([_cond("eq", "Active")], "and", CLS)
        assert 'LCASE(STR(?c0)) = "active"' in query

    def test_numeric_comparison_casts_to_double(self):
        query = ShapeConditions.sparql_target(
            [_cond("gt", "1000", prop_uri=AMOUNT, name="amount")], "and", CLS
        )
        assert "double>(?c0) > 1000" in query

    @pytest.mark.parametrize(
        "op,fn", [("startsWith", "STRSTARTS"), ("endsWith", "STRENDS"), ("contains", "CONTAINS")]
    )
    def test_string_functions(self, op, fn):
        query = ShapeConditions.sparql_target([_cond(op, "act")], "and", CLS)
        assert f"{fn}(LCASE(STR(?c0)), \"act\")" in query

    def test_a_quote_in_a_value_is_escaped(self):
        query = ShapeConditions.sparql_target([_cond("eq", 'say "hi"')], "and", CLS)
        assert '\\"hi\\"' in query

    def test_and_requires_the_triple_to_be_present(self):
        query = ShapeConditions.sparql_target([_cond("eq", "active")], "and", CLS)
        assert f"$this <{STATUS}> ?c0 ." in query
        assert "OPTIONAL" not in query

    def test_or_makes_each_triple_optional(self):
        """A subject missing one property must still match the other branch."""
        query = ShapeConditions.sparql_target(
            [_cond("eq", "active"), _cond("gt", "10", prop_uri=AMOUNT, name="amount")],
            "or",
            CLS,
        )
        assert query.count("OPTIONAL") == 2
        assert " || " in query

    def test_and_joins_the_filters(self):
        query = ShapeConditions.sparql_target(
            [_cond("eq", "active"), _cond("gt", "10", prop_uri=AMOUNT, name="amount")],
            "and",
            CLS,
        )
        assert " && " in query

    def test_existence_is_a_filter_under_and(self):
        query = ShapeConditions.sparql_target(
            [_cond("exists", prop_uri=ORDER, name="hasOrder")], "and", CLS
        )
        assert "FILTER EXISTS" in query

    def test_non_existence_is_a_negated_filter_under_and(self):
        query = ShapeConditions.sparql_target(
            [_cond("notExists", prop_uri=ORDER, name="hasOrder")], "and", CLS
        )
        assert "FILTER NOT EXISTS" in query

    def test_existence_is_bound_under_or(self):
        """A FILTER cannot be OR-ed with the other branches, so it is bound first."""
        query = ShapeConditions.sparql_target(
            [
                _cond("exists", prop_uri=ORDER, name="hasOrder"),
                _cond("eq", "active"),
            ],
            "or",
            CLS,
        )
        assert "BIND(EXISTS" in query
        assert "?e0 ||" in query

    def test_non_existence_is_negated_when_bound(self):
        query = ShapeConditions.sparql_target(
            [
                _cond("notExists", prop_uri=ORDER, name="hasOrder"),
                _cond("eq", "active"),
            ],
            "or",
            CLS,
        )
        assert "!?e0" in query

    def test_an_unusable_row_is_skipped(self):
        query = ShapeConditions.sparql_target(
            [_cond("eq", "active"), _cond("matches", "x")], "and", CLS
        )
        assert "matches" not in query
        assert "?c0" in query


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidate:
    def test_no_conditions_is_valid(self):
        for empty in (None, [], {}):
            assert ShapeConditions.validate(empty, "and", "completeness", "") is None

    def test_conditions_must_be_a_list(self):
        assert ShapeConditions.validate("nope", "and", "conformance", CLS) == (
            "Conditions must be a list"
        )

    def test_each_condition_must_be_an_object(self):
        assert ShapeConditions.validate(["nope"], "and", "conformance", CLS) == (
            "Each condition must be an object"
        )

    def test_logic_must_be_and_or_or(self):
        assert ShapeConditions.validate(
            [_cond("eq", "active")], "xor", "conformance", CLS
        ) == "Condition logic must be 'and' or 'or'"

    @pytest.mark.parametrize("category", ["completeness", "cardinality", "uniqueness"])
    def test_unsupported_dimensions_are_rejected(self, category):
        message = ShapeConditions.validate([_cond("eq", "a")], "and", category, CLS)
        assert "conformance and consistency" in message

    def test_a_value_of_zero_is_a_value(self):
        """Regression: a falsy but present value must not be read as missing."""
        assert (
            ShapeConditions.validate(
                [_cond("eq", "0", prop_uri=AMOUNT, name="amount")], "and", "conformance", CLS
            )
            is None
        )

    def test_a_blank_value_is_rejected(self):
        assert "needs a value" in ShapeConditions.validate(
            [_cond("eq", "   ")], "and", "conformance", CLS
        )
