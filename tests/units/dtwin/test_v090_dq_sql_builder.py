"""QualitySqlBuilder: legacy SQL builders for SHACL-style quality checks (0.9.0 extract)."""

import pytest

from back.objects.digitaltwin import DigitalTwin
from back.objects.digitaltwin.quality.QualitySqlBuilder import QualitySqlBuilder
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL

pytestmark = pytest.mark.unit

TABLE = "cat.sch.triples"
CUSTOMER = "https://ex.com/onto#Customer"
EMAIL = "https://ex.com/onto/email"
KNOWS = "https://ex.com/onto/knows"


def build(check_type, params=None):
    return QualitySqlBuilder.build_quality_sql(check_type, TABLE, params or {})


# ---------------------------------------------------------------------------
# Dispatcher


def test_unknown_check_type_returns_none():
    assert build("doesNotExist", {"property_uri": EMAIL}) is None


def test_digitaltwin_delegator_forwards_to_builder():
    params = {"property_uri": EMAIL}
    assert DigitalTwin.build_quality_sql("functional", TABLE, params) == build(
        "functional", params
    )


# ---------------------------------------------------------------------------
# Cardinality


@pytest.mark.parametrize(
    "constraint, having",
    [
        ("minCardinality", "HAVING COUNT(t2.object) < 2"),
        ("maxCardinality", "HAVING COUNT(t2.object) > 2"),
        ("exactCardinality", "HAVING COUNT(t2.object) != 2"),
    ],
)
def test_cardinality_sql_groups_by_subject_with_matching_having(constraint, having):
    sql = build(
        "cardinality",
        {
            "class_uri": CUSTOMER,
            "property_uri": EMAIL,
            "constraint_type": constraint,
            "cardinality_value": "2",
        },
    )
    assert f"FROM {TABLE} t1" in sql
    assert f"JOIN {TABLE} t2" in sql
    assert f"AND t2.predicate = '{EMAIL}'" in sql
    assert f"WHERE t1.predicate = '{RDF_TYPE}'" in sql
    assert f"AND t1.object = '{CUSTOMER}'" in sql
    assert "GROUP BY t1.subject" in sql
    assert sql.rstrip().endswith(having)


def test_cardinality_with_unknown_constraint_type_is_skipped():
    params = {"class_uri": CUSTOMER, "property_uri": EMAIL, "constraint_type": "atLeast"}
    assert build("cardinality", params) is None


@pytest.mark.parametrize(
    "params",
    [
        {"property_uri": EMAIL, "constraint_type": "minCardinality"},
        {"class_uri": CUSTOMER, "constraint_type": "minCardinality"},
    ],
)
def test_cardinality_without_class_or_property_is_skipped(params):
    assert build("cardinality", params) is None


def test_cardinality_escapes_quotes_in_uris():
    sql = build(
        "cardinality",
        {
            "class_uri": "https://ex.com/O'Brien",
            "property_uri": "https://ex.com/p'x",
            "constraint_type": "minCardinality",
            "cardinality_value": 1,
        },
    )
    assert "t1.object = 'https://ex.com/O''Brien'" in sql
    assert "t2.predicate = 'https://ex.com/p''x'" in sql


def test_cardinality_value_is_coerced_to_int_so_no_sql_can_be_injected():
    with pytest.raises(ValueError):
        build(
            "cardinality",
            {
                "class_uri": CUSTOMER,
                "property_uri": EMAIL,
                "constraint_type": "minCardinality",
                "cardinality_value": "1; DROP TABLE x",
            },
        )


# ---------------------------------------------------------------------------
# Value checks


def test_value_not_null_uses_left_join_anti_pattern():
    sql = build(
        "value",
        {"class_uri": CUSTOMER, "attribute_uri": EMAIL, "value_check_type": "notNull"},
    )
    assert f"LEFT JOIN {TABLE} t2" in sql
    assert f"AND t2.predicate = '{EMAIL}'" in sql
    assert f"AND t1.object = '{CUSTOMER}'" in sql
    assert sql.rstrip().endswith("AND t2.subject IS NULL")


@pytest.mark.parametrize(
    "check_type, clause",
    [
        ("startsWith", "AND NOT LOWER(t2.object) LIKE LOWER('abc%') ESCAPE '!'"),
        ("endsWith", "AND NOT LOWER(t2.object) LIKE LOWER('%abc') ESCAPE '!'"),
        ("contains", "AND NOT LOWER(t2.object) LIKE LOWER('%abc%') ESCAPE '!'"),
        ("equals", "AND LOWER(t2.object) != LOWER('abc')"),
        ("notEquals", "AND LOWER(t2.object) = LOWER('abc')"),
        ("matches", "AND NOT t2.object RLIKE 'abc'"),
    ],
)
def test_value_check_filter_clause_selects_violating_values(check_type, clause):
    sql = build(
        "value",
        {
            "class_uri": CUSTOMER,
            "attribute_uri": EMAIL,
            "value_check_type": check_type,
            "check_value": "abc",
        },
    )
    assert sql.startswith("SELECT t1.subject AS s, t2.object AS val")
    assert f"JOIN {TABLE} t2" in sql and "LEFT JOIN" not in sql
    assert sql.rstrip().endswith(clause)


def test_value_check_escapes_quotes_and_backslashes_in_literal():
    sql = build(
        "value",
        {
            "class_uri": CUSTOMER,
            "attribute_uri": EMAIL,
            "value_check_type": "equals",
            "check_value": "x' OR '1'='1\\",
        },
    )
    assert "LOWER('x'' OR ''1''=''1\\\\')" in sql
    assert "OR '1'='1'" not in sql


@pytest.mark.parametrize(
    "params",
    [
        {"attribute_uri": EMAIL, "value_check_type": "notNull"},
        {"class_uri": CUSTOMER, "value_check_type": "notNull"},
    ],
)
def test_value_check_without_class_or_attribute_is_skipped(params):
    assert build("value", params) is None


def test_value_check_with_unknown_type_is_skipped():
    sql = build(
        "value",
        {
            "class_uri": CUSTOMER,
            "attribute_uri": EMAIL,
            "value_check_type": "greaterThan",
            "check_value": "5",
        },
    )
    assert sql is None


def test_starts_with_treats_check_value_wildcards_literally():
    sql = build(
        "value",
        {
            "class_uri": CUSTOMER,
            "attribute_uri": EMAIL,
            "value_check_type": "startsWith",
            "check_value": "A_",
        },
    )
    assert "LIKE LOWER('A!_%') ESCAPE '!'" in sql


# ---------------------------------------------------------------------------
# OWL property characteristics


def test_functional_flags_subjects_with_more_than_one_object():
    sql = build("functional", {"property_uri": KNOWS})
    assert sql.startswith("SELECT subject AS s, COUNT(object) AS count")
    assert f"FROM {TABLE}" in sql
    assert f"WHERE predicate = '{KNOWS}'" in sql
    assert "GROUP BY subject" in sql
    assert sql.endswith("HAVING COUNT(object) > 1")


def test_inverse_functional_groups_by_object():
    sql = build("inverseFunctional", {"property_uri": KNOWS})
    assert sql.startswith("SELECT object AS o, COUNT(subject) AS count")
    assert "GROUP BY object" in sql
    assert sql.endswith("HAVING COUNT(subject) > 1")


def test_symmetric_finds_missing_reverse_edge():
    sql = build("symmetric", {"property_uri": KNOWS})
    assert f"LEFT JOIN {TABLE} t2" in sql
    assert "ON t1.subject = t2.object" in sql
    assert "AND t1.object = t2.subject" in sql
    assert sql.count(f"predicate = '{KNOWS}'") == 2
    assert sql.endswith("AND t2.subject IS NULL")


def test_asymmetric_finds_existing_reverse_edge():
    sql = build("asymmetric", {"property_uri": KNOWS})
    assert f"JOIN {TABLE} t2" in sql and "LEFT JOIN" not in sql
    assert sql.endswith(f"WHERE t1.predicate = '{KNOWS}'")


def test_irreflexive_finds_self_loops():
    sql = build("irreflexive", {"property_uri": KNOWS})
    assert f"WHERE predicate = '{KNOWS}'" in sql
    assert sql.endswith("AND subject = object")


@pytest.mark.parametrize(
    "check_type",
    ["functional", "inverseFunctional", "symmetric", "asymmetric", "irreflexive"],
)
def test_property_checks_without_property_uri_are_skipped(check_type):
    assert build(check_type, {}) is None


def test_property_check_escapes_quote_in_property_uri():
    sql = build("irreflexive", {"property_uri": "https://ex.com/p' OR 1=1 --"})
    assert "predicate = 'https://ex.com/p'' OR 1=1 --'" in sql


def test_property_sql_with_unexpected_type_returns_none():
    assert QualitySqlBuilder._build_property_sql(TABLE, "transitive", {"property_uri": KNOWS}) is None


# ---------------------------------------------------------------------------
# Labels / orphans


def test_require_labels_finds_typed_subjects_without_rdfs_label():
    sql = build("requireLabels")
    assert f"LEFT JOIN {TABLE} t2" in sql
    assert f"AND t2.predicate = '{RDFS_LABEL}'" in sql
    assert f"WHERE t1.predicate = '{RDF_TYPE}'" in sql
    assert sql.endswith("AND t2.subject IS NULL")


def test_no_orphans_ignores_type_and_label_triples():
    sql = build("noOrphans")
    assert "AND NOT EXISTS (" in sql
    assert f"SELECT 1 FROM {TABLE} t2" in sql
    assert f"AND t2.predicate != '{RDF_TYPE}'" in sql
    assert f"AND t2.predicate != '{RDFS_LABEL}'" in sql


# ---------------------------------------------------------------------------
# SWRL


@pytest.fixture
def reset_swrl_translator(monkeypatch):
    monkeypatch.setattr(QualitySqlBuilder, "_swrl_sql_translator", None)


def test_swrl_translator_is_created_once_and_cached(reset_swrl_translator):
    first = QualitySqlBuilder._get_swrl_translator()
    assert QualitySqlBuilder._get_swrl_translator() is first


def test_swrl_check_delegates_to_translator(reset_swrl_translator):
    sql = build(
        "swrl",
        {
            "antecedent": "Customer(?c) ^ hasOrder(?c, ?o)",
            "consequent": "VIP(?c)",
            "base_uri": "https://ex.com/onto#",
        },
    )
    assert sql.startswith("SELECT DISTINCT")
    assert f"FROM {TABLE} a1" in sql
    assert "a1.object = 'https://ex.com/onto#Customer'" in sql
    assert "NOT EXISTS" in sql and "https://ex.com/onto#VIP" in sql


def test_swrl_check_without_atoms_is_skipped(reset_swrl_translator):
    assert build("swrl", {"antecedent": "", "consequent": ""}) is None
