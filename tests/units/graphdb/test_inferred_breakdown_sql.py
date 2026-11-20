"""Oracle test for the inferred breakdown SQL.

The generated SQL runs against stdlib sqlite3 (substr/CASE/COUNT DISTINCT are
portable). The expected numbers come from an independent Python classifier
that is cross-checked against the cohort / community vocabulary classes.
"""

import sqlite3

import pytest

from back.core.graph_analysis.cohort.CohortVocabulary import CohortVocabulary
from back.core.graph_analysis.community.CommunityVocabulary import CommunityVocabulary
from back.core.graphdb.inferred_breakdown import (
    CATEGORIES,
    build_breakdown_sql,
    normalise_breakdown,
)

BASE = "http://ontobricks.io/ns/demo#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
OTHER = "http://other.example/"

ROWS = [
    # reasoning (plain inferred facts, provenance not recoverable)
    ("ex:a", RDF_TYPE, "ex:Thing"),
    ("ex:b", "ex:score", "3"),
    (OTHER + "x", OTHER + "inCohortRule1", OTHER + "y"),  # other base -> reasoning
    # cohort metadata
    (f"{BASE}cohort/rule1/c-h1", RDF_TYPE, f"{BASE}Cohort"),
    (f"{BASE}cohort/rule1/c-h1", f"{BASE}fromRule", "rule1"),
    (f"{BASE}cohort/rule1/c-h1", f"{BASE}cohortSize", "2"),
    # cohort membership
    ("ex:a", f"{BASE}inCohortRule1", f"{BASE}cohort/rule1/c-h1"),
    ("ex:b", f"{BASE}inCohortRule1", f"{BASE}cohort/rule1/c-h1"),
    # community membership + metadata
    ("ex:a", f"{BASE}inCluster", f"{BASE}cluster/c-0"),
    ("ex:b", f"{BASE}inCluster", f"{BASE}cluster/c-1"),
    (f"{BASE}cluster/c-0", f"{BASE}clusterSize", "1"),
    (f"{BASE}cluster/c-0", f"{BASE}fromAlgorithm", "lpa"),
]


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _classify(subject: str, predicate: str) -> str:
    if subject.startswith(f"{BASE}cohort/") or predicate.startswith(f"{BASE}inCohort"):
        return "cohorts"
    if predicate in (f"{BASE}fromRule", f"{BASE}cohortSize"):
        return "cohorts"
    if subject.startswith(f"{BASE}cluster/"):
        return "communities"
    if predicate in (
        f"{BASE}inCluster",
        f"{BASE}clusterSize",
        f"{BASE}fromAlgorithm",
        f"{BASE}modularity",
        f"{BASE}resolution",
    ):
        return "communities"
    return "reasoning"


def _oracle(rows):
    expected = {key: {"triples": 0, "subjects": set(), "groups": set()} for key, _ in CATEGORIES}
    for s, p, o in rows:
        cat = _classify(s, p)
        bucket = expected[cat]
        bucket["triples"] += 1
        bucket["subjects"].add(s)
        if cat != "reasoning" and (p.startswith(f"{BASE}inCohort") or p == f"{BASE}inCluster"):
            bucket["groups"].add(o)
    return {
        key: (b["triples"], len(b["subjects"]), None if key == "reasoning" else len(b["groups"]))
        for key, b in expected.items()
    }


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE g (subject TEXT, predicate TEXT, object TEXT)")
    conn.executemany("INSERT INTO g VALUES (?, ?, ?)", ROWS)
    yield conn
    conn.close()


def _run(conn, base=BASE):
    sql = build_breakdown_sql("g", base, _escape)
    return normalise_breakdown([dict(r) for r in conn.execute(sql).fetchall()])


def test_breakdown_sql_matches_python_oracle(db):
    result = _run(db)
    got = {r["category"]: (r["triples"], r["entities"], r["groups"]) for r in result}
    assert got == _oracle(ROWS)


def test_categories_sum_to_total_rows(db):
    result = _run(db)
    assert sum(r["triples"] for r in result) == len(ROWS)


def test_python_classifier_agrees_with_vocabulary_classes():
    """The oracle's hard-coded fragments must match the vocabulary helpers."""
    assert CohortVocabulary.in_cohort(BASE, "Rule1").startswith(f"{BASE}inCohort")
    assert CohortVocabulary.cohort_prefix(BASE, "rule1").startswith(f"{BASE}cohort/")
    assert CohortVocabulary.from_rule(BASE) == f"{BASE}fromRule"
    assert CohortVocabulary.cohort_size(BASE) == f"{BASE}cohortSize"
    assert CommunityVocabulary.in_cluster(BASE) == f"{BASE}inCluster"
    assert CommunityVocabulary.cluster_size(BASE) == f"{BASE}clusterSize"
    assert CommunityVocabulary.from_algorithm(BASE) == f"{BASE}fromAlgorithm"
    assert CommunityVocabulary.modularity(BASE) == f"{BASE}modularity"
    assert CommunityVocabulary.resolution(BASE) == f"{BASE}resolution"
    assert CommunityVocabulary.cluster_prefix(BASE) == f"{BASE}cluster/"


def test_base_uri_without_trailing_separator_uses_join_convention():
    """A bare base (no ``/`` or ``#``) gets ``/`` inserted, as the vocabulary does."""
    bare = "http://ontobricks.io/ns/demo"
    rows = [
        ("ex:a", f"{bare}/inCluster", f"{bare}/cluster/c-0"),
        (f"{bare}/cluster/c-0", f"{bare}/clusterSize", "1"),
    ]
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE g (subject TEXT, predicate TEXT, object TEXT)")
    conn.executemany("INSERT INTO g VALUES (?, ?, ?)", rows)
    try:
        sql = build_breakdown_sql("g", bare, _escape)
        result = normalise_breakdown([dict(r) for r in conn.execute(sql).fetchall()])
    finally:
        conn.close()
    comm = next(r for r in result if r["category"] == "communities")
    assert comm["triples"] == 2
    assert comm["groups"] == 1


def test_normalise_fills_missing_categories_with_zero():
    result = normalise_breakdown([])
    assert [r["category"] for r in result] == ["reasoning", "cohorts", "communities"]
    assert [r["triples"] for r in result] == [0, 0, 0]
    assert result[0]["groups"] is None
    assert result[1]["groups"] == 0


def test_labels_are_stable():
    assert [label for _, label in CATEGORIES] == ["Reasoning", "Cohorts", "Communities"]


def test_quote_in_base_uri_is_escaped():
    base = "http://o'brien.example/ns#"
    sql = build_breakdown_sql("g", base, _escape)
    assert "o''brien" in sql
