"""Tests for the LadybugDB triple store backend (flat model)."""

import os
import shutil
import uuid
import pytest

real_ladybug = pytest.importorskip("real_ladybug", reason="real_ladybug not installed")

from back.core.graphdb.ladybugdb import _safe_table_id
from back.core.graphdb.ladybugdb.LadybugFlatStore import LadybugFlatStore
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.errors import ValidationError

LadybugTripleStore = LadybugFlatStore

TEST_DB_BASE = "/tmp/ontobricks"


@pytest.fixture()
def store():
    """Create a LadybugTripleStore with a unique DB name, cleaned up after the test."""
    name = f"test_{uuid.uuid4().hex[:8]}"
    s = LadybugTripleStore(db_path=TEST_DB_BASE, db_name=name)
    yield s
    s.close()
    path = s._get_db_path()
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)


SAMPLE_TRIPLES = [
    {
        "subject": "http://ex.org/Customer/1",
        "predicate": RDF_TYPE,
        "object": "http://ex.org/Customer",
    },
    {"subject": "http://ex.org/Customer/1", "predicate": RDFS_LABEL, "object": "Alice"},
    {
        "subject": "http://ex.org/Customer/2",
        "predicate": RDF_TYPE,
        "object": "http://ex.org/Customer",
    },
    {"subject": "http://ex.org/Customer/2", "predicate": RDFS_LABEL, "object": "Bob"},
    {
        "subject": "http://ex.org/Order/10",
        "predicate": RDF_TYPE,
        "object": "http://ex.org/Order",
    },
    {
        "subject": "http://ex.org/Order/10",
        "predicate": RDFS_LABEL,
        "object": "Order-10",
    },
    {
        "subject": "http://ex.org/Customer/1",
        "predicate": "http://ex.org/hasOrder",
        "object": "http://ex.org/Order/10",
    },
]


def _loaded_store(store):
    """Create table and insert sample triples."""
    store.create_table("t")
    store.insert_triples("t", SAMPLE_TRIPLES)
    return store


class TestSafeTableId:
    def test_simple_name(self):
        assert _safe_table_id("triples") == "triples"

    def test_dotted_name(self):
        assert _safe_table_id("cat.sch.my_table") == "my_table"

    def test_special_chars(self):
        assert _safe_table_id("my-table!@#") == "my_table___"

    def test_empty_string(self):
        assert _safe_table_id("") == "triples"


class TestLadybugTripleStoreLifecycle:
    def test_create_and_close(self, store):
        store.create_table("test_table")
        assert store.table_exists("test_table")

    def test_drop_table(self, store):
        store.create_table("test_table")
        assert store.table_exists("test_table")
        store.drop_table("test_table")
        assert not store.table_exists("test_table")

    def test_create_table_empty_name_raises(self, store):
        with pytest.raises(ValidationError, match="table_name cannot be empty"):
            store.create_table("")

    def test_drop_table_empty_name_raises(self, store):
        with pytest.raises(ValidationError, match="table_name cannot be empty"):
            store.drop_table("")


class TestInsertAndQuery:
    def test_insert_and_count(self, store):
        store.create_table("t")
        count = store.insert_triples("t", SAMPLE_TRIPLES)
        assert count == len(SAMPLE_TRIPLES)
        assert store.count_triples("t") == len(SAMPLE_TRIPLES)

    def test_insert_empty_list(self, store):
        store.create_table("t")
        assert store.insert_triples("t", []) == 0

    def test_insert_empty_table_raises(self, store):
        with pytest.raises(ValidationError, match="table_name cannot be empty"):
            store.insert_triples(
                "", [{"subject": "a", "predicate": "b", "object": "c"}]
            )

    def test_query_triples_returns_all(self, store):
        _loaded_store(store)
        rows = store.query_triples("t")
        assert len(rows) == len(SAMPLE_TRIPLES)
        subjects = {r["subject"] for r in rows}
        assert "http://ex.org/Customer/1" in subjects
        assert "http://ex.org/Order/10" in subjects

    def test_on_progress_callback(self, store):
        store.create_table("t")
        progress_calls = []
        store.insert_triples(
            "t",
            SAMPLE_TRIPLES,
            batch_size=3,
            on_progress=lambda done, total: progress_calls.append((done, total)),
        )
        assert len(progress_calls) > 0
        assert progress_calls[-1][0] == len(SAMPLE_TRIPLES)


class TestGetStatus:
    def test_status_fields(self, store):
        _loaded_store(store)
        status = store.get_status("t")
        assert status["count"] == len(SAMPLE_TRIPLES)
        assert status["format"] == "ladybug"
        assert "path" in status

    def test_status_empty_name_raises(self, store):
        with pytest.raises(ValidationError):
            store.get_status("")


class TestExecuteQuery:
    def test_raises_not_implemented(self, store):
        with pytest.raises(NotImplementedError, match="Cypher"):
            store.execute_query("SELECT 1")


class TestAggregateStats:
    def test_stats_correct(self, store):
        _loaded_store(store)
        stats = store.get_aggregate_stats("t")
        assert stats["total"] == len(SAMPLE_TRIPLES)
        assert stats["distinct_subjects"] == 3
        assert stats["type_assertion_count"] == 3
        assert stats["label_count"] == 3


class TestTypeDistribution:
    def test_distribution(self, store):
        _loaded_store(store)
        dist = store.get_type_distribution("t")
        type_map = {d["type_uri"]: d["cnt"] for d in dist}
        assert type_map["http://ex.org/Customer"] == 2
        assert type_map["http://ex.org/Order"] == 1


class TestPredicateDistribution:
    def test_distribution(self, store):
        _loaded_store(store)
        dist = store.get_predicate_distribution("t")
        pred_map = {d["predicate"]: d["cnt"] for d in dist}
        assert pred_map[RDF_TYPE] == 3
        assert pred_map[RDFS_LABEL] == 3
        assert pred_map["http://ex.org/hasOrder"] == 1


class TestFindSubjectsByType:
    def test_find_customers(self, store):
        _loaded_store(store)
        subjects = store.find_subjects_by_type("t", "http://ex.org/Customer")
        assert len(subjects) == 2
        assert "http://ex.org/Customer/1" in subjects

    def test_find_with_search(self, store):
        _loaded_store(store)
        subjects = store.find_subjects_by_type(
            "t", "http://ex.org/Customer", search="alice"
        )
        assert len(subjects) == 1
        assert subjects[0] == "http://ex.org/Customer/1"

    def test_find_with_limit_offset(self, store):
        _loaded_store(store)
        page1 = store.find_subjects_by_type(
            "t", "http://ex.org/Customer", limit=1, offset=0
        )
        page2 = store.find_subjects_by_type(
            "t", "http://ex.org/Customer", limit=1, offset=1
        )
        assert len(page1) == 1
        assert len(page2) == 1
        assert page1[0] != page2[0]


class TestResolveSubjectById:
    def test_resolve_existing(self, store):
        _loaded_store(store)
        uri = store.resolve_subject_by_id("t", "http://ex.org/Customer", "1")
        assert uri == "http://ex.org/Customer/1"

    def test_resolve_missing(self, store):
        _loaded_store(store)
        uri = store.resolve_subject_by_id("t", "http://ex.org/Customer", "999")
        assert uri is None


class TestGetTriplesForSubjects:
    def test_returns_triples(self, store):
        _loaded_store(store)
        triples = store.get_triples_for_subjects("t", ["http://ex.org/Customer/1"])
        subjects = {t["subject"] for t in triples}
        assert subjects == {"http://ex.org/Customer/1"}
        assert len(triples) == 3  # rdf:type + rdfs:label + hasOrder

    def test_empty_subjects(self, store):
        assert store.get_triples_for_subjects("t", []) == []


class TestGetPredicatesForType:
    def test_returns_predicates(self, store):
        _loaded_store(store)
        preds = store.get_predicates_for_type("t", "http://ex.org/Customer")
        assert RDF_TYPE in preds
        assert RDFS_LABEL in preds


class TestFindSeedSubjects:
    def test_by_type_only(self, store):
        _loaded_store(store)
        seeds = store.find_seed_subjects("t", entity_type="http://ex.org/Customer")
        assert len(seeds) == 2

    def test_by_label_contains(self, store):
        _loaded_store(store)
        seeds = store.find_seed_subjects(
            "t", value="bob", field="label", match_type="contains"
        )
        assert "http://ex.org/Customer/2" in seeds

    def test_by_type_and_label(self, store):
        _loaded_store(store)
        seeds = store.find_seed_subjects(
            "t",
            entity_type="http://ex.org/Customer",
            value="alice",
            field="label",
            match_type="contains",
        )
        assert seeds == {"http://ex.org/Customer/1"}

    def test_by_id_field(self, store):
        _loaded_store(store)
        seeds = store.find_seed_subjects(
            "t", value="Customer/1", field="id", match_type="contains"
        )
        assert "http://ex.org/Customer/1" in seeds


class TestPaginatedTriples:
    def test_pagination(self, store):
        _loaded_store(store)
        page = store.paginated_triples("t", [], limit=3, offset=0)
        assert len(page) == 3

    def test_with_conditions(self, store):
        _loaded_store(store)
        page = store.paginated_triples(
            "t", [f"predicate = '{RDF_TYPE}'"], limit=10, offset=0
        )
        assert len(page) == 3
        for row in page:
            assert row["predicate"] == RDF_TYPE


class TestPaginatedCount:
    def test_count_no_conditions(self, store):
        _loaded_store(store)
        assert store.paginated_count("t", []) == len(SAMPLE_TRIPLES)

    def test_count_with_condition(self, store):
        _loaded_store(store)
        assert store.paginated_count("t", [f"predicate = '{RDF_TYPE}'"]) == 3


class TestBfsTraversal:
    def test_returns_entities(self, store):
        _loaded_store(store)
        result = store.bfs_traversal("t", f" WHERE predicate = '{RDF_TYPE}'", depth=1)
        entities = {r["entity"] for r in result}
        assert "http://ex.org/Customer/1" in entities


class TestFindSubjectsByPatterns:
    def test_pattern_match(self, store):
        _loaded_store(store)
        found = store.find_subjects_by_patterns("t", ["%Customer/1"])
        assert "http://ex.org/Customer/1" in found

    def test_empty_patterns(self, store):
        assert store.find_subjects_by_patterns("t", []) == set()


class TestExpandEntityNeighbors:
    def test_finds_neighbors(self, store):
        _loaded_store(store)
        neighbors = store.expand_entity_neighbors("t", {"http://ex.org/Customer/1"})
        assert "http://ex.org/Order/10" in neighbors

    def test_empty_set(self, store):
        assert store.expand_entity_neighbors("t", set()) == set()


class TestTranslateConditions:
    def test_eq_condition(self):
        result = LadybugTripleStore._translate_conditions(
            ["predicate = 'http://x'"], "t"
        )
        assert result == ["t.predicate = 'http://x'"]

    def test_like_contains(self):
        result = LadybugTripleStore._translate_conditions(["subject LIKE '%foo%'"], "t")
        assert result == ["t.subject CONTAINS 'foo'"]

    def test_like_ends_with(self):
        result = LadybugTripleStore._translate_conditions(["subject LIKE '%/bar'"], "t")
        assert result == ["t.subject ENDS WITH '/bar'"]

    def test_like_starts_with(self):
        result = LadybugTripleStore._translate_conditions(["subject LIKE 'http%'"], "t")
        assert result == ["t.subject STARTS WITH 'http'"]

    def test_unsupported_dropped(self):
        result = LadybugTripleStore._translate_conditions(
            ["subject IN (SELECT ...)"], "t"
        )
        assert result == []
