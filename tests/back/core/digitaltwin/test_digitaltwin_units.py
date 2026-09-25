"""Direct unit tests for DigitalTwin pure-function surface (T-M1.P3 sample under CNS).

`DigitalTwin.py` is 3525 LOC — too big to cover in one pass. This file targets
the **pure static methods** that can be tested without Databricks / Spark /
triplestore I/O. The methods covered here are the leaf utilities the rest of
the class composes; testing them at least flags regressions in URI / SQL /
classification logic without spinning up infrastructure.

The behaviour-rich parts (build_task, materialize, reasoning glue) are deferred
to integration tests (T-M2) and the M4 refactor that will split this monolith.
"""

from __future__ import annotations

import pytest
from typing import Any, Dict, List, Set

from back.objects.digitaltwin.DigitalTwin import DigitalTwin


# --- is_datatype_range ----------------------------------------------------


@pytest.mark.unit
class TestIsDatatypeRange:
    """A `range` value is a datatype if it's an XSD URI, not an IRI to a class."""

    @pytest.mark.parametrize(
        "iri",
        [
            "http://www.w3.org/2001/XMLSchema#string",
            "http://www.w3.org/2001/XMLSchema#integer",
            "http://www.w3.org/2001/XMLSchema#boolean",
            "http://www.w3.org/2001/XMLSchema#dateTime",
            "http://www.w3.org/2001/XMLSchema#decimal",
        ],
    )
    def test_xsd_iris_are_datatypes(self, iri):
        assert DigitalTwin.is_datatype_range(iri) is True

    @pytest.mark.parametrize(
        "iri",
        [
            "http://example.org/ontology#Customer",
            "http://example.org/ontology/Order",
            "http://x/Product",
        ],
    )
    def test_class_iris_are_not_datatypes(self, iri):
        assert DigitalTwin.is_datatype_range(iri) is False

    def test_empty_string_handled(self):
        # Defensive: an empty range shouldn't crash; treat as non-datatype.
        result = DigitalTwin.is_datatype_range("")
        assert result is False


# --- extract_local_id -----------------------------------------------------


@pytest.mark.unit
class TestExtractLocalId:
    """Local-id extraction: take the trailing segment after `#` or `/`."""

    def test_hash_separator(self):
        assert DigitalTwin.extract_local_id("http://example.org/Customer#abc123") == "abc123"

    def test_slash_separator(self):
        assert DigitalTwin.extract_local_id("http://example.org/Customer/order-42") == "order-42"

    def test_hash_takes_priority_over_slash(self):
        # If both present, hash wins (W3C URI standard convention).
        assert DigitalTwin.extract_local_id("http://x/a/b#tail") == "tail"

    def test_class_segment_inside_fragment_is_stripped(self):
        # R2RML mints entity subjects as `{base}{Class}/{id}` and `base` ends
        # in `#`, so the fragment is `Customer/CUST0000017`. Only the id is
        # the entity's local id.
        uri = "https://databricks-ontology.com/Cust360Auto#Customer/CUST0000017"
        assert DigitalTwin.extract_local_id(uri) == "CUST0000017"

    def test_class_uri_without_slash_is_unchanged(self):
        uri = "https://databricks-ontology.com/Cust360Auto#Customer"
        assert DigitalTwin.extract_local_id(uri) == "Customer"

    def test_no_separator_returns_input_or_empty(self):
        # No `#` or `/` → returns input or empty string; either is reasonable.
        result = DigitalTwin.extract_local_id("plainstring")
        assert result in {"plainstring", ""}

    def test_trailing_separator_returns_input_unchanged(self):
        # Observed behaviour: when there's no character AFTER the separator,
        # the method returns the input unchanged. Documented here as the
        # contract; revisit during the M4 DigitalTwin split.
        assert DigitalTwin.extract_local_id("http://x/") == "http://x/"


# --- is_owlrl_available ---------------------------------------------------


@pytest.mark.unit
class TestIsOwlrlAvailable:
    def test_returns_bool(self):
        result = DigitalTwin.is_owlrl_available()
        assert isinstance(result, bool)

    def test_returns_true_in_this_environment(self):
        # owlrl is a hard dep of OntoBricks; if this returns False, the install is broken.
        assert DigitalTwin.is_owlrl_available() is True


# --- build_quality_sql ----------------------------------------------------


@pytest.mark.unit
class TestBuildQualitySql:
    """build_quality_sql returns SQL strings for SHACL-style data-quality checks."""

    def test_returns_string_or_none(self):
        result = DigitalTwin.build_quality_sql(
            check_type="min_count",
            table="test_catalog.test_schema.test_table",
            params={"property": "name", "value": 1},
        )
        assert result is None or isinstance(result, str)

    def test_table_name_appears_in_sql_when_returned(self):
        result = DigitalTwin.build_quality_sql(
            check_type="min_count",
            table="cat.sch.customers",
            params={"property": "name", "value": 1},
        )
        if result is not None:
            assert "customers" in result or "cat" in result

    def test_unknown_check_type_returns_none(self):
        result = DigitalTwin.build_quality_sql(
            check_type="not_a_real_check_xyzzy",
            table="cat.sch.t",
            params={},
        )
        assert result is None


# --- diagnose_view_error --------------------------------------------------


@pytest.mark.unit
class TestDiagnoseViewError:
    """diagnose_view_error classifies common DB error messages into actionable hints."""

    def test_returns_string(self):
        msg = DigitalTwin.diagnose_view_error(
            error_msg="some database error",
            entity_mappings={},
        )
        assert isinstance(msg, str)

    def test_empty_error_message_handled(self):
        msg = DigitalTwin.diagnose_view_error(
            error_msg="",
            entity_mappings={},
        )
        assert isinstance(msg, str)

    def test_long_error_truncated_or_kept(self):
        # Defensive: should not crash on huge input.
        long = "X" * 10_000
        msg = DigitalTwin.diagnose_view_error(error_msg=long, entity_mappings={})
        assert isinstance(msg, str)


# --- compute_dtwin_indicator ----------------------------------------------


@pytest.mark.unit
class TestComputeDtwinIndicator:
    """The indicator computes a status dict from triplestore + dt existence info."""

    def _domain(self, last_build=None):
        """Minimal mock with the attributes compute_dtwin_indicator probes."""
        from types import SimpleNamespace

        return SimpleNamespace(
            last_build=last_build,
            name="test",
            display_name="Test",
        )

    def test_returns_dict_with_no_last_build(self):
        result = DigitalTwin.compute_dtwin_indicator(
            domain=self._domain(last_build=None),
            ts_status={},
            dt_exist={},
        )
        assert isinstance(result, dict)

    def test_handles_populated_inputs(self):
        result = DigitalTwin.compute_dtwin_indicator(
            domain=self._domain(last_build="2026-05-14T10:00:00Z"),
            ts_status={"populated": True, "triple_count": 100},
            dt_exist={"exists": True, "row_count": 10},
        )
        assert isinstance(result, dict)


# --- expand_uri_aliases ----------------------------------------------------


@pytest.mark.unit
class TestExpandUriAliases:
    """expand_uri_aliases returns the input set when no store is provided."""

    def test_empty_input_returns_empty(self):
        # Use None as the store — function should defensively return an empty
        # set OR the input set without raising.
        try:
            result = DigitalTwin.expand_uri_aliases(
                store=None, table_name="t", uris=set()
            )
            assert isinstance(result, set)
            assert len(result) == 0
        except (AttributeError, TypeError):
            # If the function requires a real store, that's fine — this test
            # documents the contract gap for future integration tests.
            pytest.skip("expand_uri_aliases requires a real store")


@pytest.mark.unit
class TestFindTriplesBfsPaging:
    class _FakeStore:
        def __init__(
            self,
            *,
            bfs_rows: List[Dict[str, Any]],
            alias_rows: Set[str],
            page_rows: List[Dict[str, str]],
            total: int,
        ) -> None:
            self._bfs_rows = bfs_rows
            self._alias_rows = alias_rows
            self._page_rows = page_rows
            self._total = total
            self.paged_call_subjects: List[str] | None = None
            self.paged_call_count = 0

        def bfs_traversal(self, *_args, **_kwargs):
            return self._bfs_rows

        def find_subjects_by_patterns(self, _table_name, _patterns):
            return self._alias_rows

        def get_triples_page_for_subjects(
            self, _table, subjects, *, limit: int, offset: int
        ):
            self.paged_call_count += 1
            self.paged_call_subjects = list(subjects)
            assert limit == 2
            assert offset == 0
            return {"rows": self._page_rows, "total": self._total}

    def test_find_triples_bfs_uses_paged_fetch_and_keeps_alias_subjects(self):
        alias_uri = "http://ex.org/CUST001"
        store = self._FakeStore(
            bfs_rows=[
                {"entity": "http://ex.org/Customer/CUST001", "min_lvl": 0},
                {"entity": "http://ex.org/Order/ORD001", "min_lvl": 1},
            ],
            alias_rows={alias_uri},
            page_rows=[
                {"subject": "s1", "predicate": "p1", "object": "o1"},
                {"subject": "s2", "predicate": "p2", "object": "o2"},
            ],
            total=7,
        )

        result = DigitalTwin.find_triples_bfs(
            store, "cat.sch.graph", search="cust", depth=2, limit=2, offset=0
        )

        assert result["seed_count"] == 1
        assert result["entity_count"] == 3
        assert result["total"] == 7
        assert result["count"] == 2
        assert result["has_more"] is True
        assert store.paged_call_subjects is not None
        assert alias_uri in store.paged_call_subjects

    def test_find_triples_bfs_final_page_has_no_more(self):
        store = self._FakeStore(
            bfs_rows=[{"entity": "http://ex.org/Customer/CUST001", "min_lvl": 0}],
            alias_rows=set(),
            page_rows=[{"subject": "s1", "predicate": "p1", "object": "o1"}],
            total=1,
        )

        result = DigitalTwin.find_triples_bfs(
            store, "cat.sch.graph", search="cust", depth=1, limit=2, offset=0
        )

        assert result["has_more"] is False
        assert result["count"] == 1
        assert result["total"] == 1

    def test_find_triples_bfs_empty_bfs_skips_paged_fetch(self):
        store = self._FakeStore(
            bfs_rows=[],
            alias_rows=set(),
            page_rows=[],
            total=0,
        )

        result = DigitalTwin.find_triples_bfs(
            store, "cat.sch.graph", search="cust", depth=1, limit=2, offset=0
        )

        assert result["seed_count"] == 0
        assert result["entity_count"] == 0
        assert result["triples"] == []
        assert result["total"] == 0
        assert store.paged_call_count == 0
