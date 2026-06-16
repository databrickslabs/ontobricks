"""Direct unit tests for CohortService pure-function surface.

`CohortService` (`src/back/objects/digitaltwin/CohortService.py`, 609 LOC)
landed via the upstream merge on 2026-05-26. It is the extracted home for
every Cohort Discovery operation that used to live on `DigitalTwin`.
This file covers the **stateless** helpers — the three `@staticmethod`s
plus the pure logic of `suggest_uc_target` — without needing a real
domain session, store, or SQL warehouse.

Behaviour-rich paths (`dry_run`, `materialize`, `path_trace`,
`sample_values`, `explain`) require a real cohort builder + store and
are out of scope here; they are exercised by `tests/test_dtwin_cohort.py`
(upstream-authored) and the eval harness once landed.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from back.objects.digitaltwin.CohortService import CohortService


# --- _snake_case ---------------------------------------------------------


@pytest.mark.unit
class TestSnakeCase:
    """`_snake_case` powers the suggested UC table name (`cohorts_<slug>`)."""

    @pytest.mark.parametrize(
        "camel,expected",
        [
            ("ExemptStaffingPool", "exempt_staffing_pool"),
            ("Customer", "customer"),
            ("URLPath", "url_path"),  # trailing-acronym shape
            ("HTTPSConnection", "https_connection"),
            ("simple", "simple"),
            ("Already_Snake", "already_snake"),
            ("with spaces", "with_spaces"),
            ("dash-separated", "dash_separated"),
            ("MixOf123Numbers", "mix_of123_numbers"),
            ("ABC", "abc"),
        ],
    )
    def test_canonical_conversions(self, camel: str, expected: str) -> None:
        assert CohortService._snake_case(camel) == expected

    def test_empty_input_returns_empty(self) -> None:
        assert CohortService._snake_case("") == ""

    def test_only_punctuation_returns_empty(self) -> None:
        # All non-alnum chars collapse and then get trimmed.
        assert CohortService._snake_case("---") == ""

    def test_leading_and_trailing_punct_stripped(self) -> None:
        assert CohortService._snake_case("__Foo__") == "foo"

    def test_non_string_input_does_not_crash(self) -> None:
        # The implementation calls `str(name)` so numeric inputs work.
        assert CohortService._snake_case(42) == "42"  # type: ignore[arg-type]


# --- _result_to_dict -----------------------------------------------------


def _fake_cohort(idx: int, members: List[str]) -> SimpleNamespace:
    return SimpleNamespace(
        id=f"c{idx}", idx=idx, size=len(members), members=members
    )


def _fake_stats(**kw: Any) -> SimpleNamespace:
    base = dict(
        rule_id="rule-1",
        class_member_count=10,
        survivor_count=8,
        edge_count=12,
        cohort_count=2,
        grouped_member_count=8,
        elapsed_ms=42,
    )
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.mark.unit
class TestResultToDict:
    """The cohort-builder's dataclass result projects to a JSON-shaped dict."""

    def test_minimal_result_projects_correctly(self) -> None:
        result = SimpleNamespace(
            rule_id="r-min",
            cohorts=[],
            stats=_fake_stats(rule_id="r-min", cohort_count=0),
        )

        out = CohortService._result_to_dict(result)

        assert out["rule_id"] == "r-min"
        assert out["cohorts"] == []
        assert out["stats"]["cohort_count"] == 0
        assert out["stats"]["elapsed_ms"] == 42

    def test_cohort_members_preserved_as_uris(self) -> None:
        members = ["http://ex/a", "http://ex/b"]
        result = SimpleNamespace(
            rule_id="r1",
            cohorts=[_fake_cohort(0, members)],
            stats=_fake_stats(),
        )

        out = CohortService._result_to_dict(result)

        # At this stage `_result_to_dict` keeps the raw URI list; enrichment
        # happens later in `_enrich_members`.
        assert out["cohorts"][0]["members"] == members
        assert out["cohorts"][0]["size"] == 2
        assert out["cohorts"][0]["idx"] == 0

    def test_all_stats_fields_present(self) -> None:
        result = SimpleNamespace(
            rule_id="r2",
            cohorts=[_fake_cohort(0, [])],
            stats=_fake_stats(),
        )
        out = CohortService._result_to_dict(result)

        required = {
            "rule_id",
            "class_member_count",
            "survivor_count",
            "edge_count",
            "cohort_count",
            "grouped_member_count",
            "elapsed_ms",
        }
        assert required.issubset(out["stats"].keys())


# --- _enrich_members -----------------------------------------------------


class _StubStore:
    """In-memory store stub for `_enrich_members` tests.

    Returns the seeded metadata when `get_entity_metadata` is called;
    raises if `_raise` is True (to exercise the exception fall-through).
    """

    def __init__(
        self, rows: List[Dict[str, str]] | None = None, raise_exc: bool = False
    ):
        self._rows = rows or []
        self._raise = raise_exc
        self.calls: List[tuple] = []

    def get_entity_metadata(self, graph_name: str, uris: List[str]):
        self.calls.append((graph_name, tuple(uris)))
        if self._raise:
            raise RuntimeError("simulated store outage")
        return self._rows


@pytest.mark.unit
class TestEnrichMembers:
    """`_enrich_members` turns raw URI lists into `{uri, id, label}` records."""

    def test_no_cohorts_is_a_noop(self) -> None:
        payload: Dict[str, Any] = {"cohorts": []}
        store = _StubStore()
        CohortService._enrich_members(payload, store, "g")
        assert payload == {"cohorts": []}
        assert store.calls == []  # never touched

    def test_no_members_is_a_noop(self) -> None:
        payload: Dict[str, Any] = {"cohorts": [{"members": []}]}
        store = _StubStore()
        CohortService._enrich_members(payload, store, "g")
        assert payload["cohorts"][0]["members"] == []
        assert store.calls == []

    def test_enriches_with_label_when_available(self) -> None:
        payload = {
            "cohorts": [{"members": ["http://ex/Alice", "http://ex/Bob"]}]
        }
        store = _StubStore(
            rows=[
                {"uri": "http://ex/Alice", "label": "Alice"},
                {"uri": "http://ex/Bob", "label": "Bob"},
            ]
        )
        CohortService._enrich_members(payload, store, "g1")
        enriched = payload["cohorts"][0]["members"]
        assert enriched == [
            {"uri": "http://ex/Alice", "id": "Alice", "label": "Alice"},
            {"uri": "http://ex/Bob", "id": "Bob", "label": "Bob"},
        ]

    def test_missing_label_degrades_to_empty_string(self) -> None:
        payload = {"cohorts": [{"members": ["http://ex/Anon"]}]}
        store = _StubStore(rows=[])  # no metadata returned
        CohortService._enrich_members(payload, store, "g1")
        m = payload["cohorts"][0]["members"][0]
        assert m["uri"] == "http://ex/Anon"
        assert m["id"] == "Anon"
        assert m["label"] == ""

    def test_store_exception_leaves_payload_unchanged(self) -> None:
        payload = {"cohorts": [{"members": ["http://ex/X"]}]}
        store = _StubStore(raise_exc=True)
        CohortService._enrich_members(payload, store, "g1")
        # The preview must not crash on store errors — payload stays raw.
        assert payload["cohorts"][0]["members"] == ["http://ex/X"]

    def test_id_extraction_handles_hash_uris(self) -> None:
        payload = {"cohorts": [{"members": ["http://ex/ns#Alice"]}]}
        store = _StubStore(
            rows=[{"uri": "http://ex/ns#Alice", "label": "Alice Doe"}]
        )
        CohortService._enrich_members(payload, store, "g1")
        m = payload["cohorts"][0]["members"][0]
        assert m["id"] == "Alice"  # local name after `#`
        assert m["label"] == "Alice Doe"


# --- probe_uc_write ------------------------------------------------------


class _StubClient:
    """SQL warehouse client stub for `probe_uc_write`.

    `scripts` is a list of (sql_substring_match, return_value_or_exception).
    Each call to `execute_query` matches against the scripts in order.
    """

    def __init__(self, scripts):
        self._scripts = list(scripts)
        self.calls: List[str] = []

    def execute_query(self, sql: str):
        self.calls.append(sql)
        for match, value in self._scripts:
            if match in sql:
                if isinstance(value, Exception):
                    raise value
                return value
        raise AssertionError(f"Unscripted query: {sql}")


@pytest.mark.unit
class TestProbeUcWrite:
    def test_missing_client_returns_error(self) -> None:
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, None
        )
        assert out["ok"] is False
        assert out["checks"][0]["name"] == "client"
        assert "not configured" in out["checks"][0]["message"]

    @pytest.mark.parametrize(
        "missing_field",
        ["catalog", "schema", "table_name"],
    )
    def test_missing_target_field_returns_error(self, missing_field: str) -> None:
        target = {"catalog": "c", "schema": "s", "table_name": "t"}
        target[missing_field] = ""
        client = _StubClient([])
        out = CohortService.probe_uc_write(target, client)
        assert out["ok"] is False
        assert out["checks"][0]["name"] == "input"

    def test_catalog_describe_failure_short_circuits(self) -> None:
        client = _StubClient(
            [("DESCRIBE CATALOG", PermissionError("denied"))]
        )
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is False
        assert any(
            c["name"] == "catalog" and c["status"] == "error"
            for c in out["checks"]
        )
        # Should not have tried the SCHEMA probe
        assert all("DESCRIBE SCHEMA" not in sql for sql in client.calls)

    def test_schema_describe_failure_short_circuits(self) -> None:
        client = _StubClient(
            [
                ("DESCRIBE CATALOG", [{"col_name": "ok"}]),
                ("DESCRIBE SCHEMA", PermissionError("schema-denied")),
            ]
        )
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is False
        assert any(
            c["name"] == "schema" and c["status"] == "error"
            for c in out["checks"]
        )
        # Should not have tried the TABLE probe
        assert all("DESCRIBE TABLE" not in sql for sql in client.calls)

    def test_existing_compatible_table_reports_ok(self) -> None:
        describe_rows = [
            {"col_name": "rule_id"},
            {"col_name": "cohort_uri"},
            {"col_name": "member_uri"},
            {"col_name": "cohort_size"},
            {"col_name": "extra_column"},
        ]
        client = _StubClient(
            [
                ("DESCRIBE CATALOG", []),
                ("DESCRIBE SCHEMA", []),
                ("DESCRIBE TABLE", describe_rows),
            ]
        )
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is True
        statuses = {c["name"]: c["status"] for c in out["checks"]}
        assert statuses == {"catalog": "ok", "schema": "ok", "table": "ok"}

    def test_existing_table_missing_columns_warns_but_passes(self) -> None:
        # Table exists but missing `cohort_size` -> warning, overall still ok.
        describe_rows = [
            {"col_name": "rule_id"},
            {"col_name": "cohort_uri"},
            {"col_name": "member_uri"},
        ]
        client = _StubClient(
            [
                ("DESCRIBE CATALOG", []),
                ("DESCRIBE SCHEMA", []),
                ("DESCRIBE TABLE", describe_rows),
            ]
        )
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        # `ok` is true because there are no `error` statuses (only `warning`).
        assert out["ok"] is True
        table_check = next(c for c in out["checks"] if c["name"] == "table")
        assert table_check["status"] == "warning"
        assert "cohort_size" in table_check["message"]

    def test_missing_table_with_visible_grants_reports_ok(self) -> None:
        client = _StubClient(
            [
                ("DESCRIBE CATALOG", []),
                ("DESCRIBE SCHEMA", []),
                ("DESCRIBE TABLE", FileNotFoundError("no such table")),
                ("SHOW GRANTS", [{"grant": "SELECT"}]),
            ]
        )
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is True
        table_check = next(c for c in out["checks"] if c["name"] == "table")
        assert table_check["status"] == "ok"
        assert "created on first materialise" in table_check["message"]

    def test_missing_table_and_no_grants_introspection_warns(self) -> None:
        client = _StubClient(
            [
                ("DESCRIBE CATALOG", []),
                ("DESCRIBE SCHEMA", []),
                ("DESCRIBE TABLE", FileNotFoundError("no such table")),
                ("SHOW GRANTS", PermissionError("denied")),
            ]
        )
        out = CohortService.probe_uc_write(
            {"catalog": "c", "schema": "s", "table_name": "t"}, client
        )
        # warning, not error, so `ok` stays True
        assert out["ok"] is True
        table_check = next(c for c in out["checks"] if c["name"] == "table")
        assert table_check["status"] == "warning"
        assert "grant introspection failed" in table_check["message"]

    def test_whitespace_only_inputs_are_treated_as_missing(self) -> None:
        # The implementation strips inputs; "   " becomes "".
        client = _StubClient([])
        out = CohortService.probe_uc_write(
            {"catalog": "  ", "schema": "s", "table_name": "t"}, client
        )
        assert out["ok"] is False
        assert out["checks"][0]["name"] == "input"


# --- suggest_uc_target ---------------------------------------------------


def _make_domain(
    name: str = "sales",
    settings: Dict[str, Any] | None = None,
    catalog_metadata: Dict[str, Any] | None = None,
) -> Any:
    return SimpleNamespace(
        info={"name": name},
        settings=settings if settings is not None else {},
        catalog_metadata=catalog_metadata if catalog_metadata is not None else {},
    )


@pytest.mark.unit
class TestSuggestUcTarget:
    """`suggest_uc_target` resolves catalog/schema from a priority chain."""

    def test_domain_settings_take_precedence(self) -> None:
        domain = _make_domain(
            settings={
                "databricks": {"catalog": "main", "schema": "cohorts"}
            },
            catalog_metadata={
                "tables": [{"catalog": "OTHER", "schema": "OTHER"}]
            },
        )
        svc = CohortService(domain)
        out = svc.suggest_uc_target(rule_name="ExemptStaffingPool")
        assert out["catalog"] == "main"
        assert out["schema"] == "cohorts"
        assert out["table_name"] == "cohorts_exempt_staffing_pool"
        assert out["provenance"]["catalog"] == "domain.settings.databricks.catalog"
        assert out["provenance"]["schema"] == "domain.settings.databricks.schema"

    def test_falls_back_to_first_source_table(self) -> None:
        domain = _make_domain(
            settings={},
            catalog_metadata={
                "tables": [
                    {"catalog": "src_catalog", "schema": "src_schema"},
                    {"catalog": "ignored", "schema": "ignored"},
                ]
            },
        )
        svc = CohortService(domain)
        out = svc.suggest_uc_target(rule_name="MyRule")
        assert out["catalog"] == "src_catalog"
        assert out["schema"] == "src_schema"
        assert out["provenance"]["catalog"] == "first source table"
        assert out["provenance"]["schema"] == "first source table"

    def test_falls_back_to_cohorts_when_schema_unknown(self) -> None:
        # No settings, no metadata, no registry config -> schema='cohorts'.
        domain = _make_domain(settings={}, catalog_metadata={})
        svc = CohortService(domain)
        out = svc.suggest_uc_target(rule_name="")
        # Catalog stays empty (the route surfaces an error to the user);
        # schema falls through to the literal 'cohorts'.
        assert out["schema"] == "cohorts"
        assert out["provenance"]["schema"] == "fallback"

    def test_rule_name_overrides_domain_slug_in_table_name(self) -> None:
        domain = _make_domain(name="Sales Domain")
        svc = CohortService(domain)
        with_rule = svc.suggest_uc_target(rule_name="HighValueCustomers")
        without = svc.suggest_uc_target(rule_name="")
        assert with_rule["table_name"] == "cohorts_high_value_customers"
        # With no rule, falls back to domain name slug.
        assert without["table_name"] == "cohorts_sales_domain"

    def test_empty_domain_name_falls_back_to_literal(self) -> None:
        domain = _make_domain(name="")
        svc = CohortService(domain)
        out = svc.suggest_uc_target(rule_name="")
        # Domain slug empty -> table_name becomes 'cohorts_domain' (literal).
        assert out["table_name"] == "cohorts_domain"
