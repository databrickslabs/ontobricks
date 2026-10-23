"""SqlQualityChecks: SHACL / SWRL / decision-table / aggregate SQL data-quality runs (0.9.0 extract)."""

import re
import threading
from types import SimpleNamespace

import pytest

from back.core.reasoning.swrl.SWRLEngine import SWRLEngine
from back.core.reasoning.swrl.SWRLSQLTranslator import SWRLSQLTranslator
from back.objects.digitaltwin.quality.SqlQualityChecks import SqlQualityChecks
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL

pytestmark = pytest.mark.unit

TABLE = "cat.sch.triples"
BASE = "https://ex.com/onto#"
CUSTOMER = "https://ex.com/onto#Customer"
EMAIL = "https://ex.com/onto/email"

ONTOLOGY = {
    "base_uri": BASE,
    "classes": [{"name": "Customer"}, {"name": "VIP"}, {"name": "Gold"}],
    "properties": [
        {"name": "hasOrder", "type": "ObjectProperty"},
        {"name": "amount", "type": "DatatypeProperty"},
    ],
}


class FakeStore:
    """Record SQL; answer with the first handler whose predicate matches."""

    def __init__(self, handlers=None, default=None):
        self.handlers = list(handlers or [])
        self.default = default if default is not None else []
        self.sql = []
        self._lock = threading.Lock()

    def execute_query(self, sql):
        with self._lock:
            self.sql.append(sql)
        for match, answer in self.handlers:
            if match(sql):
                if isinstance(answer, Exception):
                    raise answer
                return answer(sql) if callable(answer) else answer
        return self.default


class FakeTaskManager:
    def __init__(self):
        self.progress = []
        self.failed = []
        self.completed = []
        self._lock = threading.Lock()

    def update_progress(self, task_id, progress, message):
        with self._lock:
            self.progress.append((task_id, progress, message))

    def fail_task(self, task_id, message):
        self.failed.append((task_id, message))

    def complete_task(self, task_id, result=None, message=""):
        self.completed.append((task_id, result, message))


def starts(prefix):
    return lambda sql: sql.lstrip().startswith(prefix)


def contains(fragment):
    return lambda sql: fragment in sql


IS_PREDICATE_LIST = starts("SELECT DISTINCT predicate")
IS_POPULATION = starts("SELECT COUNT(DISTINCT subject) AS cnt")
IS_TRUE_COUNT = starts("SELECT COUNT(*) AS cnt FROM (")


def min_count_shape(**over):
    shape = {
        "id": "shape-email",
        "label": "Customer has email",
        "category": "completeness",
        "shacl_type": "sh:minCount",
        "target_class_uri": CUSTOMER,
        "property_uri": EMAIL,
        "parameters": {"sh:minCount": 1},
        "severity": "sh:Warning",
    }
    shape.update(over)
    return shape


def run(shapes, store, **kw):
    tm = FakeTaskManager()
    task = SimpleNamespace(id="task-1")
    total = kw.pop("total", None)
    if total is None:
        total = (
            len(shapes)
            + len(kw.get("swrl_rules") or [])
            + len(kw.get("decision_tables") or [])
            + len(kw.get("aggregate_rules") or [])
        ) or 1
    SqlQualityChecks.run_sql_checks(tm, task, shapes, TABLE, store, 0.0, total, **kw)
    return tm


def results_of(tm):
    assert tm.completed, f"task not completed (failed={tm.failed})"
    return tm.completed[0][1]["results"]


# ---------------------------------------------------------------------------
# classify_predicates


def test_classify_predicates_uses_ontology_property_types_and_builtins():
    domain = SimpleNamespace(
        get_properties=lambda: [
            {"uri": "https://ex.com/onto/hasOrder", "type": "ObjectProperty"},
            {"uri": "https://ex.com/onto/amount", "type": "DatatypeProperty"},
        ]
    )
    rows = [
        {"predicate": RDF_TYPE, "cnt": "10"},
        {"predicate": RDFS_LABEL, "cnt": 4},
        {"predicate": "https://ex.com/onto/amount", "cnt": 3},
        {"predicate": "https://ex.com/onto/hasOrder", "cnt": 2},
        {"predicate": "http://www.w3.org/2002/07/owl#sameAs", "cnt": 1},
        {"predicate": "https://ex.com/unknown", "cnt": 1},
    ]
    out = SqlQualityChecks(domain).classify_predicates(rows)
    assert [(c["uri"].rsplit("/", 1)[-1], c["count"], c["kind"]) for c in out] == [
        ("22-rdf-syntax-ns#type", 10, "attribute"),
        ("rdf-schema#label", 4, "attribute"),
        ("amount", 3, "attribute"),
        ("hasOrder", 2, "relationship"),
        ("owl#sameAs", 1, "relationship"),
        ("unknown", 1, "relationship"),
    ]


# ---------------------------------------------------------------------------
# Population counts and pass percentage


def test_population_count_queries_rdf_type_and_caches_per_table_and_class():
    store = FakeStore([(IS_POPULATION, [{"cnt": "42"}])])
    cache = {}
    assert SqlQualityChecks._count_class_population_sql(store, TABLE, CUSTOMER, cache) == 42
    assert SqlQualityChecks._count_class_population_sql(store, TABLE, CUSTOMER, cache) == 42
    assert len(store.sql) == 1
    assert f"FROM {TABLE}" in store.sql[0]
    assert f"predicate = '{RDF_TYPE}'" in store.sql[0]
    assert f"object = '{CUSTOMER}'" in store.sql[0]
    assert cache == {(TABLE, CUSTOMER): 42}


def test_population_count_escapes_class_uri_literal():
    store = FakeStore([(IS_POPULATION, [{"cnt": 0}])])
    SqlQualityChecks._count_class_population_sql(store, TABLE, "https://ex.com/O'Brien\\")
    assert "object = 'https://ex.com/O''Brien\\\\'" in store.sql[0]


def test_population_count_without_class_uri_skips_query():
    store = FakeStore()
    assert SqlQualityChecks._count_class_population_sql(store, TABLE, "") is None
    assert store.sql == []


def test_population_count_returns_zero_for_empty_and_none_on_error():
    assert SqlQualityChecks._count_class_population_sql(FakeStore(default=[]), TABLE, CUSTOMER) == 0
    failing = FakeStore([(IS_POPULATION, RuntimeError("boom"))])
    assert SqlQualityChecks._count_class_population_sql(failing, TABLE, CUSTOMER) is None


def test_enrich_uses_uncapped_violation_total_for_pass_pct():
    result = {"violations": [{"s": "a"}], "violation_total": 25, "message": "x"}
    SqlQualityChecks._enrich_with_population(result, 100)
    assert result["total_population"] == 100
    assert result["pass_pct"] == 75.0
    assert result["message"] == "25 violations found — 75.0% pass on 100 entities"


def test_enrich_caps_pass_pct_below_100_when_any_violation_exists():
    result = {"violations": [{"s": "a"}], "violation_total": 1}
    SqlQualityChecks._enrich_with_population(result, 100000)
    assert result["pass_pct"] == 99.9


def test_enrich_falls_back_to_violation_list_and_floors_at_zero():
    result = {"violations": [{"s": str(i)} for i in range(5)]}
    SqlQualityChecks._enrich_with_population(result, 2)
    assert result["pass_pct"] == 0.0


def test_enrich_without_violations_keeps_message_and_reports_full_pass():
    result = {"violations": [], "violation_total": 0, "message": "No violations"}
    SqlQualityChecks._enrich_with_population(result, 10)
    assert result["pass_pct"] == 100.0
    assert result["message"] == "No violations"


@pytest.mark.parametrize("population", [None, 0])
def test_enrich_without_population_leaves_result_untouched(population):
    result = {"violations": [{"s": "a"}], "message": "1 violations found"}
    SqlQualityChecks._enrich_with_population(result, population)
    assert result == {"violations": [{"s": "a"}], "message": "1 violations found"}


# ---------------------------------------------------------------------------
# Violation limit / true count


def test_violation_limit_appends_limit_plus_one_and_strips_trailing_semicolon():
    store = FakeStore(default=[{"s": "a"}])
    violations, total, status, msg = SqlQualityChecks._apply_sql_violation_limit(
        store, "SELECT s FROM t;  ", 10
    )
    assert store.sql == ["SELECT s FROM t LIMIT 11"]
    assert (violations, total, status, msg) == ([{"s": "a"}], 1, "error", "1 violations found")


def test_violation_limit_hit_runs_true_count_on_unlimited_sql_and_truncates():
    rows = [{"s": str(i)} for i in range(3)]
    store = FakeStore([(IS_TRUE_COUNT, [{"cnt": "57"}])], default=rows)
    violations, total, status, msg = SqlQualityChecks._apply_sql_violation_limit(
        store, "SELECT s FROM t;", 2, row_mapper=lambda r: {"subject": r["s"]}
    )
    assert store.sql == ["SELECT s FROM t LIMIT 3", "SELECT COUNT(*) AS cnt FROM (SELECT s FROM t)"]
    assert violations == [{"subject": "0"}, {"subject": "1"}]
    assert total == 57
    assert status == "error" and msg == "57 violations found"


def test_violation_limit_hit_keeps_sample_size_when_true_count_fails():
    store = FakeStore([(IS_TRUE_COUNT, RuntimeError("warehouse gone"))], default=[{"s": 1}, {"s": 2}])
    violations, total, _, _ = SqlQualityChecks._apply_sql_violation_limit(store, "SELECT s FROM t", 1)
    assert violations == [{"s": 1}]
    assert total == 2


def test_no_violation_limit_runs_sql_unchanged_and_reports_success_when_empty():
    store = FakeStore()
    store.default = None
    out = SqlQualityChecks._apply_sql_violation_limit(store, "SELECT s FROM t", None)
    assert store.sql == ["SELECT s FROM t"]
    assert out == ([], 0, "success", "No violations")


def test_count_violations_returns_none_for_empty_result():
    assert SqlQualityChecks._count_violations_sql(FakeStore(default=[]), "SELECT 1") is None


# ---------------------------------------------------------------------------
# Predicate loading / URI resolution


def test_load_predicates_returns_non_empty_distinct_predicates():
    store = FakeStore([(IS_PREDICATE_LIST, [{"predicate": EMAIL}, {"predicate": ""}, {}])])
    assert SqlQualityChecks._load_predicates_from_table(store, TABLE) == {EMAIL}
    assert store.sql == [f"SELECT DISTINCT predicate FROM {TABLE}"]


def test_load_predicates_swallows_errors():
    store = FakeStore([(IS_PREDICATE_LIST, RuntimeError("nope"))])
    assert SqlQualityChecks._load_predicates_from_table(store, TABLE) == set()


def test_resolve_shape_uri_swaps_hash_for_slash_without_mutating_input():
    shape = {"id": "s1", "property_uri": "https://ex.com/onto#email"}
    out = SqlQualityChecks._resolve_shape_uri_for_sql(shape, {EMAIL})
    assert out["property_uri"] == EMAIL
    assert shape["property_uri"] == "https://ex.com/onto#email"


@pytest.mark.parametrize(
    "shape, preds",
    [
        ({"property_uri": ""}, {EMAIL}),
        ({"property_uri": EMAIL}, set()),
        ({"property_uri": EMAIL}, {EMAIL}),
    ],
)
def test_resolve_shape_uri_returns_same_shape_when_nothing_to_resolve(shape, preds):
    assert SqlQualityChecks._resolve_shape_uri_for_sql(shape, preds) is shape


# ---------------------------------------------------------------------------
# Error detail / failed results / check ids


def test_sql_error_detail_collapses_whitespace_and_truncates():
    detail = SqlQualityChecks._sql_error_detail(RuntimeError("a\n  b\t" + "x" * 500))
    assert detail.startswith("a b x")
    assert detail.endswith("…")
    assert len(detail) == SqlQualityChecks._SQL_ERROR_MAX_CHARS + 1


def test_sql_error_detail_falls_back_to_exception_class_name():
    assert SqlQualityChecks._sql_error_detail(ValueError("")) == "ValueError"


def test_failed_check_result_carries_cause_and_sql_as_warning():
    res = SqlQualityChecks._failed_check_result(
        "Rule", "structural", "swrl:Rule", "SELECT 1", RuntimeError("[PARSE_ERROR] bad")
    )
    assert res == {
        "name": "Rule",
        "category": "structural",
        "shape_id": "swrl:Rule",
        "status": "warning",
        "message": "Query failed: [PARSE_ERROR] bad",
        "violations": [],
        "sql": "SELECT 1",
    }


def test_rule_check_id_prefers_stamped_check_id_then_name_then_index():
    assert SqlQualityChecks._rule_check_id("swrl", {"check_id": "swrl:3", "name": "X"}, 0) == "swrl:3"
    assert SqlQualityChecks._rule_check_id("dt", {"name": "Tier"}, 4) == "dt:Tier"
    assert SqlQualityChecks._rule_check_id("agg", {}, 4) == "agg:4"


# ---------------------------------------------------------------------------
# SWRL helpers


def test_swrl_target_class_is_the_violation_subject_class():
    rule = {"antecedent": "Order(?o) ^ Customer(?c) ^ hasOrder(?c, ?o)", "consequent": "VIP(?c)"}
    assert SqlQualityChecks._swrl_target_class_uri(rule, BASE, {}) == CUSTOMER


def test_swrl_target_class_falls_back_to_first_class_atom():
    rule = {"antecedent": "Customer(?c) ^ hasOrder(?c, ?o)", "consequent": "Flag(?o)"}
    assert SqlQualityChecks._swrl_target_class_uri(rule, BASE, {"customer": "urn:C"}) == "urn:C"


def test_swrl_target_class_none_without_class_atoms():
    rule = {"antecedent": "hasOrder(?c, ?o)", "consequent": "VIP(?c)"}
    assert SqlQualityChecks._swrl_target_class_uri(rule, BASE, {}) is None


def test_swrl_antecedent_population_counts_rule_scope():
    translator = SimpleNamespace(build_antecedent_count_sql=lambda t, p: f"SELECT COUNT(*) AS cnt FROM {t}")
    store = FakeStore(default=[{"cnt": "8"}])
    assert SqlQualityChecks._swrl_antecedent_population_sql(translator, store, TABLE, {}) == 8
    assert store.sql == [f"SELECT COUNT(*) AS cnt FROM {TABLE}"]


@pytest.mark.parametrize(
    "build, store",
    [
        (lambda t, p: None, FakeStore()),
        (lambda t, p: "SELECT 1", FakeStore(default=[])),
        (lambda t, p: "SELECT 1", FakeStore([(starts("SELECT"), RuntimeError("x"))])),
    ],
)
def test_swrl_antecedent_population_returns_none_when_unavailable(build, store):
    translator = SimpleNamespace(build_antecedent_count_sql=build)
    assert SqlQualityChecks._swrl_antecedent_population_sql(translator, store, TABLE, {}) is None


# ---------------------------------------------------------------------------
# run_sql_checks: SHACL shapes


def test_shacl_shape_violations_are_sampled_counted_and_enriched():
    sample = [{"s": f"urn:c{i}", "count": 0} for i in range(3)]
    store = FakeStore(
        [
            (IS_PREDICATE_LIST, [{"predicate": EMAIL}]),
            (IS_TRUE_COUNT, [{"cnt": 40}]),
            (IS_POPULATION, [{"cnt": 200}]),
            (contains("HAVING COUNT(t2.object) < 1"), sample),
        ]
    )
    tm = run([min_count_shape()], store, violation_limit=2)

    [res] = results_of(tm)
    assert res["name"] == "Customer has email"
    assert res["category"] == "completeness"
    assert res["shape_id"] == "shape-email"
    assert res["status"] == "error"
    assert res["severity"] == "sh:Warning"
    assert res["violations"] == sample[:2]
    assert res["violation_total"] == 40
    assert res["total_population"] == 200
    assert res["pass_pct"] == 80.0
    assert res["message"] == "40 violations found — 80.0% pass on 200 entities"

    check_sql = res["sql"]
    assert f"FROM {TABLE} t1" in check_sql
    assert f"t2.predicate = '{EMAIL}'" in check_sql
    assert f"t1.object = '{CUSTOMER}'" in check_sql
    assert f"{check_sql} LIMIT 3" in store.sql
    assert f"SELECT COUNT(*) AS cnt FROM ({check_sql})" in store.sql

    assert tm.progress[0] == ("task-1", 0, "Check 1/1: Customer has email")
    _, payload, message = tm.completed[0]
    assert payload["summary"] == {"total": 1, "passed": 0, "failed": 1, "warnings": 0}
    assert message == "Data quality checks complete: 0 passed, 1 failed, 0 warnings"


def test_shacl_shape_property_uri_is_resolved_against_store_predicates():
    store = FakeStore([(IS_PREDICATE_LIST, [{"predicate": EMAIL}])])
    tm = run([min_count_shape(property_uri="https://ex.com/onto#email")], store)
    [res] = results_of(tm)
    assert f"t2.predicate = '{EMAIL}'" in res["sql"]
    assert res["status"] == "success" and res["message"] == "No violations"
    assert res["severity"] == "sh:Warning"


def test_shacl_shape_population_is_queried_once_per_class():
    store = FakeStore([(IS_POPULATION, [{"cnt": 5}])], default=[])
    run([min_count_shape(id="a"), min_count_shape(id="b")], store)
    assert sum(1 for s in store.sql if IS_POPULATION(s)) == 1


def test_untranslatable_shape_is_reported_as_info_and_run_continues():
    store = FakeStore()
    shapes = [
        {"id": "weird", "category": "x", "shacl_type": "sh:bogus", "parameters": {}},
        min_count_shape(),
    ]
    tm = run(shapes, store)
    first, second = results_of(tm)
    assert first == {
        "name": "weird",
        "category": "x",
        "shape_id": "weird",
        "status": "info",
        "message": "Cannot translate to SQL",
        "violations": [],
        "sql": "",
    }
    assert second["status"] == "success"
    assert tm.completed[0][1]["summary"]["warnings"] == 1


def test_shape_label_defaults_to_position_when_no_label_or_id():
    tm = run([{"shacl_type": "sh:bogus"}], FakeStore())
    assert results_of(tm)[0]["name"] == "Shape 1"
    assert results_of(tm)[0]["category"] == "unknown"


def test_shacl_query_error_becomes_failed_check_with_detail_and_sql():
    store = FakeStore([(contains("HAVING"), RuntimeError("[DIVIDE_BY_ZERO]\n  division by zero"))])
    tm = run([min_count_shape()], store)
    [res] = results_of(tm)
    assert res["status"] == "warning"
    assert res["message"] == "Query failed: [DIVIDE_BY_ZERO] division by zero"
    assert res["shape_id"] == "shape-email"
    assert "HAVING COUNT(t2.object) < 1" in res["sql"]
    assert not tm.failed


@pytest.mark.parametrize(
    "error",
    [
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `cat`.`sch`.`triples` cannot be found",
        "Relation cat.sch.triples Does Not Exist",
    ],
)
def test_missing_view_fails_whole_task_with_build_hint(error):
    store = FakeStore([(contains("HAVING"), RuntimeError(error))])
    tm = run([min_count_shape(), min_count_shape(id="never-run")], store)
    assert tm.failed == [("task-1", f"View {TABLE} not found. Build first.")]
    assert tm.completed == []
    assert sum(1 for s in store.sql if "HAVING" in s) == 1


# ---------------------------------------------------------------------------
# run_sql_checks: SWRL rules


VIP_RULE = {
    "name": "VipCustomer",
    "antecedent": "Customer(?c) ^ hasOrder(?c, ?o)",
    "consequent": "VIP(?c)",
}
IS_SWRL_VIOLATION = starts("SELECT DISTINCT a1.subject AS s")
IS_SWRL_SCOPE = starts("SELECT COUNT(DISTINCT a1.subject) AS cnt")


def test_swrl_rule_violations_are_mapped_and_scoped_to_antecedent_population():
    store = FakeStore(
        [
            (IS_SWRL_VIOLATION, [{"s": "urn:c1", "extra": 1}, {"s": "urn:c2"}]),
            (IS_SWRL_SCOPE, [{"cnt": 10}]),
        ]
    )
    tm = run([], store, swrl_rules=[VIP_RULE], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["name"] == "VipCustomer"
    assert res["category"] == "structural"
    assert res["shape_id"] == "swrl:VipCustomer"
    assert res["status"] == "error"
    assert res["violations"] == [{"s": "urn:c1"}, {"s": "urn:c2"}]
    assert res["violation_total"] == 2
    assert res["total_population"] == 10 and res["pass_pct"] == 80.0
    assert res["severity"] == "sh:Violation"

    [violation_sql] = [s for s in store.sql if IS_SWRL_VIOLATION(s)]
    assert f"FROM {TABLE} a1" in violation_sql
    assert f"a1.object = '{CUSTOMER}'" in violation_sql
    assert "NOT EXISTS" in violation_sql and "https://ex.com/onto#VIP" in violation_sql
    assert tm.progress[0][2] == "SWRL 1/1: VipCustomer"


def test_swrl_rule_without_violations_skips_population_query():
    store = FakeStore([(IS_SWRL_VIOLATION, [])])
    tm = run([], store, swrl_rules=[VIP_RULE], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["status"] == "success"
    assert "total_population" not in res
    assert not any(IS_SWRL_SCOPE(s) for s in store.sql)


def test_swrl_rule_respects_violation_limit():
    rows = [{"s": f"urn:c{i}"} for i in range(4)]
    store = FakeStore([(IS_TRUE_COUNT, [{"cnt": 99}]), (IS_SWRL_VIOLATION, rows)])
    tm = run([], store, swrl_rules=[VIP_RULE], ontology=ONTOLOGY, violation_limit=3)
    [res] = results_of(tm)
    assert len(res["violations"]) == 3 and res["violation_total"] == 99
    assert any(IS_SWRL_VIOLATION(s) and s.endswith(" LIMIT 4") for s in store.sql)


def test_disabled_and_untranslatable_swrl_rules():
    rules = [
        {**VIP_RULE, "enabled": False},
        {"antecedent": "hasOrder(?c, ?o)", "consequent": "VIP(?c)", "check_id": "swrl:7"},
    ]
    tm = run([], FakeStore(), swrl_rules=rules, ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["name"] == "SWRL Rule 2"
    assert res["shape_id"] == "swrl:7"
    assert res["status"] == "info" and res["message"] == "Cannot translate to SQL"


def test_swrl_query_error_becomes_failed_check_with_rule_sql():
    store = FakeStore([(IS_SWRL_VIOLATION, RuntimeError("UNRESOLVED_COLUMN a9"))])
    tm = run([], store, swrl_rules=[VIP_RULE], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["status"] == "warning"
    assert res["message"] == "Query failed: UNRESOLVED_COLUMN a9"
    assert res["shape_id"] == "swrl:VipCustomer"
    assert IS_SWRL_VIOLATION(res["sql"])
    assert not tm.failed


def test_swrl_rule_works_without_ontology():
    rule = {"name": "R", "antecedent": "Customer(?c)", "consequent": "VIP(?c)"}
    store = FakeStore()
    tm = run([], store, swrl_rules=[rule], ontology=None)
    [res] = results_of(tm)
    assert res["status"] in ("success", "info")


# ---------------------------------------------------------------------------
# run_sql_checks: decision tables


TIER_DT = {
    "name": "Tier",
    "target_class": "Customer",
    "input_columns": [{"property": "amount"}],
    "output_column": {"property": "tier", "action": "set_value"},
    "rows": [{"conditions": [{"op": "gt", "value": "100"}], "action_value": "gold"}],
}
IS_DT_VIOLATION = starts("SELECT DISTINCT t0.subject AS s")


def test_decision_table_violations_include_sql_and_class_population():
    store = FakeStore(
        [
            (IS_DT_VIOLATION, [{"s": "urn:c1", "noise": "x"}]),
            (IS_POPULATION, [{"cnt": 4}]),
        ]
    )
    tm = run([], store, decision_tables=[TIER_DT], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["name"] == "Tier"
    assert res["category"] == "conformance"
    assert res["shape_id"] == "dt:Tier"
    assert res["violations"] == [{"s": "urn:c1"}]
    assert res["total_population"] == 4 and res["pass_pct"] == 75.0
    assert f"FROM {TABLE} t0" in res["sql"]
    assert f"t0.object = '{CUSTOMER}'" in res["sql"]
    assert "inp0.predicate = 'https://ex.com/onto/amount'" in res["sql"]
    [pop_sql] = [s for s in store.sql if IS_POPULATION(s)]
    assert f"object = '{CUSTOMER}'" in pop_sql
    assert tm.progress[0][2] == "DT 1/1: Tier"


def test_decision_table_without_violations_has_no_population():
    tm = run([], FakeStore(), decision_tables=[TIER_DT], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["status"] == "success" and "total_population" not in res


def test_disabled_and_untranslatable_decision_tables():
    tables = [{**TIER_DT, "enabled": False}, {"target_class": "Customer", "rows": []}]
    tm = run([], FakeStore(), decision_tables=tables, ontology=None)
    [res] = results_of(tm)
    assert res["name"] == "Decision Table 2"
    assert res["shape_id"] == "dt:1"
    assert res["status"] == "info" and res["sql"] == ""


def test_decision_table_query_error_becomes_failed_check():
    store = FakeStore([(IS_DT_VIOLATION, RuntimeError("CAST_INVALID_INPUT"))])
    tm = run([], store, decision_tables=[TIER_DT], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["status"] == "warning"
    assert res["category"] == "conformance"
    assert res["message"] == "Query failed: CAST_INVALID_INPUT"
    assert IS_DT_VIOLATION(res["sql"])


# ---------------------------------------------------------------------------
# run_sql_checks: aggregate rules


ORDER_COUNT = {
    "name": "TooManyOrders",
    "target_class": "Customer",
    "group_by_property": "hasOrder",
    "aggregate_function": "count",
    "operator": "gt",
    "threshold": "5",
}
IS_AGG_VIOLATION = contains("AS agg_val")


def test_aggregate_rule_violations_keep_aggregate_value_and_population():
    store = FakeStore(
        [
            (IS_AGG_VIOLATION, [{"s": "urn:c1", "agg_val": 9, "x": 1}, {"s": "urn:c2"}]),
            (IS_POPULATION, [{"cnt": 20}]),
        ]
    )
    tm = run([], store, aggregate_rules=[ORDER_COUNT], ontology=ONTOLOGY)
    [res] = results_of(tm)
    assert res["shape_id"] == "agg:TooManyOrders"
    assert res["category"] == "conformance"
    assert res["violations"] == [{"s": "urn:c1", "agg_val": 9}, {"s": "urn:c2", "agg_val": ""}]
    assert res["total_population"] == 20 and res["pass_pct"] == 90.0
    assert f"FROM {TABLE} t0" in res["sql"]
    assert "HAVING COUNT(t_grp.object) > 5" in res["sql"]
    assert tm.progress[0][2] == "Agg 1/1: TooManyOrders"


def test_aggregate_rule_without_violations_and_query_error():
    rules = [ORDER_COUNT, {**ORDER_COUNT, "name": "Broken", "threshold": "7"}]
    store = FakeStore([(contains("> 7"), RuntimeError("boom"))])
    tm = run([], store, aggregate_rules=rules, ontology=ONTOLOGY)
    ok, broken = results_of(tm)
    assert ok["status"] == "success" and "total_population" not in ok
    assert broken["status"] == "warning"
    assert broken["message"] == "Query failed: boom"
    assert broken["shape_id"] == "agg:Broken"


def test_disabled_and_untranslatable_aggregate_rules():
    rules = [
        {**ORDER_COUNT, "enabled": False},
        {"target_class": "Customer", "aggregate_function": "median"},
    ]
    tm = run([], FakeStore(), aggregate_rules=rules, ontology=None)
    [res] = results_of(tm)
    assert res["name"] == "Aggregate Rule 2"
    assert res["shape_id"] == "agg:1"
    assert res["status"] == "info" and res["message"] == "Cannot translate to SQL"


# ---------------------------------------------------------------------------
# run_sql_checks: mixed run


def test_mixed_run_reports_every_family_in_order_with_summary():
    store = FakeStore(default=[])
    tm = run(
        [min_count_shape()],
        store,
        swrl_rules=[VIP_RULE],
        decision_tables=[TIER_DT],
        aggregate_rules=[ORDER_COUNT],
        ontology=ONTOLOGY,
    )
    res = results_of(tm)
    assert [r["shape_id"] for r in res] == [
        "shape-email",
        "swrl:VipCustomer",
        "dt:Tier",
        "agg:TooManyOrders",
    ]
    assert tm.completed[0][1]["summary"] == {"total": 4, "passed": 4, "failed": 0, "warnings": 0}
    assert all(re.search(r"\b" + re.escape(TABLE) + r"\b", s) for s in store.sql)


def test_mixed_run_progress_increases_monotonically_per_check():
    tm = run(
        [min_count_shape()],
        FakeStore(default=[]),
        swrl_rules=[VIP_RULE],
        decision_tables=[TIER_DT],
        aggregate_rules=[ORDER_COUNT],
        ontology=ONTOLOGY,
    )
    assert [p for _, p, _ in tm.progress] == [0, 25, 50, 75]


def test_parallel_mixed_run_keeps_input_order():
    tm = run(
        [min_count_shape()],
        FakeStore(default=[]),
        swrl_rules=[VIP_RULE],
        decision_tables=[TIER_DT],
        aggregate_rules=[ORDER_COUNT],
        ontology=ONTOLOGY,
        parallel=True,
    )
    res = results_of(tm)
    assert [r["shape_id"] for r in res] == [
        "shape-email",
        "swrl:VipCustomer",
        "dt:Tier",
        "agg:TooManyOrders",
    ]
    assert tm.completed[0][1]["summary"]["total"] == 4
    assert len(tm.progress) == 4
    assert all(msg.startswith("Check ") for _, _, msg in tm.progress)


def test_digitaltwin_delegators_reach_sql_quality_checks():
    from back.objects.digitaltwin import DigitalTwin

    assert DigitalTwin._SQL_ERROR_MAX_CHARS == SqlQualityChecks._SQL_ERROR_MAX_CHARS
    tm = FakeTaskManager()
    DigitalTwin.run_sql_checks(tm, SimpleNamespace(id="t"), [], TABLE, FakeStore(), 0.0, 1)
    assert tm.completed[0][1]["results"] == []
