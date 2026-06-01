"""Tests for the mapping-PGE orchestrator (Sprint 7).

The orchestrator wires Planner -> Generator(s) -> Evaluator(s) into a single
``run_agent`` entry. These tests exercise the control flow with fake versions
of each sub-agent — no real LLM, no real Databricks. Each test patches the
module-level references in :mod:`engine` so the orchestrator calls the fakes
instead of the production functions.

What we DO exercise:
* Happy path with both entities and relationships.
* Planner failure aborts cleanly.
* Generator failure records FAIL but continues.
* Evaluator FAIL (non-bubble) drives a retry with a hint.
* Bubble-to-planner triggers Planner re-invocation; budget is global.
* 3-attempt retry budget exhaustion records FAIL_BUDGET.
* Critic PASS / FAIL paths, and the ``skip_semantic_critic`` short-circuit.
* Pre-seeded entity mappings and Planner skip[] entries.
* on_step pct stays non-decreasing across the run.
* Id-universe cache shares entity universes across relationships.
"""

from typing import Any, Dict, List, Optional, Tuple

import pytest

from agents.agent_mapping_pge import engine as engine_mod
from agents.agent_mapping_pge.contracts import (
    CanonicalId,
    EvalFailure,
    EvalReport,
    JoinKey,
    MappingPlan,
    SkipItem,
    SourceModel,
    TableRole,
    TableRoleCandidate,
)
from agents.agent_mapping_pge.engine import AgentResult, run_agent
from agents.agent_mapping_pge.evaluator.critic import CriticResult
from agents.agent_mapping_pge.generators.entity import EntityGenResult
from agents.agent_mapping_pge.generators.relationship import RelationshipGenResult
from agents.agent_mapping_pge.planner import PlannerResult


# =====================================================
# Ontology + SourceModel fixtures
# =====================================================


CUSTOMER_URI = "http://test.org/ontology#Customer"
ORDER_URI = "http://test.org/ontology#Order"
HAS_ORDER_URI = "http://test.org/ontology#hasOrder"
ITEM_URI = "http://test.org/ontology#Item"
CONTAINS_URI = "http://test.org/ontology#contains"

T_CUSTOMERS = "cat.sch.customers"
T_ORDERS = "cat.sch.orders"
T_ITEMS = "cat.sch.items"


def _ontology() -> dict:
    return {
        "entities": [
            {
                "uri": CUSTOMER_URI,
                "name": "Customer",
                "label": "Customer",
                "attributes": [{"name": "firstName", "type": "xsd:string"}],
            },
            {
                "uri": ORDER_URI,
                "name": "Order",
                "label": "Order",
                "attributes": [{"name": "orderDate", "type": "xsd:string"}],
            },
            {
                "uri": ITEM_URI,
                "name": "Item",
                "label": "Item",
                "attributes": [],
            },
        ],
        "relationships": [
            {
                "uri": HAS_ORDER_URI,
                "name": "hasOrder",
                "label": "hasOrder",
                "domain": CUSTOMER_URI,
                "range": ORDER_URI,
            },
            {
                "uri": CONTAINS_URI,
                "name": "contains",
                "label": "contains",
                "domain": ORDER_URI,
                "range": ITEM_URI,
            },
        ],
    }


def _source_model(*, with_items: bool = False) -> SourceModel:
    table_roles = [
        TableRole(
            table=T_CUSTOMERS,
            ontology_class_candidates=[
                TableRoleCandidate(uri=CUSTOMER_URI, confidence=0.9, reason="ok")
            ],
        ),
        TableRole(
            table=T_ORDERS,
            ontology_class_candidates=[
                TableRoleCandidate(uri=ORDER_URI, confidence=0.9, reason="ok")
            ],
        ),
    ]
    if with_items:
        table_roles.append(
            TableRole(
                table=T_ITEMS,
                ontology_class_candidates=[
                    TableRoleCandidate(uri=ITEM_URI, confidence=0.9, reason="ok")
                ],
            )
        )
    canonical_ids = [
        CanonicalId(
            ontology_class=CUSTOMER_URI,
            canonical_column_per_table={T_CUSTOMERS: "customer_id"},
        ),
        CanonicalId(
            ontology_class=ORDER_URI,
            canonical_column_per_table={T_ORDERS: "order_id"},
        ),
    ]
    if with_items:
        canonical_ids.append(
            CanonicalId(
                ontology_class=ITEM_URI,
                canonical_column_per_table={T_ITEMS: "item_id"},
            )
        )
    join_keys = [
        JoinKey(
            from_ref=f"{T_ORDERS}.customer_id",
            to_ref=f"{T_CUSTOMERS}.customer_id",
            confidence=0.9,
            overlap_pct=0.95,
            kind="same_trust_fk",
        ),
    ]
    if with_items:
        join_keys.append(
            JoinKey(
                from_ref=f"{T_ITEMS}.order_id",
                to_ref=f"{T_ORDERS}.order_id",
                confidence=0.9,
                overlap_pct=0.95,
                kind="same_trust_fk",
            )
        )

    entity_order = [CUSTOMER_URI, ORDER_URI]
    relationship_order = [HAS_ORDER_URI]
    if with_items:
        entity_order.append(ITEM_URI)
        relationship_order.append(CONTAINS_URI)

    return SourceModel(
        table_roles=table_roles,
        canonical_ids=canonical_ids,
        join_keys=join_keys,
        mapping_plan=MappingPlan(
            entity_order=entity_order,
            relationship_order=relationship_order,
            skip=[],
        ),
    )


def _entity_mapping(class_uri: str, id_col: str, sql: str) -> dict:
    """Shape produced by the EntityGenerator's submit handler."""
    return {
        "ontology_class": class_uri,
        "class_name": class_uri.rsplit("#", 1)[-1],
        "sql_query": sql,
        "id_column": id_col,
        "label_column": id_col,
        "attribute_mappings": {},
        "unmapped_attributes": [],
    }


def _relationship_mapping(
    prop_uri: str, source_col: str, target_col: str, sql: str
) -> dict:
    return {
        "property": prop_uri,
        "property_name": prop_uri.rsplit("#", 1)[-1],
        "sql_query": sql,
        "source_id_column": source_col,
        "target_id_column": target_col,
        "domain": CUSTOMER_URI,
        "range_class": ORDER_URI,
    }


# =====================================================
# Fake sub-agent factories
# =====================================================


class FakePlanner:
    """Fake ``run_planner`` returning canned :class:`PlannerResult` values."""

    def __init__(self, results: List[PlannerResult]):
        self.results = list(results)
        self.calls = 0

    def __call__(self, *args: Any, **kwargs: Any) -> PlannerResult:
        self.calls += 1
        if not self.results:
            raise AssertionError(
                f"FakePlanner ran out of canned results on call #{self.calls}"
            )
        return self.results.pop(0)


class FakeEntityGenerator:
    """Routes the call by ontology_class URI to a per-URI list of results."""

    def __init__(self, results_by_uri: Dict[str, List[EntityGenResult]]):
        self.results_by_uri = {k: list(v) for k, v in results_by_uri.items()}
        self.calls: List[Tuple[str, Optional[str]]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> EntityGenResult:
        ontology_class = kwargs["ontology_class"]
        uri = ontology_class.get("uri", "")
        hint = kwargs.get("retry_hint")
        self.calls.append((uri, hint))
        queue = self.results_by_uri.get(uri, [])
        if not queue:
            raise AssertionError(
                f"FakeEntityGenerator: no canned result for {uri} (call "
                f"#{len(self.calls)})"
            )
        return queue.pop(0)


class FakeRelationshipGenerator:
    """Routes the call by ontology_property URI."""

    def __init__(self, results_by_uri: Dict[str, List[RelationshipGenResult]]):
        self.results_by_uri = {k: list(v) for k, v in results_by_uri.items()}
        self.calls: List[Tuple[str, Optional[str]]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> RelationshipGenResult:
        prop = kwargs["ontology_property"]
        uri = prop.get("uri", "")
        hint = kwargs.get("retry_hint")
        self.calls.append((uri, hint))
        queue = self.results_by_uri.get(uri, [])
        if not queue:
            raise AssertionError(
                f"FakeRelationshipGenerator: no canned result for {uri}"
            )
        return queue.pop(0)


class FakeCritic:
    """Routes by item_uri."""

    def __init__(self, reports_by_uri: Dict[str, List[CriticResult]]):
        self.reports_by_uri = {k: list(v) for k, v in reports_by_uri.items()}
        self.calls: List[str] = []

    def __call__(self, *args: Any, **kwargs: Any) -> CriticResult:
        uri = kwargs["item_uri"]
        self.calls.append(uri)
        queue = self.reports_by_uri.get(uri, [])
        if not queue:
            # Default: PASS so tests that don't care about critic still work.
            return CriticResult(
                success=True,
                report=EvalReport(
                    status="PASS",
                    stage="semantic",
                    metrics={},
                    failures=[],
                    bubble_to_planner=False,
                ),
            )
        return queue.pop(0)


class FakeDeterministicEvaluator:
    """Stage-1 evaluator stub keyed by mapping uri (class or property)."""

    def __init__(self, reports_by_uri: Dict[str, List[EvalReport]]):
        self.reports_by_uri = {k: list(v) for k, v in reports_by_uri.items()}
        self.calls: List[str] = []

    def for_entity(self, *args: Any, **kwargs: Any) -> EvalReport:
        mapping = kwargs["mapping"]
        uri = mapping.get("ontology_class", "")
        return self._next(uri)

    def for_relationship(self, *args: Any, **kwargs: Any) -> EvalReport:
        mapping = kwargs["mapping"]
        uri = mapping.get("property", "")
        return self._next(uri)

    def _next(self, uri: str) -> EvalReport:
        self.calls.append(uri)
        queue = self.reports_by_uri.get(uri, [])
        if not queue:
            return EvalReport(
                status="PASS",
                stage="deterministic",
                metrics={},
                failures=[],
                bubble_to_planner=False,
            )
        return queue.pop(0)


# =====================================================
# Helpers — build typical canned results
# =====================================================


def _ok_entity_gen(class_uri: str, sql: Optional[str] = None) -> EntityGenResult:
    id_col = {
        CUSTOMER_URI: "customer_id",
        ORDER_URI: "order_id",
        ITEM_URI: "item_id",
    }.get(class_uri, "id")
    sql = sql or f"SELECT {id_col} AS ID, {id_col} AS Label FROM tbl_for_{class_uri[-3:]}"
    return EntityGenResult(
        success=True,
        mapping=_entity_mapping(class_uri, id_col, sql),
        iterations=2,
        usage={"prompt_tokens": 10, "completion_tokens": 5},
    )


def _ok_rel_gen(prop_uri: str) -> RelationshipGenResult:
    sql = "SELECT customer_id AS source_id, order_id AS target_id FROM orders"
    return RelationshipGenResult(
        success=True,
        mapping=_relationship_mapping(prop_uri, "customer_id", "order_id", sql),
        iterations=2,
        usage={"prompt_tokens": 10, "completion_tokens": 5},
    )


def _pass_report(stage: str = "deterministic") -> EvalReport:
    return EvalReport(
        status="PASS",
        stage=stage,
        metrics={"row_count": 100},
        failures=[],
        bubble_to_planner=False,
    )


def _fail_report(
    *,
    stage: str = "deterministic",
    hint: str = "wrong column",
    bubble: bool = False,
) -> EvalReport:
    return EvalReport(
        status="FAIL",
        stage=stage,
        metrics={"row_count": 5},
        failures=[
            EvalFailure(
                kind="structural" if stage == "deterministic" else "semantic",
                check="some_check",
                expected=">0",
                observed="0",
                hint=hint,
            )
        ],
        bubble_to_planner=bubble,
    )


# =====================================================
# Common fixtures
# =====================================================


@pytest.fixture
def fake_client() -> Any:
    """Lightweight stub with the ``execute_query`` method the orchestrator wraps."""

    class _Client:
        def __init__(self):
            self.calls: List[str] = []

        def execute_query(self, sql: str):
            self.calls.append(sql)
            # Echo three rows so ``row_count > 0`` if the real evaluator is
            # invoked. (Most tests stub the evaluator and never hit this.)
            return [
                {"customer_id": 1, "order_id": 10},
                {"customer_id": 2, "order_id": 20},
                {"customer_id": 3, "order_id": 30},
            ]

    return _Client()


def _patch_sub_agents(
    monkeypatch,
    *,
    planner: Any,
    entity_gen: Any = None,
    rel_gen: Any = None,
    critic: Any = None,
    det_eval: Optional[FakeDeterministicEvaluator] = None,
) -> None:
    monkeypatch.setattr(engine_mod, "run_planner", planner)
    if entity_gen is not None:
        monkeypatch.setattr(engine_mod, "run_entity_generator", entity_gen)
    if rel_gen is not None:
        monkeypatch.setattr(engine_mod, "run_relationship_generator", rel_gen)
    if critic is not None:
        monkeypatch.setattr(engine_mod, "run_critic", critic)
    if det_eval is not None:
        monkeypatch.setattr(
            engine_mod, "evaluate_entity_mapping", det_eval.for_entity
        )
        monkeypatch.setattr(
            engine_mod,
            "evaluate_relationship_mapping",
            det_eval.for_relationship,
        )


def _run(client: Any, **overrides) -> AgentResult:
    kwargs = dict(
        host="https://test",
        token="t",
        endpoint_name="ep",
        client=client,
        metadata={},
        ontology=_ontology(),
        skip_semantic_critic=True,
    )
    kwargs.update(overrides)
    return run_agent(**kwargs)


# =====================================================
# Tests
# =====================================================


def test_happy_path_two_entities_one_relationship(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI)],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})  # all default PASS
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    result = _run(fake_client)

    assert result.success is True
    assert len(result.entity_mappings) == 2
    assert len(result.relationship_mappings) == 1
    assert {m["ontology_class"] for m in result.entity_mappings} == {
        CUSTOMER_URI,
        ORDER_URI,
    }
    # 3 mapping_run_log entries, all PASS.
    assert len(result.mapping_run_log) == 3
    assert all(entry["final_status"] == "PASS" for entry in result.mapping_run_log)
    # source_model serialised onto the result.
    assert result.source_model is not None
    assert "table_roles" in result.source_model


def test_planner_failure_aborts(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=False, source_model=None, error="LLM rejected tools")]
    )
    _patch_sub_agents(monkeypatch, planner=planner)

    result = _run(fake_client)

    assert result.success is False
    assert "LLM rejected tools" in result.error
    assert result.entity_mappings == []
    assert result.relationship_mappings == []


def test_generator_failure_records_item_failure_continues_run(
    monkeypatch, fake_client
):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    # Customer generator fails 3 times; Order succeeds.
    fail = EntityGenResult(success=False, mapping=None, error="generator crashed")
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [fail, fail, fail],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    result = _run(fake_client)

    # Order entity mapped despite Customer failing.
    assert any(m["ontology_class"] == ORDER_URI for m in result.entity_mappings)
    assert not any(m["ontology_class"] == CUSTOMER_URI for m in result.entity_mappings)
    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "FAIL_BUDGET"
    # The relationship endpoint for hasOrder requires Customer; with Customer
    # missing the relationship is recorded but not mapped.
    rel_log = next(e for e in result.mapping_run_log if e["item"] == HAS_ORDER_URI)
    assert rel_log["final_status"] in {"FAIL_BUDGET", "PASS"}


def test_evaluator_fail_retry_with_hint(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [
                _ok_entity_gen(CUSTOMER_URI, "SELECT bad_col AS ID FROM x"),
                _ok_entity_gen(CUSTOMER_URI),
            ],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    # First attempt fails, second passes — non-bubble FAIL with a hint.
    det = FakeDeterministicEvaluator(
        {
            CUSTOMER_URI: [
                _fail_report(hint="use customer_id, not bad_col", bubble=False),
                _pass_report(),
            ]
        }
    )
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    result = _run(fake_client)

    assert result.success is True
    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "PASS"
    assert len(customer_log["attempts"]) == 2
    assert customer_log["attempts"][0]["stage1_status"] == "FAIL"
    assert customer_log["attempts"][0]["hint"] == "use customer_id, not bad_col"
    # Second EntityGenerator call must have been given the hint.
    customer_calls = [c for c in entity_gen.calls if c[0] == CUSTOMER_URI]
    assert customer_calls[0][1] is None
    assert customer_calls[1][1] == "use customer_id, not bad_col"


def test_bubble_to_planner_triggers_replanning(monkeypatch, fake_client):
    planner = FakePlanner(
        [
            PlannerResult(success=True, source_model=_source_model(), iterations=1),
            PlannerResult(success=True, source_model=_source_model(), iterations=1),
        ]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [
                _ok_entity_gen(CUSTOMER_URI),  # attempt 1 (bubbles)
                _ok_entity_gen(CUSTOMER_URI),  # attempt 1 of replan iteration
            ],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator(
        {
            CUSTOMER_URI: [
                _fail_report(hint="wrong table", bubble=True),
                _pass_report(),
            ]
        }
    )
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    result = _run(fake_client)

    assert result.success is True
    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "PASS"
    # Planner was invoked twice (initial + 1 replan).
    assert planner.calls == 2
    assert result.stats["planner_reinvocations"] == 1


def test_planner_reinvocation_budget_exhausted(monkeypatch, fake_client):
    # 3 planner results in total: initial + 2 replans (matches the global budget).
    planner = FakePlanner(
        [
            PlannerResult(success=True, source_model=_source_model(), iterations=1)
            for _ in range(3)
        ]
    )
    bubble = _fail_report(hint="wrong table", bubble=True)
    # Customer entity bubbles on every attempt forever.
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI) for _ in range(20)],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator(
        {
            CUSTOMER_URI: [bubble for _ in range(20)],
        }
    )
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    result = _run(fake_client)

    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "FAIL_BUBBLE"
    # 1 initial planner call + exactly 2 replans = 3 total.
    assert planner.calls == 3
    assert result.stats["planner_reinvocations"] == 2
    # Other items still attempted; Order succeeded.
    assert any(m["ontology_class"] == ORDER_URI for m in result.entity_mappings)


def test_retry_budget_exhausted_marks_item_fail_budget(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI) for _ in range(5)],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator(
        {
            CUSTOMER_URI: [
                _fail_report(hint="hint-1", bubble=False),
                _fail_report(hint="hint-2", bubble=False),
                _fail_report(hint="hint-3", bubble=False),
            ],
        }
    )
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    result = _run(fake_client)

    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "FAIL_BUDGET"
    assert len(customer_log["attempts"]) == 3
    assert all(a["stage1_status"] == "FAIL" for a in customer_log["attempts"])
    assert planner.calls == 1
    # Order still mapped.
    assert any(m["ontology_class"] == ORDER_URI for m in result.entity_mappings)


def test_critic_pass_full_pipeline(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI)],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})  # default PASS
    critic = FakeCritic(
        {
            CUSTOMER_URI: [CriticResult(success=True, report=_pass_report("semantic"))],
            ORDER_URI: [CriticResult(success=True, report=_pass_report("semantic"))],
            HAS_ORDER_URI: [
                CriticResult(success=True, report=_pass_report("semantic"))
            ],
        }
    )
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        critic=critic,
        det_eval=det,
    )

    result = _run(fake_client, skip_semantic_critic=False)

    assert result.success is True
    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    last_attempt = customer_log["attempts"][-1]
    assert last_attempt["stage1_status"] == "PASS"
    assert last_attempt["critic_status"] == "PASS"
    # Critic was actually called.
    assert CUSTOMER_URI in critic.calls


def test_critic_fail_with_bubble(monkeypatch, fake_client):
    planner = FakePlanner(
        [
            PlannerResult(success=True, source_model=_source_model(), iterations=1),
            PlannerResult(success=True, source_model=_source_model(), iterations=1),
        ]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [
                _ok_entity_gen(CUSTOMER_URI),  # initial attempt — critic bubbles
                _ok_entity_gen(CUSTOMER_URI),  # post-replan attempt — passes
            ],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})  # default PASS on stage 1
    critic = FakeCritic(
        {
            CUSTOMER_URI: [
                CriticResult(
                    success=True,
                    report=_fail_report(
                        stage="semantic", hint="wrong table", bubble=True
                    ),
                ),
                CriticResult(success=True, report=_pass_report("semantic")),
            ],
            ORDER_URI: [CriticResult(success=True, report=_pass_report("semantic"))],
            HAS_ORDER_URI: [
                CriticResult(success=True, report=_pass_report("semantic"))
            ],
        }
    )
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        critic=critic,
        det_eval=det,
    )

    result = _run(fake_client, skip_semantic_critic=False)

    assert result.success is True
    assert planner.calls == 2
    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "PASS"


def test_skip_semantic_critic_true_skips_critic(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI)],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})
    critic = FakeCritic({})  # would default-PASS if called
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        critic=critic,
        det_eval=det,
    )

    result = _run(fake_client, skip_semantic_critic=True)

    assert result.success is True
    # Critic was never called.
    assert critic.calls == []
    # Every attempt records critic_status="skipped".
    for entry in result.mapping_run_log:
        for attempt in entry["attempts"]:
            assert attempt["critic_status"] == "skipped"


def test_preseeded_entity_skipped(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            # Customer must NOT be generated — it's pre-seeded.
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    pre = [
        _entity_mapping(
            CUSTOMER_URI,
            "customer_id",
            "SELECT customer_id AS ID FROM cat.sch.customers",
        )
    ]

    result = _run(fake_client, entity_mappings=pre)

    assert result.success is True
    customer_log = next(
        e for e in result.mapping_run_log if e["item"] == CUSTOMER_URI
    )
    assert customer_log["final_status"] == "PRESEEDED"
    assert customer_log["attempts"] == []
    # The pre-seeded mapping is still in the result list.
    assert any(m["ontology_class"] == CUSTOMER_URI for m in result.entity_mappings)
    # EntityGenerator never called for Customer.
    assert not any(c[0] == CUSTOMER_URI for c in entity_gen.calls)


def test_skip_list_honoured(monkeypatch, fake_client):
    sm = _source_model()
    # Planner asks the orchestrator to skip Order entirely.
    sm.mapping_plan.skip.append(SkipItem(item=ORDER_URI, reason="no source table"))
    sm.mapping_plan.entity_order = [CUSTOMER_URI, ORDER_URI]
    sm.mapping_plan.relationship_order = []  # nothing depending on Order

    planner = FakePlanner([PlannerResult(success=True, source_model=sm, iterations=1)])
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI)],
            # Order MUST NOT be generated.
        }
    )
    det = FakeDeterministicEvaluator({})
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=FakeRelationshipGenerator({}),
        det_eval=det,
    )

    result = _run(fake_client)

    order_log = next(e for e in result.mapping_run_log if e["item"] == ORDER_URI)
    assert order_log["final_status"] == "SKIPPED"
    assert not any(c[0] == ORDER_URI for c in entity_gen.calls)


def test_on_step_pct_monotonic(monkeypatch, fake_client):
    planner = FakePlanner(
        [PlannerResult(success=True, source_model=_source_model(), iterations=1)]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [_ok_entity_gen(CUSTOMER_URI)],
            ORDER_URI: [_ok_entity_gen(ORDER_URI)],
        }
    )
    rel_gen = FakeRelationshipGenerator({HAS_ORDER_URI: [_ok_rel_gen(HAS_ORDER_URI)]})
    det = FakeDeterministicEvaluator({})
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        det_eval=det,
    )

    pcts: List[int] = []

    def on_step(msg: str, pct: int) -> None:
        pcts.append(pct)

    result = _run(fake_client, on_step=on_step)

    assert result.success is True
    assert pcts, "expected at least one on_step call"
    # Monotonic non-decreasing — captures the documented design contract
    # (we only replan on bubble, which this test does not trigger).
    for prev, curr in zip(pcts, pcts[1:]):
        assert curr >= prev, f"pct went backwards: {prev} -> {curr}"
    # First call planning at low pct, last call completion at 100.
    assert pcts[0] <= 5
    assert pcts[-1] == 100


def test_id_universe_cache_used_across_relationships(monkeypatch, fake_client):
    # Bare ontology with no attributes so the real deterministic evaluator
    # doesn't fire on unmapped_attribute_pct.
    bare_ontology = {
        "entities": [
            {"uri": CUSTOMER_URI, "name": "Customer", "label": "Customer", "attributes": []},
            {"uri": ORDER_URI, "name": "Order", "label": "Order", "attributes": []},
            {"uri": ITEM_URI, "name": "Item", "label": "Item", "attributes": []},
        ],
        "relationships": [
            {
                "uri": HAS_ORDER_URI,
                "name": "hasOrder",
                "label": "hasOrder",
                "domain": CUSTOMER_URI,
                "range": ORDER_URI,
            },
            {
                "uri": CONTAINS_URI,
                "name": "contains",
                "label": "contains",
                "domain": ORDER_URI,
                "range": ITEM_URI,
            },
        ],
    }

    # Distinct, recognisable SQL strings per entity — used both as cache keys
    # and as a discriminator for the CountingClient routing below.
    customer_sql = "SELECT customer_id AS ID, customer_id AS Label FROM cat.sch.customers"
    order_sql = "SELECT order_id AS ID, order_id AS Label FROM cat.sch.orders"
    item_sql = "SELECT item_id AS ID, item_id AS Label FROM cat.sch.items"

    planner = FakePlanner(
        [
            PlannerResult(
                success=True, source_model=_source_model(with_items=True), iterations=1
            )
        ]
    )
    entity_gen = FakeEntityGenerator(
        {
            CUSTOMER_URI: [
                EntityGenResult(
                    success=True,
                    mapping=_entity_mapping(CUSTOMER_URI, "customer_id", customer_sql),
                    iterations=1,
                )
            ],
            ORDER_URI: [
                EntityGenResult(
                    success=True,
                    mapping=_entity_mapping(ORDER_URI, "order_id", order_sql),
                    iterations=1,
                )
            ],
            ITEM_URI: [
                EntityGenResult(
                    success=True,
                    mapping=_entity_mapping(ITEM_URI, "item_id", item_sql),
                    iterations=1,
                )
            ],
        }
    )

    # Relationship edges return rows whose source/target values fall inside
    # the entity universes so the deterministic evaluator passes.
    has_order_sql = "SELECT customer_id AS source_id, order_id AS target_id FROM has_order_edge"
    contains_sql = "SELECT order_id AS source_id, item_id AS target_id FROM contains_edge"

    rel_gen = FakeRelationshipGenerator(
        {
            HAS_ORDER_URI: [
                RelationshipGenResult(
                    success=True,
                    mapping={
                        "property": HAS_ORDER_URI,
                        "property_name": "hasOrder",
                        "sql_query": has_order_sql,
                        "source_id_column": "source_id",
                        "target_id_column": "target_id",
                    },
                    iterations=1,
                )
            ],
            CONTAINS_URI: [
                RelationshipGenResult(
                    success=True,
                    mapping={
                        "property": CONTAINS_URI,
                        "property_name": "contains",
                        "sql_query": contains_sql,
                        "source_id_column": "source_id",
                        "target_id_column": "target_id",
                    },
                    iterations=1,
                )
            ],
        }
    )
    # Use the REAL deterministic evaluators here so the cache codepath is
    # actually exercised against execute_sql_fn.
    _patch_sub_agents(
        monkeypatch,
        planner=planner,
        entity_gen=entity_gen,
        rel_gen=rel_gen,
        # No det_eval override -> real evaluators used.
    )

    class CountingClient:
        """Records every ``execute_query`` call so we can count cache hits."""

        def __init__(self):
            self.sql_calls: List[str] = []

        def execute_query(self, sql: str):
            self.sql_calls.append(sql)
            # Entity-universe queries return rows keyed by the entity's id_column.
            if sql == customer_sql:
                return [{"customer_id": i, "ID": i} for i in range(1, 4)]
            if sql == order_sql:
                return [{"order_id": i, "ID": i} for i in range(1, 4)]
            if sql == item_sql:
                return [{"item_id": i, "ID": i} for i in range(1, 4)]
            # Edge SQLs: values must overlap with the entity universes so
            # dangling_*_pct stays low and the report PASSes.
            if sql == has_order_sql:
                return [
                    {"source_id": i, "target_id": i, "customer_id": i, "order_id": i}
                    for i in range(1, 4)
                ]
            if sql == contains_sql:
                return [
                    {"source_id": i, "target_id": i, "order_id": i, "item_id": i}
                    for i in range(1, 4)
                ]
            return []

    client = CountingClient()
    result = _run(client, ontology=bare_ontology)

    assert len(result.entity_mappings) == 3, result.mapping_run_log
    assert len(result.relationship_mappings) == 2, result.mapping_run_log

    # Each unique entity SQL is run by the entity evaluator (1) + at most
    # ONCE more from the first relationship that references it (cached for
    # subsequent relationships).  Without the cache, each entity SQL would
    # fire from EVERY relationship that touches it — order_sql in
    # particular would run 1 (entity) + 2 (hasOrder + contains) = 3 times.
    for sql in (customer_sql, order_sql, item_sql):
        count = sum(1 for c in client.sql_calls if c == sql)
        assert count <= 2, (
            f"entity SQL ran {count} times — id_universe_cache failed:\n{sql}"
        )
