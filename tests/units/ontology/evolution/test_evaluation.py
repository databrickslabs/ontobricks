"""Stage 3: arm builds, paired GraphQL evaluation, accept and the review link (offline)."""

from __future__ import annotations

import copy
import re

import pytest

from back.core.errors import (
    ConflictError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from back.core.task_manager import get_task_manager
from back.objects.ontology.evolution import (
    CandidateBuilder,
    Decision,
    EvaluationService,
    EvolutionPipeline,
    EvolutionReviewLink,
    EvolutionService,
    Intent,
    OntologyIndex,
    PathQueryBuilder,
    SourceAccessDenied,
    VersionAllocator,
)
from back.objects.ontology.evolution.ArmGraphQuery import ArmGraphQuery
from back.objects.ontology.evolution.EvaluationService import source_tables
from tests.fixtures.evolution.memory_graph import memory_query_factory
from tests.fixtures.evolution.memory_probe import MemorySourceProbe
from tests.fixtures.evolution.memory_registry import FakeRegistry
from tests.fixtures.evolution.variants import variant_document

pytestmark = pytest.mark.unit

ACME = {
    "output_class": "Order",
    "mentions": ["orders", "Acme"],
    "filters": [{"class": "Supplier", "attribute": "supplierName", "value": "Acme"}],
}
ACME_PRODUCTS = {
    "output_class": "Product",
    "filters": [{"class": "Supplier", "attribute": "supplierName", "value": "Acme"}],
}


class FakeGraphBuilder:
    """Delta builder stand-in: derives run-owned names, completes the task."""

    def __init__(self, domain, log, fail=False, on_build=None):
        self._domain = domain
        self._log = log
        self._fail = fail
        self._on_build = on_build

    def _stem(self):
        safe = re.sub(r"[^a-z0-9_]", "_", self._domain.info["name"].lower())
        return f"cat.reg.triplestore_{safe}_V{self._domain.current_version}"

    def data_table_fqn(self):
        return self._stem() + "_data"

    def inferred_table_fqn(self):
        return self._stem() + "_inferred"

    def graph_view_fqn(self):
        return self._stem() + "_graph"

    def build_steps(self, materialization):
        return [{"name": "build", "description": materialization}]

    def run_triplestore_build(self, *, tm, task_id, record_history, **kw):
        self._log.append(("build", kw["data_table"], record_history))
        if self._on_build:
            self._on_build()
        if self._fail:
            tm.fail_task(task_id, "warehouse stopped")
        else:
            tm.complete_task(task_id, result={"triple_count": 42})

    def drop_relation(self, fqn, *, kind="table"):
        self._log.append(("drop", fqn, kind))


@pytest.fixture(autouse=True)
def _credentials(monkeypatch):
    from back.core.helpers.DatabricksHelpers import DatabricksHelpers

    monkeypatch.setattr(
        DatabricksHelpers,
        "get_triplestore_sql_credentials",
        staticmethod(lambda d, s: ("https://h", "", "wh")),
    )


def _world(variant="base", fail_build=False, on_build=None, fail_arms=()):
    registry = FakeRegistry({"1": variant_document(variant)})
    svc = EvolutionService(registry, "EvoSupplyChain")
    probe = MemorySourceProbe()
    log = []
    builder = CandidateBuilder(
        None,
        {"registry": {"catalog": "cat", "schema": "reg"}},
        task_manager=get_task_manager(),
        builder_factory=lambda d: FakeGraphBuilder(d, log, fail_build, on_build),
    )
    evaluator = EvaluationService(
        svc, builder, probe, query_factory=memory_query_factory(fail_arms)
    )
    return svc, evaluator, probe, registry, log


def _proposed(svc, probe, intent=ACME, key="k"):
    pipe = EvolutionPipeline(svc, probe)
    run = svc.create_run("1", intent, question="q", owner="o", idempotency_key=key)
    run = pipe.assess(run["id"], run["revision"])
    return pipe.propose(run["id"], run["revision"])


def _evaluate(evaluator, svc, run, body):
    run = evaluator.prepare(run["id"], run["revision"], body)
    assert run["status"] == "RUNNING"
    return evaluator.execute(run["id"])


GOLD = {"gold_table": "cat.fix.gold_answers", "target_question_id": "acme_orders"}


class TestPathQueries:
    def _plan(self, intent, variant="base", candidate_ops=None):
        doc = variant_document(variant)
        sl = doc["versions"]["1"]
        return PathQueryBuilder(OntologyIndex(sl["ontology"], sl["assignment"])).plan(
            Intent.from_dict(intent)
        )

    def test_parent_has_no_plan_for_the_missing_link(self):
        plan = self._plan(ACME)
        assert plan["queries"] == []
        assert plan["unresolved"] == [
            {"filter": "Supplier.supplierName=Acme", "reason": "no_unique_path"}
        ]

    def test_forward_path_query_uses_schema_field_names(self):
        plan = self._plan(ACME_PRODUCTS)
        (item,) = plan["queries"]
        assert item["mode"] == "forward" and plan["depth"] == 1
        assert item["query"] == (
            "{ products(limit: 1000) { id suppliedBy { id label } } }"
        )

    def test_reverse_path_is_queried_from_the_filter_class(self):
        intent = {
            "output_class": "OrderLine",
            "filters": [{"class": "Order", "attribute": "customerName", "value": "N"}],
        }
        (item,) = self._plan(intent)["queries"]
        # OrderLine -> Order walks hasLine against its direction.
        assert item["mode"] == "reverse" and item["root_field"] == "orders"
        assert item["query"] == (
            "{ orders(limit: 1000) { id customerName hasLine { id } } }"
        )
        intent = {
            "output_class": "Supplier",
            "filters": [{"class": "Product", "attribute": "sku", "value": "X"}],
        }
        (item,) = self._plan(intent)["queries"]
        assert item["mode"] == "reverse" and item["root_field"] == "products"

    def test_root_filter_and_no_filter(self):
        intent = {
            "output_class": "Supplier",
            "filters": [
                {"class": "Supplier", "attribute": "supplierName", "value": "Acme"}
            ],
        }
        (item,) = self._plan(intent)["queries"]
        assert item["query"] == "{ suppliers(limit: 1000) { id label } }"
        (item,) = self._plan({"output_class": "Supplier"})["queries"]
        assert item["mode"] == "root" and item["attribute"] == ""

    @pytest.mark.parametrize(
        "intent,reason",
        [
            ({"output_class": "Nope"}, "unknown_output_class"),
            (
                {
                    "output_class": "Order",
                    "filters": [
                        {"class": "Supplier", "attribute": "rating", "value": "A"}
                    ],
                },
                "unknown_attribute",
            ),
            (
                {
                    "output_class": "Order",
                    "filters": [
                        {
                            "class": "Order",
                            "attribute": "orderDate",
                            "op": "gt",
                            "value": "x",
                        }
                    ],
                },
                "unsupported_operator",
            ),
        ],
    )
    def test_unresolved_reasons(self, intent, reason):
        assert self._plan(intent)["unresolved"][0]["reason"] == reason

    def test_mixed_direction_path_is_unresolved(self):
        doc = variant_document("base")
        sl = doc["versions"]["1"]
        # Order -hasLine-> OrderLine <-lineOf- Product: one edge each way.
        sl["ontology"]["properties"].append(
            {
                "name": "lineOf",
                "type": "ObjectProperty",
                "domain": "Product",
                "range": "OrderLine",
            }
        )
        index = OntologyIndex(sl["ontology"], sl["assignment"])
        intent = {
            "output_class": "Order",
            "filters": [{"class": "Product", "attribute": "sku", "value": "X"}],
        }
        plan = PathQueryBuilder(index).plan(Intent.from_dict(intent))
        assert plan["unresolved"][0]["reason"] == "mixed_direction_path"

    def test_answer_flags_truncation(self):
        item = {
            "root_field": "suppliers",
            "mode": "root",
            "attribute": "",
            "value": None,
            "steps": [],
        }
        rows = [{"id": str(i)} for i in range(PathQueryBuilder.LIMIT)]
        assert PathQueryBuilder.answer(item, {"suppliers": rows})["truncated"] is True


class TestEvaluation:
    def test_candidate_fixes_the_question_and_parent_cannot_answer(self):
        svc, evaluator, probe, _, log = _world()
        run = _evaluate(
            evaluator,
            svc,
            _proposed(svc, probe),
            {
                **GOLD,
                "checks": [{"question_id": "acme_products", "intent": ACME_PRODUCTS}],
            },
        )
        ev = run["evaluation"]
        assert run["status"] == "EVALUATED" and ev["decision"] == "PASS", ev
        target, check = ev["questions"]
        assert target["parent"]["status"] == "unresolved"
        assert target["candidate"]["ids"] == ["O1", "O3", "O5"]
        assert target["outcome"] == "fixed"
        assert check["outcome"] == "unchanged"
        assert check["parent"]["ids"] == check["candidate"]["ids"] == ["P1", "P3"]
        assert len(ev["evaluation_hash"]) == 64
        builds = [entry for entry in log if entry[0] == "build"]
        assert [b[2] for b in builds] == [False, False]  # no build history
        assert all("__evo" in b[1] for b in builds)
        assert len(svc.list_results(run["id"])) == 4
        assert [b["arm"] for b in run["manifest"]["builds"]] == ["parent", "candidate"]

    def test_inline_expected_that_does_not_match_fails(self):
        svc, evaluator, probe, *_ = _world()
        run = _evaluate(evaluator, svc, _proposed(svc, probe), {"expected_ids": ["O1"]})
        assert run["evaluation"]["decision"] == "FAIL"
        assert "candidate_does_not_match_expected" in run["evaluation"]["reasons"]

    def test_source_change_between_arms_is_inconclusive(self):
        svc, evaluator, probe, *_ = _world()
        bump = lambda: probe.versions.update({"cat.fix.suppliers": 2})  # noqa: E731
        evaluator._builder._builder_factory = lambda d: FakeGraphBuilder(
            d, [], on_build=bump
        )
        run = _evaluate(evaluator, svc, _proposed(svc, probe), GOLD)
        assert run["evaluation"]["decision"] == "INCONCLUSIVE"
        assert "source_changed_between_arms" in run["evaluation"]["reasons"]

    def test_unreadable_arm_graph_is_inconclusive_not_empty(self):
        svc, evaluator, probe, *_ = _world(fail_arms=("candidate",))
        run = _evaluate(evaluator, svc, _proposed(svc, probe), GOLD)
        assert run["evaluation"]["decision"] == "INCONCLUSIVE"
        assert run["evaluation"]["questions"][0]["candidate"]["status"] == "error"

    def test_failed_build_leaves_an_inconclusive_retryable_run(self):
        svc, evaluator, probe, *_ = _world(fail_build=True)
        run = _evaluate(evaluator, svc, _proposed(svc, probe), GOLD)
        assert run["status"] == "EVALUATED"
        assert run["evaluation"]["decision"] == "INCONCLUSIVE"
        assert run["error_code"] == "evaluation_error"
        assert (
            evaluator.prepare(run["id"], run["revision"], GOLD)["status"] == "RUNNING"
        )

    def test_cancel_during_evaluation(self):
        svc, evaluator, probe, *_ = _world()
        run = evaluator.prepare(*_ids(_proposed(svc, probe)), GOLD)
        svc.cancel(*_ids(run))
        run = evaluator.execute(run["id"])
        assert run["status"] == "CANCELLED"

    def test_unexpected_error_never_leaves_the_run_running(self, monkeypatch):
        svc, evaluator, probe, *_ = _world()
        run = evaluator.prepare(*_ids(_proposed(svc, probe)), GOLD)
        monkeypatch.setattr(evaluator, "_evaluate", lambda *a: 1 / 0)
        assert evaluator.execute(run["id"])["evaluation"]["decision"] == "INCONCLUSIVE"

    def test_parent_already_answering_is_no_change(self):
        svc, evaluator, probe, *_ = _world()
        run = _proposed(svc, probe)
        run = svc.update(
            *_ids(run), {"intent": Intent.from_dict(ACME_PRODUCTS).to_dict()}
        )
        run = _evaluate(evaluator, svc, run, {"expected_ids": ["P1", "P3"]})
        assert run["evaluation"]["decision"] == "NO_CHANGE"


def _ids(run):
    return run["id"], run["revision"]


class TestOutcomes:
    def _q(self, parent, cand, gold, role="protected"):
        def arm(status, ids, truncated=False):
            return {"status": status, "ids": ids, "truncated": truncated}

        return {
            "parent": arm(*parent),
            "candidate": arm(*cand),
            "expected_ids": gold,
            "role": role,
        }

    @pytest.mark.parametrize(
        "parent,cand,gold,outcome",
        [
            (("answered", ["a"]), ("answered", ["b"]), ["a"], "regressed"),
            (("answered", ["a"]), ("answered", ["a"]), ["a"], "unchanged"),
            (("answered", ["x"]), ("answered", ["a"]), ["a"], "fixed"),
            (("answered", ["x"]), ("answered", ["y"]), ["a"], "still_broken"),
            (("answered", ["a"]), ("answered", ["b"]), None, "regressed"),
            (("unresolved", []), ("answered", ["b"]), None, "new_answer"),
            (("unresolved", []), ("unresolved", []), None, "unchanged"),
            (("error", []), ("answered", ["a"]), ["a"], "inconclusive"),
            (("answered", ["a"], True), ("answered", ["a"]), ["a"], "inconclusive"),
        ],
    )
    def test_outcome_table(self, parent, cand, gold, outcome):
        assert EvaluationService._outcome(self._q(parent, cand, gold)) == outcome

    def test_regression_fails_even_when_target_is_fixed(self):
        questions = [
            {"role": "target", "outcome": "fixed"},
            {"role": "protected", "outcome": "regressed"},
        ]
        reasons = []
        assert EvaluationService._decide(questions, reasons) == Decision.FAIL.value
        assert reasons == ["protected_question_regressed"]


class TestPrepare:
    def test_needs_a_validated_proposal(self):
        svc, evaluator, probe, *_ = _world()
        run = svc.create_run("1", ACME, question="q", owner="o", idempotency_key="x")
        with pytest.raises(ConflictError):
            evaluator.prepare(*_ids(run), GOLD)
        run = EvolutionPipeline(svc, probe).assess(*_ids(run))
        bad = [
            {
                "op": "add_relationship",
                "target": {"name": "x"},
                "params": {"uri": "u:x", "domain": "OrderLine", "range": "Nope"},
            }
        ]
        run = EvolutionPipeline(svc, probe).propose(run["id"], run["revision"], bad)
        with pytest.raises(ValidationError):
            evaluator.prepare(*_ids(run), GOLD)

    def test_needs_an_expected_answer(self):
        svc, evaluator, probe, *_ = _world()
        with pytest.raises(ValidationError):
            evaluator.prepare(*_ids(_proposed(svc, probe)), {})

    def test_unknown_gold_question(self):
        svc, evaluator, probe, *_ = _world()
        with pytest.raises(NotFoundError):
            evaluator.prepare(
                *_ids(_proposed(svc, probe)), {**GOLD, "target_question_id": "nope"}
            )

    def test_gold_table_name_is_validated(self):
        svc, evaluator, probe, *_ = _world()
        with pytest.raises(ValidationError):
            evaluator.prepare(
                *_ids(_proposed(svc, probe)), {**GOLD, "gold_table": "x; DROP TABLE t"}
            )

    def test_too_many_checks(self):
        svc, evaluator, probe, *_ = _world()
        checks = [{"intent": ACME_PRODUCTS}] * 21
        with pytest.raises(ValidationError):
            evaluator.prepare(*_ids(_proposed(svc, probe)), {**GOLD, "checks": checks})

    def test_access_is_checked_as_the_caller(self):
        svc, evaluator, probe, *_ = _world()
        run = _proposed(svc, probe)
        evaluator._probe = MemorySourceProbe(denied=["suppliers"])
        with pytest.raises(SourceAccessDenied):
            evaluator.prepare(*_ids(run), {"expected_ids": ["O1"]})

    def test_source_tables_cover_entities_and_mapping_sql(self):
        sl = variant_document("broken_mapping")["versions"]["1"]
        tables = source_tables(sl["ontology"], sl["assignment"])
        assert "cat.fix.order_lines" in tables and "cat.fix.suppliers" in tables


def _passed():
    svc, evaluator, probe, registry, log = _world()
    run = _evaluate(evaluator, svc, _proposed(svc, probe), GOLD)
    assert run["evaluation"]["decision"] == "PASS"
    return svc, registry, probe, run


class TestAccept:
    def test_creates_one_draft_version_with_the_evidence_link(self):
        svc, registry, probe, run = _passed()
        before = copy.deepcopy(registry.documents["1"])
        out = VersionAllocator(svc, registry, probe).accept(*_ids(run))
        assert out["created"] is True and out["version"] == "2"
        assert registry.documents["1"] == before
        doc = registry.documents["2"]
        assert doc["info"]["status"] == "DRAFT"
        link = doc["info"]["evolution"]
        assert link["run_id"] == run["id"] and link["decision"] == "PASS"
        names = [p["name"] for p in doc["versions"]["2"]["ontology"]["properties"]]
        assert "hasProduct" in names
        assert ("copy_documents", "1", "2") in registry.writes
        again = VersionAllocator(svc, registry, probe).accept(*_ids(out["run"]))
        assert again["created"] is False and again["version"] == "2"
        assert set(registry.documents) == {"1", "2"}

    def test_only_pass_can_be_accepted(self):
        svc, evaluator, probe, *_ = _world()
        run = _evaluate(evaluator, svc, _proposed(svc, probe), {"expected_ids": ["O9"]})
        with pytest.raises(ValidationError):
            VersionAllocator(svc, svc.registry, probe).accept(*_ids(run))

    def test_tampered_candidate_is_refused(self):
        svc, registry, probe, run = _passed()
        cand = copy.deepcopy(run["candidate"])
        cand["ontology"]["properties"].pop()
        run = svc.update(*_ids(run), {"candidate": cand})
        with pytest.raises(ConflictError):
            VersionAllocator(svc, registry, probe).accept(*_ids(run))

    def test_access_is_rechecked(self):
        svc, registry, _probe, run = _passed()
        with pytest.raises(SourceAccessDenied):
            VersionAllocator(
                svc, registry, MemorySourceProbe(denied=["products"])
            ).accept(*_ids(run))

    def test_document_copy_failure_is_reported_not_raised(self, monkeypatch):
        svc, registry, probe, run = _passed()
        monkeypatch.setattr(registry, "copy_version_documents", lambda *a: 1 / 0)
        out = VersionAllocator(svc, registry, probe).accept(*_ids(run))
        assert out["created"] and out["documents"]


class TestReviewLink:
    def _accepted(self):
        svc, registry, probe, run = _passed()
        VersionAllocator(svc, registry, probe).accept(*_ids(run))
        return registry.documents["2"]

    def test_review_meta_carries_the_evaluation(self):
        meta = EvolutionReviewLink.review_meta(self._accepted(), "2")
        assert meta["evolution"]["decision"] == "PASS"
        assert len(meta["evolution"]["evaluation_hash"]) == 64

    def test_edit_after_evaluation_blocks_submission(self):
        doc = self._accepted()
        doc["versions"]["2"]["ontology"]["classes"].pop()
        with pytest.raises(ValidationError):
            EvolutionReviewLink.review_meta(doc, "2")

    def test_ordinary_versions_are_unaffected(self):
        assert EvolutionReviewLink.review_meta(variant_document("base"), "1") == {}


class TestCandidateBuilder:
    def _builder(self, log, **kw):
        return CandidateBuilder(
            None,
            {"registry": {"catalog": "cat"}},
            task_manager=get_task_manager(),
            builder_factory=lambda d: FakeGraphBuilder(d, log, **kw),
        )

    def _run(self):
        svc, _evaluator, probe, *_ = _world()
        return _proposed(svc, probe)

    def test_arm_domains_are_isolated_and_renamed(self):
        doc = variant_document("base")
        before = copy.deepcopy(doc)
        run = self._run()
        b = self._builder([])
        parent = b.arm_domain(doc, run, "parent", "EvoSupplyChain")
        cand = b.arm_domain(doc, run, "candidate", "EvoSupplyChain")
        assert doc == before
        assert parent.info["name"] == f"EvoSupplyChain__evo{run['id'][:8]}p"
        assert cand.info["name"].endswith("c")
        assert parent.info["graph_cache_enabled"] is False
        assert "hasProduct" not in [p["name"] for p in parent.ontology["properties"]]
        assert "hasProduct" in [p["name"] for p in cand.ontology["properties"]]
        assert cand.get_r2rml()

    def test_candidate_arm_needs_a_candidate(self):
        run = {**self._run(), "candidate": {}}
        with pytest.raises(ValidationError):
            self._builder([]).arm_domain(
                variant_document("base"), run, "candidate", "F"
            )
        with pytest.raises(ValidationError):
            CandidateBuilder.arm_name("x", "abc", "other")

    def test_only_delta_backend(self):
        doc = variant_document("base")
        doc["info"]["graph_backend"] = "lakebase"
        b = self._builder([])
        domain = b.arm_domain(doc, self._run(), "parent", "F")
        with pytest.raises(ValidationError):
            b.build(domain, "abcdef123456", "parent")

    def test_failed_build_raises(self):
        b = self._builder([], fail=True)
        domain = b.arm_domain(variant_document("base"), self._run(), "parent", "F")
        with pytest.raises(InfrastructureError):
            b.build(domain, "abcdef123456", "parent")

    def test_refuses_a_name_that_is_not_run_owned(self):
        b = self._builder([])
        domain = b.arm_domain(variant_document("base"), self._run(), "parent", "F")
        domain.info["name"] = "EvoSupplyChain"
        with pytest.raises(ValidationError):
            b.build(domain, "abcdef123456", "parent")

    def test_drop_touches_only_run_owned_relations(self):
        log = []
        builds = [
            {
                "view_table": "c.s.triplestore_x__evoabcdef12p_V1",
                "data_table": "c.s.triplestore_x__evoabcdef12p_V1_data",
                "inferred_table": "c.s.triplestore_x__evoabcdef12p_V1_inferred",
                "graph_view": "c.s.triplestore_x__evoabcdef12p_V1_graph",
            },
            {"data_table": "c.s.triplestore_x_V1_data", "graph_view": "c.s.other"},
        ]
        dropped = self._builder(log).drop(object(), builds)
        assert len(dropped) == 4
        kinds = {fqn: kind for _, fqn, kind in log}
        assert kinds["c.s.triplestore_x__evoabcdef12p_V1_data"] == "table"
        assert kinds["c.s.triplestore_x__evoabcdef12p_V1_graph"] == "view"
        assert "c.s.triplestore_x_V1_data" not in kinds


class TestArmGraphQuery:
    def test_graphql_errors_raise(self):
        from tests.fixtures.evolution.memory_graph import MemoryTripleStore

        domain = CandidateBuilder(None, {}).arm_domain(
            variant_document("base"),
            {"id": "abcdef123456", "parent_version": "1"},
            "parent",
            "F",
        )
        q = ArmGraphQuery(domain, None, store=MemoryTripleStore([]), table="t")
        with pytest.raises(InfrastructureError):
            q.execute("{ nope }", 1)
        assert q.table == "t"

    def test_missing_store_raises(self, monkeypatch):
        import back.core.graphdb as graphdb

        monkeypatch.setattr(graphdb, "get_graphdb", lambda d, s: None)
        domain = CandidateBuilder(None, {}).arm_domain(
            variant_document("base"),
            {"id": "abcdef123456", "parent_version": "1"},
            "parent",
            "F",
        )
        with pytest.raises(InfrastructureError):
            ArmGraphQuery(domain, None)


def test_arm_store_reads_without_search_companions_and_probes_triple_loading():
    from tests.fixtures.evolution.memory_graph import MemoryTripleStore, arm_triples

    domain = CandidateBuilder(None, {}).arm_domain(
        variant_document("base"),
        {"id": "abcdef123456", "parent_version": "1"},
        "parent",
        "F",
    )
    store = MemoryTripleStore(arm_triples(domain), fail_load=True)
    q = ArmGraphQuery(domain, None, store=store, table="t")
    assert store.search_cache_mode == "force_off"
    with pytest.raises(InfrastructureError):
        q.probe("Supplier")
