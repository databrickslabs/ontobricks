"""Paired parent/candidate evaluation of one Evolve run.

``prepare`` runs in the request: it checks the run can be evaluated,
resolves every expected answer (inline or from a UC gold table) and the
caller's access to the sources, then marks the run RUNNING. ``execute``
runs in a worker: it records the source tables' Delta versions, builds
both arms into run-owned graphs, re-reads the versions (a change makes the
run INCONCLUSIVE), runs the same path queries on both arms, appends one
result row per question and arm, and stores the decision.

Only PASS lets a candidate be accepted. Store errors, truncated answers and
drifting sources are INCONCLUSIVE, never a pass.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ConflictError, OntoBricksError, ValidationError
from back.core.logging import get_logger
from back.objects.ontology.evolution.ArmSqlQuery import ArmSqlQuery
from back.objects.ontology.evolution.CandidateBuilder import ARMS, CandidateBuilder
from back.objects.ontology.evolution.Decision import Decision
from back.objects.ontology.evolution.EvolutionPipeline import parent_slice
from back.objects.ontology.evolution.EvolutionService import EvolutionService
from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex
from back.objects.ontology.evolution.PathQueryBuilder import PathQueryBuilder
from back.objects.ontology.evolution.RunStatus import RunStatus
from back.objects.ontology.evolution.SourceProbe import SourceProbe
from back.objects.ontology.evolution.WarehouseSourceProbe import split_fqn

logger = get_logger(__name__)

SCHEMA_VERSION = 1
MAX_CHECKS = 20
_EVALUABLE = {RunStatus.PROPOSED.value, RunStatus.EVALUATED.value, "INTERRUPTED"}
_TABLE_REF = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){2})\b", re.I
)

QueryFactory = Callable[[Any], ArmSqlQuery]


def source_tables(ontology: Dict[str, Any], assignment: Dict[str, Any]) -> List[str]:
    """Every UC table an arm's mappings read (entity tables and mapping SQL)."""
    index = OntologyIndex(ontology, assignment)
    tables = {
        e["table"] for e in (index.entity(c) for c in index.classes) if e and e["table"]
    }
    for mapping in (assignment.get("entities") or []) + (
        assignment.get("relationships") or []
    ):
        tables.update(_TABLE_REF.findall(str(mapping.get("sql_query") or "")))
    return sorted(tables)


def evaluation_hash(payload: Dict[str, Any]) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class EvaluationService:
    """Prepare (request) and execute (worker) the paired evaluation."""

    def __init__(
        self,
        service: EvolutionService,
        builder: CandidateBuilder,
        probe: SourceProbe,
        query_factory: Optional[QueryFactory] = None,
        settings: Any = None,
    ):
        self._svc = service
        self._builder = builder
        self._probe = probe
        self._settings = settings
        self._query_factory = query_factory or (lambda d: ArmSqlQuery(d, settings))

    # -- request side -----------------------------------------------------

    def prepare(
        self, run_id: str, revision: int, body: Dict[str, Any], *, task_id: str = ""
    ) -> Dict[str, Any]:
        run = self._svc.get_run(run_id)
        if run["status"] not in _EVALUABLE:
            raise ConflictError(f"A {run['status']} run cannot be evaluated")
        validation = (run.get("proposal") or {}).get("validation") or {}
        if not validation.get("passed") or not (run.get("candidate") or {}).get(
            "ontology"
        ):
            raise ValidationError("Only a validated proposal can be evaluated")
        if run["cancel_requested"]:
            raise ConflictError("The run was cancelled")
        self._svc.ensure_parent_unchanged(run)
        gold_table = str(body.get("gold_table") or "")
        if gold_table:
            split_fqn(gold_table)
        target = {
            "question_id": str(body.get("target_question_id") or "target"),
            "role": "target",
            "intent": run["intent"],
            "expected_ids": self._expected(
                body.get("expected_ids"), gold_table, body.get("target_question_id")
            ),
        }
        if target["expected_ids"] is None:
            raise ValidationError(
                "Give the expected answer for the question, inline or from a gold table"
            )
        checks = list(body.get("checks") or [])
        if len(checks) > MAX_CHECKS:
            raise ValidationError(f"At most {MAX_CHECKS} protected questions")
        questions = [target]
        for i, check in enumerate(checks):
            qid = str(check.get("question_id") or f"check_{i + 1}")
            questions.append(
                {
                    "question_id": qid,
                    "role": "protected",
                    "intent": Intent.from_dict(check.get("intent") or {}).to_dict(),
                    "expected_ids": self._expected(
                        check.get("expected_ids"),
                        gold_table,
                        check.get("question_id") if gold_table else None,
                    ),
                }
            )
        # Authorize in the request context: the worker builds as the app.
        document = self._svc.read_parent(run["parent_version"])
        sl = parent_slice(document, run["parent_version"])
        cand = run["candidate"]
        tables = sorted(
            set(source_tables(sl.get("ontology") or {}, sl.get("assignment") or {}))
            | set(source_tables(cand["ontology"], cand["assignment"]))
        )
        for table in tables:
            self._probe.columns(table)
        return self._svc.update(
            run_id,
            revision,
            {
                "status": RunStatus.RUNNING.value,
                "stage": "evaluation",
                "task_id": task_id,
                "evaluation": {
                    "schema_version": SCHEMA_VERSION,
                    "request": {"questions": questions, "gold_table": gold_table},
                    "source_tables": tables,
                },
                "error_code": "",
                "error": "",
                "touch_heartbeat": True,
            },
        )

    def _expected(self, inline, gold_table: str, question_id) -> Optional[List[str]]:
        if inline is not None:
            if not isinstance(inline, list):
                raise ValidationError("expected_ids must be a list")
            return sorted(str(i) for i in inline)
        if gold_table and question_id:
            return self._probe.gold_ids(gold_table, str(question_id))
        return None

    # -- worker side ------------------------------------------------------

    def _write(self, run_id: str, changes: Dict[str, Any]) -> Dict[str, Any]:
        """Re-read then CAS-write: the worker is the only writer while RUNNING."""
        run = self._svc.get_run(run_id)
        return self._svc.update(run_id, run["revision"], changes)

    def _check_cancel(self, run_id: str) -> None:
        if self._svc.get_run(run_id)["cancel_requested"]:
            raise _Cancelled()

    def execute(self, run_id: str) -> Dict[str, Any]:
        run = self._svc.get_run(run_id)
        evaluation = dict(run.get("evaluation") or {})
        try:
            outcome = self._evaluate(run, evaluation)
        except _Cancelled:
            return self._write(
                run_id,
                {
                    "status": RunStatus.CANCELLED.value,
                    "evaluation": {**evaluation, "decision": Decision.CANCELLED.value},
                },
            )
        except OntoBricksError as exc:
            logger.warning("Evolve evaluation %s inconclusive: %s", run_id, exc)
            outcome = self._inconclusive(evaluation, "evaluation_error", exc)
        except Exception as exc:  # noqa: BLE001 — the run must not stay RUNNING
            logger.exception("Evolve evaluation %s failed", run_id)
            outcome = self._inconclusive(evaluation, "evaluation_error", exc)
        return self._write(
            run_id,
            {
                "status": RunStatus.EVALUATED.value,
                "stage": "evaluation",
                "evaluation": outcome,
                "error_code": outcome.get("error_code", ""),
                "error": outcome.get("error", ""),
            },
        )

    @staticmethod
    def _inconclusive(evaluation, code: str, exc: Exception) -> Dict[str, Any]:
        detail = getattr(exc, "detail", "") or ""
        message = getattr(exc, "message", "") or str(exc)
        return {
            **evaluation,
            "decision": Decision.INCONCLUSIVE.value,
            "reasons": [code],
            "error_code": code,
            "error": f"{message} {detail}".strip()[:500],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    def _evaluate(
        self, run: Dict[str, Any], evaluation: Dict[str, Any]
    ) -> Dict[str, Any]:
        run_id = run["id"]
        self._svc.ensure_parent_unchanged(run)
        document = self._svc.read_parent(run["parent_version"])
        tables = evaluation.get("source_tables") or []
        before = {t: self._probe.table_version(t) for t in tables}
        domains, builds = {}, []
        for arm in ARMS:
            self._check_cancel(run_id)
            domains[arm] = self._builder.arm_domain(
                document, run, arm, self._svc.folder
            )
            builds.append(self._builder.build(domains[arm], run_id, arm))
            self._write(
                run_id,
                {
                    "manifest": {**run["manifest"], "builds": builds},
                    "touch_heartbeat": True,
                },
            )
        after = {t: self._probe.table_version(t) for t in tables}
        self._check_cancel(run_id)
        queries = {arm: self._query_factory(domains[arm]) for arm in ARMS}
        questions = [
            self._question(run_id, q, domains, queries)
            for q in (evaluation.get("request") or {}).get("questions") or []
        ]
        reasons: List[str] = []
        if before != after:
            reasons.append("source_changed_between_arms")
        decision = self._decide(questions, reasons)
        # Views have no Delta version, so a change between the two builds
        # cannot be ruled out; say so next to the decision.
        unpinned = sorted(t for t, v in before.items() if v is None)
        notes = ["unpinned_sources"] if unpinned else []
        payload = {
            "run_id": run_id,
            "parent_content_hash": run["manifest"].get("parent_content_hash"),
            "candidate_design_hash": run["candidate"].get("design_hash"),
            "decision": decision,
            "questions": [
                {k: q[k] for k in ("question_id", "role", "outcome", "expected_ids")}
                | {arm: q[arm]["ids"] for arm in ARMS}
                for q in questions
            ],
            "source_versions": before,
        }
        return {
            **evaluation,
            "decision": decision,
            "reasons": reasons,
            "questions": questions,
            "builds": builds,
            "source_versions": {"before": before, "after": after},
            "unpinned_sources": unpinned,
            "notes": notes,
            "candidate_design_hash": payload["candidate_design_hash"],
            "evaluation_hash": evaluation_hash(payload),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }

    def _question(self, run_id, q, domains, queries) -> Dict[str, Any]:
        intent = Intent.from_dict(q["intent"])
        out = {
            "question_id": q["question_id"],
            "role": q["role"],
            "expected_ids": q["expected_ids"],
        }
        for arm in ARMS:
            domain = domains[arm]
            plan = PathQueryBuilder(
                OntologyIndex(domain.ontology or {}, domain.assignment or {})
            ).plan(intent)
            result = queries[arm].run(plan)
            out[arm] = result
            self._svc.append_result(
                run_id,
                {
                    "question_id": q["question_id"],
                    "arm": arm,
                    "status": result["status"],
                    "observed": {**result, "expected_ids": q["expected_ids"]},
                },
            )
        out["outcome"] = self._outcome(out)
        return out

    @staticmethod
    def _outcome(q: Dict[str, Any]) -> str:
        parent, cand, gold = q["parent"], q["candidate"], q["expected_ids"]
        if any(a["status"] == "error" or a["truncated"] for a in (parent, cand)):
            return "inconclusive"
        p_ok = parent["status"] == "answered" and (
            parent["ids"] == gold if gold is not None else True
        )
        c_ans = cand["status"] == "answered"
        if gold is None:
            if parent["status"] == "answered":
                return (
                    "unchanged"
                    if c_ans and cand["ids"] == parent["ids"]
                    else "regressed"
                )
            return "new_answer" if c_ans else "unchanged"
        c_ok = c_ans and cand["ids"] == gold
        if p_ok and c_ok:
            return "already_correct" if q["role"] == "target" else "unchanged"
        if p_ok:
            return "regressed"
        return "fixed" if c_ok else "still_broken"

    @staticmethod
    def _decide(questions: List[Dict[str, Any]], reasons: List[str]) -> str:
        outcomes = [q["outcome"] for q in questions]
        if reasons or "inconclusive" in outcomes:
            if "inconclusive" in outcomes:
                reasons.append("query_inconclusive")
            return Decision.INCONCLUSIVE.value
        if "regressed" in outcomes:
            reasons.append("protected_question_regressed")
            return Decision.FAIL.value
        target = next(q for q in questions if q["role"] == "target")
        if target["outcome"] == "already_correct":
            reasons.append("parent_already_answers")
            return Decision.NO_CHANGE.value
        if target["outcome"] == "fixed":
            return Decision.PASS.value
        reasons.append("candidate_does_not_match_expected")
        return Decision.FAIL.value


class _Cancelled(Exception):
    """Raised inside the worker when the run's cancel flag is set."""
