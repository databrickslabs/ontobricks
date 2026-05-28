"""
OntoBricks Mapping-PGE Orchestrator.

Sprint 7 of the Planner-Generator-Evaluator (PGE) redesign — wires the
Planner, the Entity/Relationship Generators, and the two-stage Evaluator
(deterministic + semantic critic) into a single ``run_agent`` entry point.

The public signature mirrors ``agents.agent_auto_assignment.engine.run_agent``
exactly so Sprint 8 can swap implementations with a one-line import change in
``back/objects/mapping/Mapping.py``.

Control flow per item (entity or relationship)
==============================================

1. Build a focused slice from the Planner's :class:`SourceModel`.
2. Run the appropriate Generator with ``retry_hint=None``.
3. Run the deterministic evaluator. On FAIL:
   * if ``bubble_to_planner=True`` -> escalate to Planner (capped at 2 global
     replans across the whole run);
   * else retry the Generator with the first failure's hint.
4. On stage-1 PASS, run the semantic critic (unless ``skip_semantic_critic``
   is set).  Same bubble / hint logic on FAIL.
5. After 3 unsuccessful attempts, the item is recorded as ``FAIL_BUDGET`` and
   the orchestrator moves on to the next item.

Step-log design
===============

``AgentResult.steps`` is a HIGH-LEVEL log — one entry per stage transition
(planner-start, generator-start, evaluator-result, critic-result, item-done).
The detailed per-tool steps emitted by each sub-agent stay on the sub-agent's
own result dataclass (``PlannerResult.steps``, ``EntityGenResult.steps``, …)
and are NOT merged into the orchestrator's ``steps`` field. This keeps the
top-level log readable in the UI; the persistence layer can attach sub-agent
step lists separately when needed.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from agents.agent_mapping_pge.contracts import EvalReport, SourceModel
from agents.agent_mapping_pge.evaluator.critic import run_critic
from agents.agent_mapping_pge.evaluator.deterministic import (
    evaluate_entity_mapping,
    evaluate_relationship_mapping,
)
from agents.agent_mapping_pge.generators.entity import run_entity_generator
from agents.agent_mapping_pge.generators.relationship import (
    run_relationship_generator,
)
from agents.agent_mapping_pge.planner import run_planner
from agents.tracing import trace_agent

logger = get_logger(__name__)

# Per-item retry budget for the Generator->Evaluator inner loop.
_PER_ITEM_GENERATOR_ATTEMPTS = 3
# Global cap on Planner re-invocations triggered by escalated failures.
_PLANNER_REINVOCATION_BUDGET = 2


# =====================================================
# Public dataclasses — mirror agent_auto_assignment shapes
# =====================================================


@dataclass
class AgentStep:
    """One observable step of the orchestrator's execution.

    Same shape as :class:`agents.engine_base.AgentStep` plus a few extra
    ``step_type`` values used by the PGE orchestrator:

    * ``"planner"`` / ``"generator"`` / ``"evaluator"`` / ``"critic"`` for
      stage transitions; the legacy ``"tool_call"`` / ``"tool_result"`` /
      ``"output"`` types remain valid so this struct is fully drop-in-
      compatible with the prior orchestrator.
    """

    step_type: str
    content: str
    tool_name: str = ""
    duration_ms: int = 0


@dataclass
class AgentResult:
    """Outcome of a full PGE orchestration run.

    The first eight fields mirror
    :class:`agents.agent_auto_assignment.engine.AgentResult` exactly so
    callers can swap engines without touching their downstream code. The
    last three are PGE-specific extras the caller (Sprint 8) can choose to
    persist.
    """

    success: bool
    entity_mappings: list = field(default_factory=list)
    relationship_mappings: list = field(default_factory=list)
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)
    stats: Dict[str, int] = field(default_factory=dict)
    # PGE-specific extras
    source_model: Optional[dict] = None
    mapping_evaluations: Dict[str, dict] = field(default_factory=dict)
    mapping_run_log: List[dict] = field(default_factory=list)


# =====================================================
# Internal helpers
# =====================================================


def _ontology_index(ontology: dict) -> Dict[str, dict]:
    """Build ``uri -> entity dict`` for fast lookup by URI."""
    out: Dict[str, dict] = {}
    for e in (ontology or {}).get("entities", []) or []:
        uri = e.get("uri") or e.get("name")
        if uri:
            out[uri] = e
    return out


def _relationship_index(ontology: dict) -> Dict[str, dict]:
    """Build ``uri -> relationship dict`` for fast lookup by URI."""
    out: Dict[str, dict] = {}
    for r in (ontology or {}).get("relationships", []) or []:
        uri = r.get("uri") or r.get("name")
        if uri:
            out[uri] = r
    return out


def _slice_for_entity(source_model: SourceModel, class_uri: str) -> dict:
    """Render the SourceModel slice consumed by the EntityGenerator.

    The slice surfaces only what's relevant to one ontology class:
    candidate tables, the canonical-ID per chosen table, and any joins
    naming a candidate table on at least one side.
    """
    candidate_tables: List[dict] = []
    candidate_table_names: set = set()
    for role in source_model.table_roles:
        for cand in role.ontology_class_candidates:
            if cand.uri == class_uri:
                candidate_tables.append(
                    {
                        "table": role.table,
                        "confidence": cand.confidence,
                        "reason": cand.reason,
                    }
                )
                candidate_table_names.add(role.table)
                break  # one entry per role is enough

    canonical_id_obj: Dict[str, Any] = {
        "ontology_class": class_uri,
        "canonical_column_per_table": {},
        "format_note": "",
    }
    for c in source_model.canonical_ids:
        if c.ontology_class == class_uri:
            canonical_id_obj = c.to_dict()
            break

    relevant_joins: List[dict] = []
    for j in source_model.join_keys:
        from_table = j.from_ref.split(".")[0] if j.from_ref else ""
        to_table = j.to_ref.split(".")[0] if j.to_ref else ""
        if any(
            ft == from_table or ft.endswith("." + from_table)
            for ft in candidate_table_names
        ) or any(
            tt == to_table or tt.endswith("." + to_table)
            for tt in candidate_table_names
        ):
            relevant_joins.append(j.to_dict())

    return {
        "candidate_tables": candidate_tables,
        "canonical_id": canonical_id_obj,
        "relevant_joins": relevant_joins,
    }


def _slice_for_relationship(
    source_model: SourceModel,
    property_uri: str,
    source_entity_mapping: dict,
    target_entity_mapping: dict,
) -> dict:
    """Render the SourceModel slice consumed by the RelationshipGenerator.

    The slice surfaces every join key the Planner produced (the Generator
    picks among them), plus the candidate-table list filtered to the
    source/target classes when those classes are known.
    """
    src_class = (source_entity_mapping or {}).get("ontology_class") or (
        source_entity_mapping or {}
    ).get("class_uri", "")
    tgt_class = (target_entity_mapping or {}).get("ontology_class") or (
        target_entity_mapping or {}
    ).get("class_uri", "")
    endpoint_classes = {c for c in (src_class, tgt_class) if c}

    candidate_tables: List[dict] = []
    for role in source_model.table_roles:
        for cand in role.ontology_class_candidates:
            if not endpoint_classes or cand.uri in endpoint_classes:
                candidate_tables.append(
                    {
                        "table": role.table,
                        "ontology_class": cand.uri,
                        "confidence": cand.confidence,
                        "reason": cand.reason,
                    }
                )
                break

    relevant_joins = [j.to_dict() for j in source_model.join_keys]

    return {
        "property_uri": property_uri,
        "relevant_joins": relevant_joins,
        "candidate_tables": candidate_tables,
    }


def _wrap_execute_sql(client: Any) -> Callable[[str], dict]:
    """Adapt ``client.execute_query`` to the evaluator's expected shape.

    The deterministic evaluator wants ``{"columns": [...], "rows": [{...}]}``
    with FULL rows. ``client.execute_query`` returns ``List[Dict[str, Any]]``
    — we promote that to the evaluator's shape and derive columns from the
    first row. Calling the underlying client directly (rather than the
    sampling ``tool_execute_sql``) is load-bearing: the deterministic
    evaluator's count-based checks need real values, not stringified ones.
    """

    def _run(sql: str) -> dict:
        rows = client.execute_query(sql) or []
        if isinstance(rows, dict) and "rows" in rows:
            return rows  # client already returns the evaluator's shape
        columns: List[str] = []
        if rows and isinstance(rows[0], dict):
            columns = list(rows[0].keys())
        return {"columns": columns, "rows": list(rows)}

    return _run


def _first_hint(report: EvalReport) -> Optional[str]:
    """Return the first failure's hint (or ``None`` when the report has none)."""
    for f in report.failures:
        if f.hint:
            return f.hint
    return None


def _resolve_endpoint_em(
    ref: str,
    by_uri: Dict[str, dict],
    entity_index: Dict[str, dict],
) -> Optional[dict]:
    """Best-effort lookup of an endpoint entity mapping.

    The ontology's ``domain`` / ``range`` may use either the entity's full
    URI or its short name. We try direct lookup, then a name-match scan.
    """
    if not ref:
        return None
    if ref in by_uri:
        return by_uri[ref]
    for uri, ent in entity_index.items():
        if ent.get("name") == ref or ent.get("label") == ref:
            if uri in by_uri:
                return by_uri[uri]
    return None


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="mapping_pge_engine")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    client: Any,
    metadata: dict,
    ontology: dict,
    entity_mappings: Optional[list] = None,
    relationship_mappings: Optional[list] = None,
    documents: Optional[list] = None,
    on_step: Optional[Callable[[str, int], None]] = None,
    max_iterations: Optional[int] = None,
    *,
    skip_semantic_critic: bool = False,
) -> AgentResult:
    """Run the PGE mapping orchestrator.

    Drop-in replacement for :func:`agents.agent_auto_assignment.engine.run_agent`.

    Args:
        host: Databricks workspace URL.
        token: Bearer token for the serving endpoint.
        endpoint_name: Foundation Model serving endpoint name.
        client: Databricks SQL client exposing ``execute_query(sql)``.
        metadata: Imported table metadata to hand to the Planner.
        ontology: Ontology dict with ``entities`` and ``relationships``.
        entity_mappings: Pre-seeded entity mappings (URI matched -> skipped).
        relationship_mappings: Pre-seeded relationship mappings (likewise).
        documents: Optional pre-loaded domain documents.
        on_step: Optional progress callback ``(msg, pct)``.
        max_iterations: Per-item override for the Generator's iteration cap.
            Kept for API parity with the legacy engine; ``None`` uses each
            sub-agent's default.
        skip_semantic_critic: When ``True``, the orchestrator skips the
            stage-2 critic and accepts every stage-1 PASS as a final PASS.
            Production callers leave this ``False``; tests flip it ``True``
            to avoid LLM calls in the orchestrator's unit tests.

    Returns:
        An :class:`AgentResult` with the submitted mappings, a high-level
        ``steps`` log, per-item ``mapping_run_log``, and PGE-specific
        extras (``source_model``, ``mapping_evaluations``).
    """
    # ------------------------------------------------------------------
    # Per-call state lives entirely on this RunState object — no module-
    # level mutables, so concurrent calls (and tests) cannot collide.
    # ------------------------------------------------------------------
    state = _RunState(
        host=host,
        token=token,
        endpoint_name=endpoint_name,
        client=client,
        metadata=metadata or {},
        ontology=ontology or {},
        documents=list(documents or []),
        on_step=on_step,
        max_iterations=max_iterations,
        skip_semantic_critic=skip_semantic_critic,
    )

    # Pre-seeded mappings carry over verbatim — we never overwrite a URI the
    # caller already mapped.
    pre_entity_list = list(entity_mappings or [])
    pre_rel_list = list(relationship_mappings or [])
    preseeded_entity_uris = {
        m.get("ontology_class") or m.get("class_uri") or "" for m in pre_entity_list
    }
    preseeded_entity_uris.discard("")
    preseeded_rel_uris = {
        m.get("property") or m.get("property_uri") or "" for m in pre_rel_list
    }
    preseeded_rel_uris.discard("")

    state.entity_mappings.extend(pre_entity_list)
    state.relationship_mappings.extend(pre_rel_list)
    for m in pre_entity_list:
        uri = m.get("ontology_class") or m.get("class_uri")
        if uri:
            state.entity_mapping_by_uri[uri] = m

    entities_in_scope = state.ontology.get("entities", []) or []
    relationships_in_scope = state.ontology.get("relationships", []) or []

    logger.info(
        "===== MAPPING-PGE ENGINE START ===== endpoint=%s, entities=%d, "
        "relationships=%d, preseeded_entities=%d, preseeded_rels=%d, "
        "skip_critic=%s",
        endpoint_name,
        len(entities_in_scope),
        len(relationships_in_scope),
        len(preseeded_entity_uris),
        len(preseeded_rel_uris),
        skip_semantic_critic,
    )

    # ------------------------------------------------------------------
    # 1. Planner
    # ------------------------------------------------------------------
    state.notify("Planning…", pct=2)
    state.add_step("planner", "planner-start")

    t0 = time.time()
    try:
        planner_result = run_planner(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            client=client,
            metadata=state.metadata,
            ontology=state.ontology,
            documents=state.documents,
            on_step=None,
        )
    except Exception as exc:  # noqa: BLE001 — surface any failure as run failure
        logger.error("Planner raised an exception: %s", exc, exc_info=True)
        return state.finalise(error=f"planner exception: {exc}")

    planner_ms = int((time.time() - t0) * 1000)
    state.add_iterations(planner_result.iterations)
    state.accumulate_usage(planner_result.usage)

    if not planner_result.success or planner_result.source_model is None:
        state.add_step(
            "planner",
            f"planner-fail: {planner_result.error}",
            duration_ms=planner_ms,
        )
        logger.error("===== MAPPING-PGE ENGINE FAILED ===== planner failed")
        state.notify("Planner failed — aborting.", pct=10)
        return state.finalise(
            error=f"planner failed: {planner_result.error or 'no source model'}"
        )

    state.source_model = planner_result.source_model
    state.refresh_plan()
    state.add_step(
        "planner",
        f"planner-done: entities={len(state.entity_order)}, "
        f"relationships={len(state.relationship_order)}",
        duration_ms=planner_ms,
    )

    # ------------------------------------------------------------------
    # 2. Walk the plan — entities first, then relationships.
    # ------------------------------------------------------------------
    state.entity_index = _ontology_index(state.ontology)
    state.rel_index = _relationship_index(state.ontology)
    state.execute_sql_fn = _wrap_execute_sql(client)
    state.total_items_planned = len(state.entity_order) + len(
        state.relationship_order
    )

    # ------------------ Entity walk ------------------
    for entity_uri in list(state.entity_order):
        ontology_class = state.entity_index.get(entity_uri, {"uri": entity_uri})
        label = ontology_class.get("label") or ontology_class.get(
            "name", entity_uri
        )

        if entity_uri in preseeded_entity_uris:
            state.mapping_run_log.append(
                {
                    "item": entity_uri,
                    "kind": "entity",
                    "attempts": [],
                    "final_status": "PRESEEDED",
                }
            )
            state.notify(f"Skipping pre-seeded {label}")
            state.items_done += 1
            continue

        if entity_uri in state.skip_reasons:
            state.mapping_run_log.append(
                {
                    "item": entity_uri,
                    "kind": "entity",
                    "attempts": [],
                    "final_status": "SKIPPED",
                }
            )
            state.notify(
                f"Skipped {label}: {state.skip_reasons[entity_uri]}"
            )
            state.items_done += 1
            continue

        final_status, attempts_log, last_mapping, last_report = _run_entity_item(
            state, ontology_class
        )

        state.mapping_run_log.append(
            {
                "item": entity_uri,
                "kind": "entity",
                "attempts": attempts_log,
                "final_status": final_status,
            }
        )
        if final_status == "PASS" and last_mapping is not None:
            state.entity_mappings.append(last_mapping)
            state.entity_mapping_by_uri[entity_uri] = last_mapping
            state.submitted_any = True
            if last_report is not None:
                state.mapping_evaluations[entity_uri] = last_report.to_dict()
            state.notify(f"Mapped {label}")
        state.items_done += 1

    # ------------------ Relationship walk ------------------
    for property_uri in list(state.relationship_order):
        prop = state.rel_index.get(property_uri, {"uri": property_uri})
        label = prop.get("label") or prop.get("name", property_uri)

        if property_uri in preseeded_rel_uris:
            state.mapping_run_log.append(
                {
                    "item": property_uri,
                    "kind": "relationship",
                    "attempts": [],
                    "final_status": "PRESEEDED",
                }
            )
            state.notify(f"Skipping pre-seeded {label}")
            state.items_done += 1
            continue

        if property_uri in state.skip_reasons:
            state.mapping_run_log.append(
                {
                    "item": property_uri,
                    "kind": "relationship",
                    "attempts": [],
                    "final_status": "SKIPPED",
                }
            )
            state.notify(f"Skipped {label}: {state.skip_reasons[property_uri]}")
            state.items_done += 1
            continue

        domain_ref = prop.get("domain", "") or ""
        range_ref = prop.get("range", "") or ""
        source_em = state.entity_mapping_by_uri.get(
            domain_ref
        ) or _resolve_endpoint_em(
            domain_ref, state.entity_mapping_by_uri, state.entity_index
        )
        target_em = state.entity_mapping_by_uri.get(
            range_ref
        ) or _resolve_endpoint_em(
            range_ref, state.entity_mapping_by_uri, state.entity_index
        )
        if source_em is None or target_em is None:
            state.mapping_run_log.append(
                {
                    "item": property_uri,
                    "kind": "relationship",
                    "attempts": [],
                    "final_status": "FAIL_BUDGET",
                }
            )
            state.add_step(
                "evaluator",
                f"relationship {property_uri}: endpoint mapping missing — skipped",
            )
            state.notify(f"Cannot map {label}: endpoint entity not available")
            state.items_done += 1
            continue

        final_status, attempts_log, last_mapping, last_report = _run_relationship_item(
            state, prop, source_em, target_em
        )

        state.mapping_run_log.append(
            {
                "item": property_uri,
                "kind": "relationship",
                "attempts": attempts_log,
                "final_status": final_status,
            }
        )
        if final_status == "PASS" and last_mapping is not None:
            state.relationship_mappings.append(last_mapping)
            state.submitted_any = True
            if last_report is not None:
                state.mapping_evaluations[property_uri] = last_report.to_dict()
            state.notify(f"Mapped {label}")
        state.items_done += 1

    state.notify("Agent completed!", pct=100)
    return state.finalise()


# =====================================================
# Run-scoped mutable state
# =====================================================


@dataclass
class _RunState:
    """Encapsulates per-call mutable state — keeps ``run_agent`` re-entrant.

    All counters, mapping lists, and accumulators that need to evolve as the
    walk progresses live here so the orchestrator never relies on module-
    level globals.  This also keeps the per-item helpers (``_run_*_item``)
    pure functions of state + item input.
    """

    host: str
    token: str
    endpoint_name: str
    client: Any
    metadata: dict
    ontology: dict
    documents: List[Any]
    on_step: Optional[Callable[[str, int], None]]
    max_iterations: Optional[int]
    skip_semantic_critic: bool

    # Output accumulators
    entity_mappings: List[dict] = field(default_factory=list)
    relationship_mappings: List[dict] = field(default_factory=list)
    entity_mapping_by_uri: Dict[str, dict] = field(default_factory=dict)
    mapping_run_log: List[dict] = field(default_factory=list)
    mapping_evaluations: Dict[str, dict] = field(default_factory=dict)
    steps: List[AgentStep] = field(default_factory=list)
    usage: Dict[str, int] = field(
        default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0}
    )
    iterations: int = 0
    submitted_any: bool = False

    # Plan-derived state — refreshed on (re)plan.
    source_model: Optional[SourceModel] = None
    entity_order: List[str] = field(default_factory=list)
    relationship_order: List[str] = field(default_factory=list)
    skip_reasons: Dict[str, str] = field(default_factory=dict)
    planner_reinvocations: int = 0

    # Walk progress
    items_done: int = 0
    total_items_planned: int = 0

    # Per-run caches & lookups
    id_universe_cache: Dict[str, set] = field(default_factory=dict)
    entity_index: Dict[str, dict] = field(default_factory=dict)
    rel_index: Dict[str, dict] = field(default_factory=dict)
    execute_sql_fn: Optional[Callable[[str], dict]] = None

    # -- helpers ----------------------------------------------------------

    def add_step(
        self,
        step_type: str,
        content: str,
        *,
        tool_name: str = "",
        duration_ms: int = 0,
    ) -> None:
        self.steps.append(
            AgentStep(
                step_type=step_type,
                content=content,
                tool_name=tool_name,
                duration_ms=duration_ms,
            )
        )

    def pct(self) -> int:
        total = max(self.total_items_planned, 1)
        return min(5 + int((self.items_done / total) * 90), 95)

    def notify(self, msg: str, *, pct: Optional[int] = None) -> None:
        actual_pct = pct if pct is not None else self.pct()
        logger.info("PGE STEP [%d%%] %s", actual_pct, msg)
        if self.on_step:
            self.on_step(msg, actual_pct)

    def add_iterations(self, n: int) -> None:
        self.iterations += int(n or 0)

    def accumulate_usage(self, src: Dict[str, int]) -> None:
        for k in ("prompt_tokens", "completion_tokens"):
            self.usage[k] = self.usage.get(k, 0) + int((src or {}).get(k, 0))

    def refresh_plan(self) -> None:
        sm = self.source_model
        if sm is None:
            return
        self.entity_order = list(sm.mapping_plan.entity_order)
        self.relationship_order = list(sm.mapping_plan.relationship_order)
        self.skip_reasons = {s.item: s.reason for s in sm.mapping_plan.skip}

    def replan_once(self) -> bool:
        """Re-invoke the Planner once (subject to the global budget).

        Returns ``True`` on success (and updates the plan in place), ``False``
        when the budget is exhausted or the new Planner run failed.
        """
        if self.planner_reinvocations >= _PLANNER_REINVOCATION_BUDGET:
            return False
        self.planner_reinvocations += 1
        self.notify("Re-planning (escalated)…", pct=self.pct())
        self.add_step(
            "planner",
            f"replan-start (reinvocation #{self.planner_reinvocations})",
        )
        t_rp = time.time()
        try:
            new_result = run_planner(
                host=self.host,
                token=self.token,
                endpoint_name=self.endpoint_name,
                client=self.client,
                metadata=self.metadata,
                ontology=self.ontology,
                documents=self.documents,
                on_step=None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Replan raised an exception: %s", exc, exc_info=True)
            self.add_step("planner", f"replan-exception: {exc}")
            return False
        replan_ms = int((time.time() - t_rp) * 1000)
        self.add_iterations(new_result.iterations)
        self.accumulate_usage(new_result.usage)
        if not new_result.success or new_result.source_model is None:
            self.add_step(
                "planner",
                f"replan-fail: {new_result.error}",
                duration_ms=replan_ms,
            )
            return False
        self.source_model = new_result.source_model
        self.refresh_plan()
        self.add_step("planner", "replan-done", duration_ms=replan_ms)
        return True

    def finalise(self, *, error: str = "") -> AgentResult:
        """Build the final :class:`AgentResult`."""
        result = AgentResult(success=False)
        result.entity_mappings = list(self.entity_mappings)
        result.relationship_mappings = list(self.relationship_mappings)
        result.steps = list(self.steps)
        result.iterations = self.iterations
        result.usage = dict(self.usage)
        result.mapping_run_log = list(self.mapping_run_log)
        result.mapping_evaluations = dict(self.mapping_evaluations)
        result.source_model = (
            self.source_model.to_dict() if self.source_model is not None else None
        )
        result.stats = {
            "total": len(self.entity_order) + len(self.relationship_order),
            "entities": len(self.entity_mappings),
            "relationships": len(self.relationship_mappings),
            "planner_reinvocations": self.planner_reinvocations,
        }
        if error:
            result.error = error
            result.success = False
            return result

        # Success when at least one mapping was submitted, OR when there was
        # nothing to map (legitimate empty run).
        nothing_to_map = (
            not self.entity_order and not self.relationship_order
        )
        result.success = self.submitted_any or nothing_to_map
        if not result.success:
            result.error = (
                "no mappings submitted (all items failed or were skipped)"
            )
        logger.info(
            "===== MAPPING-PGE ENGINE COMPLETE ===== success=%s, entities=%d, "
            "relationships=%d, iterations=%d, replans=%d",
            result.success,
            len(self.entity_mappings),
            len(self.relationship_mappings),
            self.iterations,
            self.planner_reinvocations,
        )
        return result


# =====================================================
# Per-item walk helpers
# =====================================================


def _run_entity_item(
    state: _RunState,
    ontology_class: dict,
) -> Tuple[str, List[dict], Optional[dict], Optional[EvalReport]]:
    """Run the G->E loop for one entity.

    Returns ``(final_status, attempts_log, last_mapping, last_report)``.
    The outer ``while True`` lets a successful replan restart the inner
    retry budget fresh, which is the intent of the bubble-to-planner path.
    """
    class_uri = ontology_class.get("uri", "")
    class_label = ontology_class.get("label") or ontology_class.get(
        "name", class_uri
    )
    attempts_log: List[dict] = []
    last_mapping: Optional[dict] = None
    last_report: Optional[EvalReport] = None

    while True:
        retry_hint: Optional[str] = None
        bubble_requested = False
        for attempt_idx in range(_PER_ITEM_GENERATOR_ATTEMPTS):
            attempt_num = attempt_idx + 1
            slice_dict = _slice_for_entity(state.source_model, class_uri)
            state.notify(
                f"Mapping {class_label} (attempt {attempt_num}/{_PER_ITEM_GENERATOR_ATTEMPTS})…",
                pct=state.pct(),
            )
            state.add_step(
                "generator",
                f"entity-gen-start: {class_uri} attempt {attempt_num}",
            )
            t_g = time.time()
            try:
                gen_result = run_entity_generator(
                    host=state.host,
                    token=state.token,
                    endpoint_name=state.endpoint_name,
                    client=state.client,
                    ontology_class=ontology_class,
                    source_model_slice=slice_dict,
                    retry_hint=retry_hint,
                    on_step=None,
                    **(
                        {"max_iterations": state.max_iterations}
                        if state.max_iterations is not None
                        else {}
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "EntityGenerator raised on %s attempt %d: %s",
                    class_uri,
                    attempt_num,
                    exc,
                    exc_info=True,
                )
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": int((time.time() - t_g) * 1000),
                        "stage1_status": "skipped",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": f"generator exception: {exc}",
                    }
                )
                continue
            gen_ms = int((time.time() - t_g) * 1000)
            state.add_iterations(gen_result.iterations)
            state.accumulate_usage(gen_result.usage)

            if not gen_result.success or gen_result.mapping is None:
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "skipped",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": gen_result.error or "generator failed",
                    }
                )
                state.add_step(
                    "generator",
                    f"entity-gen-fail: {class_uri} attempt {attempt_num}: "
                    f"{gen_result.error}",
                    duration_ms=gen_ms,
                )
                retry_hint = gen_result.error or retry_hint
                continue

            mapping = gen_result.mapping
            last_mapping = mapping

            state.notify(f"Evaluating {class_label}…", pct=state.pct())
            t_e = time.time()
            stage1_report = evaluate_entity_mapping(
                mapping=mapping,
                ontology_class=ontology_class,
                execute_sql_fn=state.execute_sql_fn,
            )
            eval_ms = int((time.time() - t_e) * 1000)
            last_report = stage1_report
            state.add_step(
                "evaluator",
                f"entity-stage1: {class_uri} status={stage1_report.status} "
                f"bubble={stage1_report.bubble_to_planner}",
                duration_ms=eval_ms,
            )

            if stage1_report.status == "FAIL":
                hint = _first_hint(stage1_report)
                bubble = bool(stage1_report.bubble_to_planner)
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "FAIL",
                        "critic_status": "skipped",
                        "bubble": bubble,
                        "hint": hint,
                    }
                )
                if bubble:
                    bubble_requested = True
                    break
                retry_hint = hint or retry_hint
                continue

            # Stage 1 PASS — optionally run the critic.
            if state.skip_semantic_critic:
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                    }
                )
                return "PASS", attempts_log, mapping, stage1_report

            state.notify(f"Critiquing {class_label}…", pct=state.pct())
            t_c = time.time()
            try:
                critic_result = run_critic(
                    host=state.host,
                    token=state.token,
                    endpoint_name=state.endpoint_name,
                    client=state.client,
                    item_kind="entity",
                    item_uri=class_uri,
                    item_definition=ontology_class,
                    submitted_mapping=mapping,
                    source_model_slice=slice_dict,
                    stage1_metrics=dict(stage1_report.metrics),
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Critic raised on %s attempt %d: %s",
                    class_uri,
                    attempt_num,
                    exc,
                    exc_info=True,
                )
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": f"critic exception: {exc}",
                    }
                )
                return "PASS", attempts_log, mapping, stage1_report
            critic_ms = int((time.time() - t_c) * 1000)
            state.add_iterations(critic_result.iterations)
            state.accumulate_usage(critic_result.usage)

            critic_report = critic_result.report
            state.add_step(
                "critic",
                f"entity-critic: {class_uri} status="
                f"{critic_report.status if critic_report else '?'} "
                f"bubble="
                f"{critic_report.bubble_to_planner if critic_report else '?'}",
                duration_ms=critic_ms,
            )

            if not critic_result.success or critic_report is None:
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": critic_result.error or "critic failed",
                    }
                )
                return "PASS", attempts_log, mapping, stage1_report

            if critic_report.status == "PASS":
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "PASS",
                        "bubble": False,
                        "hint": None,
                    }
                )
                return "PASS", attempts_log, mapping, critic_report

            hint = _first_hint(critic_report)
            bubble = bool(critic_report.bubble_to_planner)
            attempts_log.append(
                {
                    "attempt": attempt_num,
                    "generator_ms": gen_ms,
                    "stage1_status": "PASS",
                    "critic_status": "FAIL",
                    "bubble": bubble,
                    "hint": hint,
                }
            )
            last_report = critic_report
            if bubble:
                bubble_requested = True
                break
            retry_hint = hint or retry_hint
            continue

        if bubble_requested:
            if state.replan_once():
                continue  # restart the item with the new plan
            return "FAIL_BUBBLE", attempts_log, last_mapping, last_report
        return "FAIL_BUDGET", attempts_log, last_mapping, last_report


def _run_relationship_item(
    state: _RunState,
    ontology_property: dict,
    source_em: dict,
    target_em: dict,
) -> Tuple[str, List[dict], Optional[dict], Optional[EvalReport]]:
    """Run the G->E loop for one relationship.

    Returns ``(final_status, attempts_log, last_mapping, last_report)``.
    """
    property_uri = ontology_property.get("uri", "")
    property_label = ontology_property.get("label") or ontology_property.get(
        "name", property_uri
    )
    attempts_log: List[dict] = []
    last_mapping: Optional[dict] = None
    last_report: Optional[EvalReport] = None

    while True:
        retry_hint: Optional[str] = None
        bubble_requested = False
        for attempt_idx in range(_PER_ITEM_GENERATOR_ATTEMPTS):
            attempt_num = attempt_idx + 1
            slice_dict = _slice_for_relationship(
                state.source_model,
                property_uri,
                source_em,
                target_em,
            )
            state.notify(
                f"Mapping {property_label} (attempt {attempt_num}/"
                f"{_PER_ITEM_GENERATOR_ATTEMPTS})…",
                pct=state.pct(),
            )
            state.add_step(
                "generator",
                f"rel-gen-start: {property_uri} attempt {attempt_num}",
            )
            t_g = time.time()
            try:
                gen_result = run_relationship_generator(
                    host=state.host,
                    token=state.token,
                    endpoint_name=state.endpoint_name,
                    client=state.client,
                    ontology_property=ontology_property,
                    source_entity_mapping=source_em,
                    target_entity_mapping=target_em,
                    source_model_slice=slice_dict,
                    retry_hint=retry_hint,
                    on_step=None,
                    **(
                        {"max_iterations": state.max_iterations}
                        if state.max_iterations is not None
                        else {}
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "RelationshipGenerator raised on %s attempt %d: %s",
                    property_uri,
                    attempt_num,
                    exc,
                    exc_info=True,
                )
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": int((time.time() - t_g) * 1000),
                        "stage1_status": "skipped",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": f"generator exception: {exc}",
                    }
                )
                continue
            gen_ms = int((time.time() - t_g) * 1000)
            state.add_iterations(gen_result.iterations)
            state.accumulate_usage(gen_result.usage)

            if not gen_result.success or gen_result.mapping is None:
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "skipped",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": gen_result.error or "generator failed",
                    }
                )
                state.add_step(
                    "generator",
                    f"rel-gen-fail: {property_uri} attempt {attempt_num}: "
                    f"{gen_result.error}",
                    duration_ms=gen_ms,
                )
                retry_hint = gen_result.error or retry_hint
                continue

            mapping = gen_result.mapping
            last_mapping = mapping

            state.notify(f"Evaluating {property_label}…", pct=state.pct())
            t_e = time.time()
            stage1_report = evaluate_relationship_mapping(
                mapping=mapping,
                source_entity_mapping=source_em,
                target_entity_mapping=target_em,
                execute_sql_fn=state.execute_sql_fn,
                id_universe_cache=state.id_universe_cache,
            )
            eval_ms = int((time.time() - t_e) * 1000)
            last_report = stage1_report
            state.add_step(
                "evaluator",
                f"rel-stage1: {property_uri} status={stage1_report.status} "
                f"bubble={stage1_report.bubble_to_planner}",
                duration_ms=eval_ms,
            )

            if stage1_report.status == "FAIL":
                hint = _first_hint(stage1_report)
                bubble = bool(stage1_report.bubble_to_planner)
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "FAIL",
                        "critic_status": "skipped",
                        "bubble": bubble,
                        "hint": hint,
                    }
                )
                if bubble:
                    bubble_requested = True
                    break
                retry_hint = hint or retry_hint
                continue

            if state.skip_semantic_critic:
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                    }
                )
                return "PASS", attempts_log, mapping, stage1_report

            state.notify(f"Critiquing {property_label}…", pct=state.pct())
            t_c = time.time()
            try:
                critic_result = run_critic(
                    host=state.host,
                    token=state.token,
                    endpoint_name=state.endpoint_name,
                    client=state.client,
                    item_kind="relationship",
                    item_uri=property_uri,
                    item_definition=ontology_property,
                    submitted_mapping=mapping,
                    source_model_slice=slice_dict,
                    stage1_metrics=dict(stage1_report.metrics),
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Critic raised on %s attempt %d: %s",
                    property_uri,
                    attempt_num,
                    exc,
                    exc_info=True,
                )
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": f"critic exception: {exc}",
                    }
                )
                return "PASS", attempts_log, mapping, stage1_report
            critic_ms = int((time.time() - t_c) * 1000)
            state.add_iterations(critic_result.iterations)
            state.accumulate_usage(critic_result.usage)

            critic_report = critic_result.report
            state.add_step(
                "critic",
                f"rel-critic: {property_uri} status="
                f"{critic_report.status if critic_report else '?'} "
                f"bubble="
                f"{critic_report.bubble_to_planner if critic_report else '?'}",
                duration_ms=critic_ms,
            )

            if not critic_result.success or critic_report is None:
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "skipped",
                        "bubble": False,
                        "hint": None,
                        "error": critic_result.error or "critic failed",
                    }
                )
                return "PASS", attempts_log, mapping, stage1_report

            if critic_report.status == "PASS":
                attempts_log.append(
                    {
                        "attempt": attempt_num,
                        "generator_ms": gen_ms,
                        "stage1_status": "PASS",
                        "critic_status": "PASS",
                        "bubble": False,
                        "hint": None,
                    }
                )
                return "PASS", attempts_log, mapping, critic_report

            hint = _first_hint(critic_report)
            bubble = bool(critic_report.bubble_to_planner)
            attempts_log.append(
                {
                    "attempt": attempt_num,
                    "generator_ms": gen_ms,
                    "stage1_status": "PASS",
                    "critic_status": "FAIL",
                    "bubble": bubble,
                    "hint": hint,
                }
            )
            last_report = critic_report
            if bubble:
                bubble_requested = True
                break
            retry_hint = hint or retry_hint
            continue

        if bubble_requested:
            if state.replan_once():
                continue
            return "FAIL_BUBBLE", attempts_log, last_mapping, last_report
        return "FAIL_BUDGET", attempts_log, last_mapping, last_report
