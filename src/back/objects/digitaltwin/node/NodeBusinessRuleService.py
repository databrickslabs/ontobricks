"""Declare and trigger entity business rules (SWRL) for an ontology class.

A business rule is an existing SWRL rule of the domain, referenced by name
from ``cls["business_rules"]``. Triggering it on one entity runs the rule's
inference restricted to that entity (the class-typed variables of the rule
are bound to the entity URI) and writes the inferred triples to the graph.

Kept out of :mod:`NodeContextService` so the declare/trigger pair reads as one
unit; that module only adds the policy check and class matching in front.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb import get_graphdb
from back.core.helpers import run_blocking
from back.core.logging import get_logger
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess
from back.objects.digitaltwin.models import DomainSnapshot
from back.objects.digitaltwin.build.TwinInferredMaterialize import TwinInferredMaterialize
from back.objects.ontology.Rules import Rules

logger = get_logger(__name__)

# Name of this element in the per-domain MCP context policy.
BUSINESS_RULES_FEATURE = "business_rules"

# Cap on the triples echoed back to the caller (all of them are written).
MAX_RETURNED_TRIPLES = 200


class NodeBusinessRuleService:
    """Business rule declaration reading and entity-scoped execution."""

    @staticmethod
    def domain_swrl_rules(domain: Any) -> List[Dict[str, Any]]:
        rules = getattr(domain, "swrl_rules", None)
        if rules is None:
            ontology = getattr(domain, "ontology", None) or {}
            rules = ontology.get("swrl_rules", []) if isinstance(ontology, dict) else []
        return [r for r in rules or [] if isinstance(r, dict)]

    @staticmethod
    def class_entries(
        cls: Dict[str, Any], swrl_rules: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Return the class's business rules that exist and are enabled.

        References to a rule that was deleted, renamed outside the editor or
        disabled are dropped with a warning instead of raising, so one stale
        entry never breaks the whole node context.
        """
        by_name = {r.get("name"): r for r in swrl_rules if r.get("name")}
        entries: List[Dict[str, Any]] = []
        seen = set()
        for ref in cls.get("business_rules") or []:
            name = str((ref or {}).get("name") or "").strip() if isinstance(ref, dict) else ""
            if not name or name in seen:
                continue
            seen.add(name)
            rule = by_name.get(name)
            if rule is None:
                logger.warning(
                    "Skipping business rule %r on class %s: rule not found",
                    name,
                    cls.get("name", ""),
                )
                continue
            if rule.get("enabled", True) is False:
                continue
            entries.append(
                {
                    "name": name,
                    "description": str(rule.get("description") or "").strip() or None,
                    "antecedent": rule.get("antecedent", ""),
                    "consequent": rule.get("consequent", ""),
                }
            )
        return entries

    @staticmethod
    async def execute(
        domain: Any,
        settings: Any,
        *,
        entity_uri: str,
        matched_cls: Dict[str, Any],
        rule_name: str,
        simulation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run *rule_name* on *entity_uri* and materialise the inferred triples.

        When *simulation_id* is set, the rule runs against that ready sandbox
        and inferred triples are written there, not to the live graph.

        Raises:
            ValidationError: rule not declared on the class, or the rule has no
                variable typed by the entity's class (or one of its ancestors).
            InfrastructureError: no writable graph store, or the rule failed.
        """
        class_name, requested, focus_vars = NodeBusinessRuleService._focus_rule(
            domain, matched_cls, rule_name
        )
        local_id = DigitalTwin.extract_local_id(entity_uri)
        snap = DomainSnapshot(domain)
        sid = TwinGraphAccess.sandbox_id(simulation_id)
        store = get_graphdb(snap, settings, for_write=sid is None)
        if store is None:
            raise InfrastructureError("Graph store is not available for this domain")

        svc, graph_target, after_write = NodeBusinessRuleService._reasoning_target(
            domain, settings, snap, store, entity_uri, sid
        )
        logger.info(
            "nodes/business-rule: running %s on entity=%s class=%s focus=%s target=%s",
            requested,
            local_id,
            class_name,
            focus_vars,
            graph_target,
        )
        result = await run_blocking(
            svc.run_swrl_rules,
            rule_names={requested},
            focus={"vars": focus_vars, "uri": entity_uri},
        )
        if (result.stats or {}).get("errors"):
            raise InfrastructureError(f"Business rule {requested!r} failed to run")
        triples, written = await NodeBusinessRuleService._materialize_inferred(
            svc, requested, entity_uri, result.inferred_triples
        )
        if after_write is not None:
            after_write()

        logger.info(
            "nodes/business-rule: entity=%s class=%s rule=%s inferred=%d written=%d target=%s",
            local_id,
            class_name,
            requested,
            len(triples),
            written,
            graph_target,
        )
        return {
            "success": True,
            "entity_uri": entity_uri,
            "entity_local_id": local_id,
            "class_name": class_name,
            "rule": requested,
            "graph_target": graph_target,
            "inferred_count": len(triples),
            "materialized_count": written,
            "triples": triples[:MAX_RETURNED_TRIPLES],
            "truncated": len(triples) > MAX_RETURNED_TRIPLES,
        }

    @staticmethod
    def _focus_rule(domain: Any, matched_cls: Dict[str, Any], rule_name: str):
        """Return ``(class_name, rule_name, focus_vars)`` or raise."""
        class_name = matched_cls.get("name", "")
        requested = (rule_name or "").strip()
        swrl_rules = NodeBusinessRuleService.domain_swrl_rules(domain)
        declared = {
            e["name"]
            for e in NodeBusinessRuleService.class_entries(matched_cls, swrl_rules)
        }
        if requested not in declared:
            logger.warning(
                "nodes/business-rule: %r is not declared on class %s — rejected",
                requested,
                class_name,
            )
            raise ValidationError(
                f"Business rule {requested!r} is not configured on class {class_name!r}"
            )
        rule = next(r for r in swrl_rules if r.get("name") == requested)
        focus_vars = Rules.swrl_focus_variables(
            rule, Rules.class_lineage(domain.get_classes() or [], class_name)
        )
        if not focus_vars:
            raise ValidationError(
                f"Business rule {requested!r} has no variable typed by {class_name!r}"
            )
        return class_name, requested, focus_vars

    @staticmethod
    async def _materialize_inferred(
        svc: Any, requested: str, entity_uri: str, inferred_triples: Any
    ):
        """Write inferred URI triples; return ``(triples, written_count)``."""
        from back.core.reasoning import InferredTriple, ReasoningResult

        triples = TwinInferredMaterialize.uri_triples(
            [
                {"subject": t.subject, "predicate": t.predicate, "object": t.object}
                for t in inferred_triples
            ]
        )
        if not triples:
            return triples, 0
        inferred = [
            InferredTriple(
                subject=t["subject"],
                predicate=t["predicate"],
                object=t["object"],
                provenance=f"swrl:{requested}",
                rule_name=requested,
            )
            for t in triples
        ]
        try:
            written = await run_blocking(
                svc.materialize_inferred, ReasoningResult(inferred_triples=inferred)
            )
        except Exception as exc:
            logger.warning(
                "nodes/business-rule: materialising %s failed for %s: %s",
                requested,
                entity_uri,
                exc,
            )
            raise InfrastructureError(
                "Writing inferred triples to the graph failed", detail=str(exc)
            ) from exc
        return triples, written

    @staticmethod
    def _reasoning_target(
        domain: Any,
        settings: Any,
        snap: Any,
        store: Any,
        entity_uri: str,
        simulation_id: Optional[str],
    ):
        """Return ``(service, graph_target, after_write)`` for live or sandbox SWRL."""
        from back.core.reasoning import ReasoningService

        if not simulation_id:
            return ReasoningService(snap, store), "live", None

        from back.core.helpers import effective_graph_name, effective_graph_query_table
        from back.objects.simulation import (
            Simulation,
            Reasoning,
            Sandbox,
        )

        sim = Simulation(domain, settings)
        sim.sandbox_relation(simulation_id)
        sandbox = Sandbox(store, effective_graph_name(domain), simulation_id)
        seed = getattr(sandbox, "seed_focus_for_swrl", None)
        if callable(seed):
            seed(entity_uri)
        svc = Reasoning(snap, sandbox.store, sandbox.table)

        def _refresh_views() -> None:
            ensure = getattr(sandbox, "ensure_explorer_views", None)
            if not callable(ensure):
                return
            live_union = effective_graph_query_table(
                domain, settings, include_inferred=True, store=store
            )
            ensure(live_union)

        return svc, "simulation", _refresh_views
