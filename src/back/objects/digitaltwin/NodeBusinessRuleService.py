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
from back.objects.digitaltwin.models import DomainSnapshot
from back.objects.digitaltwin.TwinInferredMaterialize import TwinInferredMaterialize
from back.objects.ontology.OntologyRules import OntologyRules

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
    ) -> Dict[str, Any]:
        """Run *rule_name* on *entity_uri* and materialise the inferred triples.

        Raises:
            ValidationError: rule not declared on the class, or the rule has no
                variable typed by the entity's class (or one of its ancestors).
            InfrastructureError: no writable graph store, or the rule failed.
        """
        from back.core.reasoning import InferredTriple, ReasoningResult, ReasoningService

        class_name = matched_cls.get("name", "")
        requested = (rule_name or "").strip()
        swrl_rules = NodeBusinessRuleService.domain_swrl_rules(domain)
        declared = {e["name"] for e in NodeBusinessRuleService.class_entries(matched_cls, swrl_rules)}
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
        lineage = OntologyRules.class_lineage(domain.get_classes() or [], class_name)
        focus_vars = OntologyRules.swrl_focus_variables(rule, lineage)
        if not focus_vars:
            raise ValidationError(
                f"Business rule {requested!r} has no variable typed by {class_name!r}"
            )

        local_id = DigitalTwin.extract_local_id(entity_uri)
        snap = DomainSnapshot(domain)
        store = get_graphdb(snap, settings)
        if store is None:
            raise InfrastructureError("Graph store is not available for this domain")

        svc = ReasoningService(snap, store)
        focus = {"vars": focus_vars, "uri": entity_uri}
        logger.info(
            "nodes/business-rule: running %s on entity=%s class=%s focus=%s",
            requested,
            local_id,
            class_name,
            focus_vars,
        )
        result = await run_blocking(
            svc.run_swrl_rules, rule_names={requested}, focus=focus
        )
        if (result.stats or {}).get("errors"):
            raise InfrastructureError(f"Business rule {requested!r} failed to run")

        triples = TwinInferredMaterialize.uri_triples(
            [
                {"subject": t.subject, "predicate": t.predicate, "object": t.object}
                for t in result.inferred_triples
            ]
        )
        written = 0
        if triples:
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

        logger.info(
            "nodes/business-rule: entity=%s class=%s rule=%s inferred=%d written=%d",
            local_id,
            class_name,
            requested,
            len(triples),
            written,
        )
        return {
            "success": True,
            "entity_uri": entity_uri,
            "entity_local_id": local_id,
            "class_name": class_name,
            "rule": requested,
            "inferred_count": len(triples),
            "materialized_count": written,
            "triples": triples[:MAX_RETURNED_TRIPLES],
            "truncated": len(triples) > MAX_RETURNED_TRIPLES,
        }
