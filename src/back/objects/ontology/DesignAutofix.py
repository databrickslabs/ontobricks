"""Propose and apply Sanity Checks Auto-fix plans."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger
from back.objects.ontology.autofix_apply import apply_operations
from back.objects.ontology.autofix_ops import (
    AI_RULES,
    NO_AUTOFIX_RULES,
    RULE_PLANNER,
    RULE_RULES,
    empty_plan,
    finding_id,
    humanize_id,
    make_operation,
)
from back.objects.ontology.autofix_rule_fixers import propose_rule_fix

logger = get_logger(__name__)


class DesignAutofix:
    """Orchestrate hybrid Auto-fix propose / apply for design-check findings."""

    @staticmethod
    def propose(
        findings: List[Dict[str, Any]],
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        groups: List[Dict[str, Any]],
        *,
        llm_available: bool = False,
        llm_runner: Optional[Callable[..., Any]] = None,
    ) -> Dict[str, Any]:
        """Build a FixPlan for one or more findings of the same rule."""
        if not findings:
            return {
                "success": False,
                "error": "No findings provided.",
                "llm_available": llm_available,
            }

        rules = {(f.get("rule") or "") for f in findings}
        if len(rules) != 1:
            return {
                "success": False,
                "error": "All findings must share the same rule.",
                "llm_available": llm_available,
            }
        rule = next(iter(rules))
        planner = RULE_PLANNER.get(rule, "none")

        if planner == "none" or rule in NO_AUTOFIX_RULES:
            return {
                "success": False,
                "error": f"No Auto-fix available for {rule}.",
                "llm_available": llm_available,
            }

        if planner == "rule" or rule in RULE_RULES:
            return DesignAutofix._propose_rule(
                findings, classes, properties, groups, llm_available
            )

        if planner == "ai" or rule in AI_RULES:
            return DesignAutofix._propose_ai(
                findings,
                classes,
                properties,
                groups,
                llm_available=llm_available,
                llm_runner=llm_runner,
            )

        return {
            "success": False,
            "error": f"Unknown planner for {rule}.",
            "llm_available": llm_available,
        }

    @staticmethod
    def apply(domain, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply confirmed operations to the domain session."""
        return apply_operations(domain, operations or [])

    @staticmethod
    def _propose_rule(
        findings: List[Dict[str, Any]],
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        groups: List[Dict[str, Any]],
        llm_available: bool,
    ) -> Dict[str, Any]:
        rule = findings[0].get("rule") or ""
        merged = empty_plan(rule, f"Apply mechanical fixes for {rule}.")
        summaries: List[str] = []
        for finding in findings:
            plan = propose_rule_fix(finding, classes, properties, groups)
            summaries.append(plan.get("summary") or "")
            merged["operations"].extend(plan.get("operations") or [])
        if summaries:
            merged["summary"] = " ".join(s for s in summaries if s) or merged["summary"]
        if not merged["operations"]:
            return {
                "success": False,
                "error": merged.get("summary") or "No mechanical fix available.",
                "llm_available": llm_available,
                "plan": merged,
            }
        return {
            "success": True,
            "llm_available": llm_available,
            "plan": merged,
        }

    @staticmethod
    def _propose_ai(
        findings: List[Dict[str, Any]],
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        groups: List[Dict[str, Any]],
        *,
        llm_available: bool,
        llm_runner: Optional[Callable[..., Any]],
    ) -> Dict[str, Any]:
        rule = findings[0].get("rule") or ""

        if not llm_available:
            return DesignAutofix._propose_ai_fallback(
                findings, classes, properties, groups
            )

        if llm_runner is None:
            return {
                "success": False,
                "error": "LLM runner not configured.",
                "llm_available": True,
            }

        merged = empty_plan(rule, f"AI proposals for {rule}.")
        summaries: List[str] = []
        for finding in findings:
            try:
                result = llm_runner(
                    finding,
                    classes,
                    properties,
                    groups,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("design fixer llm_runner failed")
                return {
                    "success": False,
                    "error": str(exc),
                    "llm_available": True,
                }
            success = getattr(result, "success", None)
            if success is None and isinstance(result, dict):
                success = result.get("success")
                plan = result.get("plan") or {}
                error = result.get("error") or ""
            else:
                plan = getattr(result, "plan", {}) or {}
                error = getattr(result, "error", "") or ""
            if not success:
                return {
                    "success": False,
                    "error": error or "Agent failed to propose a fix.",
                    "llm_available": True,
                }
            summaries.append(plan.get("summary") or "")
            ops = plan.get("operations") or []
            alts = plan.get("alternatives") or []
            # When Fix-all: independent findings → keep ops checked; alts stay alts
            if len(findings) > 1:
                for op in ops:
                    op = dict(op)
                    op.pop("group", None)
                    merged["operations"].append(op)
                for alt in alts:
                    merged["alternatives"].append(alt)
            else:
                merged["operations"].extend(ops)
                merged["alternatives"].extend(alts)

        if summaries:
            merged["summary"] = " ".join(s for s in summaries if s) or merged["summary"]
        return {"success": True, "llm_available": True, "plan": merged}

    @staticmethod
    def _propose_ai_fallback(
        findings: List[Dict[str, Any]],
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        groups: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        rule = findings[0].get("rule") or ""

        if rule == "DC-W02":
            merged = empty_plan(rule, "Propose labels by humanising IDs (No LLM).")
            for finding in findings:
                name = finding.get("name") or ""
                label = humanize_id(name)
                if not label:
                    continue
                merged["operations"].append(
                    make_operation(
                        op="set_label",
                        target={
                            "kind": finding.get("kind") or "",
                            "name": name,
                            **(
                                {"domain": finding["domain"]}
                                if finding.get("domain")
                                else {}
                            ),
                        },
                        params={"label": label},
                        rationale=f"Humanise ID '{name}' as the label.",
                        source="fallback",
                        finding_id=finding_id(finding),
                    )
                )
            if not merged["operations"]:
                return {
                    "success": False,
                    "error": "Could not derive labels.",
                    "llm_available": False,
                }
            return {
                "success": True,
                "llm_available": False,
                "plan": merged,
            }

        if rule == "DC-W03":
            # Entity picker + Remove; UI shows dropdown when needs_entity_picker
            merged = empty_plan(
                rule,
                "Choose an entity to attach the orphan attribute, or remove it.",
            )
            merged["needs_entity_picker"] = True
            for finding in findings:
                name = finding.get("name") or ""
                group = f"alt-{name}"
                target = {
                    "kind": finding.get("kind") or "attribute",
                    "name": name,
                }
                fid = finding_id(finding)
                # Placeholder attach — UI fills entity via picker
                merged["operations"].append(
                    make_operation(
                        op="attach_attribute",
                        target=target,
                        params={"entity": ""},
                        rationale="Attach to an entity you select.",
                        source="fallback",
                        finding_id=fid,
                        selected=True,
                        group=group,
                    )
                )
                merged["alternatives"].append(
                    make_operation(
                        op="remove_property",
                        target=target,
                        rationale="Remove the orphan attribute.",
                        source="fallback",
                        finding_id=fid,
                        selected=False,
                        group=group,
                    )
                )
            return {
                "success": True,
                "llm_available": False,
                "plan": merged,
            }

        return {
            "success": False,
            "error": "LLM required for this Auto-fix.",
            "llm_available": False,
        }
