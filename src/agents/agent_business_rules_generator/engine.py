"""
OntoBricks Business Rules Generator Agent Engine.

Implements an agentic loop that uses the Databricks Foundation Model API with
function calling to autonomously gather context (ontology design + uploaded
documents) and propose business rules across four paradigms:

  * SWRL rules            (IF/THEN antecedent → consequent)
  * Decision tables       (tabular condition → action)
  * SPARQL CONSTRUCT rules (graph-pattern inference)
  * Aggregate rules       (GROUP BY / HAVING statistical conditions)

The agent emits a single JSON object with the four rule lists as its final
text answer (mirrors ``agent_owl_generator`` which emits Turtle). The engine
parses and normalises it; the route then validates each rule and lets the
user review/accept before persistence.

Fallback: if the LLM endpoint does not support the ``tools`` parameter the
engine transparently degrades to a single-shot generation (no tool calls).
"""

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import requests

from back.core.logging import get_logger
from agents.agent_business_rules_generator.tools import (
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
)
from agents.tools.context import ToolContext
from agents.engine_base import (
    AgentStep,
    call_serving_endpoint,
    dispatch_tool,
    extract_message_content,
    accumulate_usage,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 10
LLM_TIMEOUT = 180

_TRACE_NAME = "business_rules_generator"

# Rule-list keys the agent produces, in the order they are presented.
_RULE_KEYS = ("swrl_rules", "decision_tables", "sparql_rules", "aggregate_rules")


# =====================================================
# Data classes
# =====================================================


@dataclass
class AgentResult:
    """Outcome of a full business-rules generation run."""

    success: bool
    swrl_rules: List[dict] = field(default_factory=list)
    decision_tables: List[dict] = field(default_factory=list)
    sparql_rules: List[dict] = field(default_factory=list)
    aggregate_rules: List[dict] = field(default_factory=list)
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)

    def total_rules(self) -> int:
        return (
            len(self.swrl_rules)
            + len(self.decision_tables)
            + len(self.sparql_rules)
            + len(self.aggregate_rules)
        )


# =====================================================
# System prompt
# =====================================================


SYSTEM_PROMPT = """\
You are a business-rules engineer for a knowledge-graph platform (OntoBricks).
Your goal is to propose useful, valid business rules for the user's domain,
grounded in the ontology design and the uploaded documents. The user will
REVIEW your proposals in a form and pick which to keep — you never persist
anything yourself.

# TOOLS
  • get_ontology_design – classes (with datatype attributes) + object-property
                          relationships of the active domain. CALL THIS FIRST.
  • list_documents      – discover uploaded domain documents.
  • read_document       – read a document's text content.

# WORKFLOW
1. Call get_ontology_design to learn the real class and property names.
2. Call list_documents, then read the relevant ones to find business logic,
   thresholds, classifications, and constraints worth encoding as rules.
3. Output ONLY a single JSON object (no prose, no markdown fences) with the
   four rule lists described below. Use ONLY class/property names that exist
   in the ontology design. If a rule type does not apply, return an empty list
   for it. Aim for quality over quantity (typically 1–6 rules per applicable
   type).

# OUTPUT JSON SHAPE
{
  "swrl_rules": [
    {
      "name": "LoyalCustomer",
      "description": "A Customer with 1000+ loyalty points is a loyal customer",
      "antecedent": "Customer(?c) ^ loyaltyPoints(?c, ?lp) ^ swrlb:greaterThanOrEqual(?lp, 1000)",
      "consequent": "LoyalCustomer(?c)",   // ONLY valid if LoyalCustomer is an existing ontology class; otherwise pick an existing class or skip
      "enabled": true
    }
  ],
  "decision_tables": [
    {
      "name": "RiskTier",
      "target_class": "Account",
      "hit_policy": "first",            // "first" | "all" | "unique"
      "row_logic": "and",               // "and" = all conditions in a row, "or"
      "input_columns": [ { "property": "balance", "label": "Balance" } ],
      "output_column": { "property": "riskTier", "action": "set", "value": "" },
      "rows": [
        { "conditions": [ { "op": "gt", "value": "100000" } ], "action_value": "high" }
      ],
      "enabled": true
    }
  ],
  "sparql_rules": [
    {
      "name": "FlagBigOrder",
      "description": "Orders over 10k are large",
      "query": "CONSTRUCT { ?o a :LargeOrder } WHERE { ?o a :Order . ?o :total ?t . FILTER(?t > 10000) }",
      "enabled": true
    }
  ],
  "aggregate_rules": [
    {
      "name": "FrequentBuyer",
      "target_class": "Customer",
      "group_by_property": "placesOrder",
      "aggregate_property": "",
      "aggregate_function": "count",    // count | sum | avg | min | max
      "operator": "gte",                // lt | gt | eq | lte | gte | neq
      "threshold": "10",
      "result_class": "FrequentBuyer",
      "enabled": true
    }
  ]
}

# RULES OF THUMB
• Decision tables: every row's "conditions" array MUST have exactly one entry
  per input column, in the same order. Decision-table condition operators:
  eq, neq, gt, gte, lt, lte, startsWith, endsWith, contains (use "any" to skip
  a column in a row).
• SPARQL rules MUST be a CONSTRUCT { ... } WHERE { ... } query. Prefix-less
  local names (e.g. :Order, :total) resolve against the domain base URI.
• Aggregate rules: aggregate_function ∈ {count, sum, avg, min, max};
  operator ∈ {lt, gt, eq, lte, gte, neq}. For count you may leave
  aggregate_property empty. Provide a result_class to classify matches.
• SWRL antecedent/consequent are space-separated atoms joined with " ^ ".
• SWRL — the consequent MUST be semantically tied to the antecedent:
  - Every variable used in the consequent MUST be bound in the antecedent.
  - The consequent classifies the subject that actually participates in the
    antecedent's relationships (e.g. the Customer who holds a Contract).
  - The consequent class MUST be one of the EXISTING ontology classes. You may
    NOT invent a new class name (no "LoyalCustomer", "EngagedCustomer", etc.
    unless that exact class is already in the ontology design). Pick the most
    specific existing class that the inference implies. If NO existing class
    correctly captures the conclusion, DO NOT emit the rule.
  - The consequent MUST infer something NEW. It may NOT simply repeat an atom
    that is already stated in the antecedent. Re-asserting a type the subject
    already has is meaningless and FORBIDDEN. WRONG:
    "Contract(?c) ^ generates(?c, ?i) ^ Invoice(?i) ^ settledBy(?i, ?p) ^
     Payment(?p) → Invoice(?i)"  — ?i is already an Invoice in the IF, so the
    rule concludes nothing. A valid consequent either classifies a variable
    into a DIFFERENT existing class it does not already have, or asserts an
    EXISTING object/data property between entities already bound in the
    antecedent. If you cannot infer anything new using existing terms, DO NOT
    emit the SWRL rule.
• SWRL — PREFER relationship-inference (property-chain) rules. The most
  valuable SWRL rules close a chain of relationships into a NEW direct
  relationship between two entities that are connected only indirectly in the
  antecedent. Walk the relationship graph for A →(rel1)→ B →(rel2)→ C patterns
  and, when an EXISTING property directly links A to C, infer it. Example
  (only valid if every term exists in the ontology):
    "Customer(?c) ^ raised(?c, ?cl) ^ Claim(?cl) ^ relatedTo(?cl, ?ct) ^
     Contract(?ct) → holds(?c, ?ct)"
  i.e. a customer who raised a claim about a contract holds that contract. The
  consequent property (holds) must already exist and connect the two endpoint
  classes; do not invent it.
• EVERY class and property you reference — in any rule type, antecedent or
  consequent, WHERE pattern or CONSTRUCT head, target_class or result_class —
  MUST already exist in the ontology design returned by get_ontology_design.
  Never invent, pluralise, abbreviate, or rename a term. When in doubt, omit
  the rule rather than guess a name.

# CRITICAL OUTPUT RULES
• When you have gathered enough context, output ONLY the JSON object.
• Do NOT include any natural language, explanations, or commentary.
• Do NOT wrap the JSON in markdown code fences.
• Your ENTIRE response must be a single valid JSON object starting with '{'.
"""


# =====================================================
# Internal helpers
# =====================================================


def _strip_code_fences(text: str) -> str:
    """Remove an optional ```json ... ``` wrapper from *text*."""
    t = text.strip()
    if t.startswith("```"):
        first_nl = t.find("\n")
        if first_nl != -1:
            t = t[first_nl + 1 :]
        if t.rstrip().endswith("```"):
            t = t.rstrip()[: -3]
    return t.strip()


def _extract_json_object(text: str) -> Optional[dict]:
    """Parse a JSON object out of *text*, tolerating fences and surrounding prose."""
    candidate = _strip_code_fences(text)
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    # Fall back to the outermost {...} slice.
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            parsed = json.loads(candidate[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            return None
    return None


def _normalise_result(parsed: dict, result: AgentResult) -> None:
    """Copy the four rule lists from *parsed* onto *result* (defensive)."""
    for key in _RULE_KEYS:
        value = parsed.get(key, [])
        rules = [r for r in value if isinstance(r, dict)] if isinstance(value, list) else []
        for r in rules:
            r.setdefault("enabled", True)
        setattr(result, key, rules)


def _ontology_name_sets(ontology_design: dict) -> tuple:
    """Build (class_names, property_names) lowercased sets from the design snapshot.

    Entities are classes; relationships and entity attributes are properties.
    """
    class_names = set()
    property_names = set()
    for ent in (ontology_design or {}).get("entities", []):
        name = ent.get("name")
        if name:
            class_names.add(name.lower())
        for attr in ent.get("attributes", []) or []:
            if attr:
                property_names.add(str(attr).lower())
    for rel in (ontology_design or {}).get("relationships", []):
        name = rel.get("name")
        if name:
            property_names.add(name.lower())
    return class_names, property_names


def _drop_rules_with_unknown_refs(
    result: "AgentResult", ontology_design: dict, notify: Callable[[str], None]
) -> None:
    """Remove generated rules that reference non-existent ontology terms.

    Applies to all four rule types. Derived/inferred targets (SWRL consequent
    subtypes, SPARQL CONSTRUCT heads, aggregate ``result_class``, decision-table
    ``output_column``) may be new; everything a rule *reads* (antecedent /
    WHERE pattern / target class / input columns / grouped properties) must
    exist. This is a hard guarantee so the review modal never shows rules that
    mention entities/relationships absent from the ontology.
    """
    from back.objects.ontology import Ontology

    class_names, property_names = _ontology_name_sets(ontology_design)
    total_dropped = 0
    for key in _RULE_KEYS:
        rules = getattr(result, key)
        if not rules:
            continue
        kept = []
        for rule in rules:
            errors = Ontology.rule_reference_errors(
                key, rule, class_names, property_names
            )
            if errors:
                logger.warning(
                    "business_rules_agent: dropping %s rule '%s' — %s",
                    key,
                    rule.get("name", "?"),
                    "; ".join(errors),
                )
                total_dropped += 1
                continue
            kept.append(rule)
        setattr(result, key, kept)
    if total_dropped:
        notify(
            f"Discarded {total_dropped} rule(s) referencing unknown ontology terms"
        )


def _build_user_prompt(base_uri: str, options: dict, guidelines: str) -> str:
    parts = [f"Domain base URI: {base_uri}"]
    if guidelines:
        parts.append(f"Additional guidance from the user: {guidelines}")
    parts.append(
        "Inspect the ontology design and the uploaded documents, then propose "
        "business rules across the four paradigms. Start by calling "
        "get_ontology_design."
    )
    return "\n".join(parts)


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="business_rules_generator")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    registry: dict,
    ontology_design: dict,
    base_uri: str,
    options: Optional[dict] = None,
    guidelines: str = "",
    domain_name: Optional[str] = None,
    domain_folder: Optional[str] = None,
    domain_version: Optional[str] = None,
    selected_docs: Optional[List[str]] = None,
    warehouse_id: Optional[str] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> AgentResult:
    """Run the business-rules generation agent.

    The agent autonomously gathers context via tool calls (ontology design +
    documents) and produces a JSON object with four rule lists. If the serving
    endpoint does not support function calling the engine falls back to a
    direct single-shot prompt.
    """
    logger.info(
        "===== BUSINESS RULES AGENT START ===== endpoint=%s, base_uri=%s, docs=%s",
        endpoint_name,
        base_uri,
        selected_docs,
    )

    ctx = ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        registry=registry or {},
        domain_name=domain_name or "",
        domain_folder=domain_folder or "",
        domain_version=domain_version or "1",
        warehouse_id=warehouse_id or "",
        ontology=ontology_design or {},
    )

    result = AgentResult(success=False)

    user_prompt = _build_user_prompt(base_uri, options or {}, guidelines or "")
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}

    def notify(msg: str):
        if on_step:
            on_step(msg)

    notify("Starting agent…")
    tools_supported = True
    _json_retries = 0

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1
        notify(f"Agent thinking… (step {iteration + 1})")

        is_last = iteration >= MAX_ITERATIONS - 1
        send_tools = TOOL_DEFINITIONS if (tools_supported and not is_last) else None

        t0 = time.time()
        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=send_tools,
                max_tokens=4096,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            if exc.response is not None and status in (400, 422) and tools_supported:
                logger.warning(
                    "business_rules_agent: endpoint rejected tools (HTTP %s) — "
                    "falling back to direct mode",
                    status,
                )
                tools_supported = False
                notify("Endpoint does not support tools – using direct generation…")
                try:
                    llm_response = call_serving_endpoint(
                        host,
                        token,
                        endpoint_name,
                        messages,
                        tools=None,
                        max_tokens=4096,
                        temperature=0.1,
                        timeout=LLM_TIMEOUT,
                        trace_name=_TRACE_NAME,
                    )
                except Exception as inner:
                    result.error = f"LLM request failed: {inner}"
                    logger.error("business_rules_agent: fallback call failed: %s", inner)
                    return result
            else:
                result.error = f"LLM request failed: {exc}"
                logger.error(
                    "business_rules_agent: LLM request failed at iter %d: %s",
                    iteration + 1,
                    exc,
                )
                return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            logger.error("business_rules_agent: timeout at iter %d", iteration + 1)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            logger.error("business_rules_agent: request exception: %s", exc)
            return result
        except Exception as exc:  # pragma: no cover - defensive
            result.error = f"LLM request failed: {exc}"
            logger.error("business_rules_agent: unexpected error at iter %d: %s", iteration + 1, exc)
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        accumulate_usage(total_usage, llm_response.get("usage", {}))

        choices = llm_response.get("choices", [])
        if not choices:
            result.error = "No choices in LLM response"
            logger.warning("business_rules_agent: empty choices at iter %d", iteration + 1)
            return result

        message = choices[0].get("message", {})
        tool_calls = message.get("tool_calls", [])

        if tool_calls:
            messages.append(message)
            for tc in tool_calls:
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                tool_id = tc.get("id", "")
                raw_args = func.get("arguments", "{}")
                try:
                    arguments = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    arguments = {}

                notify(f"Calling {tool_name}…")
                result.steps.append(
                    AgentStep(
                        step_type="tool_call",
                        content=json.dumps(arguments, default=str)[:600],
                        tool_name=tool_name,
                    )
                )

                t1 = time.time()
                tool_result = dispatch_tool(
                    TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME
                )
                tool_ms = int((time.time() - t1) * 1000)

                result.steps.append(
                    AgentStep(
                        step_type="tool_result",
                        content=(tool_result[:500] + "…") if len(tool_result) > 500 else tool_result,
                        tool_name=tool_name,
                        duration_ms=tool_ms,
                    )
                )
                messages.append(
                    {"role": "tool", "tool_call_id": tool_id, "content": tool_result}
                )
            continue

        # ---- Agent produced a text response — expect the JSON object ----
        content = extract_message_content(llm_response)
        result.steps.append(
            AgentStep(
                step_type="output",
                content=(content[:200] + "…") if len(content) > 200 else content,
                duration_ms=elapsed_ms,
            )
        )

        parsed = _extract_json_object(content)
        if parsed is None:
            if _json_retries < 2:
                _json_retries += 1
                logger.warning(
                    "business_rules_agent: could not parse JSON (retry %d)", _json_retries
                )
                messages.append({"role": "assistant", "content": content})
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "That was not valid JSON. Output ONLY a single JSON "
                            "object with keys swrl_rules, decision_tables, "
                            "sparql_rules, aggregate_rules. No prose, no code fences."
                        ),
                    }
                )
                continue
            result.error = "Agent did not produce valid JSON output"
            logger.error("business_rules_agent: giving up after JSON parse failures")
            return result

        _normalise_result(parsed, result)
        _drop_rules_with_unknown_refs(result, ctx.ontology or {}, notify)
        result.success = True
        result.usage = total_usage
        logger.info(
            "===== BUSINESS RULES AGENT DONE ===== iterations=%d, swrl=%d, dt=%d, "
            "sparql=%d, agg=%d, prompt_tokens=%d, completion_tokens=%d",
            result.iterations,
            len(result.swrl_rules),
            len(result.decision_tables),
            len(result.sparql_rules),
            len(result.aggregate_rules),
            total_usage["prompt_tokens"],
            total_usage["completion_tokens"],
        )
        notify(f"Proposed {result.total_rules()} rule(s)")
        return result

    result.error = f"Agent reached maximum iterations ({MAX_ITERATIONS}) without output"
    result.usage = total_usage
    logger.error("===== BUSINESS RULES AGENT FAILED ===== %s", result.error)
    return result
