"""
Graph Interpreter Agent — engine.

Receives pre-computed graph centrality metrics and calls the LLM in a
tool-enabled loop. The agent may invoke ``get_entity_details`` to look up
specific entities before writing its structured insights.

Output contract: ``AgentResult.sections`` is a list of dicts matching:
    { "title": str, "body": str }            (Key Findings)
    { "title": str, "items": list[dict] }    (Notable Entities)
    { "title": str, "items": list[str] }     (Recommendations)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from back.core.logging import get_logger
from back.core.helpers import URIHelpers
from agents.agent_graph_interpreter.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.tools.context import ToolContext
from agents.engine_base import (
    AgentStep,
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 8
LLM_TIMEOUT = 120
_TRACE_NAME = "graph_interpreter"


@dataclass
class AgentResult:
    """Outcome of a graph-interpreter run."""

    success: bool
    sections: List[Dict[str, Any]] = field(default_factory=list)
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


_SYSTEM_PROMPT = """\
You are a knowledge-graph analytics expert and ontology architect.
You receive centrality metrics (PageRank, betweenness, degree, closeness, clustering)
for a set of entities in a business knowledge graph, together with a per-type
Data Model Health report.

Your task is to produce concise, actionable insights in the following JSON structure
(and ONLY that structure — no markdown fences, no prose outside the JSON):

{
  "sections": [
    { "title": "Key Findings",       "body": "<2-4 sentences summarising the graph structure and standout patterns>" },
    { "title": "Ontology Modeling",  "items": [ "<per-type ontology improvement 1>", "<improvement 2>", "..." ] },
    { "title": "Notable Entities",   "items": [ { "label": "<entity name>", "reason": "<why it stands out>" } ] },
    { "title": "Recommendations",    "items": [ "<graph-topology action 1>", "<action 2>", "<action 3>" ] }
  ]
}

RULES — Graph centrality
- Only mention entities whose labels appear in the provided metrics payload.
- Use the ``get_entity_details`` tool to look up the top-1 or top-2 PageRank nodes
  BEFORE writing your final answer. This grounds your observations in real data.
- If ``zero_metrics`` is non-empty, explain what that means in Key Findings
  (e.g. clustering = 0 is typical for bipartite graphs).
- If ``top_pagerank`` is empty, state that no ranked entities are available.
- Be specific: mention entity names, scores, and structural observations.
- Keep "Notable Entities" to at most 5 items.
- Keep "Recommendations" to 3-5 actionable items focused on graph usage and topology.

RULES — Ontology Modeling section (driven by ``entity_type_health``)
Each entry in entity_type_health has: type, count, distinct_predicates, avg_degree,
is_flat, has_temporal_predicates, and optional reasons[].
Populate the "Ontology Modeling" section with one bullet per problematic type.
Each bullet must name the type, give the count, state the problem, and propose a concrete fix.
Format each item as: "[TypeName] (N instances) — <problem> → <proposed fix>"

Signal → diagnosis and fix:

- is_flat=true + reasons contains "no entity-entity relationships":
  The type is fully isolated — no edges to any other node.
  Fix: either remove it from the graph sync entirely and store its attributes
  as properties on its owning entity, or add missing relationship mappings in R2RML.

- is_flat=true + reasons contains "only 1 distinct relationship predicate":
  The type behaves like a flat table — all instances share exactly one edge type.
  Fix: consider collapsing the type into a counter/aggregate on its parent entity,
  or enrich the ontology with additional relationship types to justify graph nodes.

- has_temporal_predicates=true (regardless of is_flat):
  The type carries timestamped data that may represent time-series events.
  Fix: externalise time-series facts to a dedicated Delta table or event log;
  keep only the entity node with a reference, unless temporal graph traversal
  is a core query use-case.

- distinct_predicates=0:
  Fully isolated — no outgoing or incoming typed edges at all.
  Fix: review whether this type should be modelled as a node property instead,
  or whether foreign-key relationships are missing from the R2RML mapping.

- avg_degree < 1.0 and count > 50:
  Weakly connected — most instances have fewer than one edge on average.
  Fix: review R2RML mappings to ensure all relevant foreign keys are captured
  as object properties in the ontology.

- Healthy types (is_flat=false, avg_degree > 1, distinct_predicates > 1):
  Do NOT suggest changes — mention them only if there is a genuine structural issue.

- Prioritise items by count descending (most-impactful changes first).
- If entity_type_health is absent or empty, omit the "Ontology Modeling" section entirely.
"""


def _build_user_message(payload: Dict[str, Any]) -> str:
    stats = payload.get("stats", {})
    nodes = payload.get("nodes", {})
    node_labels = payload.get("node_labels", {})
    class_filter = payload.get("class_filter") or []
    entity_type = (
        URIHelpers.extract_local_name(class_filter[0]) if class_filter else None
    )

    top_nodes = sorted(
        nodes.items(), key=lambda x: x[1].get("pagerank", 0), reverse=True
    )[:10]
    top_rows = []
    for uri, m in top_nodes:
        label = node_labels.get(uri, URIHelpers.extract_local_name(uri))
        top_rows.append({
            "uri": uri,
            "label": label,
            "pagerank": round(m.get("pagerank", 0), 6),
            "degree": round(m.get("degree", 0), 6),
            "betweenness": round(m.get("betweenness", 0), 6),
            "closeness": round(m.get("closeness", 0), 6),
            "clustering": round(m.get("clustering", 0), 6),
        })

    zero_metrics = [
        key
        for key in ("pagerank", "degree", "betweenness", "closeness", "clustering")
        if nodes and all(n.get(key, 0) == 0 for _, n in nodes.items())
    ]

    # Full per-type Data Model Health table (superset of the old flat_types list)
    type_health: list[dict] = []
    for profile in (payload.get("entity_type_profiles") or {}).values():
        entry: dict = {
            "type": (
                URIHelpers.extract_local_name(profile.get("uri", ""))
                or profile.get("uri", "")
            ),
            "count": profile.get("count"),
            "distinct_predicates": profile.get("distinct_predicates"),
            "avg_degree": round(profile.get("avg_degree", 0), 4),
            "is_flat": profile.get("is_flat", False),
            "has_temporal_predicates": profile.get("has_temporal_predicates", False),
        }
        if profile.get("flat_reasons"):
            entry["reasons"] = profile["flat_reasons"]
        type_health.append(entry)

    # Sort: flat types first (most actionable), then by instance count descending
    type_health.sort(key=lambda x: (not x["is_flat"], -(x["count"] or 0)))

    summary = {
        "entity_type": entity_type,
        "class_filter_applied": bool(class_filter),
        "stats": {
            "node_count": stats.get("node_count"),
            "graph_node_count": stats.get("graph_node_count"),
            "edge_count": stats.get("edge_count"),
            "connected_components": stats.get("connected_components"),
            "avg_degree": stats.get("avg_degree"),
            "density": stats.get("density"),
        },
        "top_pagerank": top_rows,
        "zero_metrics": zero_metrics,
    }
    if type_health:
        summary["entity_type_health"] = type_health

    return (
        "Analyze the following graph centrality metrics and return insights.\n\n"
        + json.dumps(summary, indent=2)
    )


def _parse_sections(content: str) -> List[Dict[str, Any]]:
    """Extract the sections list from LLM text, tolerating common wrapping patterns.

    Tries in order:
    1. Strip markdown fences (```json … ```) then JSON-parse.
    2. Extract the first {...} block in the response (handles prose preamble).
    3. Fall back to a single "Interpretation" body section.
    """
    text = content.strip()

    # Strip markdown fences
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            candidate = part.lstrip("json").strip()
            try:
                return json.loads(candidate).get("sections", [])
            except (json.JSONDecodeError, AttributeError):
                continue

    # Direct parse (clean JSON output)
    try:
        return json.loads(text).get("sections", [])
    except (json.JSONDecodeError, AttributeError):
        pass

    # Extract first {...} block — handles prose before/after the JSON object
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end + 1]).get("sections", [])
        except (json.JSONDecodeError, AttributeError):
            pass

    logger.warning("_parse_sections: could not extract JSON from LLM response")
    return [{"title": "Interpretation", "body": text}]


@trace_agent(name=_TRACE_NAME)
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    metrics_payload: Dict[str, Any],
    base_url: str,
    domain_name: str,
    session_cookies: Dict[str, str],
    session_headers: Optional[Dict[str, str]] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> AgentResult:
    """Run one interpretation turn for a metrics payload.

    Args:
        host, token, endpoint_name: Databricks serving-endpoint credentials.
        metrics_payload: The full JSON returned by ``/dtwin/metrics/compute``
            plus an optional ``class_filter`` list.
        base_url: Loopback OntoBricks URL, e.g. ``http://localhost:8000``.
        domain_name: Active domain name (forwarded to ToolContext).
        session_cookies: User session cookies forwarded to loopback routes.
        session_headers: Optional Databricks-Apps identity headers.
        on_step: Optional progress callback.
    """
    logger.info(
        "===== GRAPH INTERPRETER START ===== endpoint=%s domain=%s",
        endpoint_name,
        domain_name,
    )

    ctx = ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        dtwin_base_url=base_url,
        dtwin_session_cookies=session_cookies or {},
        dtwin_session_headers=session_headers or {},
        dtwin_domain_name=domain_name or "",
    )

    result = AgentResult(success=False)
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": _build_user_message(metrics_payload)},
    ]

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1
        is_last = iteration == MAX_ITERATIONS - 1
        send_tools = TOOL_DEFINITIONS if not is_last else None

        if on_step:
            on_step(f"Iteration {iteration + 1}…")

        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=send_tools,
                max_tokens=2048,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except Exception as exc:
            error_msg = f"LLM request failed: {exc}"
            logger.error("graph_interpreter: %s at iteration %d", error_msg, iteration + 1)
            result.error = error_msg
            return result

        accumulate_usage(result.usage, llm_response.get("usage", {}))

        choices = llm_response.get("choices", [])
        if not choices:
            result.error = "No choices in LLM response"
            return result

        message = choices[0].get("message", {})
        content = message.get("content", "") or ""
        tool_calls = message.get("tool_calls")

        if tool_calls:
            messages.append(message)

            for tc in tool_calls:
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                tool_id = tc.get("id", "")
                raw_args = func.get("arguments", "{}")

                try:
                    arguments = (
                        json.loads(raw_args)
                        if isinstance(raw_args, str)
                        else raw_args
                    )
                except json.JSONDecodeError:
                    arguments = {}

                logger.info(
                    "graph_interpreter: iteration %d — tool_call '%s'",
                    iteration + 1,
                    tool_name,
                )

                call_step = AgentStep(
                    step_type="tool_call",
                    content=json.dumps(arguments, default=str),
                    tool_name=tool_name,
                )
                result.steps.append(call_step)

                t0 = time.time()
                tool_result = dispatch_tool(
                    TOOL_HANDLERS, ctx, tool_name, arguments,
                    trace_name=_TRACE_NAME,
                )
                tool_elapsed = int((time.time() - t0) * 1000)

                result.steps.append(AgentStep(
                    step_type="tool_result",
                    content=tool_result[:500],
                    tool_name=tool_name,
                    duration_ms=tool_elapsed,
                ))

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": tool_result,
                })
        else:
            result.success = True
            result.sections = _parse_sections(content)
            result.steps.append(AgentStep(step_type="output", content=content[:500]))
            logger.info(
                "===== GRAPH INTERPRETER DONE ===== iterations=%d sections=%d",
                result.iterations,
                len(result.sections),
            )
            return result

    result.error = "Max iterations reached without a final answer"
    return result
