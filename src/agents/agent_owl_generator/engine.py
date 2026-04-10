"""
OntoBricks OWL Generator Agent Engine.

Implements an agentic loop that uses the Databricks Foundation Model API
with function calling to autonomously gather context (metadata + documents)
and generate an OWL ontology in Turtle format.

Fallback: if the LLM endpoint does not support the ``tools`` parameter the
engine transparently degrades to a single-shot generation (no tool calls).
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import requests

from back.core.logging import get_logger
from agents.agent_owl_generator.tools import ToolContext, TOOL_DEFINITIONS, TOOL_HANDLERS
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

_TRACE_NAME = "owl_generator"


# =====================================================
# Data classes
# =====================================================

@dataclass
class AgentResult:
    """Outcome of a full agent run."""
    success: bool
    owl_content: str = ""
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================

SYSTEM_PROMPT = """\

You are an ontology engineer.  
Your goal is to generate a concise, logically consistent OWL ontology (**Turtle** format) from business documents and requirements, \
    suitable for automated reasoning and query answering.

# TOOLS
You have four tools:
  • list_documents  – discover documents in the project volume
  • read_document   – read a document's text content
  • get_metadata    – get all table schemas (names, columns, types)
  • get_table_detail – get detailed info for one table

# WORKFLOW
1. Call get_metadata to understand the database schema.
2. Call list_documents to discover available documents.
3. Read relevant documents with read_document.
4. Generate the Turtle ontology from all gathered information.

# NAMING RULES (CRITICAL – NO EXCEPTIONS)
• Classes: PascalCase (Customer, SalesOrder)
• Properties: lowerCamelCase (hasName, firstName)
• NO spaces, underscores or hyphens in local names.
• Convert: street_address → streetAddress, Sales Order → SalesOrder

# PROPERTY TYPES
• owl:DatatypeProperty + xsd:type for attributes (string, integer, date …)
• owl:ObjectProperty for relationships between classes only

# RELATIONSHIP RULES
• NEVER create bidirectional relationships.
• Between any two classes A and B create at most ONE ObjectProperty.
• Choose the most natural direction.

# CRITICAL OUTPUT RULES:
• When you have gathered enough context, output ONLY valid Turtle syntax.
• Do NOT include any natural language, explanations, or commentary.
• Do NOT wrap in markdown code fences.
• Your ENTIRE response must be valid Turtle starting with @prefix declarations.
• NEVER start with phrases like "Here is", "Now I will", "Based on", etc."""

GENERIC_GUIDELINES = """\

# OWL GENERATION GUIDELINES:

## Role and goal
Always follow the constraints and checks below.  
If you cannot satisfy them with the given input, say so explicitly and ask for clarification instead of guessing.

## 1. Scope and requirements
1. Work in a **narrow** and clearly defined domain.  
   - Ignore concepts that are not supported by the provided documents or requirements.
2. Model only what is needed to answer the specified competency questions (CQs).  
   - Every class or property you introduce must support at least one CQ or explicit requirement.
3. Size limits per iteration:  
   - 30–60 classes.  
   - At most 4 subclass levels (max depth = 4).  
   - At most 3 direct superclasses per class; default is a single superclass.
If the user does not give CQs, propose a short list of candidate CQs first, get them confirmed, then model.

## 2. Class and property design rules
For each **class** you create:[1][2][3][4]
1. Provide:  
   - A short, clear natural-language definition (1–2 sentences).  
   - At least 1 object property (unless the class is explicitly abstract).  
   - At least 2 datatype properties, when meaningful in the domain.  
2. Naming conventions:  
   - Classes: UpperCamelCase (e.g., `CustomerOrder`).  
   - Object properties: lowerCamelCase verbs or verb-like phrases (e.g., `placesOrder`).  
   - Datatype properties: lowerCamelCase nouns or attributes (e.g., `orderDate`).  
   - Do not create two classes or properties that are synonyms or near-duplicates; merge or explain the difference in their definitions.
3. Inheritance constraints:  
   - Default to single inheritance (`rdfs:subClassOf` one parent).  
   - Allow multiple inheritance only if strictly necessary and documented in a comment.  
   - Avoid deep chains: no more than 4 levels from the topmost domain root.
For each **property** you create:
1. Always specify:  
   - Domain class.  
   - Range class (for object properties) or XSD datatype (for datatype properties).  
2. Use functional properties only when the domain concept is truly single-valued (e.g., `hasIdentifier`).  
   - Default cardinality is “0..*” unless a stricter constraint is clearly justified by the documents or CQs.  
3. Do not duplicate properties with the same domain and range and similar meaning.  

## 3. Logical expressivity and OWL profile
1. Use OWL 2 **EL-like** modeling by default, prioritizing tractable reasoning:  
   - Allowed: class hierarchies, existential restrictions, simple domain and range axioms, disjointness when clear.  
   - Avoid unless explicitly required: property chains, complex nested class expressions, qualified cardinalities, heavy use of `owl:equivalentClass`.[5][14][2]
2. Only introduce complex axioms when they directly implement a CQ or an explicit business rule shown in the input.
3. Use disjointness, covering axioms, and equivalence axioms sparingly.  
   - Add them only when the source text or CQs strongly justify the constraint.
When in doubt between a simpler or more complex axiom, choose the simpler one.

## 4. Anti-pattern and complexity constraints
Avoid the following **anti-patterns**:
1. Cycles in subclass hierarchies or property hierarchies.  
2. Very generic “container” or “bag” classes with no clear semantics (e.g., `Entity`, `Thing` as domain-specific classes).  
3. Unsatisfiable classes (e.g., a class declared as a subclass of two disjoint classes without justification).  
4. Mixing roles and types in the same class (e.g., `CustomerWhoPaidInvoice` as a primitive type).  
5. Huge classes with dozens of unrelated properties; split into more specific concepts when needed.  
Additionally:
- Prefer flat, readable structures over deep or overly intricate constructions.  
- Do not invent complex patterns if a simple subclass or property restriction is enough.  

## 5. Traceability and grounding in documents
For every class and property, indicate its **origin**:
- Reference at least one:  
  - Document snippet or paragraph ID.  
  - Competency question ID.  
  - User-provided requirement.  
If you cannot find any support in the documents or CQs, do not introduce that class or property.  

## 6. Output format and quality checks
1. Output structure:  
   - First: a short natural-language summary of the ontology (1–2 paragraphs).  
   - Second: a list of classes and properties with their informal definitions and traceability information.  
   - Third: the OWL ontology in Turtle syntax only.[5][1][2]
2. Before you output, perform **self-checks** in natural language:
   - Check that no class is a subclass of two disjoint classes.  
   - Check there are no explicit subclass cycles.  
   - Check that every class and property has a clear name, definition, and at least one traceability reference.  
   - Check that the ontology stays within the intended OWL 2 EL-like subset.  
If you detect a likely inconsistency or anti-pattern, correct it before output and briefly explain what you changed.  
3. Never leave the ontology empty.  
   - If the input is insufficient, explain what is missing and ask the user for more precise domain descriptions or CQs instead of fabricating content.

## 7. In general
- Avoid orphans entities (try to have entities with relationships)

"""


# =====================================================
# Internal helpers
# =====================================================

def _build_user_prompt(
    guidelines: str,
    options: dict,
    base_uri: str,
    selected_tables: List[str],
    selected_docs: List[str],
) -> str:
    logger.debug("_build_user_prompt: base_uri=%s, options=%s", base_uri, options)
    logger.debug(
        "_build_user_prompt: selected_tables=%s, selected_docs=%s",
        selected_tables, selected_docs,
    )

    include_attrs = options.get("includeDataProperties", True)
    include_rels = options.get("includeRelationships", True)
    include_inherit = options.get("includeInheritance", True)

    parts = [f"Base URI: {base_uri}"]
    if selected_tables:
        parts.append(f"Selected tables: {', '.join(selected_tables)}")
    if selected_docs:
        parts.append(f"Selected documents: {', '.join(selected_docs)}")
    parts.append(
        f"Guidelines: {guidelines or 'Create a domain ontology from the available schema and documents.'}"
    )

    gen = []
    if include_attrs:
        gen.append("DatatypeProperties for attributes")
    if include_rels:
        gen.append("ObjectProperties for relationships")
    if include_inherit:
        gen.append("class hierarchy with rdfs:subClassOf")
    gen.append("rdfs:labels for all elements")
    parts.append(f"Generate OWL with: {', '.join(gen)}.")
    parts.append(
        "Start by using your tools to gather the metadata and read the "
        "relevant documents, then generate the ontology."
    )
    prompt = "\n".join(parts)
    logger.debug("_build_user_prompt: final prompt (%d chars):\n%s", len(prompt), prompt)
    return prompt


# =====================================================
# Public entry point
# =====================================================

@trace_agent(name="owl_generator")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    registry: dict,
    metadata: dict,
    guidelines: str,
    options: dict,
    base_uri: str,
    project_name: Optional[str] = None,
    project_folder: Optional[str] = None,
    selected_tables: Optional[List[str]] = None,
    selected_docs: Optional[List[str]] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> AgentResult:
    """Run the ontology-generation agent.

    The agent autonomously gathers context via tool calls and produces an
    OWL/Turtle ontology.  If the serving endpoint does not support
    function calling the engine falls back to a direct single-shot prompt.
    """
    logger.info(
        "===== AGENT START ===== endpoint=%s, base_uri=%s",
        endpoint_name, base_uri,
    )
    logger.debug(
        "run_agent params: registry=%s, guidelines=%d chars, options=%s, "
        "selected_tables=%s, selected_docs=%s",
        registry, len(guidelines or ""), options, selected_tables, selected_docs,
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        registry=registry or {},
        project_name=project_name or "",
        project_folder=project_folder or "",
        metadata=metadata or {},
    )
    logger.info("Agent context created — host=%s, registry=%s",
                ctx.host, ctx.registry)
    logger.debug(
        "Agent metadata summary: %d table(s), table names=[%s]",
        len((metadata or {}).get("tables", [])),
        ", ".join(t.get("name", "?") for t in (metadata or {}).get("tables", [])),
    )

    result = AgentResult(success=False)

    # Narrow metadata to selected tables when a subset was chosen
    if selected_tables and metadata.get("tables"):
        all_table_names = [t.get("full_name") or t.get("name") for t in metadata["tables"]]
        logger.debug("Agent table filtering: all=%s, selected=%s", all_table_names, selected_tables)
        filtered = [
            t for t in metadata["tables"]
            if (t.get("full_name") or t.get("name")) in selected_tables
        ]
        if filtered:
            ctx.metadata = {**metadata, "tables": filtered}
            logger.info(
                "Agent filtered metadata to %d/%d selected table(s): [%s]",
                len(filtered), len(metadata["tables"]),
                ", ".join(t.get("name", "?") for t in filtered),
            )
        else:
            logger.warning("Agent table filter matched 0 tables — keeping all %d", len(metadata["tables"]))
    else:
        logger.info("Agent using all %d available table(s) (no filter)", len((metadata or {}).get("tables", [])))

    # Build initial conversation
    logger.info("Agent building user prompt — guidelines=%d chars, options=%s", len(guidelines or ""), options)
    user_prompt = _build_user_prompt(
        guidelines=guidelines,
        options=options,
        base_uri=base_uri,
        selected_tables=selected_tables or [],
        selected_docs=selected_docs or [],
    )
    system_content = SYSTEM_PROMPT
    if GENERIC_GUIDELINES.strip():
        system_content = f"{SYSTEM_PROMPT}\n\n{GENERIC_GUIDELINES}"

    messages: List[dict] = [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_prompt},
    ]
    logger.info(
        "Agent conversation initialized: system_prompt=%d chars, user_prompt=%d chars",
        len(system_content), len(user_prompt),
    )
    logger.debug("Agent system prompt:\n%s", system_content)
    logger.debug("Agent user prompt:\n%s", user_prompt)

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}

    def notify(msg: str):
        if on_step:
            on_step(msg)

    notify("Starting agent…")
    logger.info(
        "Agent entering main loop — endpoint=%s, tables=%d, docs=%s, max_iterations=%d",
        endpoint_name,
        len(ctx.metadata.get("tables", [])),
        selected_docs,
        MAX_ITERATIONS,
    )

    # ------------------------------------------------------------------
    # Agent loop
    # ------------------------------------------------------------------
    tools_supported = True

    for iteration in range(MAX_ITERATIONS):
        logger.info(
            "----- Iteration %d/%d — %d messages in conversation -----",
            iteration + 1, MAX_ITERATIONS, len(messages),
        )
        logger.debug(
            "Iteration %d message roles: [%s]",
            iteration + 1,
            ", ".join(m.get("role", "?") for m in messages),
        )
        notify(f"Agent thinking… (step {iteration + 1})")

        # On the last iteration, withhold tools to force a text answer
        is_last = iteration >= MAX_ITERATIONS - 1
        send_tools = TOOL_DEFINITIONS if (tools_supported and not is_last) else None
        logger.info(
            "Iteration %d: tools_enabled=%s (supported=%s, last_iter=%s)",
            iteration + 1, send_tools is not None, tools_supported, is_last,
        )

        t0 = time.time()
        try:
            llm_response = call_serving_endpoint(
                host, token, endpoint_name, messages,
                tools=send_tools, max_tokens=4096, temperature=0.1,
                timeout=LLM_TIMEOUT, trace_name=_TRACE_NAME,
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning(
                "Iteration %d: HTTPError status=%s, tools_supported=%s",
                iteration + 1, status, tools_supported,
            )
            logger.debug("Iteration %d: HTTPError body: %.500s", iteration + 1,
                         exc.response.text if exc.response is not None else "N/A")
            if exc.response is not None and status in (400, 422) and tools_supported:
                logger.warning("Agent: endpoint rejected tools param (HTTP %s) — falling back to direct mode", status)
                tools_supported = False
                notify("Endpoint does not support tools – using direct generation…")
                try:
                    llm_response = call_serving_endpoint(
                        host, token, endpoint_name, messages,
                        tools=None, max_tokens=4096, temperature=0.1,
                        timeout=LLM_TIMEOUT, trace_name=_TRACE_NAME,
                    )
                except Exception as inner:
                    result.error = f"LLM request failed: {inner}"
                    logger.error("Agent: fallback LLM call also failed: %s", inner)
                    return result
            else:
                result.error = f"LLM request failed: {exc}"
                logger.error("Agent: LLM request failed at iteration %d: %s", iteration + 1, exc)
                return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            logger.error("Agent: timeout at iteration %d (limit=%ds)", iteration + 1, LLM_TIMEOUT)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            logger.error("Agent: request exception at iteration %d: %s", iteration + 1, exc)
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info("Iteration %d: LLM responded in %dms", iteration + 1, elapsed_ms)

        accumulate_usage(total_usage, llm_response.get("usage", {}))

        # Parse assistant message
        choice = llm_response.get("choices", [{}])[0]
        finish_reason = choice.get("finish_reason", "?")
        message = choice.get("message", {})
        tool_calls = message.get("tool_calls", [])
        has_content = bool(message.get("content"))
        logger.info(
            "Iteration %d: finish_reason=%s, tool_calls=%d, has_content=%s",
            iteration + 1, finish_reason, len(tool_calls), has_content,
        )
        if has_content:
            logger.debug(
                "Iteration %d: content preview (200 chars): %.200s",
                iteration + 1, message.get("content", ""),
            )

        if tool_calls:
            # ---- Agent wants to call tools ----
            logger.info(
                "Iteration %d: processing %d tool call(s): [%s]",
                iteration + 1, len(tool_calls),
                ", ".join(tc.get("function", {}).get("name", "?") for tc in tool_calls),
            )
            messages.append(message)

            for tc_idx, tc in enumerate(tool_calls, 1):
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                raw_args = func.get("arguments", "{}")
                tool_id = tc.get("id", "")

                logger.debug(
                    "Iteration %d tool_call %d/%d: name=%s, id=%s, raw_args=%.300s",
                    iteration + 1, tc_idx, len(tool_calls), tool_name, tool_id, raw_args,
                )

                try:
                    arguments = json.loads(raw_args)
                except json.JSONDecodeError as je:
                    logger.warning(
                        "Iteration %d: JSON decode error for tool '%s' args: %s",
                        iteration + 1, tool_name, je,
                    )
                    arguments = {}

                logger.info(
                    "Iteration %d: calling tool '%s' (%d/%d)",
                    iteration + 1, tool_name, tc_idx, len(tool_calls),
                )
                notify(f"Calling {tool_name}…")

                result.steps.append(AgentStep(
                    step_type="tool_call",
                    content=json.dumps(arguments),
                    tool_name=tool_name,
                ))

                t1 = time.time()
                tool_result = dispatch_tool(TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME)
                tool_ms = int((time.time() - t1) * 1000)

                logger.info(
                    "Iteration %d: tool '%s' returned %d chars in %dms",
                    iteration + 1, tool_name, len(tool_result), tool_ms,
                )
                logger.debug(
                    "Iteration %d: tool '%s' result preview: %.500s",
                    iteration + 1, tool_name, tool_result,
                )

                result.steps.append(AgentStep(
                    step_type="tool_result",
                    content=(tool_result[:500] + "…") if len(tool_result) > 500 else tool_result,
                    tool_name=tool_name,
                    duration_ms=tool_ms,
                ))

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": tool_result,
                })
            logger.info(
                "Iteration %d: all tool calls processed, conversation now has %d messages",
                iteration + 1, len(messages),
            )
        else:
            # ---- Agent produced a text response (should be OWL) ----
            content = extract_message_content(llm_response)
            starts_with_prefix = content.strip().startswith("@prefix")
            logger.info(
                "Iteration %d: agent produced final text output — %d chars, starts_with_@prefix=%s",
                iteration + 1, len(content), starts_with_prefix,
            )
            if not starts_with_prefix:
                logger.warning(
                    "Iteration %d: output does NOT start with @prefix — first 200 chars: %.200s",
                    iteration + 1, content.strip(),
                )
            logger.debug(
                "Iteration %d: full OWL output (%d chars):\n%s",
                iteration + 1, len(content), content[:2000],
            )

            result.steps.append(AgentStep(
                step_type="output",
                content=(content[:200] + "…") if len(content) > 200 else content,
                duration_ms=elapsed_ms,
            ))

            result.success = True
            result.owl_content = content
            result.iterations = iteration + 1
            result.usage = total_usage

            logger.info(
                "===== AGENT COMPLETE ===== iterations=%d, prompt_tokens=%d, completion_tokens=%d, owl_chars=%d",
                result.iterations,
                total_usage["prompt_tokens"],
                total_usage["completion_tokens"],
                len(content),
            )
            notify("Agent completed!")
            return result

    # Exhausted all iterations
    result.error = f"Agent reached maximum iterations ({MAX_ITERATIONS}) without producing output"
    logger.error(
        "===== AGENT FAILED ===== %s — total prompt_tokens=%d, completion_tokens=%d",
        result.error, total_usage["prompt_tokens"], total_usage["completion_tokens"],
    )
    return result
