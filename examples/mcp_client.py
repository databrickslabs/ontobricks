#!/usr/bin/env python3
"""Generic OntoBricks MCP client — works with any Databricks-hosted OntoBricks deployment.

Connects an LLM (via Databricks AI Gateway) to the OntoBricks MCP server.
No ANTHROPIC_API_KEY needed — a single Databricks OAuth token authenticates both.

Architecture
------------
Databricks Claude endpoint (OpenAI-compatible)
    ↕  tool calls / results (manual agent loop)
OntoBricks MCP server (streamable HTTP, Databricks auth)

Quick start
-----------
    export OBX_MCP_URL="https://<mcp-app>.<workspace>.databricksapps.com/mcp"
    export OBX_LLM_ENDPOINT="databricks-claude-haiku-4-5"
    export OBX_PROFILE="<your-cli-profile>"

    python3 mcp_client.py --domain <domain-name> -q "your question"

Optimizations applied (all on by default, individually opt-out-able)
----------------------------------------------------------------------
OPT1  Curated tool descriptions   shorter role-specific descriptions, fewer tool tokens
OPT2  Domain pre-selection        warms session before agent starts — skips 2 rounds
OPT3  Schema pre-load             fetches GraphQL schema once, injects into system prompt
OPT4  Result char cap             per-tool safety cap before context insertion
OPT5  Result summarization        parses entity-block format, keeps top-N by relevance;
                                  zero-score guard: falls back to char-cap when no
                                  question keyword matches (indirect references)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from typing import Any, Optional

import httpx

# ── Required environment variables ───────────────────────────────────────────

MCP_URL    = os.environ.get("OBX_MCP_URL", "")
ENDPOINT   = os.environ.get("OBX_LLM_ENDPOINT", "databricks-claude-haiku-4-5")
PROFILE    = os.environ.get("OBX_PROFILE", os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
DBRX_HOST  = os.environ.get("DATABRICKS_HOST", "")

# ── OPT 1: curated tool descriptions ─────────────────────────────────────────
# Replaces the full multi-paragraph docstrings exposed by the MCP server with
# concise action-oriented one-liners.  Reduces token use on every LLM call
# without losing routing accuracy.
_TOOL_DESCRIPTIONS: dict[str, str] = {
    "list_domains":
        "List all published knowledge-graph domains. Call once at start.",
    "select_domain":
        "Activate a domain by name. Must be called before any domain-scoped tool.",
    "list_domain_versions":
        "List versions of a domain (status, build date). Use when version matters.",
    "get_design_status":
        "Check ontology/mapping/build readiness for a domain.",
    "describe_ontology":
        "Return the domain's OWL structure (classes, attributes, relationships). "
        "Works without a built graph.",
    "list_entity_types":
        "Return all entity types with instance counts and predicate stats.",
    "describe_entity":
        "PRIMARY tool for any specific named item. "
        "BFS traversal — returns ALL attributes, relationships, and inferred triples. "
        "Pass search='<name>'. Keep depth=1 for type-wide scans.",
    "get_entity_context":
        "Return cross-domain bridges, linked dataset, virtual attributes and actions.",
    "compute_virtual_attributes":
        "Compute live (non-stored) virtual attributes for an entity via UC functions.",
    "invoke_entity_action":
        "Run a UC function action on an entity.",
    "get_status":
        "Diagnostics: triple count, view name, graph name.",
    "get_graphql_schema":
        "Return the GraphQL SDL. Skip if the schema is already in the system prompt.",
    "query_graphql":
        "Typed, filtered GraphQL query. "
        "Use for bulk queries on entity types. "
        "NOT for finding a specific named item — use describe_entity for that. "
        "Fields must match the SDL.",
}

# ── OPT 4 + 5: result sizing ──────────────────────────────────────────────────
# Hard caps (chars) per tool — final safety net after entity-level summarization.
_RESULT_CAPS: dict[str, int] = {
    "query_graphql":     3500,
    "get_graphql_schema": 6000,
    "list_entity_types":  2000,
    "describe_entity":    5000,
    "get_entity_context": 3000,
    "describe_ontology":  4000,
}
_DEFAULT_CAP = 8000

# OPT 5: max entity blocks to keep after relevance scoring
_MAX_ROWS: dict[str, int] = {
    "query_graphql":    8,
    "list_entity_types": 30,
    "describe_entity":   1,
}
_DEFAULT_MAX_ROWS = 10


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_token(profile: str) -> str:
    """Fetch a live Databricks OAuth token via the CLI."""
    out = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"ERROR: databricks auth token --profile {profile} failed:\n{out.stderr}")
    return json.loads(out.stdout)["access_token"]


# ── MCP client ────────────────────────────────────────────────────────────────

class MCPClient:
    """Minimal MCP client over streamable HTTP POST /mcp."""

    def __init__(self, url: str, token: str) -> None:
        self.url = url
        self._hdrs = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json, text/event-stream",
        }
        self._session_id: Optional[str] = None
        self._http = httpx.Client(timeout=120)

    def _send(self, method: str, params: dict | None = None) -> Any:
        msg: dict = {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method}
        if params:
            msg["params"] = params
        hdrs = dict(self._hdrs)
        if self._session_id:
            hdrs["Mcp-Session-Id"] = self._session_id

        with self._http.stream("POST", self.url, json=msg, headers=hdrs) as resp:
            if "Mcp-Session-Id" in resp.headers:
                self._session_id = resp.headers["Mcp-Session-Id"]
            data_lines, raw_lines = [], []
            for line in resp.iter_lines():
                line = line.strip()
                if line.startswith("data:") and line != "data:":
                    data_lines.append(line[5:].strip())
                elif line and not line.startswith(("event:", "id:", ":")):
                    raw_lines.append(line)

        for candidate in reversed(data_lines or raw_lines):
            try:
                parsed = json.loads(candidate)
                if "error" in parsed:
                    raise RuntimeError(f"MCP error: {parsed['error']}")
                return parsed.get("result")
            except json.JSONDecodeError:
                continue
        return None

    def initialize(self) -> None:
        self._send("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities":    {},
            "clientInfo":      {"name": "obx-client", "version": "1.0"},
        })
        try:
            hdrs = dict(self._hdrs)
            if self._session_id:
                hdrs["Mcp-Session-Id"] = self._session_id
            self._http.post(
                self.url,
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers=hdrs,
            )
        except Exception:
            pass

    def list_tools(self) -> list[dict]:
        return (self._send("tools/list") or {}).get("tools", [])

    def call_tool(self, name: str, arguments: dict) -> str:
        result = self._send("tools/call", {"name": name, "arguments": arguments})
        if not result:
            return "(no result)"
        parts = [i["text"] for i in result.get("content", []) if i.get("type") == "text"]
        return "\n".join(parts) if parts else json.dumps(result)

    def close(self) -> None:
        self._http.close()


# ── LLM (Databricks, OpenAI-compatible) ──────────────────────────────────────

class DatabricksLLM:
    """Thin wrapper around a Databricks serving endpoint in OpenAI chat format."""

    def __init__(self, host: str, endpoint: str, token: str) -> None:
        self.url = f"{host.rstrip('/')}/serving-endpoints/{endpoint}/invocations"
        self._hdrs = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }
        self.model = endpoint
        self._http = httpx.Client(timeout=180)

    def chat(self, messages: list[dict], tools: list[dict] | None = None) -> dict:
        body: dict[str, Any] = {
            "model":      self.model,
            "messages":   messages,
            "max_tokens": 4096,
        }
        if tools:
            body["tools"] = tools
            body["tool_choice"] = "auto"
        resp = self._http.post(self.url, json=body, headers=self._hdrs)
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        self._http.close()


# ── OPT 1: tools → OpenAI format with curated descriptions ───────────────────

def tools_to_openai(mcp_tools: list[dict]) -> list[dict]:
    result = []
    for t in mcp_tools:
        desc = _TOOL_DESCRIPTIONS.get(t["name"]) or (
            next((l for l in t.get("description", "").splitlines() if l.strip()), "")[:200]
        )
        result.append({
            "type": "function",
            "function": {
                "name":        t["name"],
                "description": desc,
                "parameters":  t.get("inputSchema", {"type": "object", "properties": {}}),
            },
        })
    return result


# ── OPT 2 + 3: domain warm-up ─────────────────────────────────────────────────

def warm_domain(mcp: MCPClient, domain: str, preload_schema: bool = True) -> Optional[str]:
    """
    OPT 2 — pre-select the domain so the agent skips list_domains +
    select_domain (saves ~2 rounds, ~1 200 tokens per question).

    OPT 3 — optionally fetch the GraphQL schema once and return it for
    injection into the system prompt, eliminating get_graphql_schema calls.
    """
    mcp.call_tool("select_domain", {"domain_name": domain})
    if preload_schema:
        return mcp.call_tool("get_graphql_schema", {})
    return None


# ── OPT 4 + 5: result summarization ──────────────────────────────────────────

def _score_block(block: str, tokens: set[str]) -> float:
    if not tokens:
        return 0.0
    bl = block.lower()
    return sum(1.0 for t in tokens if t in bl)


def _summarize_graphql(text: str, question: str, max_rows: int) -> str:
    """
    OPT 5 — parse the OntoBricks GraphQL text format into entity blocks,
    score by question-keyword overlap, keep top-N most relevant.

    Zero-score guard: when no block matches any question keyword (e.g. entity
    stores a foreign-key ID rather than the queried name), skip entity-level
    filtering — falling back to the char cap preserves the data.

    The format parsed here is produced by OntoBricks' ``_format_graphql_response``:
        GraphQL Result — <domain>
        ==================================================
        <FieldName> (<N> results)
        ----------------------------------------
          key: value
          ...
          (blank line between entities)
    """
    sep = "=" * 50
    header, _, body = text.partition(sep) if sep in text else ("", "", text)

    section_pat = re.compile(r"^([A-Za-z][A-Za-z0-9_ ]*)\s+\((\d+)\s+results?\)", re.MULTILINE)
    sections = list(section_pat.finditer(body))
    if not sections:
        return text

    q_tokens = {w.lower() for w in re.split(r"\W+", question) if len(w) > 3}
    parts = [header.strip(), sep]

    for idx, sec in enumerate(sections):
        field = sec.group(1).strip()
        total = int(sec.group(2))
        start = sec.end()
        end = sections[idx + 1].start() if idx + 1 < len(sections) else len(body)
        raw = re.sub(r"^-{10,}\s*\n?", "", body[start:end].strip())
        blocks = [b.strip() for b in re.split(r"\n\s*\n", raw) if b.strip()]

        if total <= max_rows or not blocks:
            kept = blocks
            suffix = ""
        else:
            scored = sorted(
                enumerate(blocks),
                key=lambda x: (_score_block(x[1], q_tokens), -x[0]),
                reverse=True,
            )
            best = _score_block(scored[0][1], q_tokens) if scored else 0
            if best == 0:
                # No keyword overlap — can't rank; keep all to avoid data loss
                kept = blocks
                suffix = ""
            else:
                kept_idxs = sorted(i for i, _ in scored[:max_rows])
                kept = [blocks[i] for i in kept_idxs]
                suffix = f"  [showing {len(kept)} of {total}]"

        parts.append(f"\n{field} ({total} results){suffix}")
        parts.append("-" * 40)
        parts.append("\n\n".join(kept))

    return "\n".join(parts)


def compress_result(tool_name: str, text: str, question: str = "", enabled: bool = True) -> str:
    """
    OPT 4 + 5 — entity-block summarization followed by a hard char cap.

    Summarization only activates for query_graphql results with more entity
    blocks than max_rows; other tools go straight to the cap.
    """
    if not enabled:
        return text

    if tool_name == "query_graphql" and len(text) > 2000:
        max_rows = _MAX_ROWS.get(tool_name, _DEFAULT_MAX_ROWS)
        m = re.search(r"\((\d+)\s+results?\)", text)
        total = int(m.group(1)) if m else 0
        if total > max_rows:
            text = _summarize_graphql(text, question, max_rows)

    cap = _RESULT_CAPS.get(tool_name, _DEFAULT_CAP)
    if len(text) > cap:
        text = text[:cap] + f"\n… [capped at {cap} of {len(text)} chars]"
    return text


# ── System prompt builder ─────────────────────────────────────────────────────

def build_system_prompt(domain: Optional[str], graphql_schema: Optional[str]) -> str:
    """
    OPT 2 + 3 contributions:
    - Domain pre-selection removes list_domains + select_domain guidance
    - Injected schema removes get_graphql_schema calls entirely
    """
    parts = ["You are a data analyst with access to a knowledge graph via tools."]

    if domain:
        parts.append(
            f"\nDomain already selected: **{domain}**. "
            "Do NOT call list_domains or select_domain — the session is ready."
        )
    else:
        parts.append("\nWorkflow: (1) list_domains, (2) select_domain, (3) query.")

    if graphql_schema:
        parts.append(
            f"\nGraphQL schema (do NOT call get_graphql_schema — already loaded):\n"
            f"```graphql\n{graphql_schema}\n```"
        )

    parts.append(
        "\nRules:"
        "\n- For a SPECIFIC named item: use describe_entity(search='<name>') "
        "— BFS traversal returns all linked data query_graphql cannot see."
        "\n- For BULK queries (all items of a type matching a condition): use query_graphql."
        "\n- Be concise and data-driven. Show actual values."
    )
    return "\n".join(parts)


# ── Agent loop ────────────────────────────────────────────────────────────────

def ask(
    question: str,
    mcp: MCPClient,
    llm: DatabricksLLM,
    tools_raw: list[dict],
    *,
    domain: Optional[str] = None,
    graphql_schema: Optional[str] = None,
    compress: bool = True,
) -> str:
    """Run a question through the optimized agent loop."""
    tools_oi = tools_to_openai(tools_raw)
    messages: list[dict] = [
        {"role": "system", "content": build_system_prompt(domain, graphql_schema)},
        {"role": "user",   "content": question},
    ]

    for _ in range(12):
        resp = llm.chat(messages, tools=tools_oi)
        choice = resp["choices"][0]
        msg = choice["message"]
        tc_list = msg.get("tool_calls") or []

        if choice.get("finish_reason") == "stop" or not tc_list:
            raw = msg.get("content") or "(no response)"
            if isinstance(raw, list):
                return "\n".join(
                    b.get("text", "") for b in raw
                    if isinstance(b, dict) and b.get("type") == "text"
                ).strip() or "(no response)"
            return str(raw) if raw else "(no response)"

        # Strip extended-thinking reasoning blocks before appending to history
        raw_c = msg.get("content")
        clean_c: Any = (
            [b for b in raw_c if isinstance(b, dict) and b.get("type") == "text"] or None
            if isinstance(raw_c, list) else raw_c
        )
        messages.append({"role": "assistant", "content": clean_c, "tool_calls": tc_list})

        tool_results = []
        for tc in tc_list:
            fn = tc["function"]
            args = json.loads(fn.get("arguments", "{}") or "{}")
            result = mcp.call_tool(fn["name"], args)
            result = compress_result(fn["name"], result, question=question, enabled=compress)
            tool_results.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result,
            })
        messages.extend(tool_results)

    return "(max rounds reached)"


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="OntoBricks MCP client")
    ap.add_argument("-q", "--question", help="Question to ask")
    ap.add_argument("--list",   action="store_true",  help="List MCP tools and exit")
    ap.add_argument("--domain", default=None,         help="Pre-select domain (OPT 2+3)")
    ap.add_argument("--no-preload-schema", dest="preload_schema", action="store_false", default=True)
    ap.add_argument("--no-compress",       dest="compress",       action="store_false", default=True)
    ap.add_argument("--no-opt-tools",      dest="opt_tools",      action="store_false", default=True)
    ap.add_argument("--endpoint", default=ENDPOINT)
    ap.add_argument("--profile",  default=PROFILE)
    ap.add_argument("--host",     default=DBRX_HOST)
    args = ap.parse_args()

    if not args.host:
        sys.exit("ERROR: set DATABRICKS_HOST env var or pass --host")
    if not MCP_URL:
        sys.exit("ERROR: set OBX_MCP_URL env var")

    token = get_token(args.profile)
    mcp = MCPClient(MCP_URL, token)
    try:
        mcp.initialize()
    except Exception:
        pass
    tools = mcp.list_tools()
    print(f"Connected — {len(tools)} tools, endpoint: {args.endpoint}")

    schema: Optional[str] = None
    if args.domain:
        schema = warm_domain(mcp, args.domain, preload_schema=args.preload_schema)
        print(f"Domain '{args.domain}' warmed (schema {len(schema or '')} chars)")

    if args.list:
        for t in tools_to_openai(tools):
            print(f"  {t['function']['name']:35} {t['function']['description'][:70]}")
        mcp.close()
        return

    llm = DatabricksLLM(args.host, args.endpoint, token)
    questions = [args.question] if args.question else []
    if not questions:
        ap.print_help()
        mcp.close()
        llm.close()
        return

    try:
        for q in questions:
            print(f"\n{'─'*70}\n{q}\n{'─'*70}")
            print(ask(q, mcp, llm, tools,
                      domain=args.domain, graphql_schema=schema,
                      compress=args.compress))
    finally:
        mcp.close()
        llm.close()


if __name__ == "__main__":
    main()
