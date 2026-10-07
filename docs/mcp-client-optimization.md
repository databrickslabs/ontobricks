# OntoBricks MCP Client — Optimization Guide

Five optimizations that cut latency by **51%** and token use by **52%** without any
loss of answer quality, verified on a Databricks-hosted Claude deployment (Haiku 4.5).

See [`examples/mcp_client.py`](../examples/mcp_client.py) for a reference implementation
and [`examples/mcp_bench.py`](../examples/mcp_bench.py) to reproduce the numbers.

---

## Why it matters

An MCP client talking to OntoBricks runs an agentic loop: the LLM calls tools,
receives results, and reasons over them across several rounds. Without optimizations,
a single question can:

- Spend **2 of its first 3 rounds** just warming up (discovering domains and selecting one)
- Re-fetch the GraphQL schema on **every question** even though it never changes
- Append raw tool results of **14–76k chars** to the growing message history
- Reach a **34:1 prompt:completion token ratio** — 34 tokens in for every 1 out

On six representative questions against `mercuriatrading` (Databricks Claude Haiku 4.5):

| Metric | Baseline | Optimized | Δ |
|--------|----------|-----------|---|
| avg total time | 17 022 ms | **8 379 ms** | −51 % |
| avg rounds | 5.5 | **2.8** | −49 % |
| prompt tokens | 251 798 | **120 357** | −52 % |
| completion tokens | 7 437 | 3 450 | −54 % |
| LLM time | 83 630 ms | 41 031 ms | −51 % |
| MCP time | 18 503 ms | 9 240 ms | −50 % |

---

## OPT 1 — Curated tool descriptions

**Problem.** The MCP server exposes full multi-paragraph docstrings on each tool.
Every LLM call includes the complete tool-definition block in the prompt. With 13
tools and long descriptions, this adds ~2 000 tokens to every round. Multiplied
across 5.5 avg rounds and 6 questions, that's ~66 000 tokens wasted on routing
information the model already internalized after round 1.

**Fix.** Replace each tool's description with a single action-oriented sentence.
Routing accuracy is unchanged — the key verbs are still there.

```python
_TOOL_DESCRIPTIONS = {
    "describe_entity":
        "PRIMARY tool for any specific named item. "
        "BFS traversal — returns ALL attributes, relationships, and inferred triples. "
        "Pass search='<name>'.",
    "query_graphql":
        "Typed, filtered GraphQL query. Use for bulk queries on entity types. "
        "NOT for finding a specific named item — use describe_entity for that.",
    # ... one line per tool
}
```

**Gain.** Reduces per-call prompt tokens by ~15–20 %. Effect compounds with more rounds.

---

## OPT 2 — Domain pre-selection

**Problem.** Every question starts with the same two-round warm-up:

```
Round 1: list_domains  →  "mercuriatrading, mercuriamarkets, ..."
Round 2: select_domain →  "Domain selected. 984 000 triples."
```

These rounds add ~1.5 s and ~1 200 tokens to every question even when the target
domain is known at client start-up.

**Fix.** Call `select_domain` once before the first question. Tell the system prompt
to skip the warm-up.

```python
def warm_domain(mcp, domain, preload_schema=True):
    mcp.call_tool("select_domain", {"domain_name": domain})
    if preload_schema:
        return mcp.call_tool("get_graphql_schema", {})  # OPT 3
    return None
```

```
# System prompt when domain is pre-selected:
"Domain already selected: **mercuriatrading**.
Do NOT call list_domains or select_domain — the session is ready."
```

**Gain.** −2 rounds per question (5.5 → 2.8 avg), −~1.5 s, −~1 200 tokens.

---

## OPT 3 — GraphQL schema pre-load

**Problem.** `get_graphql_schema` (avg 490 ms, ~9 859 chars) was called on 4 of 6
questions in the baseline. Each call adds ~2 000 tokens to that round's context and
takes another network round-trip to the MCP server.

**Fix.** Fetch the schema once at startup (as part of `warm_domain`). Inject it into
the system prompt. Tell the model not to call the tool.

```
# System prompt injection:
"GraphQL schema (do NOT call get_graphql_schema — already loaded):
```graphql
type CreditExposure {
  id: ID!
  counterpartyId: String
  utilizationPct: String
  ...
}
```"
```

**Gain.** `get_graphql_schema` calls drop from 4 to 0, saving ~490 ms × N calls and
~9 859 chars × N from message history growth.

---

## OPT 4 — Per-tool result cap

**Problem.** Raw tool results can be very large: `query_graphql` averaged 14 166 chars
(worst case 76 770 chars) per call. At 8 rounds, that fills message history with ~100k
chars of serialized data most of which repeats context the model already has.

**Fix.** Apply a hard character cap per tool before appending to message history.

```python
_RESULT_CAPS = {
    "query_graphql":      3 500,   # was avg 14 166 chars
    "get_graphql_schema":  6 000,   # was avg  9 859 chars
    "list_entity_types":   2 000,   # was avg  6 247 chars
    "describe_entity":     5 000,
}
```

**Gain.** Each capped call reduces its contribution to subsequent-round prompt tokens.
Cumulative effect on a 6-round question: −60–80k chars of context.

---

## OPT 5 — Entity-block summarization

**Problem.** A char cap cuts at an arbitrary byte boundary — it may truncate the middle
of an entity block or keep 8 irrelevant entities and discard the one that matters.

**Fix.** Parse the OntoBricks `_format_graphql_response` entity-block structure, score
each block by keyword overlap with the question, keep the top-N most relevant, then
apply the char cap as a safety net.

The format produced by OntoBricks is:
```
GraphQL Result — <domain>
==================================================
<TypeName> (<N> results)
----------------------------------------
  key: value
  key: value

  key: value         ← next entity, blank-line separated
  ...
```

```python
def _summarize_graphql(text, question, max_rows=8):
    # 1. Split into entity blocks (blank-line separated)
    # 2. Score each block: count question keywords that appear in the block
    # 3. Keep top max_rows by score; preserve original order within tied scores
    # 4. Zero-score guard: if NO block matches any keyword (indirect FK
    #    references, e.g. counterpartyId vs counterparty name), skip
    #    entity-level filtering — fall back to char cap only
    ...
```

**Zero-score guard** is the critical correctness property. Without it, a question
about "Vitol" would score 0 on all `CreditExposure` blocks (which store
`counterpartyId: CP_XXX` not the name) and cut the right data. The guard ensures
the summarizer is conservative: it only filters when it can determine relevance.

**Gain.** `query_graphql` results compressed from avg 14 166 to ~2 500 chars, with
the most relevant entities retained. Token ratio improves from 34:1 to ~31:1.

---

## Using the reference implementation

```bash
# Install deps (httpx only)
pip install httpx

# Environment
export OBX_MCP_URL="https://mcp-<id>.<workspace>.databricksapps.com/mcp"
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export OBX_LLM_ENDPOINT="databricks-claude-haiku-4-5"   # or sonnet-5
export OBX_PROFILE="default"

# Ask a question with all optimizations (default)
python3 examples/mcp_client.py --domain <domain-name> -q "your question"

# Disable specific optimizations for comparison
python3 examples/mcp_client.py --domain <name> --no-compress -q "..."

# Benchmark: baseline vs optimized
python3 examples/mcp_bench.py --baseline --output before.json \
    --questions "What entity types exist?" "Show me 5 recent trades"
python3 examples/mcp_bench.py --domain <name> --output after.json \
    --questions "What entity types exist?" "Show me 5 recent trades"
```

---

## Which optimization to apply when

| Situation | Apply |
|-----------|-------|
| Always | OPT 1 (curated descriptions) |
| Domain known at client start | OPT 2 + 3 (pre-select + schema preload) |
| Results are large (ratio > 15:1) | OPT 4 + 5 (cap + summarization) |
| Multiple questions in one session | OPT 2 + 3 are especially valuable (one-time cost) |
| Single query, domain unknown | OPT 1 + 4 + 5 only |
| Agent with `sonnet-5` (extended thinking) | Strip `reasoning` blocks from assistant messages before appending to history (see `mcp_client.py` `ask()`) |

---

## Notes on extended-thinking models

Databricks Claude Sonnet 5 returns `content` as a list of blocks:

```json
[{"type": "reasoning", "summary": [...]}, {"type": "text", "text": "..."}]
```

The `reasoning` blocks must be stripped before appending the assistant message to
history — they are internal to the model and will cause errors if echoed back.
The reference client handles this:

```python
raw_c = msg.get("content")
clean_c = (
    [b for b in raw_c if isinstance(b, dict) and b.get("type") == "text"] or None
    if isinstance(raw_c, list) else raw_c
)
messages.append({"role": "assistant", "content": clean_c, "tool_calls": tc_list})
```
