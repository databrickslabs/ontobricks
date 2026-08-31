# Graph Chat ↔ MCP Alignment (Datasets, Bridges, Actions) — Design Spec

**Date:** 2026-08-06  
**Version target:** v0.7.0  
**Status:** Draft — awaiting user review  
**Related:** `documentation/superpowers/specs/2026-07-27-mcp-node-context-design.md`, `.planning/agents/agent_dtwin_chat/SPEC.md`

---

## 1. Goal

Bring Graph Chat (`agent_dtwin_chat`) to parity with MCP for ontology-design **Datasets**, **Bridges**, and **Actions**, while preserving Graph Chat’s session-aware unpublished-domain behavior and requiring an explicit Confirm/Cancel UI before any Action executes.

---

## 2. Scope

**In scope**

- Session-aware internal routes that reuse `NodeContextService` (no duplicated business logic)
- Graph Chat tools: `get_entity_context`, `request_entity_action` (propose only)
- Enrich `list_entity_types` / `describe_entity` with Dataset / Bridge / Action metadata
- Shared formatters for context blocks (agent-side; MCP wheel may keep its copy)
- Confirm/Cancel action card in Graph Chat UI + one-time pending token
- Fill / update AI-feature artifacts (SPEC, ≥10 eval examples, runner/thresholds as needed)

**Out of scope**

- Changing MCP’s direct `invoke_entity_action` (MCP clients remain unattended)
- Bridge traversal depth > 1
- Dashboard / dashboardParams Actions
- Making unpublished bridge *targets* resolvable (still registry-only, same as today)

---

## 3. Decisions (locked)

| Decision | Choice |
|---|---|
| Architecture | Session-aware internal `/dtwin/nodes/*` + shared `NodeContextService` |
| Action policy | Explicit user confirmation before every Action |
| Confirmation UX | Structured Confirm / Cancel controls in chat (not typed “yes”) |
| Domain fidelity | Source domain from live session (unpublished OK); bridge targets remain published-registry |

---

## 4. Architecture

```
User (Graph Chat UI)
        │
        ▼
POST /dtwin/assistant/chat[/stream]
        │
        ▼
agent_dtwin_chat (tools over loopback)
   list_entity_types / describe_entity  ──► enriched with class Actions metadata
   get_entity_context                   ──► GET  /dtwin/nodes/context
   request_entity_action                ──► POST /dtwin/nodes/action/request
        │                                         │
        │                                         ▼
        │                              NodeContextService
        │                              (resolve_context / allow-list check)
        │
        ▼
SSE / reply may include structured pending_action
        │
        ▼
UI Confirm / Cancel card
        │ Confirm
        ▼
POST /dtwin/nodes/action/confirm  ──► NodeContextService.invoke_action
```

### 4.1 New / extended internal routes

All resolve the **session** domain via `get_domain(session_mgr)` (same pattern as `/dtwin/triples/find`).

| Route | Role |
|---|---|
| `GET /dtwin/nodes/context` | Same query params semantics as public `GET /api/v1/digitaltwin/nodes/context`; calls `NodeContextService.resolve_context` |
| `GET /dtwin/classes` | Lightweight class Actions list for the session domain (`dataset` / `bridges` / `actions` only) — cache source for enrichment |
| `POST /dtwin/nodes/action/request` | Validate entity + allow-listed action; mint one-time pending token; **do not** invoke UC |
| `POST /dtwin/nodes/action/confirm` | Consume token; invoke via `NodeContextService.invoke_action` |

Public `/api/v1/digitaltwin/nodes/context` and `/nodes/action` remain unchanged for MCP.

### 4.2 Agent tool surface (delta)

Existing tools keep working. Add / change:

| Tool | Behavior |
|---|---|
| `list_entity_types` | Append Dataset / Actions lines per type when class cache has them (MCP parity) |
| `describe_entity` | Append `[Context]` block via shared formatter when class has dataset/bridges/actions |
| `get_entity_context` | Mirror MCP params: `entity_uri`, `fetch_dataset_rows`, `dataset_row_limit`, `follow_bridges` → `/dtwin/nodes/context` |
| `request_entity_action` | `entity_uri` + `action` (fullName). Returns human text **and** a structured `pending_action` payload for the UI. Never executes. |

There is **no** `invoke_entity_action` tool on Graph Chat. Execution is only via the confirm route triggered by the UI.

### 4.3 Formatters

Extend `src/agents/tools/graph_formatting.py` with:

- `format_class_context_block`
- `format_node_context_response`
- `format_node_action_response`
- `format_find_response(..., class_actions=...)`

Text semantics match MCP’s blocks in `src/mcp-server/server/app.py`. The MCP wheel may keep its independent copy (isolated packaging); this change does not require shipping the shared module into the MCP wheel.

### 4.4 Class-actions cache

Add `dtwin_class_actions: dict` on `ToolContext`. Lazily populate from `GET /dtwin/classes` (once per turn / context lifetime), keyed by class URI → `{name, dataset, bridges, actions}`.

### 4.5 Pending-action token

On successful `request_entity_action`:

1. Server stores under the Graph Chat session bucket a pending entry:  
   `{token, domain, entity_uri, action_full_name, expires_at, used=false}`
2. Agent result / SSE `done` event includes:

```json
{
  "pending_action": {
    "token": "<opaque>",
    "entity_uri": "...",
    "entity_label": "...",
    "action": "catalog.schema.fn",
    "description": "...",
    "expires_in_sec": 120
  }
}
```

3. UI renders Confirm / Cancel.
4. Confirm → `POST /dtwin/nodes/action/confirm` with `{token}`; server verifies session + unused + unexpired + domain match + allow-list, then invokes once and marks used.
5. Cancel / expiry / mismatch → discard; no UC call.

Token TTL: **120 seconds**. Tokens are single-use.

### 4.6 System prompt delta

Extend `SYSTEM_PROMPT` to:

- Treat Dataset / Bridge / Action metadata in `[Context]` as authoritative ontology design (not inventable).
- Prefer `get_entity_context` when the user asks for table rows or cross-domain bridge data.
- For Actions: call `request_entity_action` only when the user asks to run one; tell the user confirmation is required in the UI; never claim the Action already ran.

---

## 5. Data flow

1. User asks about an entity → `describe_entity` (unchanged primary path).
2. If the class has Dataset / Bridges / Actions, response includes a `[Context]` block (MCP-equivalent).
3. Rows / bridge entities → `get_entity_context` → `GET /dtwin/nodes/context` → `NodeContextService.resolve_context`.
4. Action → `request_entity_action` → mint pending token → UI Confirm / Cancel card.
5. Confirm → `POST /dtwin/nodes/action/confirm` → `NodeContextService.invoke_action` → result appended as an assistant message in chat.
6. Cancel / expiry / reuse → no execution.

**Known constraint:** bridge *target* domains resolve via `DigitalTwin.resolve_domain` (registry). Unpublished targets are skipped with a note (same soft-failure model as MCP/backend today).

---

## 6. Error handling

| Situation | Behavior |
|---|---|
| Class has no Dataset / Bridges / Actions | Omit those sections; behave like today’s Graph Chat |
| Dataset present but no `key_column` | Metadata only; note rows cannot be fetched |
| Dataset SQL / bridge traversal fails | Soft failure: partial context + `message`; turn continues |
| Bridge target unpublished / inaccessible | Skip that bridge; note in response |
| Action not on class allow-list | Reject at request and confirm; no token / no execution |
| Confirm with expired / used / wrong-session token | 4xx; chat shows “Action expired — request again” |
| User cancels | Discard pending token; no UC call |
| No Databricks client / view-only constraints | Metadata OK; row fetch and confirm fail with a clear message |

---

## 7. Frontend

**File:** `src/front/static/query/js/query-chat.js` (+ minimal CSS if needed)

- On stream `done` (or blocking reply) with `pending_action`, render an inline card under the assistant bubble: action name, entity label, Confirm / Cancel.
- Confirm: `POST /dtwin/nodes/action/confirm` with the token; append success/error as an assistant message; disable the card.
- Cancel: clear the card; optional best-effort cancel endpoint or client-only discard (server TTL still expires the token).
- Do not auto-confirm. Do not treat typed chat “yes” as confirmation.

---

## 8. Testing & eval

### 8.1 Unit

- Internal `/dtwin/nodes/context` and action request/confirm routes (session domain).
- Tool handlers via existing `httpx.MockTransport` pattern in `tests/units/agents/test_agent_dtwin_chat.py`.
- Formatter parity for `[Context]` / node context / action result text.
- Pending-token lifecycle: mint → confirm once → reject reuse / expiry / wrong session.

### 8.2 Frontend

- Confirm/Cancel card renders from `pending_action`.
- Confirm posts token; Cancel clears UI without calling invoke.

### 8.3 AI-feature gate (mandatory)

Material tool + prompt change under `src/agents/**`:

1. Update `.planning/agents/agent_dtwin_chat/SPEC.md` (tool surface, success criteria, failure modes, eval dimensions).
2. Expand `tests/eval/datasets/agent_dtwin_chat/baseline.jsonl` to **≥ 10** examples covering: discovery, dataset rows, bridges, action request (no auto-invoke), adversarial “don’t invent actions”.
3. Add / wire `tests/eval/run_agent_dtwin_chat.py` and thresholds as required by the gate.
4. Record MLflow eval run URI in the PR body.

---

## 9. Files to create or modify

| File | Change |
|---|---|
| `src/api/routers/internal/dtwin.py` | `/nodes/context`, `/classes`, `/nodes/action/request`, `/nodes/action/confirm` |
| `src/agents/agent_dtwin_chat/tools.py` | New tools + enrichment of list/describe |
| `src/agents/agent_dtwin_chat/engine.py` | System prompt + surface `pending_action` on result |
| `src/agents/tools/graph_formatting.py` | Shared context/action formatters |
| `src/agents/tools/context.py` | `dtwin_class_actions` cache field |
| `src/front/static/query/js/query-chat.js` | Confirm/Cancel card |
| `.planning/agents/agent_dtwin_chat/SPEC.md` | Fill material-change sections |
| `tests/units/agents/test_agent_dtwin_chat.py` | New tool / formatter tests |
| `tests/units/api/...` | Internal route tests as needed |
| `tests/eval/datasets/agent_dtwin_chat/*` | Eval dataset expansion |
| `tests/eval/run_agent_dtwin_chat.py` | Eval runner (if missing) |

---

## 10. Success criteria

- Graph Chat discovers Dataset / Bridge / Action metadata the same way MCP does for a configured class.
- `get_entity_context` returns dataset rows and/or bridge entities for session domains.
- Actions never execute without a Confirm click; Cancel / expiry never invoke UC.
- Unpublished source domains continue to work for context (except published bridge targets).
- Unit tests green; AI-feature SPEC + eval artifacts present for the PR.

---

## 11. Non-goals / deferred

- Unifying MCP wheel formatters onto the shared `graph_formatting` module (optional follow-up).
- Viewer write-gate policy for Actions on the public API (tracked separately via view-mode write gates).
- Typed natural-language confirmation as an alternative to the button card.
