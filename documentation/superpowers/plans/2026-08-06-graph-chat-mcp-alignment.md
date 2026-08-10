# Graph Chat ↔ MCP Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align Graph Chat (`agent_dtwin_chat`) with MCP for ontology Datasets, Bridges, and Actions — session-aware routes, enriched discovery tools, `get_entity_context`, and Confirm/Cancel Action execution.

**Architecture:** Internal `/dtwin/nodes/*` and `/dtwin/classes` routes resolve the live session domain and delegate to existing `NodeContextService`. Graph Chat tools call those routes over loopback. Actions use a two-phase request → UI Confirm/Cancel → confirm route. Shared formatters live in `agents/tools/graph_formatting.py`.

**Tech Stack:** Python / FastAPI / Pydantic, agent tool loop (`engine_base`), httpx loopback, vanilla JS Graph Chat UI, pytest + `httpx.MockTransport`, AI-feature eval artifacts.

**Spec:** `documentation/superpowers/specs/2026-08-06-graph-chat-mcp-alignment-design.md`

## Global Constraints

- Python ≥ 3.10; always run tests with `uv run --frozen pytest …` (never bare `uv run`)
- Source domain = session (unpublished OK); bridge *targets* still registry-published
- Graph Chat must **not** expose `invoke_entity_action`; only `request_entity_action` + confirm route
- Pending Action token TTL = 120s, single-use, session-bound
- Material agent change → update `.planning/agents/agent_dtwin_chat/SPEC.md` + ≥10 eval examples
- Changelog under `changelogs/v0.7.0/` after implementation (version from `pyproject.toml`)
- No MCP wheel packaging changes required (MCP keeps its own formatters)

---

### Task 1: Shared context/action formatters

**Files:**
- Modify: `src/agents/tools/graph_formatting.py`
- Create: `tests/units/agents/test_graph_formatting_context.py`

**Interfaces:**
- Produces:
  - `format_class_context_block(local_id: str, cls_actions: dict, *, action_invoke_hint: str = "…") -> str`
  - `format_node_context_response(data: dict, *, action_invoke_hint: str | None = None) -> str`
  - `format_node_action_response(data: dict) -> str`
  - `format_find_response(data, ontology_labels=None, class_actions=None, *, action_invoke_hint=...) -> str`
- Consumed by: Task 4 (agent tools)

- [ ] **Step 1: Write failing tests**

```python
# tests/units/agents/test_graph_formatting_context.py
from agents.tools.graph_formatting import (
    format_class_context_block,
    format_find_response,
    format_node_action_response,
    format_node_context_response,
)

def test_class_context_includes_dataset_bridges_actions():
    text = format_class_context_block(
        "CUST1",
        {
            "name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
            "bridges": [{"target_domain": "Finance", "target_class_name": "Contract", "label": "Owns"}],
            "actions": [{"fullName": "main.ops.recompute_risk", "description": "Risk"}],
        },
        action_invoke_hint="call request_entity_action(entity_uri, action) to propose one",
    )
    assert "Dataset: main.crm.customers" in text
    assert "Finance / Contract" in text
    assert "main.ops.recompute_risk" in text
    assert "request_entity_action" in text

def test_class_context_empty_when_no_actions():
    assert format_class_context_block("x", {"name": "A"}) == ""

def test_format_find_appends_context_for_typed_entity():
    data = {
        "success": True,
        "seed_count": 1,
        "depth": 1,
        "total": 1,
        "triples": [
            {
                "subject": "https://ex/Customer/CUST1",
                "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "object": "https://ex/Customer",
            },
            {
                "subject": "https://ex/Customer/CUST1",
                "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
                "object": "Cust One",
            },
        ],
    }
    class_actions = {
        "https://ex/Customer": {
            "name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
            "bridges": [],
            "actions": [],
        }
    }
    text = format_find_response(data, class_actions=class_actions)
    assert "[Context — class: Customer]" in text
    assert "main.crm.customers" in text

def test_node_context_and_action_formatters():
    ctx = format_node_context_response(
        {
            "success": True,
            "entity_uri": "https://ex/Customer/CUST1",
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id", "rows": [{"id": "CUST1"}]},
            "bridges": None,
            "actions": [{"fullName": "main.ops.recompute_risk", "description": "Risk"}],
        },
        action_invoke_hint="call request_entity_action(...)",
    )
    assert "Rows (1):" in ctx
    assert "request_entity_action" in ctx
    act = format_node_action_response(
        {"success": True, "action": "main.ops.recompute_risk", "entity_local_id": "CUST1",
         "class_name": "Customer", "rows": [{"result": 1}]}
    )
    assert "main.ops.recompute_risk" in act
    assert "result: 1" in act
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
uv run --frozen pytest -q tests/units/agents/test_graph_formatting_context.py
```

Expected: import / missing function failures.

- [ ] **Step 3: Implement formatters**

Port semantics from `src/mcp-server/server/app.py` (`_format_class_context_block`, `_format_node_context_response`, `_format_node_action_response`). Differences:

- Parameterize the Action next-step line via `action_invoke_hint`.
- In `format_find_response`, after each seed entity block, if `class_actions` is provided, resolve `rdf:type` object URI against `class_actions` and append `format_class_context_block(local_name(uri), …)`.

Default `action_invoke_hint` for class/context blocks:

```text
call request_entity_action(entity_uri, action) to propose one (UI confirmation required)
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
uv run --frozen pytest -q tests/units/agents/test_graph_formatting_context.py
```

- [ ] **Step 5: Commit** (only if user asked to commit; otherwise skip)

---

### Task 2: ToolContext class-actions + pending_action fields

**Files:**
- Modify: `src/agents/tools/context.py`
- Modify: `src/agents/agent_dtwin_chat/engine.py` (`AgentResult`)

**Interfaces:**
- Produces:
  - `ToolContext.dtwin_class_actions: dict` (class URI → metadata)
  - `ToolContext.pending_action: dict | None`
  - `AgentResult.pending_action: dict | None`
- Consumed by: Tasks 4–6

- [ ] **Step 1: Write a small failing unit assertion**

Extend `tests/units/agents/test_agent_dtwin_chat_engine.py`:

```python
from agents.agent_dtwin_chat.engine import AgentResult
from agents.tools.context import ToolContext

def test_agent_result_and_context_carry_pending_action():
    ctx = ToolContext(host="h", token="t")
    assert ctx.dtwin_class_actions == {}
    assert ctx.pending_action is None
    result = AgentResult(success=True, pending_action={"token": "abc"})
    assert result.pending_action["token"] == "abc"
```

- [ ] **Step 2: Run — expect FAIL** (missing fields)

- [ ] **Step 3: Add fields**

In `ToolContext`:

```python
dtwin_class_actions: dict = field(default_factory=dict)
pending_action: dict | None = None
```

In `AgentResult`:

```python
pending_action: Optional[dict] = None
```

At end of `run_agent` success path (and max-iteration path if a pending action was minted mid-turn), copy:

```python
result.pending_action = ctx.pending_action
```

- [ ] **Step 4: Run — expect PASS**

---

### Task 3: Internal `/dtwin/classes` and `/dtwin/nodes/context`

**Files:**
- Modify: `src/api/routers/internal/dtwin.py`
- Create: `tests/units/api/test_dtwin_nodes_context.py`

**Interfaces:**
- Produces:
  - `GET /dtwin/classes` → `{success, domain_name, classes:[{name,uri,dataset,bridges,actions}]}`
  - `GET /dtwin/nodes/context?entity_uri&fetch_dataset_rows&dataset_row_limit&follow_bridges&bridge_depth`
- Consumes: `get_domain(session_mgr)`, `NodeContextService.resolve_context`, `get_settings`

- [ ] **Step 1: Write failing route tests**

Mirror `tests/units/api/test_node_context_endpoint.py` but hit `/dtwin/...` and patch `get_domain` (or the helper used by internal routes) instead of `DigitalTwin.resolve_domain`.

```python
def test_dtwin_classes_returns_session_actions(client, monkeypatch):
    # mock get_domain → domain with Customer class actions
    resp = client.get("/dtwin/classes")
    assert resp.status_code == 200
    assert resp.json()["classes"][0]["dataset"]["fullName"] == "main.crm.customers"

def test_dtwin_nodes_context_metadata(client):
    resp = client.get(
        "/dtwin/nodes/context",
        params={"entity_uri": "https://example.com/Customer/CUST001"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["class_name"] == "Customer"
    assert "rows" not in (body.get("dataset") or {})
```

Use the same `_CLASSES_WITH_ACTIONS` fixture shape as the public endpoint tests. Ensure TestClient targets the **internal** app (same pattern other `/dtwin` unit tests use — check existing `tests/units/api/` for internal router fixtures; if none, follow how other internal dtwin tests boot the app).

- [ ] **Step 2: Run — expect FAIL** (404)

- [ ] **Step 3: Implement routes** near the Graph Chat section in `internal/dtwin.py` (before or after assistant routes):

```python
from back.objects.digitaltwin import NodeContextService  # or existing import path

@router.get("/classes")
async def dtwin_classes(session_mgr: SessionManager = Depends(get_session_manager)):
    domain = get_domain(session_mgr)
    dname = _chat_resolve_domain_name(domain)
    items = []
    for cls in domain.get_classes() or []:
        items.append({
            "name": cls.get("name", ""),
            "uri": cls.get("uri", ""),
            "dataset": cls.get("dataset") or None,
            "bridges": cls.get("bridges") or [],
            "actions": cls.get("actions") or [],
        })
    return {"success": True, "domain_name": dname, "classes": items}


@router.get("/nodes/context")
async def dtwin_nodes_context(
    entity_uri: str,
    fetch_dataset_rows: bool = False,
    dataset_row_limit: int = 5,
    follow_bridges: bool = False,
    bridge_depth: int = 1,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = get_domain(session_mgr)
    # Cap limits like the public route
    dataset_row_limit = max(1, min(int(dataset_row_limit or 5), 20))
    bridge_depth = max(1, min(int(bridge_depth or 1), 1))
    payload = await NodeContextService.resolve_context(
        domain,
        settings,
        entity_uri=entity_uri,
        session_mgr=session_mgr,
        fetch_dataset_rows=fetch_dataset_rows,
        dataset_row_limit=dataset_row_limit,
        follow_bridges=follow_bridges,
        bridge_depth=bridge_depth,
        registry_catalog=None,
        registry_schema=None,
        registry_volume=None,
    )
    # Match public response_model_exclude_none behavior if needed
    return payload
```

Reuse public Pydantic models if they are importable without circular deps; otherwise return plain dicts with the same shape.

- [ ] **Step 4: Run — expect PASS**

```bash
uv run --frozen pytest -q tests/units/api/test_dtwin_nodes_context.py
```

---

### Task 4: Pending-action request + confirm routes

**Files:**
- Modify: `src/api/routers/internal/dtwin.py` (session cache helpers)
- Create / extend: `tests/units/api/test_dtwin_nodes_action_confirm.py`

**Interfaces:**
- Produces:
  - `POST /dtwin/nodes/action/request` body `{entity_uri, action_full_name}` → `{success, pending_action:{token,entity_uri,entity_label,action,description,expires_in_sec}, message}`
  - `POST /dtwin/nodes/action/confirm` body `{token}` → NodeAction-shaped result
  - Session cache key extension under `graph_chat`: `pending_actions: {token: {...}}`
- Consumes: `NodeContextService.class_action_entries`, `match_ontology_class`, `invoke_action`

- [ ] **Step 1: Write failing tests**

```python
def test_request_mints_token_without_invoke(client):
    # mock domain with allow-listed action; patch invoke_action to fail if called
    resp = client.post("/dtwin/nodes/action/request",
                       json={"entity_uri": "https://ex/Customer/CUST1",
                             "action_full_name": "main.ops.recompute_risk"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["pending_action"]["token"]
    assert body["pending_action"]["action"] == "main.ops.recompute_risk"

def test_confirm_invokes_once_then_rejects_reuse(client):
    # request → confirm OK → confirm again 4xx
    ...

def test_request_rejects_unknown_action(client):
    resp = client.post(..., json={..., "action_full_name": "evil.fn"})
    assert resp.status_code in (400, 422)  # ValidationError mapping
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement helpers + routes**

Extend `_chat_cache` shape:

```python
# Shape: {
#   "limit": int,
#   "history": {domain: [...]},
#   "pending_actions": {token: {domain, entity_uri, action_full_name, expires_at, used}},
# }
```

Constants:

```python
_PENDING_ACTION_TTL_SEC = 120
```

Request handler algorithm:

1. `domain = get_domain(session_mgr)`; resolve classes; `match_ontology_class`
2. Find action in `class_action_entries`; else `ValidationError`
3. `token = secrets.token_urlsafe(24)`; store pending entry with `time.time() + TTL`
4. Return pending_action payload (entity_label = local id or rdfs label if cheap; local id is enough)

Confirm handler:

1. Load pending by token; missing/used/expired/domain mismatch → `ValidationError` with “Action expired — request again”
2. Mark used **before** invoke (or mark after success and keep lock — prefer mark used first to prevent double-click double-invoke; if invoke fails, leave used and return error so user must re-request)
3. `await NodeContextService.invoke_action(...)`
4. Return action result dict

Optional cancel:

```python
@router.post("/nodes/action/cancel")
# delete pending token if present; always 200
```

Include cancel for UI cleanliness.

- [ ] **Step 4: Run — expect PASS**

---

### Task 5: Graph Chat tools — enrichment + new tools

**Files:**
- Modify: `src/agents/agent_dtwin_chat/tools.py`
- Modify: `tests/units/agents/test_agent_dtwin_chat.py`

**Interfaces:**
- Produces tools: `get_entity_context`, `request_entity_action`
- Updates: `list_entity_types`, `describe_entity` use class-actions cache
- Sets `ctx.pending_action` when request succeeds

- [ ] **Step 1: Write failing tool tests** (extend existing file)

```python
class TestGetEntityContext:
    def test_forwards_flags(self, patch_client):
        captured = {}
        def handler(request):
            if request.url.path == "/dtwin/nodes/context":
                captured["params"] = dict(request.url.params)
                return httpx.Response(200, json={
                    "success": True,
                    "entity_uri": "https://ex/Customer/CUST1",
                    "entity_local_id": "CUST1",
                    "class_name": "Customer",
                    "dataset": {"fullName": "main.crm.customers", "key_column": "id", "rows": []},
                })
            return httpx.Response(200, json={"success": True, "classes": []})
        patch_client(handler)
        out = chat_tools.tool_get_entity_context(
            _ctx(), entity_uri="https://ex/Customer/CUST1", fetch_dataset_rows=True
        )
        assert captured["params"]["fetch_dataset_rows"] == "true"
        assert "Node Context" in out or "CUST1" in out

class TestRequestEntityAction:
    def test_sets_pending_on_context(self, patch_client):
        def handler(request):
            return httpx.Response(200, json={
                "success": True,
                "pending_action": {
                    "token": "tok",
                    "entity_uri": "https://ex/Customer/CUST1",
                    "entity_label": "CUST1",
                    "action": "main.ops.recompute_risk",
                    "description": "Risk",
                    "expires_in_sec": 120,
                },
            })
        patch_client(handler)
        ctx = _ctx()
        out = chat_tools.tool_request_entity_action(
            ctx, entity_uri="https://ex/Customer/CUST1", action="main.ops.recompute_risk"
        )
        assert ctx.pending_action["token"] == "tok"
        assert "confirm" in out.lower() or "Confirm" in out

class TestListEntityTypesEnrichment:
    def test_shows_dataset_line(self, patch_client):
        # /dtwin/sync/stats + /dtwin/classes + /ontology/load
        ...
```

Also assert `TOOL_DEFINITIONS` names == `TOOL_HANDLERS` keys still holds with the two new tools.

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement helpers + tools**

```python
_ACTION_HINT = (
    "call request_entity_action(entity_uri, action) to propose one "
    "(UI confirmation required)"
)

def _get_class_actions(ctx: ToolContext) -> dict:
    if ctx.dtwin_class_actions:
        return ctx.dtwin_class_actions
    try:
        with _client(ctx) as c:
            resp = c.get("/dtwin/classes")
            if resp.status_code != 200:
                return {}
            data = resp.json()
        out = {}
        for item in data.get("classes") or []:
            uri = item.get("uri") or ""
            if not uri:
                continue
            out[uri] = {
                "name": item.get("name", ""),
                "dataset": item.get("dataset"),
                "bridges": item.get("bridges") or [],
                "actions": item.get("actions") or [],
            }
        ctx.dtwin_class_actions = out
    except Exception as exc:
        logger.warning("Could not load class actions: %s", exc)
    return ctx.dtwin_class_actions
```

Update `tool_describe_entity` to:

```python
return format_find_response(
    data,
    ontology_labels=_get_ontology_labels(ctx),
    class_actions=_get_class_actions(ctx),
    action_invoke_hint=_ACTION_HINT,
)
```

Update `tool_list_entity_types` to append dataset/action lines per type URI using the cache (same text style as MCP `list_entity_types`).

Add handlers + OpenAI schemas for `get_entity_context` and `request_entity_action`. Register in `TOOL_DEFINITIONS` / `TOOL_HANDLERS`.

`request_entity_action` POST body:

```json
{"entity_uri": "...", "action_full_name": "..."}
```

On success, set `ctx.pending_action = data["pending_action"]` and return a short human message telling the model confirmation will appear in the UI.

- [ ] **Step 4: Run — expect PASS**

```bash
uv run --frozen pytest -q tests/units/agents/test_agent_dtwin_chat.py tests/units/agents/test_graph_formatting_context.py
```

---

### Task 6: Engine prompt + HTTP response plumbing

**Files:**
- Modify: `src/agents/agent_dtwin_chat/engine.py`
- Modify: `src/api/routers/internal/dtwin.py` (`/assistant/chat`, `/assistant/chat/stream`)
- Modify: `src/agents/agent_dtwin_chat/__init__.py` docstring if it still claims public `/api/v1` only

**Interfaces:**
- Chat JSON / SSE `done` includes `pending_action` when present
- System prompt documents Dataset/Bridge/Action workflow

- [ ] **Step 1: Write a focused engine/route test**

If route tests are heavy, unit-test that `run_agent` copies `ctx.pending_action` (mock LLM to call `request_entity_action` once then finish) — or assert response dict construction in a small pure helper:

```python
def build_chat_payload(agent_result) -> dict:
    payload = {
        "success": True,
        "reply": agent_result.reply,
        "tools": [...],
        "iterations": agent_result.iterations,
        "usage": agent_result.usage,
    }
    if agent_result.pending_action:
        payload["pending_action"] = agent_result.pending_action
    return payload
```

Prefer inlining into the two route handlers if a helper feels like overkill — but then test via TestClient with `run_agent` patched to return `AgentResult(success=True, reply="ok", pending_action={...})`.

- [ ] **Step 2: Extend SYSTEM_PROMPT**

Add under TOOLS:

```text
  CONTEXT (ontology design)
  - get_entity_context(entity_uri, fetch_dataset_rows?, follow_bridges?)
      Fetch linked Dataset rows and/or cross-domain Bridge entities.
  - request_entity_action(entity_uri, action)
      Propose a class Action (UC function). Does NOT execute it.
      The UI will ask the user to Confirm or Cancel.
```

Add RULES:

```text
  * Never claim an Action ran unless a later tool/UI result says it completed.
  * Only request actions that appeared in tool output (allow-listed fullName).
  * Prefer describe_entity first; use get_entity_context when the user needs
    table rows or bridge-linked entities.
```

- [ ] **Step 3: Wire `pending_action` into both chat responses**

Blocking return and SSE `done` event:

```python
if agent_result.pending_action:
    payload["pending_action"] = agent_result.pending_action
```

Ensure `run_agent` sets `result.pending_action = ctx.pending_action` before return.

- [ ] **Step 4: Run relevant tests — expect PASS**

---

### Task 7: Graph Chat Confirm / Cancel UI

**Files:**
- Modify: `src/front/static/query/js/query-chat.js`
- Modify: CSS used by Graph Chat (prefer existing assistant / graph-chat classes; add minimal rules in the stylesheet already loaded by the KG chat partial — locate via `graph-chat-tools` class references)
- Optional: template only if a hidden container is required (prefer pure JS)

**Interfaces:**
- Consumes `pending_action` on stream `done`
- Confirm → `POST /dtwin/nodes/action/confirm` `{token}`
- Cancel → `POST /dtwin/nodes/action/cancel` `{token}` (or client-only discard)

- [ ] **Step 1: Add a minimal frontend contract test if the repo has a static/JS test harness for query-chat; otherwise document manual check + add a tiny Node-free assertion in an existing front static test if one exists**

Search for existing `query-chat` tests. If none, skip automated UI test and add a short manual checklist in the changelog test-result note; prefer adding a pure function testable without DOM if you extract `buildPendingActionCardModel(pending)`.

- [ ] **Step 2: Implement card rendering**

In `finalizeStreamingBubble` / after `doneEvent`:

```javascript
if (doneEvent.pending_action) {
    bodyEl.appendChild(buildPendingActionCard(doneEvent.pending_action));
}
```

Card contents: action fullName, entity label, short description, Confirm + Cancel buttons.

Confirm handler:

```javascript
async function confirmPendingAction(card, pending) {
    // disable buttons
    const resp = await fetch('/dtwin/nodes/action/confirm', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        credentials: 'same-origin',
        body: JSON.stringify({token: pending.token}),
    });
    const data = await resp.json().catch(() => ({}));
    // append assistant message with formatted rows or error
    // remove/disable card
}
```

Cancel: POST cancel (best effort) + remove card.

- [ ] **Step 3: Manual smoke** (with `./scripts/start.sh` if available): request an action in chat, confirm card appears, Confirm runs, Cancel discards.

---

### Task 8: AI-feature SPEC + eval dataset

**Files:**
- Modify: `.planning/agents/agent_dtwin_chat/SPEC.md`
- Modify: `tests/eval/datasets/agent_dtwin_chat/baseline.jsonl` (≥10 examples)
- Create: `tests/eval/run_agent_dtwin_chat.py` (dry-run capable, mirror `run_agent_graph_interpreter.py`)
- Touch: `tests/eval/thresholds.yaml` only if dimensions change

**Interfaces:**
- SPEC tool surface lists all 7 tools (5 existing + 2 new)
- Eval examples cover discovery, context, bridges, action-request-without-invoke, adversarial invent-action

- [ ] **Step 1: Expand baseline.jsonl to ≥10 rows**

Include at least:

1. list types / discovery  
2. describe entity grounded  
3. get_entity_context dataset rows expected tool  
4. follow_bridges expected tool  
5. request_entity_action when user asks to run risk fn  
6. adversarial: invent Unicorn action → must not  
7. adversarial: claim action completed without confirm → fail constraint  
8–10. happy/ambiguous variants for search + graphql unchanged paths  

Example row:

```json
{"id": "context-dataset-001", "input": {"domain": "sales", "user_message": "Show me the dataset rows for customer CUST00094"}, "expected": {"constraints": [{"kind": "tool_called", "value": "get_entity_context"}, {"kind": "does_not_invoke_action", "value": true}]}, "tags": ["happy"]}
```

- [ ] **Step 2: Fill SPEC.md sections 3–7** with the real tool schemas and updated failure modes (auto-invoke Action; invent Action fullName).

- [ ] **Step 3: Add dry-run eval runner** that validates dataset shape + rule judges (`tool_called`, `does_not_invoke_action`). Live LLM path optional behind CLI flags.

- [ ] **Step 4: Run dry-run**

```bash
uv run --frozen python tests/eval/run_agent_dtwin_chat.py --dry-run
```

Expected: PASS dataset validation.

---

### Task 9: Full verification + changelog

**Files:**
- Create/append: `changelogs/v0.7.0/benoitcayladbx_2026-08-06.log` (use GitHub username prefix already used in repo)

- [ ] **Step 1: Run non-scenario suite**

```bash
uv run --frozen pytest -q -m "not scenario"
```

- [ ] **Step 2: Append changelog section**

Title: Graph Chat MCP alignment (Datasets / Bridges / Actions)  
Context: Graph Chat lacked ontology Actions parity with MCP.  
Numbered file changes + test result line.

- [ ] **Step 3: Spec/plan cross-check** — every Success criterion in the design spec has a task deliverable above.

---

## Self-review (plan vs spec)

| Spec requirement | Task |
|---|---|
| Session-aware `/dtwin/nodes/context` + `/dtwin/classes` | Task 3 |
| Action request/confirm + token | Task 4 |
| Enrich list/describe + new tools | Task 5 |
| Shared formatters | Task 1 |
| `pending_action` on AgentResult / SSE | Tasks 2, 6 |
| Confirm/Cancel UI | Task 7 |
| Soft errors / allow-list | Tasks 3–4 |
| SPEC + ≥10 eval examples | Task 8 |
| Bridge targets published-only | Documented; no code change to `NodeContextService` |
| No Graph Chat `invoke_entity_action` tool | Task 5 (only `request_entity_action`) |

No TBD placeholders remain in this plan.
