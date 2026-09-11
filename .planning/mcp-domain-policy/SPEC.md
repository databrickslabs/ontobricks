# SPEC: mcp-domain-policy

> **Scope:** MCP tool-contract change under `src/mcp-server/**` — let each
> domain decide, from the ontology authoring UI, which MCP tools it publishes
> and how its ontology attachments (datasets, bridges, actions) are surfaced
> to an interrogating LLM.
>
> Not a new Foundation Model agent. OntoBricks does not call the LLM itself
> here — this SPEC covers the **prompt surface** (which tools appear in
> `tools/list`, and the wording of the follow-up hints) and the **response
> contract** that downstream LLMs read. Section 2 is populated accordingly.

## 1. Purpose

The MCP surface was uniform: every MCP-enabled domain published all 11 tools
and every ontology attachment, whatever its shape. That is wrong in two
directions at once.

- **Too much.** A domain with no Unity Catalog functions still advertised
  `invoke_entity_action`; a domain whose datasets are governed elsewhere still
  handed table names to the model. Every irrelevant tool is prompt budget
  spent and a wrong-tool branch offered to the planner.
- **Too flat.** A domain whose value is its cross-domain bridges had no way to
  say so. The bridge hint read the same as every other hint, so the model was
  as likely to stop at the origin graph as to hop.

This change adds a per-domain policy, edited in **Domain → Information → MCP**
and persisted in the registry, with two independent settings:

1. **Exposed tools** — the 7 domain-scoped tools are individually switchable.
   The 4 registry-level tools (`list_domains`, `select_domain`,
   `list_domain_versions`, `get_design_status`) run *before* a domain is
   resolved, so no per-domain policy can govern them; they are always exposed.
2. **Ontology context** — `dataset`, `bridges` and `actions` each take one of
   `preferred` / `normal` / `disabled`.

An unconfigured domain reproduces the pre-0.8 behaviour exactly: the policy is
stored as the empty blob `{}`, disabled tools are listed rather than enabled
ones, and a missing context key reads as `normal`. No backfill.

**Ontology-only domains (0.8).** A domain can be published with a valid
ontology but no Knowledge Graph build (no mapping, no graph): the
`DRAFT -> IN-REVIEW` precondition now accepts *either* a build (`last_build`)
*or* a valid ontology. `GET /api/v1/domains` carries `has_graph` (the
numeric-latest PUBLISHED version has been built). When `has_graph` is false the
MCP server hides every graph tool and exposes `describe_ontology` alone — a
computed restriction layered on top of the per-domain policy, not persisted in
`mcp_policy`. The MCP process mirrors the graph-tool set as `GRAPH_TOOLS`
(it cannot import the app catalog); a drift guard test keeps the two in sync.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `mcp_domain_policy` (tool-contract, not an agent) |
| `module_path` | `src/mcp-server/server/app.py` + `src/back/core/mcp_tools.py` + `src/back/objects/digitaltwin/NodeContextService.py` |
| `model_endpoint` | n/a — consumed by external LLMs (Playground, Cursor, Claude Desktop) |
| `temperature` | n/a |
| `mlflow_experiment` | `/Shared/ontobricks/mcp/domain_policy` (created on first eval run) |

## 3. Tool surface

No tool gains or loses a parameter. What changes is **which tools are listed**
and **what their responses contain**.

| Tool | Governed by policy | Change |
|---|---|---|
| `list_domains` | never hidden | Response feeds the per-domain policy cache; `mcp_policy` added to the `/api/v1/domains` payload |
| `select_domain` | never hidden | Recomputes the session tool set; response names the tools this domain does not expose |
| `list_domain_versions` | never hidden | unchanged |
| `get_design_status` | never hidden | unchanged |
| `describe_ontology` | yes (but never by `has_graph`) | New tool: serves the ontology schema (class summary + raw OWL) from `GET /api/v1/domain/ontology` + `/domain/classes`. Needs no graph, so it is the sole domain tool an ontology-only domain (`has_graph:false`) exposes |
| `list_entity_types` | yes | Dataset / action lines follow the context policy (already filtered upstream) |
| `describe_entity` | yes | `[Context]` block hint wording follows `preferred`; disabled elements absent |
| `get_status` | yes | unchanged apart from the visibility guard |
| `get_graphql_schema` | yes | unchanged apart from the visibility guard |
| `query_graphql` | yes | unchanged apart from the visibility guard |
| `get_entity_context` | yes | Refuses `fetch_dataset_rows=True` / `follow_bridges=True` when that element is disabled; hint wording follows `preferred` |
| `invoke_entity_action` | yes | Refuses every call when the `actions` element is disabled, **even when the tool itself is exposed** |

### 3.1 Policy contract (authoritative)

`GET /api/v1/domains` returns each domain with:

```json
{
  "name": "customer360",
  "description": "Customer 360 ontology",
  "mcp_policy": {
    "disabled_tools": ["query_graphql", "invoke_entity_action"],
    "context": {"bridges": "preferred", "actions": "disabled"}
  }
}
```

Rules:

- `disabled_tools` lists only **domain-scoped** tool names. Registry-level
  tool names are stripped on write (UI-side and server-side) and defensively
  removed again before `disable_components`, so no policy can hide the tools
  needed to recover.
- A context feature absent from `context` is `normal`. Only non-default
  entries are persisted, so a fully default policy is the empty blob `{}`.
- An unknown tool name, unknown context feature or unrecognised mode is
  dropped during coercion (`back/core/mcp_tools.coerce_mcp_policy`) rather
  than raising, so a hand-edited registry row cannot break domain loading.

### 3.2 Enforcement points

Three layers, deliberately redundant:

1. **`tools/list` filtering** — `select_domain` calls `ctx.reset_visibility()`
   then `ctx.disable_components(names=…, components={"tool"})`, which is
   session-scoped in FastMCP 3.4.2 and emits a
   `ToolListChangedNotification`. Reset first, otherwise the rules accumulate
   and a tool hidden by a previously selected domain stays hidden.
2. **Call-time guard** — `_ensure_tool_allowed` re-checks on every call, since
   hiding a tool from a list is a hint: a client that ignores the
   notification, or cached an older list, can still call it.
3. **Server-side payload filtering** — `NodeContextService` withholds disabled
   attachments on the *external* surfaces (`/api/v1/digitaltwin/nodes/context`,
   `/api/v1/domain/classes`, `/api/v1/digitaltwin/nodes/action`), extending
   the existing `drop_unavailable` precedent. The internal authoring routes
   pass no policy and keep showing the ontology designer everything.

### 3.3 What `preferred` means

Textual emphasis only. The neutral follow-up hint becomes a directive
instruction — for example, the bridges hint changes from "to actually query
one of these targets, call `select_domain`…" to "ALWAYS follow these bridges
before concluding…". It does **not** reorder sections, does not announce the
element earlier, and does not change the payload.

## 4. Success criteria

1. **Disabled tool disappears and stays refused**
   - input: domain `customer360` with `disabled_tools: ["query_graphql"]`;
     client calls `select_domain("customer360")`.
   - expected: `query_graphql` is absent from `tools/list`, and a client that
     calls it anyway gets a refusal naming the domain, not a query result.
2. **Registry tools survive a hostile policy**
   - input: a policy that lists `select_domain` and `list_domains` in
     `disabled_tools`.
   - expected: both remain listed and callable — coercion drops them on write
     and `_disabled_tools` subtracts them again at apply time.
3. **Disabled element is withheld everywhere**
   - input: domain with `context.actions = "disabled"`, entity whose class
     declares a UC function.
   - expected: no `Actions:` block in `get_entity_context` or
     `describe_entity`, and `invoke_entity_action` refuses even though the
     tool is still exposed.
4. **Preferred element gets a directive hint**
   - input: domain with `context.bridges = "preferred"`, bridged entity.
   - expected: the bridges hint is the directive variant, and the model hops
     with `select_domain(<target>)` rather than concluding on the origin graph.
5. **Switching domains recomputes the tool set**
   - input: `select_domain(A)` where A hides `query_graphql`, then
     `select_domain(B)` where B hides nothing.
   - expected: `query_graphql` is back in `tools/list` for B.

## 5. Eval dimensions

The CI gate parses this table — keep it well-formed.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `disabled_tool_absent` | fraction of rows where a policy-disabled tool is absent from `tools/list` AND refused on direct call | `1.00` | `0.30` | rule-based |
| `registry_tool_always_present` | fraction of rows where the 4 registry-level tools remain listed, whatever the policy | `1.00` | `0.25` | rule-based |
| `disabled_context_absent` | fraction of rows where a disabled attachment renders nowhere in the response | `1.00` | `0.25` | rule-based |
| `preferred_hint_directive` | fraction of preferred-element rows where the emitted hint is the directive variant | `0.90` | `0.20` | rule-based |

**Aggregate threshold:** weighted sum ≥ `0.90` to pass G2.

The three contract dimensions (`disabled_tool_absent`,
`registry_tool_always_present`, `disabled_context_absent`) are pass/fail
(`1.00`) and cannot be masked by the presentation dimension.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| A disabled tool is still callable | `disabled_tool_absent` < 1.00 | `_ensure_tool_allowed` is the backstop — verify the guard runs before any HTTP call in that tool |
| Client keeps a stale tool list after `select_domain` | Trace shows a call to a hidden tool | Expected for clients ignoring `ToolListChangedNotification`; the call-time guard refuses it. Not a contract break |
| Tool hidden by a previous domain stays hidden | `registry_tool_always_present` or a manual domain switch | Missing `reset_visibility()` before `disable_components` |
| Policy lost on save | Round-trip test on the Lakebase store | `mcp_policy` must be promoted in the `domains` UPSERT and re-injected into `info` on read |
| Column missing on an upgraded workspace | `_ensure_domains_mcp_policy_column` logs, preflight reports pending migration | Run `scripts/migrations/upgrade_0.7_to_0.8.sql` or `make bootstrap-lakebase` as the schema owner |
| Policy blocks the ontology designer's own UI | Manual: internal panel shows fewer attachments | Internal routes must pass **no** policy — mirrors `drop_unavailable=False` |
| Wrong domain's policy applied with two concurrent clients | Manual: two sessions selecting different domains | **Known limitation, out of scope.** `create_mcp_server()` runs once per process, so `_selected_domain` / `_domain_policy` are shared closures: the last `select_domain` wins for every session. Visibility rules stay per-session, but the call-time guard follows the shared selection. This predates the policy — the same closure already routed queries — so fixing it means moving the selection into session state, tracked separately |

## 7. Eval dataset

- **Baseline file:** [tests/eval/datasets/mcp/domain_policy.jsonl](../../tests/eval/datasets/mcp/domain_policy.jsonl)
  — 11 examples covering tool gating, registry-tool protection, each of the
  three context elements disabled, preferred emphasis, the tool-vs-element
  overlap on actions, and an ontology-only domain (`has_graph:false`) that
  exposes `describe_ontology` alone. Material-change floor per `.cursor/12`
  (≥ 10).
- **Regression file:** future — every production failure lands under
  `tests/eval/datasets/mcp/regression.jsonl`.

Dataset row shape mirrors [tests/eval/datasets/mcp/bridge_switch.jsonl](../../tests/eval/datasets/mcp/bridge_switch.jsonl):

```json
{"id": "...", "input": {"domain": "...", "mcp_policy": {...}, "user_message": "..."},
 "expected": {"contains": [...], "constraints": [{"kind": "...", "value": ...}]},
 "tags": ["happy" | "ambiguous" | "adversarial"]}
```

Constraint kinds used by this dataset:

- `tool_absent` — the named tool must not appear in the session `tools/list`.
- `tool_present` — the named tool must appear, whatever the policy says.
- `context_element_absent` — the named element (`dataset` / `bridges` /
  `actions`) renders in no block of the response.
- `hint_directive` — the follow-up hint for the named element is the directive
  variant, not the neutral one.
- `call_refused` — a direct call to the named tool returns a refusal message
  rather than a result.

## 8. MLflow tracing

MCP-side tool handlers stay lightweight (proxy calls to OntoBricks REST + text
formatting), so there is no Foundation Model API call on this path to trace.
Add `@trace_tool` on `_format_class_context_block` /
`_format_node_context_response` if the eval harness needs step-level
attribution of the emphasis decision. Downstream LLM traces come from the
client (Cursor, Playground) and are outside this repo.

The eval harness that reads `domain_policy.jsonl` must publish one MLflow run
per evaluation with parent experiment `/Shared/ontobricks/mcp/domain_policy`,
and the PR body pastes the run URI under the "MLflow eval run" heading per
`.cursor/12`.

## 9. Plan reference

Implementation plan: `/Users/benoit.cayla/.cursor/plans/mcp_tool_policy_file_77e59f80.plan.md`
(Cursor plan, not committed).

Asana ticket:
[MCP: per-domain policy — configurable tool set and ontology context handling](https://app.asana.com/1/8808412813448/project/1217637563677287/task/1217819474931376).

## 10. Sign-off

- [x] Policy contract authored in §3.1.
- [x] Eval dataset committed at `tests/eval/datasets/mcp/domain_policy.jsonl` (10 examples).
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ `0.90`.
- [ ] Reviewer waiver (if applicable): _____
