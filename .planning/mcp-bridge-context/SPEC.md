# SPEC: mcp-bridge-context

> **Scope:** MCP tool-contract change under `src/mcp-server/**` — surface class
> bridges with the target domain description so an interrogating LLM (external:
> Playground, Cursor, Claude Desktop, etc.) can decide to hop with
> `select_domain(<target>)` instead of relying on the read-only
> `follow_bridges` peek.
>
> Not a new Foundation Model agent. OntoBricks does not call the LLM itself
> here — this SPEC covers the **prompt surface** (MCP tool descriptions +
> `FastMCP.instructions`) and the **response contract** that downstream LLMs
> read. Section 2 is populated accordingly.

## 1. Purpose

When multiple domains are registered, an MCP-connected LLM must be able to
follow an ontology-authored bridge into another domain and query it. Before
v0.8.0, bridges were exposed as `{target_domain, target_class_name,
target_class_uri, label}` with no description of the target and no filter on
MCP visibility, and the only traversal path (`get_entity_context(follow_bridges=True)`)
was read-only — it never switched `_selected_domain`, so any subsequent tool
call silently ran on the origin domain.

This change:

- adds `target_domain_description` (pulled from
  `RegistryService.list_mcp_domains`) to every bridge returned by the MCP
  surface;
- **omits** bridges whose target is not MCP-visible so the LLM never proposes a
  hop it cannot perform;
- reframes the tool prompts so the primary cross-domain workflow is
  `describe_entity → select_domain(<target>) → describe_entity / GraphQL`,
  with `follow_bridges` documented as a peek that does **not** switch the
  session.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `mcp_bridge_context` (tool-contract, not an agent) |
| `module_path` | `src/mcp-server/server/app.py` + `src/back/objects/digitaltwin/NodeContextService.py` |
| `model_endpoint` | n/a — consumed by external LLMs (Playground, Cursor, Claude Desktop) |
| `temperature` | n/a |
| `mlflow_experiment` | `/Shared/ontobricks/mcp/bridge_context` (created on first eval run) |

## 3. Tool surface

Three MCP tools change. None gains or loses a parameter; their **response
payload** and **description text** change.

| Tool | Input schema | Output type | Purpose |
|---|---|---|---|
| `describe_entity` | `{search?: str, entity_type?: str, depth: int}` | text | `[Context]` block now shows each bridge's target domain description and a `select_domain(<target>)` hint |
| `get_entity_context` | `{entity_uri: str, fetch_dataset_rows: bool, dataset_row_limit: int, follow_bridges: bool}` | text | `Cross-domain Bridges:` block adds target description; `follow_bridges` doc clarifies it is a peek, not a session switch |
| `select_domain` | `{domain_name: str}` | text | Unchanged behaviour. The tool prompt and `FastMCP.instructions` now direct the LLM to use `select_domain(<target_domain>)` after seeing a bridge |

### 3.1 Bridge payload contract (authoritative)

`GET /api/v1/domain/classes` and `GET /api/v1/digitaltwin/nodes/context`
return each bridge as:

```json
{
  "target_domain": "finance",
  "target_domain_description": "Finance ontology with contracts and payments",
  "target_class_name": "Contract",
  "target_class_uri": "https://example.com/Contract",
  "label": "Owns contracts",
  "entities": null
}
```

Rules:

- `target_domain_description` is the registry description of the target
  domain, or `""` when the registry returns none.
- Bridges whose `target_domain` is **not** in
  `RegistryService.list_mcp_domains()` are **omitted** from the MCP / external
  REST surface. The internal `/dtwin/classes` route keeps every bridge (UI
  authoring path).
- Registry lookup failures **soft-fail**: bridges pass through unchanged with
  `target_domain_description = ""` and no filtering, so a broken registry
  never breaks a node-context response.

## 4. Success criteria

1. **Bridge advertises the target domain**
   - input: selected domain `customer360`; `describe_entity(search="Jacob Martinez")`
     where `Customer` has a bridge to `finance / Contract`.
   - expected: response `[Context]` block lists the bridge with
     `Target domain: <finance description>` and the `select_domain(<target>)`
     hint. `finance` is MCP-visible in the mock registry.
2. **LLM hops instead of peeking**
   - input: user asks a question that requires target-domain data (e.g.
     "list contracts owned by CUST00094").
   - expected: judge / trace shows the LLM calls `select_domain("finance")`
     followed by `describe_entity` or `query_graphql` in `finance`, rather
     than only `get_entity_context(follow_bridges=True)`.
3. **Non-visible target is hidden**
   - input: class has a bridge to `internal_only` which is not
     API/MCP-enabled.
   - expected: the bridge is absent from the MCP response, so the LLM cannot
     propose `select_domain("internal_only")`.

## 5. Eval dimensions

The CI gate parses this table — keep it well-formed.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `bridge_description_present` | fraction of bridged responses where `target_domain_description` renders when the target is MCP-visible | `1.00` | `0.30` | rule-based |
| `unavailable_bridge_omitted` | fraction of test rows where a non-MCP bridge does NOT appear in the response | `1.00` | `0.25` | rule-based |
| `hop_tool_selection` | fraction of bridge-requiring prompts where the LLM calls `select_domain(<expected_target>)` next | `0.85` | `0.30` | rule-based on trace |
| `no_silent_peek` | fraction of hop-required prompts where the LLM does NOT stop after `follow_bridges=True` (must follow with `select_domain`) | `0.90` | `0.15` | rule-based on trace |

**Aggregate threshold:** weighted sum ≥ `0.90` to pass G2.

The two contract dimensions (`bridge_description_present`,
`unavailable_bridge_omitted`) are pass/fail (`1.00`) and cannot be masked by
LLM-side dimensions.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Bridge shown without description | `bridge_description_present` < 1.00 | Check `_load_mcp_target_descriptions` return; ensure registry publish flag is set on target |
| LLM peeks with `follow_bridges` and stops there | `no_silent_peek` < 0.90 | Tighten the `describe_entity` / `get_entity_context` hint text; add regression example |
| LLM proposes a hop to a hidden target | `unavailable_bridge_omitted` < 1.00 | Contract regression — verify enricher `drop_unavailable=True` on the external routes |
| Registry unreachable → bridges silently unfiltered | Logged `enrich_bridge_targets: registry lookup failed` warning | Alert on log rate; soft-fail is intentional (per SPEC §3.1) |

## 7. Eval dataset

- **Baseline file:** [tests/eval/datasets/mcp/bridge_switch.jsonl](../../tests/eval/datasets/mcp/bridge_switch.jsonl)
  — 10 examples covering happy hop, non-visible target, peek-vs-switch,
  registry soft-fail. Material-change floor per `.cursor/12` (≥ 10).
- **Regression file:** future — every production failure lands under
  `tests/eval/datasets/mcp/regression.jsonl`.

Dataset row shape mirrors [tests/eval/datasets/agent_dtwin_chat/baseline.jsonl](../../tests/eval/datasets/agent_dtwin_chat/baseline.jsonl):

```json
{"id": "...", "input": {"domain": "...", "user_message": "..."},
 "expected": {"contains": [...], "constraints": [{"kind": "...", "value": ...}]},
 "tags": ["happy" | "ambiguous" | "adversarial"]}
```

Constraint kinds used by this dataset:

- `bridge_description_present` — the rendered `[Context]` block for the
  bridged class carries the expected target-domain description string.
- `bridge_hidden` — the response does NOT mention the given
  `unavailable_target` domain name in any bridge block.
- `tool_called` — the LLM's tool trace contains a call to the given tool.
- `next_tool_after` — the tool listed as `value.next` is called after
  `value.after` in the same trace.

## 8. MLflow tracing

MCP-side tool handlers stay lightweight (proxy calls to OntoBricks REST +
text formatting). Add `@trace_tool` on `_format_class_context_block` /
`_format_node_context_response` if the eval harness needs step-level
attribution. Downstream LLM traces come from the client (Cursor, Playground)
and are outside this repo.

The eval harness that reads `bridge_switch.jsonl` must publish one MLflow
run per evaluation with parent experiment
`/Shared/ontobricks/mcp/bridge_context`, and the PR body pastes the run URI
under the "MLflow eval run" heading per `.cursor/12`.

## 9. Plan reference

Implementation plan: `/Users/benoit.cayla/.cursor/plans/mcp_bridge_domain_switch_0203d69b.plan.md`
(Cursor plan, not committed).

Asana ticket:
[MCP: expose class bridges with target-domain description so the agent can switch domains](https://app.asana.com/1/8808412813448/project/1217637563677287/task/1217792873648378).

## 10. Sign-off

- [x] Bridge payload contract authored in §3.1.
- [x] Eval dataset committed at `tests/eval/datasets/mcp/bridge_switch.jsonl` (10 examples).
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ `0.90`.
- [ ] Reviewer waiver (if applicable): _____
