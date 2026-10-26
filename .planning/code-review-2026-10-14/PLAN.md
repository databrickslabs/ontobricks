# Code review follow-up: refactoring plans (2026-10-14)

> Status: **proposal**. No code was changed by this document.
> Scope: ten findings from the 2026-10-14 global review. Every number below was
> re-measured on the `0.9.0` worktree on 2026-10-14 (AST scans + `rg`); where the
> review brief's estimate was wrong, the corrected value is marked **(corrected)**.
> Complements `.planning/class-per-file-refactor/PLAN.md` (Phases 2–3 overlap
> items 1–2 here; this file supersedes their numbers and closes the
> `run_agent` shim contradiction noted in item 2).

Conventions used below:

- **PR** = one reviewable change; every PR keeps
  `uv run --frozen pytest -q -m "not scenario"` and `tests/units/architecture/` green.
- Any PR touching `src/agents/**` follows the `.cursor/12` gate (SPEC.md +
  dataset + MLflow eval URI in the PR body).
- Smell → refactoring names are Fowler's (`src/.coding_rules.md §9`).

---

## 1. Pass-through facades — Remove Middle Man

### Problem

AST scan (method whose body, minus docstring/local imports, is a single call
forwarding only its own parameters):

| Class | Forwarders / methods | Class LOC |
|---|---|---|
| `back/objects/domain/settings/SettingsService.py::SettingsService` | **117 / 120** | 1388 |
| `back/objects/digitaltwin/DigitalTwin.py::DigitalTwin` | **82 / 84** (corrected from ~66) | 953 |
| `back/objects/domain/settings/graphengine/GraphEngineSettings.py::GraphEngineSettings` | **26 / 36** | 935 |
| `back/objects/registry/RegistryService.py::RegistryService` | **22** store forwarders / 63 (corrected from ~19) | 1021 |
| `back/core/databricks/DatabricksClient.py::DatabricksClient` | **38 / 44** | — |

- `RegistryService` store forwarders: `is_initialized`, `list_domains`,
  `domain_exists`, `list_domain_details`, `list_versions`, `read_version`,
  `load_build_runs`, `build_analytics`, `find_mcp_version`, `get_version_status`,
  `record_review_event`, `list_review_events`, `list_all_review_events`,
  `record_change_events`, `list_change_events`, `insert_comment`,
  `list_comments`, `resolve_comment`, `insert_task`, `list_tasks`,
  `list_tasks_for_assignee`, `update_task_status`.
- `DigitalTwin` also re-exports class attributes (`RDF_TYPE`, `DomainSnapshot`,
  `_SELECT_CLAUSE_RE`, `_SQL_ERROR_MAX_CHARS`) and imports `get_domain` only
  because "tests patch `DigitalTwin.get_domain`" (`DigitalTwin.py:18`).
- **Inappropriate Intimacy / reverse dependency.** Collaborators call back
  *up* into the facade through a lazy `_dt()` helper defined **7 times**
  (`TwinAnalytics.py:14`, `TwinBackgroundTasks.py:18`, `TwinResolve.py:19`,
  `TwinGraphStats.py:11`, `quality/SqlQualityChecks.py:25`,
  `query/TwinMapping.py:19`, `query/TwinSparqlTranslate.py:12`) plus
  `_dt_mod()` in `TwinResolve.py`. **13** call sites, e.g.
  `TwinMapping.py:700` → `_dt().augment_mappings_from_config` →
  `TwinMapping.augment_mappings_from_config` (a round trip back into itself).
  The cycle exists so that tests patching `DigitalTwin.*` still intercept
  collaborator calls: the design is shaped by patch targets.
- **Three-hop chain**: `SettingsService` → `GraphEngineSettings` →
  `GraphEngineLakebaseSettings` / `GraphEngineNeo4jSettings`.
- **Locked in by tests.** `tests/units/settings/test_v090_settings_facade.py:137-151`
  discovers delegators and asserts `len(_DELEGATORS) > 80`, then parametrises
  a forwarding-contract test over each one. Test patch load: **74** patch sites
  on `SettingsService`, **60** on `DigitalTwin`; **127** `SettingsService.x`
  references in 21 src files; **76** `DigitalTwin…x(` call sites in src.

**Rule contradiction.** `.cursor/07 §Class-First 10` ("No pass-through
facades… callers import the real class") vs `.cursor/07 §Subpackage Patterns
/ databricks/` ("`DatabricksClient` is a thin Facade composing all services").
`DatabricksClient` already exposes its collaborators as attributes
(`self.sql`, `self.catalog`, `self.volumes`, `self.workspace`,
`self.dashboards`, `self.secrets`, `DatabricksClient.py:41-56`), so its 38
forwarders are pure Middle Man. They survive because they double as an
implicit duck-typed "SQL client" protocol (`execute_query`,
`execute_statement`, `iter_rows`, `create_or_replace_view`) consumed by graph
stores, `schedule/build.py` and test fakes.

**Recommended resolution.** Rule 10 wins (simplest code, `.cursor/01`).
Reword the `databricks/` pattern to: *"`DatabricksClient` is a composition
root: it builds the per-surface services from one `DatabricksAuth` and exposes
them as attributes (`client.sql`, `client.catalog`, …). It does not forward
their methods."* Make the duck-typed protocol explicit as a
`typing.Protocol` (`SQLClient` in `back/core/databricks/SQLClient.py`) that
`SQLWarehouse` / `StatementExecutionWarehouse` satisfy, and pass `client.sql`
where a SQL client is needed.

### Target design

- Routers and services import the concrete class (`RegistrySettings`,
  `WarehouseSettings`, `GraphEngineLakebaseSettings`, `TwinMapping`,
  `SqlQualityChecks`, `TwinBackgroundTasks`, …).
- `SettingsService` shrinks to the admin gate + `_resolve_context`
  (rename **SettingsAccess**, ~80 LOC) or is inlined into `_guards.py`.
- `GraphEngineSettings` keeps only its 10 real methods (engine selection /
  dispatch); per-engine calls go straight to the engine class.
- `DigitalTwin` keeps its 2 real methods or is **Inline Class**-ed away;
  collaborators import each other directly; `_dt()` / `_dt_mod()` deleted.
- `RegistryService` exposes its store read-only (`svc.store`) for pure reads;
  it keeps methods only where it adds policy (permissions, caching, folder
  sanitisation).
- New guardrail `tests/units/architecture/test_no_middle_man.py`: counts
  forwarders per class with the same AST rule, shrink-only baseline in
  `baseline.json` (`middle_man_methods`).

### Migration (small PRs)

1. **Guardrail first.** Add `test_no_middle_man.py` + baseline (today's
   counts). Invert `test_v090_settings_facade.py:151` from `> 80` to a
   ratchet (`<= baseline`) — the test now guards shrinkage, not growth.
2. **Break the cycle.** Replace each `_dt().foo` with a direct call on the
   owning collaborator (13 sites, 7 files); retarget the tests that patch
   `DigitalTwin.foo` to the collaborator (`"…query.TwinMapping.TwinMapping.foo"`,
   mind the module-vs-class lesson in class-per-file PLAN §Progress). Delete
   the 7 `_dt()` helpers + `_dt_mod()`.
3. **DigitalTwin callers, one router per PR**: `api/routers/digitaltwin.py`,
   `api/routers/domains.py`, `internal/dtwin.py`, then `back/objects` callers,
   then `schedule/build.py` (`DigitalTwin.diagnose_view_error`). Each PR
   deletes the forwarders it emptied.
4. **SettingsService, one extracted class per PR** (RegistrySettings,
   RegistryDomainSettings, ScheduleSettings, PermissionSettings,
   WorkspaceUiSettings, WarehouseSettings, ObxSettings, graph engine). Router
   `internal/settings.py` imports the concrete class; tests retarget their
   patch paths in the same PR; forwarders deleted.
5. **GraphEngineSettings** — callers go to the per-engine class; keep only
   dispatch.
6. **RegistryService** — introduce `store` property; move the 22 pure
   read/write forwards to `svc.store.x` call sites (collab services first).
7. **DatabricksClient** — add `SQLClient` Protocol; migrate the 28
   `DatabricksClient(` construction sites' consumers to `client.sql.*` /
   `client.catalog.*`; delete forwarders; amend `.cursor/07`.
8. Final: delete `test_v090_settings_facade.py` delegator section; tighten
   baseline; update Sphinx `docs/sphinx/api/`.

### Test strategy

Pure moves: no new behaviour tests. Each PR greps `patch(` targets for the
methods it deletes and retargets them in the same commit (patches that
silently stop intercepting hit live Lakebase — see class-per-file Lesson).
Middle-man baseline must strictly decrease each PR.

### Risks

- Silent patch misses → tests hit live services. Mitigation: run the touched
  test packages with `--timeout` and network disabled (`pytest-socket` if
  available, else the existing `db` marker deselection).
- Circular imports once `_dt()` is gone. Mitigation: collaborators import
  records/constants only; entry-point imports collaborators, never reverse.
- Merge churn with concurrent work on settings routers. Mitigation: one
  router per PR, land early in the week.

**Effort:** L (≈12 PRs). **Priority:** P1.

---

## 2. Agent tool-calling loop — Form Template Method

### Problem

- The loop `for iteration in range(MAX_ITERATIONS | iteration_limit)` →
  `call_serving_endpoint` → `dispatch_tool` → `accumulate_usage` → build
  result is copied **12 times** (corrected from ~8): 8 engines
  (`agent_owl_generator/engine.py:594`, `agent_ontology_assistant:192`,
  `agent_graph_interpreter:283`, `agent_business_rules_generator:403`,
  `agent_dtwin_chat:261`, `agent_auto_icon_assign:204`, `agent_cohort:194`,
  `agent_auto_assignment:259`) + 4 PGE stages
  (`agent_mapping_pge/planner.py:462`, `generators/entity.py:503`,
  `generators/relationship.py:564`, `evaluator/critic.py:487`).
- Three engines (owl_generator, auto_icon_assign, business_rules_generator)
  additionally duplicate a "retry with fallback params" second
  `call_serving_endpoint` inside the loop.
- `AgentResult` is redeclared in **9** engines (+ `SupervisorResult`);
  `agent_mapping_pge/engine.py:86` also redeclares `AgentStep`, already in
  `engine_base.py:50`.
- `ToolCallingAgent` is cited only in `src/.coding_rules.md:119` (and the
  class-per-file plan); it does not exist.
- **Tracing defect.** `engine_base.py:64` decorates `call_serving_endpoint`
  with `@trace_llm("agent:llm")`; `trace_llm` (`tracing.py:110-122`) fixes the
  span name at decoration time. Every caller passes `trace_name=_TRACE_NAME`
  but it only reaches log lines: all LLM spans of all agents are named
  `agent:llm` and are indistinguishable in MLflow.
- Class-per-file PLAN Phase 2 keeps `run_agent()` as a "3-line shim", which
  contradicts its own decision D2 (no shims). `run_agent` is not an external
  contract (routers and the MCP server are in-repo), so callers move.

### Target design

```text
src/agents/core/
  AgentStep.py          # moved from engine_base.py
  AgentResult.py        # success, steps, iterations, error, usage
  ServingEndpoint.py    # call_serving_endpoint + unsupported-param cache
  ToolCallingAgent.py   # Template Method
```

```python
class ToolCallingAgent(ABC, Generic[R]):
    trace_name: ClassVar[str]
    max_iterations: ClassVar[int] = 10

    def run(self) -> R:                       # template; wrapped in trace_agent(self.trace_name)
        messages = self.initial_messages()
        for iteration in range(self.iteration_limit()):
            response = self.call_llm(messages)          # span "<trace_name>:llm"
            calls = self.tool_calls(response)
            if not calls:
                if self.on_text(response, messages):    # True = done
                    break
                continue
            for call in calls:
                self.on_tool_result(call, self.dispatch(call))  # span "tool:<name>"
        return self.finalize()

    # hooks: system_prompt, user_prompt, tools, tool_handlers, on_text,
    # on_tool_result, finalize, llm_params (max_tokens, response_format)
```

- Each agent becomes `agent_x/<X>Agent.py` + `<X>Result.py(AgentResult)`.
- PGE stages: `PGEStage(ToolCallingAgent)` intermediate base (iteration
  limit + JSON-contract parsing shared by the 4 stages).
- Tracing: `trace_llm` resolves the span name at call time
  (`kwargs["trace_name"]`), so spans read `owl_generator:llm`,
  `dtwin_chat:llm`, … The template sets it from `trace_name`.

### Migration (small PRs, eval-gated)

0. **Precondition:** item 8 brings every agent's
   `tests/eval/datasets/<agent>/baseline.jsonl` to ≥ 10 rows and adds
   runners; without that the gate cannot produce a meaningful delta.
1. **Fix span naming** (`tracing.py`, `engine_base.py`). Gated: eval run on
   one agent to prove no score change; check existing MLflow filters on
   `agent:llm` (only `responses_agent.py:252` passes a distinct name today).
2. **Extract `agents/core/`** records + `ServingEndpoint` (Move Function);
   delete the duplicate `AgentStep` in mapping_pge. No behaviour change.
3. **Add `ToolCallingAgent`** with unit tests on a fake agent (scripted
   responses through `tests/fixtures/http.py` `MockTransport`).
4. **One agent per PR**, smallest first: cohort → ontology_assistant →
   graph_interpreter → auto_icon_assign → auto_assignment → dtwin_chat →
   business_rules_generator → owl_generator. Each PR:
   a. record **golden traces** before the change: run the agent against
      scripted LLM responses and snapshot the message list, tool calls and
      `AgentResult` (deterministic, CI-cheap);
   b. migrate; golden traces must be byte-identical;
   c. MLflow eval run before/after on the dataset; paste both URIs; delta
      must be neutral within SPEC.md tolerance;
   d. update callers of `run_agent` (routers, MCP, supervisor) to
      `XAgent(...).run()` and delete `run_agent`.
5. **PGE stages** (4 PRs) under `PGEStage`, eval on `agent_mapping_pge`
   baseline (30 rows) + `metric_view.jsonl`.

### Test strategy

Golden-trace equivalence (new, deterministic) as the refactor proof; MLflow
eval delta as the gate artefact; existing agent unit tests untouched except
import paths. Verify spans in a local MLflow run (`mlflow.db`): one `AGENT`
span per run, `LLM` spans named per agent, `TOOL` spans unchanged.

### Risks

- Subtle loop differences (early-exit rules, fallback retries, owl staged
  evaluation) get flattened. Mitigation: hooks for each variant; golden traces
  catch divergence.
- Eval noise masks regressions. Mitigation: golden traces are the hard proof;
  eval is a sanity check with temperature pinned.
- Gate is in calibration mode (`eval-gate.yml:25`), so it will not block —
  reviewers must enforce manually.

**Effort:** L (≈14 PRs). **Priority:** P1 (after item 8).

---

## 3. Scheduled vs interactive build — Extract Class + Substitute Algorithm

### Problem

`back/objects/registry/schedule/build.py` (402 LOC, module functions)
re-implements `back/objects/digitaltwin/build/_build_pipeline.py::_BuildPipeline`
(1627 LOC file):

| Step | Scheduled (`build.py`) | Interactive (`_BuildPipeline`) |
|---|---|---|
| R2RML → SQL | `_generate_sql_from_r2rml` :31-79 | `_prepare_translation` :437 |
| View/graph naming `triplestore_{safe}_V{n}` | `build.py:50` | also `SQLHelpers.py:128`, `DomainSession.py:1089` |
| Lakeflow view wrapping | `_view_sql_for_graph_store` :97 | `_wrap_view_sql_for_lakeflow` :324 |
| Managed-synced apply | `_apply_synced_pipeline` :108 ("Mirrors `_apply_via_synced_pipeline`") | `_apply_via_synced_pipeline` :968-1336 (**~369-LOC method**) |
| Count triples | `_count_view_triples` :180 | `_count_view_triples` :336 |
| Stream rows | `_stream_into_store` :82 | `_stream_triples_into_store` :914 |
| Full rebuild | `_write_graph_triples` :189 | `_apply_full_rebuild` :767 |
| Persist | `_persist_domain_metadata` :245 | `_persist_last_build_to_registry` :1498 |

**Behavioural divergence (bug risk).** The scheduled path stamps `last_build`
with `svc._store.write_version(domain_name, version, domain.export_for_save())`
(`build.py:250`) — a full JSONB rewrite. The interactive path documents that
exact call as unsafe (`_build_pipeline.py:1506`: "A full `write_version` here
is unsafe…") and uses `stamp_last_build`. Both also reach into
`svc._store` (private).

Scheduled builds also skip cancellation, adjacency rebuild
(`_rebuild_adjacency_if_supported`), build-run recording and
incremental/drift logic — some deliberately ("always full rebuilds"), some
not.

### Target design

One pipeline, two drivers. Extract the phases into `digitaltwin/build/`
classes (Template Method per `.cursor/07 §9` "build phases"):

- `BuildPlan` (record): view_table, graph_name, sql, mappings, stats — built
  once by `BuildPlanner.from_domain(domain)`; the only place naming lives
  (delete the two other `triplestore_{…}` sites in favour of it).
- `GraphApplier` strategies: `FullRebuildApplier`, `SyncedPipelineApplier`
  (Replace Conditional with Polymorphism on `store.is_synced`).
- `BuildRunRecorder` (`stamp_last_build` + build-run row).
- `_BuildPipeline` (session/API driver: progress, cancel, session cache) and
  `ScheduledBuild` (TaskContext driver) both compose the above.

### Migration

1. **Fix the divergence first** (behaviour PR): scheduled build uses
   `stamp_last_build`; test with a fake store asserting no `write_version`.
2. Extract `BuildPlanner`/`BuildPlan`; both paths call it. Golden test: plan
   for fixture domains equals today's tuple from `_generate_sql_from_r2rml`.
3. Extract `SyncedPipelineApplier` from `_apply_via_synced_pipeline`
   (Extract Method ×N first, inside the class, then Move); scheduled path
   switches to it; delete `_apply_synced_pipeline`.
4. Extract `FullRebuildApplier` (+ streaming/count); delete
   `_write_graph_triples`, `_stream_into_store`, `_count_view_triples`.
5. `schedule/build.py` → `ScheduledBuild` class (filename rule), ≈80 LOC.
   Task-type string references stay (DAB/scheduler registry contract).
6. Move `collect_domain_stats` out of the private `_build_pipeline` module
   (it is imported by `back/core/.../DeltaTripleStoreBuildPipeline.py:14`,
   see item 4).

### Test strategy

Contract tests per applier against the graphdb fake stores; one parametrised
test running both drivers on the same fixture domain and asserting identical
store calls (except progress/cancel). Existing `tests/units/dtwin` and
`tests/units/registry` schedule tests stay green.

### Risks

Managed-synced path only fully exercised against live Lakebase/Lakeflow →
run the `scenario` campaign for synced builds once after step 3 (opt-in).

**Effort:** M (≈6 PRs). **Priority:** P0 for step 1 (data safety), P1 for the rest.

---

## 4. Layer inversion and backward-compat shims

### Problem

**a. `back/core` → `back/objects`: 26 import statements in 10 files**
(brief said ~25):

| File | Imports |
|---|---|
| `core/helpers/DatabricksHelpers.py` | 8 (`RegistryCfg` ×2, `global_config_service` ×6) |
| `core/graphdb/delta/DeltaTripleStoreBuildPipeline.py` | 6 (`collect_domain_stats`, `DomainSnapshot`, `DigitalTwin` ×2, `RegistryService`, `sanitize_domain_folder`) |
| `core/databricks/DatabricksAuth.py:73-74` | `RegistryCfg`, `global_config_service` |
| `core/graphdb/GraphDBFactory.py:246,255` | `global_config_service`, `RegistryCfg` |
| `core/graphdb/lakebase/LakebaseFlatStore.py:1270,1344` | `RegistryCfg` ×2 |
| `core/industry/{cdisc,fibo,iof}/*ImportService.py` | `Ontology` ×3 |
| `core/w3c/sparql/DomainQueryService.py:10` | `resolve_domain_slice` |
| `core/__init__.py:28-35` | lazy `__getattr__` re-exporting `DomainSession`, `get_domain`, `get_empty_domain` |

Root cause: two infrastructure needs (registry coordinates, instance-wide
config) are modelled as domain objects. `DeltaTripleStoreBuildPipeline` is a
domain workflow living in `core`.

**b. Shims.** 29 src files self-describe as "backward-compatible";
**93** module-level `name = Class.method` aliases in 17 files. Main ones:
`core/helpers/__init__.py` (33), `core/graphdb/search_cache.py` (11, whole
module is an alias shim), `core/databricks/__init__.py:29-42` (9 + the
`get_local_user_email` function), `ontology/generate/GenerateWorkflow.py` (8),
`core/errors/__init__.py:21` (`register_exception_handlers` wrapper),
`core/w3c/sparql/__init__.py:10-27` (3 wrappers), `api/service.py`
(whole-module re-export facade, 3 aliases), `api/routers/digitaltwin.py:50-52`
(3 aliases), `api/routers/internal/dtwin.py:1975-2008` (9 private one-line
forwarders to `TwinAssistantCache`). All contradict `.cursor/07 §Package
Organisation 4`.

**c. FastAPI `Request` in `back/objects`.** **36** function signatures take a
`request` parameter (corrected from ~55): `registry/collab/EditLockService.py`
12, `ReviewService.py` 9, `CommentService.py` 8, `session/FileSessionMiddleware.py`
3, `session/SessionManager.py` 2, `SettingsService` 1,
`RegistryDomainSettings` 1. In the collab services `request` is only read
for identity (`EditLockService._identity` :487 → `request.state.user_email`,
display name, `session_id`; `_is_admin`). The session middleware/manager are
genuinely HTTP (class-per-file A1 #1) and belong in `shared/fastapi/`.

### Target design

- `back/core/config/RegistryCoordinates` (record: catalog, schema,
  lakebase_schema, lakebase_database) + `InstanceConfigProvider` Protocol in
  core; `back/objects` supplies the implementation at startup
  (Dependency Inversion). `RegistryCfg` builds from / wraps it.
- `DeltaTripleStoreBuildPipeline` moves to `back/objects/digitaltwin/build/`
  (Move Class) — it orchestrates domain objects.
- Industry importers return parsed OWL/records; the caller in
  `back/objects/ontology` constructs `Ontology` (Move Function).
- `DomainQueryService` receives the slice as a parameter (Replace Query with
  Parameter) and moves to `back/objects/domain/` if it keeps needing domain
  payloads.
- `CallerIdentity` frozen dataclass (`email`, `display_name`, `session_id`,
  `is_admin`) in `back/objects/session/CallerIdentity.py`, built once per
  request by a FastAPI dependency `get_caller_identity` in
  `api/routers/internal/_guards.py`. Collab services take `CallerIdentity`
  (Introduce Parameter Object).
- `FileSessionMiddleware` + `SessionManager` → `shared/fastapi/session/`.
- New guardrail `tests/units/architecture/test_layering.py`: AST check that
  `back/core/**` never imports `back.objects` / `api` / `fastapi`, and
  `back/objects/**` never imports `fastapi` / `starlette`; shrink-only
  baseline.

### Migration

1. Guardrail with today's 26 + 3 offenders baselined.
2. Delete shims whose callers are few (one PR each, callers updated in the
   same PR): `search_cache.py`, `w3c/sparql` wrappers, `errors`
   wrapper, `dtwin.py` forwarders, `digitaltwin.py` aliases, `api/service.py`
   (v1 imports the classes directly — the REST contract is the URL, not the
   module).
3. `core/helpers/__init__.py` and `core/databricks/__init__.py` aliases: one
   helper class per PR (`SQLHelpers`, `URIHelpers`, `DatabricksHelpers`,
   `MetadataService`, `UCDomainIO`), callers switch to `Class.method`.
4. `RegistryCoordinates` + `InstanceConfigProvider`; migrate
   `DatabricksHelpers` → `DatabricksAuth` → `GraphDBFactory` →
   `LakebaseFlatStore` (one PR each). Coordinate with item 7 (same fields).
5. Move `DeltaTripleStoreBuildPipeline` (with item 3 step 6).
6. Industry importers + `DomainQueryService`.
7. `CallerIdentity`: EditLockService → ReviewService → CommentService
   (one PR each; route signatures gain `identity = Depends(get_caller_identity)`).
8. Move session middleware/manager to `shared/fastapi/session/`.

### Test strategy

Layering test is the acceptance criterion (baseline → 0). Collab services get
plain `CallerIdentity(...)` in tests instead of `MagicMock(state=…)` requests
— simpler fixtures. Patch retargeting per moved module as in item 1.

### Risks

Import cycles at startup when providers are wired. Mitigation: register the
provider in the app factory (`shared/fastapi/main.py`) before routers import;
fail fast with a clear error if unset. MCP server and jobs that import `core`
standalone need a default provider (env-based).

**Effort:** L (≈15 PRs; shims are S each). **Priority:** P1 (shims P2).

---

## 5. Blocking I/O in async handlers, ad-hoc threads, hand-rolled error mapping

### Problem

**a. Sync I/O on the event loop.** AST heuristic (sync call to a known
I/O method inside `async def`, not awaited, not inside
`run_blocking`/`to_thread`/nested `def`): **61 handlers, ~62 calls**:

| File | Handlers | Calls seen |
|---|---|---|
| `api/routers/internal/ontology.py` | 20 | `domain.save`, `RegistryService.from_context`, `read_file` |
| `api/routers/digitaltwin.py` | 16 | `DigitalTwin.resolve_domain`, `resolve_registry` |
| `api/routers/v1.py` | 9 (all data endpoints, :208-407) | `load_domain_from_uc`, `list_domains_from_uc`, `execute_sparql_query` |
| `api/routers/domains.py` | 6 | `resolve_domain`, `list_mcp_domains` |
| `api/routers/internal/dtwin.py` | 4 | `execute_sync`, `resolve_registry` |
| `api/routers/internal/domain.py` | 3 | `domain.save`, `read_document`, `load_domain_from_uc` |
| `api/routers/internal/home.py` | 2 | `read_file`, `write_file` |

`run_blocking` already exists (`core/helpers/DatabricksHelpers.run_blocking`)
and is used correctly in `digitaltwin.py:629,902,1658,1733`.

**b. Ad-hoc threads.** **20** raw `threading.Thread(...)` launches (corrected
from ~10): 16 in routers (`internal/dtwin.py` 6, `internal/ontology.py` 5,
`digitaltwin.py` 3, `internal/mapping.py` 2) + 4 in `back/objects`
(`domain/Domain.py:2261,2336`, `GraphEngineLakebaseSettings.py:478`,
`node/JobActionService.py:130`). The usual shape is
`tm.create_task(...)` + nested `def _run()` + `Thread(...).start()`
(e.g. `internal/dtwin.py:1239-1265`) — exactly what
`TaskManager.run_background_task` (`TaskManager.py:377`) does, which has only
**2** callers. `GitHubMirror.py:144` (long-lived worker) and `TaskManager`
itself are legitimate. Plus `asyncio.create_task` at `internal/dtwin.py:2253`.

**c. Error mapping.** `map_route_errors` (`internal/_helpers.py:49`) is used
69 times, all in `internal/settings.py` (30), `internal/ontology.py` (37),
`internal/dtwin.py` (2). **37** hand-rolled
`except Exception … raise InfrastructureError(…)` blocks remain:
`internal/dtwin.py` 15, `internal/mapping.py` 7, `internal/domain.py` 6,
`digitaltwin.py` 4, `domains.py` 3, `_helpers.py` 2. It lives under
`internal/`, so external routers can't use it without importing from the
internal package.

### Target design

- Routes stay `async def`; every sync domain/infra call is
  `await run_blocking(fn, *args)`. Pure-CPU or trivially fast handlers may
  become plain `def` (FastAPI threadpools them) — prefer that when a handler
  does nothing async (most of `v1.py`).
- `TaskManager.run_background_task(name, task_type, target, *args, steps=…)`
  is the single launch path; it wraps `target` so an uncaught exception marks
  the task failed (today each `_run` does that by hand).
- `map_route_errors` moves to `shared/fastapi/route_errors.py` (Move Function),
  used by all routers.
- Guardrail `tests/units/architecture/test_async_discipline.py`: AST check
  (same heuristic) + `threading.Thread` outside `task_manager/` and
  `gitsync/`, shrink-only baseline.

### Migration

1. Move `map_route_errors` to `shared/fastapi/`; update 3 importers.
2. Guardrail + baseline (61 handlers, 20 threads).
3. `v1.py`: switch the 9 data handlers to `def` (no awaits inside) — one PR,
   contract tests (`tests/contract/test_openapi_contract.py`) unchanged.
4. `run_background_task` gains the fail-on-exception wrapper; convert
   threads one router per PR (`dtwin.py`, `ontology.py`, `mapping.py`,
   `digitaltwin.py`), then the 4 `back/objects` sites (those receive the
   `TaskManager` via constructor).
5. Wrap remaining sync calls with `run_blocking`, one router per PR, removing
   the hand-rolled `except Exception` blocks in the same PR (`with
   map_route_errors("…", logger):`).
6. Review `asyncio.create_task` in the chat stream: keep if it is the
   streaming producer (document why), else TaskManager.

### Test strategy

Behaviour unchanged → existing route tests. Add one concurrency test per
router family: a slow fake (`time.sleep(0.5)`) behind a handler, two
concurrent `httpx.AsyncClient` requests must finish in < 0.9 s. Error
mapping: parametrised test that a raw `RuntimeError` becomes 502 with a safe
message (no `str(e)` in `message`).

### Risks

- `run_blocking` moves code to a worker thread; anything reading
  request-scoped `ContextVar`s (forwarded identity, session) must capture
  values before the hop. `asyncio.to_thread` copies context, verify
  `run_blocking` does too.
- Thread-pool exhaustion under load (default 40 workers) → document; cap
  long-running work through TaskManager, not `run_blocking`.

**Effort:** M (≈10 PRs). **Priority:** P0 (v1/domains are the MCP hot path; one
slow Volume read stalls every request).

---

## 6. Size ratchet vs size rules — soft-budget ratchet

### Problem

`tests/units/architecture/ratchet.py:27-28` enforces `MAX_CLASS_LOC = 800`,
`MAX_FUNCTION_LOC = 100`. The rules (`.cursor/07 §Class-First 6`,
`.coding_rules §2`) say **≤ 400 / ≤ 50, hard ceiling 800 / 100**. So the
ratchet enforces the hard ceiling only; the budget itself is unenforced.
Current `src/` (SourceIndex):

| | Total | > budget | between budget and ceiling | > ceiling (baselined) |
|---|---|---|---|---|
| Classes (400 / 800) | 363 | 65 | **44** | 21 |
| Functions (50 / 100) | 4127 | 512 | **377** | 135 |

The 44 + 377 offenders between the two limits can grow freely up to the
ceiling. Also: `baseline.json` lists 136 oversized functions vs 135 measured
— one entry looks stale; `test_size_baseline_has_no_stale_entries` will flag
it on the next run (re-check after concurrent edits land).

### Target design

Two tiers in the same ratchet:

- **Hard tier (unchanged):** > 800 / > 100 → shrink-only baseline
  (`oversized_classes`, `oversized_functions`).
- **Soft tier (new):** > 400 / > 50 → `over_budget_classes`,
  `over_budget_functions` shrink-only baseline. **New code may not enter the
  soft tier**; existing entries may not grow. No new entries are ever added.
- `MAX_CLASS_LOC`/`MAX_FUNCTION_LOC` renamed `HARD_CLASS_LOC`/`HARD_FUNCTION_LOC`,
  plus `BUDGET_CLASS_LOC = 400`, `BUDGET_FUNCTION_LOC = 50`, so the code
  states the rule literally.
- `ratchet.py` reports per-PR deltas (`--report`) so reviewers see the trend.

### Migration

1. One PR: add soft tier constants, `ArchitectureRatchet.over_budget_*`,
   tests mirroring `test_size_budget.py`, seed the baseline once from today's
   values (reviewed manual edit, like `aggregate_record_files`), fix the stale
   entry.
2. Update `.cursor/07 §11` / `.coding_rules §2 Enforcement` to name both
   tiers.

### Test strategy

Unit-test `_carry_over` for the soft tier (moves keep budget only if not
grown, as today). Run `python -m tests.units.architecture.ratchet` twice → idempotent.

### Risks

Baseline churn on every refactor PR (merge conflicts in `baseline.json`).
Mitigation: sorted keys (already), and the ratchet command regenerates
deterministically — resolve conflicts by re-running it.

**Effort:** S (1–2 PRs). **Priority:** P1 (cheap; stops drift for every other item).

---

## 7. Registry Volume removal — staged (expand / contract)

### Problem

Documents moved to the Lakebase Knowledge Store on 2026-09-24
(`changelogs/v0.9.0/benoitcayladbx_2026-09-24.log`, items 5–27), and the
deploy no longer binds a Volume. The Volume triplet still survives in:

- `RegistryCfg.is_configured` → `bool(self.catalog and self.schema and self.volume)`
  (`RegistryCfg.py:241-242`); `_DEFAULT_VOLUME = "OntoBricksRegistry"` (:12);
  `as_dict` (:244-252).
- Lakebase schema: `registries.volume text NOT NULL` (`store/lakebase/schema.sql:22`);
  `Bootstrap._ensure_registry_row` upserts it (`Bootstrap.py:470-490`).
- Initialize creates the UC Volume: `RegistryService.initialize`
  (`RegistryService.py:215`, volume probe/create :230-280), `volume_root` /
  `_resolve_domains_folder` / domains folder (:166-200), Volume wipe on delete (:477).
- `RegistrySettings` probes existence + READ VOLUME (`RegistrySettings.py:205-242`)
  via `UnityCatalog.py:617-631`; settings router doc `internal/settings.py:301`.
- Session default `"volume": "OntoBricksRegistry"` (`DomainSession.py:153`);
  `uc_domain_path` / `uc_version_path` (:795, :820) consumed by
  `DatabricksHelpers.effective_uc_version_path` (:627-633).
- **`registry_volume` query/body param: 80 occurrences in 11 files**, notably
  public `api/routers/digitaltwin.py` (37, e.g. :312, :337, :477, :564, :675)
  and `api/routers/domains.py` (15, :181-192, :241), plus `internal/dtwin.py`,
  `agent_dtwin_chat/engine.py`, `NodeContextService.py`, `TwinResolve.py`,
  `_query_api.html`.
- MCP session: `mcp-server/server/session.py:90,174,188,207`.
- **Stale rule:** `.cursor/07 §Subpackage Patterns / registry` still says
  `documents/` uploads "must still go to the UC Volume via `VolumeFileService`".

### Target design

Registry identity = `(catalog, schema)` + Lakebase coordinates. No Volume
anywhere in the registry path. `VolumeFileService` remains only for the user
UC file browser / explicit Volume imports.

### Migration (staged across 0.9.x → 0.10.0)

**Stage A — stop depending (0.9.x, no schema change)**

1. Fix `.cursor/07` registry pattern text (docs-only PR).
2. `RegistryCfg.is_configured` → `catalog and schema`; keep the `volume`
   field but default `""`. Tests: config-guard + settings status.
3. `RegistryService.initialize`: delete Volume probe/create; `volume_root`
   and domains-folder helpers deleted with their callers (verify none outside
   legacy migration script). Settings → Registry drops the READ VOLUME probe
   and Volume row in the UI.
4. `_ensure_registry_row` writes `''` for `volume` (satisfies `NOT NULL` on
   DML-only deployments where the app SP cannot run DDL — `Bootstrap.py:333-343`).
5. Session: drop `registry.volume` default; `uc_domain_path` /
   `uc_version_path` → removed with `effective_uc_version_path` callers
   (check graph archive users first: `_starter_kit/ExampleStore.py:174` is a TODO only).
6. MCP `session.py`: stop sending/deriving `volume`.

**Stage B — public API deprecation (0.9.x)**

7. `registry_volume` on `/api/v1/digitaltwin/*` and `/api/v1/domain*`:
   keep accepting (external contract, `.cursor/07 §Package Organisation 4`),
   ignore the value, mark `Query(..., deprecated=True)`, emit
   `Deprecation: true` + `Sunset` headers when supplied, log once at INFO.
   Internal callers (`internal/dtwin.py`, dtwin chat agent, `_query_api.html`)
   stop sending it in the same PR. The agent change is a tool-surface change
   → `.cursor/12` gate (dtwin_chat dataset has 12 rows, enough for a change).
8. Release notes + `docs/` API page announce removal in 0.10.0.

**Stage C — schema contract (0.10.0)**

9. Owner-run migration (via `make bootstrap-lakebase` /
   `scripts/bootstrap/` with the provisioning identity, since the app SP is
   DML-only): `ALTER TABLE registries ALTER COLUMN volume DROP NOT NULL;`
   then, one release later or same release after verification,
   `ALTER TABLE registries DROP COLUMN volume;`. In-app path follows the
   existing `_ensure_*_column` pattern (best-effort, skipped when not owner).
10. Remove `registry_volume` params from the routers and OpenAPI contract;
    update `tests/contract/` snapshot.
11. Remove `RegistryCfg.volume` and `as_dict` (legacy-caller compat).

### Test strategy

- Stage A: settings/registry unit tests with a `RegistryCfg` without volume;
  Initialize test asserts no `create_volume` call.
- Stage B: route tests assert the param is accepted, ignored, and sets the
  `Deprecation` header; OpenAPI marks it deprecated.
- Stage C: `db`-marked test against Lakebase that the migration is idempotent
  and that a DML-only role still initialises.

### Risks

- Deployments still on a pre-v0.4.0 Volume registry: the migration script
  `scripts/migrate-registry-to-lakebase.sh` still needs the Volume — keep it
  self-contained (own config), not dependent on `RegistryCfg.volume`.
- External MCP/REST clients passing `registry_volume` — Stage B keeps them
  working for one minor.
- Concurrent `RegistryCfg` changes from item 4 — sequence item 7 Stage A
  before item 4 step 4.

**Effort:** M (≈9 PRs over two releases). **Priority:** P1.

---

## 8. AI-feature gate debt

### Problem

- `src/agents/agent_business_rules_generator/` (engine.py 576 LOC, tools.py;
  wired from `api/routers/internal/ontology.py` and
  `back/objects/ontology/Ontology.py`) has **no** `.planning/agents/…/SPEC.md`,
  **no** `tests/eval/datasets/…`, **no** `tests/eval/thresholds.yaml` entry.
- Dataset sizes (`tests/eval/datasets/<agent>/baseline.jsonl`, rows):

  | Agent | baseline | other files | Gate status |
  |---|---|---|---|
  | auto_assignment | **3** | observability 3 | < 10 |
  | auto_icon_assign | **3** | regression 1 | < 10 |
  | cohort | **3** | — | < 10 |
  | ontology_assistant | **3** | — | < 10 |
  | dtwin_chat | **12** (corrected: ≥ 10, but < 20) | — | ok for changes |
  | graph_interpreter | 20 | — | ok |
  | supervisor | 20 | — (no `thresholds.yaml` entry) | ok |
  | mapping_pge | 30 | metric_view 10 | ok |
  | owl_generator | 41 | metric_view 10, regression 3 | ok |
  | business_rules_generator | — | — | missing |

- Runners exist only for 4 agents (`tests/eval/run_agent_{dtwin_chat,graph_interpreter,mapping_pge,owl_generator}.py`);
  ROADMAP T-M4 still open.
- **Two dataset locations** in `.cursor/12`: line 21
  (`.planning/<slug>/eval/dataset.jsonl`) vs line 35
  (`tests/eval/datasets/<agent>/`). CI reads only
  `tests/eval/datasets/${a}/baseline.jsonl` (`eval-gate.yml:122`). The
  `.planning/agents/*/eval/` copies have drifted (mapping_pge: 10 rows there
  vs 30 in `tests/eval/`). Also `.cursor/12` says `.planning/<slug>/SPEC.md`
  while CI requires `.planning/agents/<agent>/SPEC.md` (`eval-gate.yml:67`),
  and cites a personal absolute path (`/Users/dermot.smyth/...`, line 50).
- Gate still in calibration (`CALIBRATION_MODE: "true"`, `eval-gate.yml:25`),
  so none of this blocks today.

### Target design

- One canonical location: `tests/eval/datasets/<agent>/{baseline,regression}.jsonl`;
  SPEC at `.planning/agents/<agent>/SPEC.md`. `.planning/agents/*/eval/`
  removed.
- Every agent: SPEC + ≥ 20 baseline rows + runner + thresholds entry.
- `test_eval_inventory.py` (unit, no LLM): every `src/agents/agent_*` has
  SPEC, dataset ≥ 20, runner, thresholds key — shrink-only allowlist.

### Migration

1. Docs PR: fix `.cursor/12` (single path, `.planning/agents/<agent>/`,
   drop personal path); delete `.planning/agents/*/eval/` after diffing into
   `tests/eval/datasets/` (keep the larger/newer set; move unique rows).
2. `test_eval_inventory.py` with today's gaps allowlisted.
3. business_rules_generator: SPEC.md (via `.claude/skills/ai-feature/`),
   20-row dataset (synthetic cold start, seeded from existing unit-test
   fixtures), runner, thresholds; baseline MLflow run recorded.
4. Grow cohort, ontology_assistant, auto_icon_assign, auto_assignment to
   ≥ 20 (one PR per agent, includes a runner); dtwin_chat to 20.
5. Add `supervisor` thresholds entry.
6. When all green, flip `CALIBRATION_MODE` to `"false"` (separate PR, team
   decision).

### Test strategy

Inventory test is deterministic; each new runner gets a dry-run unit test
with a mocked endpoint (pattern: `tests/eval/test_run_agent_owl_generator.py`).
Baseline MLflow runs are recorded and linked in each PR.

### Risks

Synthetic datasets overfit the prompt. Mitigation: mix in production traces
to `regression.jsonl` as they appear; judge endpoint ≠ agent endpoint.

**Effort:** M (≈8 PRs, mostly dataset authoring). **Priority:** P0 (blocks item 2).

---

## 9. Frontend rule drift (`.cursor/11`, `.cursor/05 §Frontend`)

### Problem (counts on `src/front/`, 106 JS files, 79,933 JS LOC)

| Rule | Count |
|---|---|
| `new bootstrap.Modal(` (rule: `getOrCreateInstance`) | **55** in 24 files (vs 38 `getOrCreateInstance`) |
| Hard-coded colours | JS: 298 hex + 15 `rgb(a)`; CSS outside token definitions: 999 hex + 277 `rgb(a)`; templates: 150 hex |
| Inline `on*=` handlers | templates 125; JS-built HTML strings 151 |
| Inline `style="…"` | templates 100; JS-built HTML strings 324 (+570 `.style.x =` assignments) |
| `<style>` / inline `<script>` blocks in templates | 2 / 2 |
| Modals built from JS strings | **31** in 13 files |
| Native `confirm()` | **10** (settings.js 7, ontoviz.js:4959, registry-teams.js:34, query-cohorts.js:1280) |
| Escape helpers | **56** definitions (`esc` 18, `escapeHtml` 9, `_esc` 8, `escapeAttr` 4, `escHtml` 3, `escAttr` 3, …) |
| Top-level declarations (globals) | **1,149** (corrected from ~1,174) + 183 `window.x =`; only 33/106 files use an IIFE/module scope |
| Oversized files | `global/ontoviz/ontoviz.js` 5080, `config/js/settings.js` 4836, `ontology/js/ontology-shared-panels.js` 4279, `query/js/query-sigmagraph.js` 4097, `mapping/js/mapping-design.js` 3895, `ontology/js/ontology-map.js` 3127 |
| CDN without SRI | templates: **10** tags without `integrity` (Bootstrap CSS ×2, Bootstrap Icons ×2, Bootstrap JS, chart.js ×3, d3 ×2), 2 with (marked, DOMPurify); JS loaders: 11 URLs in 6 files (unpkg `graphiql@3`, `react@18`, `react-dom@18`; cdnjs sigma/graphology; jsdelivr graphology-library; `d3js.org/d3.v7` ×4). `chart.js@4`, `react@18`, `graphiql@3` are floating versions — SRI is impossible until pinned |
| Empty catch | 31 `catch {}` + 4 `.catch(() => {})` |
| `fetch(` without status check | 479 `fetch(` vs 126 `.ok` checks; only `settings-github.js:176` has a `fetchJson` helper |

No JS linter in the repo (no `package.json` / ESLint config); frontend rules
are enforced by pytest text checks under `tests/units/front/`.

### Target design

- **Shared primitives in `global/js/`** (each its own file):
  `ob-escape.js` (`OB.escapeHtml`, `OB.escapeAttr`), `ob-fetch.js`
  (`OB.fetchJson(url, opts)` → throws `OBHttpError` on `!resp.ok`, parses
  JSON, routes to `showNotification` on opt-in), `ob-modal.js`
  (`OB.modal(id).show()` wrapper on `getOrCreateInstance`).
- **Namespacing:** each area file wraps in an IIFE and exports one object
  (`window.OB.mapping = {...}`); inline handlers replaced by delegated
  listeners on `data-action` attributes (one listener per area).
- **Vendored, pinned libs** under `static/vendor/<lib>@<version>/` (no CDN at
  runtime; also removes the third-party availability dependency inside
  Databricks Apps). If CDN is kept, pin exact versions and add
  `integrity` + `crossorigin`.
- Colours via `--db-*` / `--ovz-*` tokens only; JS reads tokens with
  `getComputedStyle` where a canvas/graph lib needs literal colours.
- **Frontend ratchet** `tests/units/front/test_frontend_ratchet.py` +
  `tests/units/front/frontend_baseline.json`: per-file counts of each metric
  in the table above; shrink-only (same semantics as the architecture
  ratchet, with a `python -m tests.units.front.ratchet` tightener).
  Two zero-tolerance checks from day one: native `alert/confirm/prompt` and
  CDN tag without `integrity`.

### Migration (per area, each PR keeps the ratchet green)

1. Ratchet + baseline (no source change).
2. Primitives: `ob-escape.js`, `ob-fetch.js`, `ob-modal.js` loaded in
   `base.html` (respecting the fixed load order; after `utils.js`, before
   area scripts).
3. Zero-tolerance fixes: 10 `confirm()` → `showConfirmDialog` /
   `showDeleteConfirm`; vendor or SRI-pin the 10 template tags + 11 loader
   URLs (`query-graphql.js`, `query-sigmagraph.js`, d3 loaders).
4. **Per area, one PR each**, ordered by risk-adjusted value:
   registry → domain → config/settings → mapping → query → ontology →
   ontoviz. Each PR: escape helpers → `OB.escapeHtml`; `fetch` →
   `OB.fetchJson`; `new bootstrap.Modal` → `OB.modal`; JS-string modals →
   partials under `templates/partials/<area>/`; inline `on*`/`style=` →
   `data-action` + CSS classes; empty catches → log + `showNotification` or
   an explicit comment why silent.
5. **Oversized files** (separate PR series after step 4 for that area):
   split by feature into `<area>-<feature>.js` (`settings.js` →
   `settings-registry.js`, `settings-graph-engine.js`, `settings-lakebase.js`,
   …; `ontology-shared-panels.js` per panel; `mapping-design.js` →
   canvas / panel / persistence). `ontoviz.js` last: it is the reusable
   library, split into modules behind the same public API.
6. Colours: per area, replace literals with tokens; add missing tokens to
   `main.css` rather than inventing per-area ones.

### Test strategy

Ratchet test per PR; existing `tests/units/front/*` contract tests; manual
smoke of the area via `web-devloop-tester` (console errors = 0, modals
open/close, Esc works). XSS: extend `test_frontend_xss_hardening.py` to
assert area files call `OB.escapeHtml` rather than local helpers.

### Risks

- Global-name removal breaks cross-file implicit dependencies (inline
  handlers in templates calling area globals). Mitigation: grep templates for
  each removed global; keep `window.OB.<area>` exports explicit.
- Big-file splits collide with feature work. Mitigation: split only after
  the area's behavioural cleanups land; announce freeze per file.
- Vendoring adds ~2 MB to the bundle; acceptable, and `.databricksignore`
  must not exclude `static/vendor/`.

**Effort:** L (≈20+ PRs, parallelisable per area). **Priority:** P2 (P0 for
step 3: SRI/pinning and native dialogs).

---

## 10. Ops hygiene

### Problem

- **Rendered app.yaml (corrected).** Root `app.yaml` is gitignored
  (`.gitignore:89`) and **not tracked**. `src/mcp-server/app.yaml` **is
  tracked** despite `.gitignore:90` (committed before the ignore rule), and
  contains the app URL with workspace id (line 31), warehouse id (line 40)
  and Lakebase project/database/schema (lines 48–52). No personal catalog in
  the tracked file. Its own template states "app.yaml is .gitignored — only
  this template is tracked". `databricks.yml:73` `sync.include`s the
  rendered file, so the bundle still ships it without tracking.
- **Demo scripts shipped.** `src/jobs/contract_demo.py` (95 LOC),
  `create_fake_contract_job.py`, `delete_contract_job.py` say "Test-only: not
  declared in the bundle", but `databricks.yml:127` keeps all of `src/jobs/`
  in sync (needed for `graph_analytics_job.py`, `resources/graph_analytics.job.yml:46`),
  so they deploy. Tests: `tests/units/jobs/test_contract_demo_jobs.py`.
- **`Settings.secret_key` is dead config.** `shared/config/settings.py:23`
  (default `"dev-secret-key-change-in-prod"`) → `shared/fastapi/main.py:696`
  → `FileSessionMiddleware.__init__` stores `self.secret_key` (:81) and never
  reads it; the cookie is a random uuid4 session id. The default suggests a
  security control that does not exist.
- **MCP `server/app.py` re-export shim** (159 LOC of `# noqa: F401`
  re-exports), created on purpose by `.planning/refactor-mcp-server/PLAN.md`
  step 8. Importers: `mcp_server.py:24`, `server/main.py:49`
  (`"server.app:combined_app"` uvicorn string), 21 references in 11 test
  files (incl. `patch("server.app.httpx…")`). The real external entry point
  is `server.main:main` (`src/mcp-server/pyproject.toml:38`), so `server.app`
  is not an external contract.
- **Module global** `_current_mode` in `mcp-server/server/http_client.py:46-52`
  (`global` write in `set_mode`, read in `_merged_headers` :77); set once per
  `MCPServerSession.__init__` (`session.py:67`). Mounted mode runs inside the
  main app process, so the last session constructed wins for every caller;
  per-request identity already uses a `ContextVar` (`_FWD`) next to it.

### Target design / Migration

1. **Untrack `src/mcp-server/app.yaml`** — `git rm --cached` (keep
   `sync.include`). Add `tests/units/settings/test_app_yaml_template.py`
   assertion: no rendered `app.yaml` is tracked (`git ls-files`). Treat the
   leaked values as non-secret but rotate nothing; note in release notes.
   **S, P0.**
2. **Demo jobs** → `examples/jobs/contract_demo/` (outside the bundle), tests
   follow; or, if they must be workspace files, add the three files to
   `sync.exclude` explicitly. Prefer moving (simplest). **S, P2.**
3. **`secret_key`** — Remove Parameter: delete the setting, the middleware
   argument and the attribute; update `test_middleware_session.py` (3 call
   sites). If signed cookies are wanted later, that is a feature with its own
   design. **S, P1.**
4. **`server/app.py` shim** — callers import from the topic modules
   (`server.factory`, `server.formatting`, `server.constants`,
   `server.http_client`); `main.py` points uvicorn at
   `"server.factory:combined_app"` (or builds the app in `main.py` and
   passes the object); tests retarget `patch` paths (21 refs); delete
   `app.py`; update `.planning/refactor-mcp-server/PLAN.md` decision. **S–M, P2.**
5. **`_current_mode`** — Replace Global with Parameter: `HttpClient` class
   holding `mode` (constructed by `MCPServerSession`, passed to tool
   handlers), or at minimum a `ContextVar` like `_FWD`. Test: two sessions in
   different modes in one process produce the right auth headers. **S, P2.**

### Test strategy

As listed per step; full MCP suite (`-m mcp`) for steps 4–5.

### Risks

Step 1: renderer must still produce the file before `bundle deploy`
(`scripts/deploy.sh:483` already checks). Step 4: Databricks Apps command in
`app.yaml.template` must keep resolving — run `make bundle-validate`.

**Effort:** S overall (5 small PRs). **Priority:** step 1 P0, rest P1–P2.

---

## Prioritised roadmap

| # | Item | Priority | Effort | Depends on |
|---|---|---|---|---|
| 10.1 | Untrack `src/mcp-server/app.yaml` | P0 | S | — |
| 3.1 | Scheduled build: `stamp_last_build` instead of full `write_version` | P0 | S | — |
| 9.3 | Native `confirm()` removal + SRI / pinned or vendored libs | P0 | S | 9.1 (ratchet) |
| 5 | Async discipline: `run_blocking`, `run_background_task`, `map_route_errors` | P0 | M | — |
| 8 | AI gate debt: business_rules SPEC + datasets ≥ 20 + runners + single dataset path | P0 | M | — |
| 6 | Soft-budget ratchet (400 / 50) | P1 | S | — |
| 1 | Remove Middle Man (SettingsService, DigitalTwin, GraphEngineSettings, RegistryService, DatabricksClient) + rule fix | P1 | L | 6 (guardrail pattern) |
| 2 | `ToolCallingAgent` Template Method + per-agent span names | P1 | L | 8 |
| 3.2+ | Unified build pipeline (`BuildPlanner`, appliers, `ScheduledBuild`) | P1 | M | 3.1 |
| 7 | Registry Volume removal (stages A → B → C) | P1 | M | — (Stage A before 4.4) |
| 4 | Layering (core ↛ objects, `CallerIdentity`, session → shared) | P1 | L | 7 Stage A, 3 step 6 |
| 4.2–4.3 | Delete backward-compat shims / alias modules | P2 | S each | 1 (overlapping callers) |
| 10.3 | Remove dead `secret_key` | P1 | S | — |
| 10.2, 10.4, 10.5 | Demo jobs out of bundle, MCP `app.py` shim, `_current_mode` | P2 | S | — |
| 9.4–9.6 | Frontend per-area cleanup + big-file splits + tokens | P2 | L | 9.1, 9.2 |
