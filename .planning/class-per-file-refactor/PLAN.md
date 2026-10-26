# Refactor program: one class per file, one purpose per class

> Status: **proposal** (2026-10-11). Nothing in `src/` has been changed yet.
> Generalises ROADMAP M4 (`digitaltwin-split`, `sparql-translator-split`,
> `settings-service-split`) into one program with shared rules and guardrails.
> Each phase below becomes its own `.planning/<slug>/PLAN.md` + PR series.

## Summary

The backend is functionally healthy (8913 tests green) but a large part of
the code is concentrated in a few god classes and multi-purpose modules:
21 classes exceed 800 LOC, 65 exceed 400 LOC, 139 methods exceed 100 LOC,
and 29 files hold several public classes. Two earlier Extract Class passes
left one-line delegating facades behind (`SettingsService`, `DigitalTwin`,
`GraphEngineSettings`), and that facade layer is where today's
"missing import" bugs lived. This program splits the code into small,
single-purpose classes (one per file), uses inheritance where it removes
real duplication (agents, graph backends, build phases, health checks,
SHACL evaluators), and adds CI ratchets so the codebase cannot regress.

---

## Part A — Code review (baseline, 2026-10-11)

### A1. Rule compliance

| # | Finding | Evidence | Rule |
|---|---|---|---|
| 1 | FastAPI types in `back/objects/` | `session/SessionManager.py:10` (`Request`, `get_session_manager` dependency), `session/FileSessionMiddleware.py` (Starlette middleware) | `.cursor/07`, `.coding_rules §1` |
| 2 | FastAPI router living under `back/` | `back/fastapi/graphql_routes.py` (`APIRouter`, 642 LOC, 13 functions + 3 models) | `.coding_rules §1` |
| 3 | Error envelopes instead of `OntoBricksError` | 41 `{"success": False}`: `agents/tools/planner.py` (19), `registry/collab/EditLockService.py` (6), `store/lakebase/Bootstrap.py` (4), PGE generators, `GraphEngineLakebaseSettings.py` | `.coding_rules §4` |
| 4 | Broad exception handling | 746 `except Exception`, 39 of which swallow (`pass` / `return None/[]/{}`). Top: `Domain.py` (35), `health.py` (26), `_build_pipeline.py` (20), `internal/dtwin.py` (20) | `.coding_rules §4` |
| 5 | Fat route handlers | 97 handlers > 30 LOC, 38 > 60 LOC. Worst: `domains.py::get_domain_design_status` (172), `internal/dtwin.py::dtwin_assistant_chat_stream` (167), `digitaltwin.py::dt_triples_find` (131) | `.coding_rules §1` ("routes are thin") |
| 6 | Multiple public classes per file | 29 files (list in Part C, phase 1) | `.cursor/07 §Class-First` |
| 7 | Filename ≠ class name | 33 single-class files (e.g. `registry/schedule/BuildScheduler.py` → `BuildScheduler`, `external/pitfalls/runner.py` → `OntologyPatternToolkit`, `store/factory.py` → `RegistryFactory`, every `agents/*/engine.py` → `AgentResult`) | `.coding_rules §3` |
| 8 | Loose module functions in `back/`, `agents/`, `shared/` | 55 non-route modules with ≥ 3 public functions (e.g. `shared/fastapi/health.py` 32 funcs / 1668 LOC, `graphdb/delta/_table_naming.py` 21, `delta/materialize.py` 18, `lakebase/_companion_ddl.py` 21) | `.coding_rules §2` |
| 9 | Ad-hoc background task | `api/routers/internal/dtwin.py:2243` `asyncio.create_task(...)` (verify whether streaming justifies it; otherwise `TaskManager`) | `.coding_rules §5` |
| 10 | Module singletons via `global` | 10 sites (`GraphDBFactory`, `AgentClient`, `query_limits`, `LakebaseAuth`, `scheduler`, `registry_cache`, `mcp http_client`…) | `.coding_rules §2` (searchability / testability) |
| 11 | Logger setup | 9 files use `logging.getLogger` instead of `get_logger(__name__)` | `.coding_rules §6` |
| 12 | Deprecated API | `TwinBackgroundTasks.py:459` `datetime.utcnow()` (warning in 17 tests) | — |

**Clean:** 0 f-string logger templates, 0 `.format()` logs, 0 `print()`, 0 bare `except:`.

### A2. Duplication clusters

1. **Agent loop ×9.** Every `agents/agent_*/engine.py` has a `run_agent()` of
   144–613 LOC re-implementing the same tool-calling loop (call endpoint →
   dispatch tools → accumulate usage → iterate → build result), and 8 of them
   redeclare an `AgentResult` dataclass sharing the same 5 fields
   (`success, steps, iterations, error, usage`). The PGE stages
   (`planner.py::run_planner` 360, `generators/entity.py` 433,
   `generators/relationship.py` 441, `evaluator/critic.py` 369) repeat it again.
2. **Three-hop facade.** `SettingsService` → `GraphEngineSettings` →
   `GraphEngineLakebaseSettings`: the same `graph_engine_lakebase_*_result`
   signature exists in three classes. 107/120 `SettingsService` methods,
   26/36 `GraphEngineSettings` methods and 76/84 `DigitalTwin` methods are
   one-line delegations (Middle Man). Two of today's 13 bug fixes were
   missing imports in this layer.
3. **SQL vs in-memory SHACL evaluation.** `SHACLService` has paired
   `_sql_cardinality`/`_mem_cardinality`, `_sql_bounded`/`_mem_bounded`,
   `_base_shape_sql`/`_base_shape_violations` — same shape semantics, two
   back-ends.
4. **Entity vs relationship symmetry.** `Mapping` (`_diagnose_entity` 140 /
   `_diagnose_relationship` 200, `_entity_drift_checks` / `_relationship_drift_checks`,
   `_merge_entity_mappings` / `_merge_relationship_mappings`) and
   `SparqlTranslator` (`_filtered_query_build_entity_unions` / `..._relationship_unions`).
5. **Two DDL modules with the same shape.** `lakebase/_companion_ddl.py` (21 funcs)
   and `lakebase/_adjacency_ddl.py` (18 funcs).

### A3. Dead code candidates

- `back/core/graph_analysis/CohortBuilder.py:141` `_predicate_alias_map` and
  `:177` `_resolve_predicate` — **redefined** at lines 238 / 265; the first
  pair is unreachable and has slightly different namespace-preference logic.
  Ruff F811 would catch it, but ruff is advisory-only on changed files.
- `DomainSession.before_meta` — no caller anywhere.
- `DomainSession.previous_values` — only called from tests.
- `back/fastapi/` package — will be empty once `graphql_routes.py` moves.

### A4. Session-data audit (partial)

Sampled accessors are all used (`groups` 45 refs, `last_build` 29,
`expressions` 23, `triplestore` 16, …). A full field-by-field audit is
part of phase 4 (`DomainSession` split), where the accessors are regrouped
anyway.

### A5. Tests

`uv run --frozen pytest -q -m "not scenario"` → **8913 passed, 326 skipped,
6 deselected, 1 xfailed**, 40 warnings in 57.5 s.

---

## Part B — Target design rules

These refine `.coding_rules §2` and become CI-enforced in phase 0.

1. **One public class per file**, file name = class name (PascalCase).
   A private helper class (`_Foo`) may share the file only if no other
   module uses it.
2. **One purpose per class**, stated in one docstring sentence. If the
   sentence needs "and", split the class.
3. **Size budget:** class file target ≤ 400 LOC, hard ceiling 800
   (existing `.coding_rules §12`); method ≤ 50 LOC (Long Method, §9).
4. **Split = sub-package.** When a class is decomposed, its collaborators go
   in a sub-package named after the concept (`mapping/diagnostics/`), and
   `__init__.py` re-exports the public names (`.coding_rules §7`).
5. **Inheritance only for real "is-a" with shared behaviour** — Template
   Method (agents, build phases, health checks), abstract interface + SQL
   default implementation (graph backends), polymorphic strategies
   (SHACL migrations, SPARQL query builders). Everything else uses
   composition. Mixins are acceptable to split a large interface; the
   Lakebase registry store (12 mixins) is the precedent.
6. **No pass-through facades.** A facade may live for one release as a
   deprecation shim (`warnings.warn`), then callers import the real class.
7. **Data records** (dataclasses / Pydantic): one file per aggregate root.
   Value objects used only inside that root may share its file
   (see decision D1).

---

## Part C — Phased plan

Each phase is a PR series of mechanical, behaviour-preserving moves with
tests green between commits (`.cursor/08 §Working Style for Refactoring`).
Order is driven by value, risk, and the number of test `patch()` targets
that each move breaks (shown as *patches*).

### Phase 0 — Guardrails (1 PR, low risk)

- `tests/units/architecture/test_class_per_file.py`: AST check, one public
  class per file + filename match, with a **ratchet allowlist** (today's
  29 + 33 offenders; the list may only shrink).
- `tests/units/architecture/test_size_budget.py`: classes > 800 LOC and
  methods > 100 LOC must be in an allowlist that may only shrink.
- `tests/units/architecture/test_package_reexports.py` (planned in
  `.coding_rules §7`, does not exist yet).
- Enable ruff `F811` as blocking for `src/` (catches the `CohortBuilder` bug).

### Phase 1 — Quick wins: mechanical file splits and renames (low risk)

**Extract to own file** (no logic change; importers updated, see D1/D2):

| File today | New files |
|---|---|
| `registry/RegistryService.py` | `RegistryCfg.py` (237 LOC) |
| `databricks/lakebase/LakebaseAuth.py` | `BranchLakebaseAuth.py` (191) |
| `ontology/GenerateDraft.py` | `GenerateEntity.py`, `GenerateDraftStore.py`, 3 errors into `ontology/errors/` |
| `registry/store/base.py` | `RegistryStore.py` + `store/records/{DomainSummary,BuildRunEntry,ReviewEvent,ChangeEvent,DomainComment,DomainTask,DocumentRow,…}.py` + `StoreError.py` |
| `task_manager/models.py` | `Task.py`, `TaskStep.py`, `TaskLogEntry.py`, `TaskStatus.py` |
| `reasoning/models.py` | `InferredTriple.py`, `RuleViolation.py`, `ReasoningResult.py`, `SWRLAtomPartition.py` |
| `w3c/owl/OntologyConflictDetector.py` | `ConflictItem.py`, `ConflictReport.py` |
| `databricks/DocumentParseService.py` | `ParseStatus.py`, `ParseSubmission.py` |
| `reasoning/SWRLBuiltinRegistry.py` | `SWRLBuiltin.py` |
| `helpers/UIBranding.py` | `BrandPalette.py` |
| `lakebase/provisioner.py` | `LakebaseGraphProvisioner.py` + `ProvisionError.py` |
| `agents/agent_supervisor/{complexity,mas,engine}.py` | `ComplexityReport.py`, `SupervisorAgentRef.py`, `SupervisorResult.py` |

**Rename to match the class** (importers updated, old module deleted): `scheduler.py → BuildScheduler.py`,
`pitfalls/runner.py → OntologyPatternToolkit.py`,
`store/factory.py → RegistryFactory.py`,
`store/lakebase/store.py → LakebaseRegistryStore.py`,
`version_status.py → VersionStatusCache.py`, `json_views.py → JsonViews.py`,
`shared/fastapi/{csrf,timing,ui_branding}.py → CSRFMiddleware.py, …`, etc.

**Layering fixes:** move `back/fastapi/graphql_routes.py` →
`api/routers/graphql.py` (+ its 3 models to `api/schemas/graphql/`);
move `FileSessionMiddleware` → `shared/fastapi/`; make `SessionManager`
accept the session mapping instead of a `Request`, and move
`get_session_manager` to `front/fastapi/dependencies.py`.

**Bug/dead code:** delete the first `CohortBuilder._predicate_alias_map` /
`_resolve_predicate` pair (after a test pins current behaviour), delete
`DomainSession.before_meta`.

### Phase 2 — Agents: Template Method base (medium risk, eval-gated)

Gated by `.cursor/12-ai-feature-lifecycle.mdc`: run each agent's eval
dataset before/after; the delta must be neutral. Agent code has few
patch targets but real LLM behaviour.

```
agents/core/
  AgentStep.py              # moved from engine_base.py
  AgentResult.py            # base dataclass: success, steps, iterations, error, usage
  ServingEndpointClient.py  # call_serving_endpoint + unsupported-params cache
  ToolDispatcher.py         # dispatch_tool
  UsageAccumulator.py       # accumulate_usage
  ToolCallingAgent.py       # ABC: run() loop + MLflow span (Template Method)
```

`ToolCallingAgent.run()` owns the loop; subclasses implement only
`system_prompt()`, `user_prompt()`, `tools()`, `on_tool_result()`,
`finalize() -> AgentResult`. Each agent becomes
`agents/agent_x/<X>Agent.py` (~100–200 LOC) + `<X>Result.py`
(subclass of `AgentResult` with its own fields). The existing
`run_agent()` stays as a 3-line shim so routers and the MCP server do not move.

PGE stages get a sibling base, `PGEStage(ToolCallingAgent)` →
`MappingPlanner`, `EntityMappingGenerator`, `RelationshipMappingGenerator`,
`MappingCritic`. The 10 PGE contract dataclasses in `contracts.py` move
one per file under `agent_mapping_pge/contracts/`.

Tool modules (`agent_dtwin_chat/tools.py`, `agent_ontology_assistant/tools.py`,
`agent_cohort/tools.py`, 13 functions each) → `AgentTool` base
(`name`, `json_schema`, `execute(ctx, args)`), one subclass per tool. Adding a
tool then means adding one file.

### Phase 3 — Remove the middle men (medium risk, high test churn)

*Patches: `SettingsService` 73, `DigitalTwin` 47.*

1. Per router (`internal/settings.py`, `internal/dtwin.py`, `digitaltwin.py`),
   switch imports from the facade to the concrete class
   (`RegistrySettings`, `WorkspaceUiSettings`, `GraphEngineNeo4jSettings`,
   `CohortService`, `SqlQualityChecks`, `TwinBackgroundTasks`, …).
2. Retarget the tests' `patch()` paths to the concrete classes.
3. Delete the delegating methods. `SettingsService` keeps only the admin
   gate and context resolution (~80 LOC, possibly renamed `SettingsAccess`);
   `DigitalTwin` keeps its 8 real methods (TS cache + status) or is inlined.
4. Retire `test_v090_settings_facade.py` (it tests the delegation itself).

### Phase 4 — God-class decompositions (one PR series each)

Ordered by patch count (safest first). LOC figures are estimates from the
method inventory.

#### 4a. `SparqlTranslator` (2417 LOC, 63 methods, 0 patches) — ROADMAP M4.P2

The method-name prefixes already name the classes:

| New class | Responsibility | ~LOC |
|---|---|---|
| `SparqlQueryParser` → returns `ParsedSparqlQuery` (Introduce Parameter Object) | prefixes, OPTIONAL blocks, triple patterns, BIND, filters | 420 |
| `SparkSQLDialect` | casts, `stack()` unpivot, subject templates, `WITH` clause | 110 |
| `MappingLookup` | predicate→column, URI template→class, table sources | 90 |
| `SparqlQueryBuilder` (ABC) | shared CTE/union assembly | 80 |
| ↳ `GenericTriplesQueryBuilder` | `_generic_triples_*` family | 420 |
| ↳ `FilteredRelationshipsQueryBuilder` | `_filtered_query_*` family | 260 |
| ↳ `PatternJoinQueryBuilder` (+ `OptionalPatternJoiner`) | `_spark_*` family | 550 + 230 |
| `SparqlTranslator` | `translate_sparql_to_spark`: parse → choose builder → SQL | 150 |

Replace Conditional with Polymorphism: today's dispatch in
`_build_spark_sql_maybe_triple_pattern_dispatch` becomes builder selection.

#### 4b. `SHACLService` (1665 LOC, 40 methods, 0 patches)

| New class | ~LOC |
|---|---|
| `ShapeCatalog` — CRUD, Turtle import/export | 120 |
| `SHACLRuleInference` — `sh:rule` inference | 150 |
| `SHACLValidator` — pySHACL validation + violation extraction | 95 |
| `ShapeSuggester` — `suggest_from_ontology` (321-LOC method split per suggestion kind) | 340 |
| `ShapePropertyResolver` — `resolve_prop_uri`, normalisation | 65 |
| `ShapeEvaluator` (ABC) → `SQLShapeEvaluator`, `InMemoryShapeEvaluator` (removes duplication cluster 3) | 80 + 230 + 200 |
| `LegacyConstraintMigration` (ABC) → 8 subclasses (`CardinalityMigration`, `FunctionalMigration`, `InverseFunctionalMigration`, `ValueCheckMigration`, `AllValuesFromMigration`, `SomeValuesFromMigration`, `HasValueMigration`, `GlobalRuleMigration`) + `LegacyConstraintMigrator` registry | 290 total |
| `SHACLService` — thin entry point kept for callers | 60 |

#### 4c. `DomainSession` (1587 LOC, 103 methods, 0 patches)

| New class | ~LOC |
|---|---|
| `SessionMigration` (ABC) + one subclass per legacy migration (`MigrateUcLocation`, `MigrateTriplestoreNode`, `MigrateMappingToAssignment`, `MigrateDesignLayout`, …) run by `DomainSessionMigrator` | 370 total |
| `SessionChangeLog` — `record_change`, `diff_meta`, `drain_change_log` | 90 |
| `SessionSnapshot` — hashing + config snapshots | 50 |
| `DomainSessionSerializer` — `export_for_save` (121), `import_from_file` (210) | 340 |
| `DomainSession` — property accessors, `save`, `reset` | ~500 |

New session migrations become one new file instead of another `_migrate_*`
method. Optional follow-up: group the 5 rule accessors (`swrl_rules`,
`decision_tables`, `sparql_rules`, `aggregate_rules`, `cohort_rules`) into a
`SessionRules` view (data clump), with the full session-data audit.

#### 4d. `shared/fastapi/health.py` (1668 LOC, 32 functions, 0 patches)

`HealthCheck` (ABC: `name`, `category`, `run()`; the base owns today's
`_safely_run` wrapper, Template Method) → one subclass per check under
`shared/fastapi/health/checks/` (26 files, 20–110 LOC each, e.g.
`DatabricksAuthCheck`, `WarehouseCheck`, `LakebasePermissionsCheck`,
`GraphDBTablesCheck`). `HealthCheckSuite` declares the diagnostics and
readiness lists, which shrinks `run_diagnostics_checks` from 230 LOC to a list.

#### 4e. `_BuildPipeline` (1471 LOC, 30 methods, 1 patch)

| New class | ~LOC |
|---|---|
| `BuildPhase` (ABC: logging, cancellation, progress) | 60 |
| ↳ `TranslationPhase`, `ViewCreationPhase`, `DataTableMaterializationPhase`, `CachePopulationPhase`, `BuildRunRecorder` | 110–200 each |
| `ApplyStrategy` (ABC) → `FullRebuildApply`, `SyncedPipelineApply` (the 368-LOC method split into steps) | 50 + 200 + 380 |
| `LakebaseBuildMode` — mode resolution + sync flags | 80 |
| `BuildPipeline` — orchestrator `run()` (file renamed from `_build_pipeline.py`) | 150 |

#### 4f. `GraphDBBackend` (1434 LOC, 71 methods, 1 patch)

`Neo4jStore` overrides 41 of the 71 base methods (Refused Bequest): it
inherits SQL defaults it cannot use. Introduce an intermediate class:

```
GraphDBBackend (ABC)            # abstract core, capabilities, connection, remote sync  ~200
├── Neo4jStore                  # Cypher, no SQL defaults inherited
└── SQLGraphDBBackend           # SQL default implementations, composed of:
    │   SQLGraphStatsMixin        aggregate stats, type distribution        ~120
    │   SQLGraphAnalyticsMixin    edge/degree CTEs, structure, top nodes    ~350
    │   SQLGraphTraversalMixin    BFS, seeds, subject lookup, pagination    ~450
    │   SQLReasoningMixin         transitive/symmetric closure, paths       ~180
    │   CompanionTablesMixin      adjacency + search-cache tables           ~100
    ├── DeltaFlatStore
    └── LakebaseBase → LakebaseFlatStore
```

`LakebaseFlatStore` (1153) then splits by composition into
`LakebaseSyncedLayout` (synced tables, union view, companion; ~250),
`LakebaseBulkWriter` (COPY insert/delete, executemany; ~300) and
`LakebaseSubgraphQueries` (~170). Covered by the existing graphdb contract tests.

#### 4g. `CohortBuilder` (1253 LOC, 37 methods)

`CohortUriNormalizer` (namespace bridging, ~200, also removes the duplicate
defs), `CohortPathTracer` (~290), `CohortMembershipExplainer` (~115),
`CohortMaterializer` (graph + UC, ~150). `CohortBuilder` keeps the
6-stage pipeline (~350).

#### 4h. `Mapping` (2755 LOC, 58 methods, 13 patches)

| New class | ~LOC |
|---|---|
| `MappingEditor` — entity/relationship CRUD, merge, exclude, save | 380 |
| `MappingChangeAudit` — diff + change events + auto-map run record | 130 |
| `MappingR2RMLCodec` — generate/parse R2RML, URI canonicalisation | 110 |
| `AutoAssignTask` (ABC) → `BatchAutoAssignTask` (today's 410-LOC method), `SingleAutoAssignTask` | 80 + 350 + 150 |
| `MappingGapAnalyzer` — gaps, issues, stats, SQL validation | 165 |
| `MappingSourceIndex` — FQN extraction, source tables, `find_mappings_referencing` | 140 |
| `SchemaDriftDetector` — live columns vs mappings | 220 |
| `MappingDiagnostics` — `run_diagnostics`, probes, permission checks | 300 |
| `MappingItemDiagnosis` (ABC) → `EntityMappingDiagnosis`, `RelationshipMappingDiagnosis` (cluster 4) | 60 + 150 + 190 |
| `Mapping` — aggregate entry point holding domain state | 100 |

#### 4i. `Domain` (2306 LOC, 54 methods, 7 patches)

`DomainProfile` (info, stats, save, coercions, base URI; ~520, its
`_coerce_*` helpers → `DomainSettingsCoercer`), `DomainVersioning`
(save/load UC, new version, version status/details; ~550),
`DomainRegistryBrowser` (domains/versions lists, build runs, audit trail,
URI redirect; ~200), `DesignViews` (~240), `MetadataTablesEditor` (~300),
`MetadataSyncJob` (initialize/update async, `update_mappings_from_metadata`;
~460). `Domain` becomes the aggregate root (~100).

#### 4j. Registry (`RegistryService` 1021 + `RegistryStore` ABC 635)

Mirror the Lakebase mixins that already exist: split the `RegistryStore` ABC
into per-concern interfaces in `store/interfaces/` (`CatalogStore`,
`BuildRunStore`, `AnalyticsStore`, `ReviewEventStore`, `ChangeEventStore`,
`CollabStore`, `ScheduleStore`, `EditLockStore`, `DocumentStore`,
`GlobalConfigStore`); each Lakebase mixin implements its interface.
`RegistryService` splits into `RegistryDomains`, `RegistryBuildHistory`,
`RegistryCollaboration`, `RegistryBridges`, exposed as properties
(`registry.history.record_build_run(...)`), which hides the delegate
without re-creating a middle man.

#### 4k. Settings back-ends

`GraphEngineLakebaseSettings` (1117 LOC, 14 methods averaging 80 LOC):
`LakebaseSettingsBase` (branch connection/auth resolution) →
`LakebaseHealthProbe`, `LakebaseCatalogBrowser` (projects, branches,
databases, schemas, roles), `LakebaseGraphProvisioning`,
`LakebaseGraphObjects` (list, sync, drop). The same treatment applies to
`GraphEngineSettings` (966) and `PermissionService` (969) once phase 3
has removed their facade callers.

### Phase 5 — Routes and API schemas

- Split the five 1300–2700 LOC routers into per-resource packages, e.g.
  `api/routers/internal/dtwin/{build,sync,quality,cohort,assistant}.py`,
  `api/routers/internal/ontology/{classes,shapes,rules,assistant,generate}.py`.
- Move the 36 Pydantic models of `api/routers/digitaltwin.py` (and those of
  `domains.py`, `v1.py`) to `api/schemas/<area>/` following rule 7.
- Push logic out of the 38 handlers > 60 LOC into domain classes, e.g.
  `get_domain_design_status` → `DomainDesignStatus`.

### Phase 6 — Module-function files → classes (opportunistic)

Convert when touched: `graphdb/engine_config.py` → `GraphEngineConfig`,
`delta/materialize.py` → `DeltaMaterializer`, `delta/_table_naming.py` →
`DeltaTableNames`, `lakebase/_companion_ddl.py` + `_adjacency_ddl.py` →
`LakebaseDDL` base with `CompanionTableDDL` / `AdjacencyTableDDL` (cluster 5),
`registry_cache.py` → `RegistryCache`, `version_lifecycle.py` →
`VersionLifecycle`, `agents/tools/graph_formatting.py` → `GraphFormatter`,
`mcp-server/server/tools.py::register_tools` (900 LOC) → one class per tool group.
Replace the 10 `global` singletons with class-level instances behind
`get_instance()` / `reset_for_tests()`.

---

## Part D — Risks and mitigations

| Risk | Mitigation |
|---|---|
| Tests patch old module paths (261 patch targets; 73 on `SettingsService`, 47 on `DigitalTwin`) | Grep the patch targets before each move; retarget in the same commit; start with 0-patch classes |
| Circular imports when splitting | Collaborators depend on data/records only; the entry-point class imports collaborators, not the reverse; lazy imports stay where they already are |
| Public API breakage | `__init__.py` re-exports + one-release shims (`.coding_rules §7`) |
| Agent behaviour drift | Phase 2 eval-gated per `.cursor/12` (before/after MLflow eval runs) |
| MRO surprises with backend mixins | Mixins define no `__init__` and hold no state; graphdb contract tests cover every backend |
| Review fatigue | One class per PR, mechanical moves only, behaviour changes in separate commits |

## Verification (per phase)

- `uv run --frozen pytest -q -m "not scenario"` green at every commit.
- Phase 0 ratchet allowlists shrink; no file enters them.
- `wc -l` on touched files: every class ≤ 800 LOC, target ≤ 400.
- Sphinx autodoc updated for moved public classes (`.cursor/08`).

## Progress

- [x] **Phase 0** (2026-10-11) — `tests/units/architecture/` guardrails +
  shrink-only `baseline.json` (+ `ratchet.py` tightener). Ruff F811 is
  covered by `test_no_duplicate_definitions.py` instead of a CI config change.
- [ ] **Phase 1** — partially done (2026-10-11): `CohortBuilder` duplicate
  defs and `DomainSession.before_meta` removed; split `task_manager/models.py`,
  `reasoning/models.py`, `SWRLBuiltinRegistry`, `OntologyConflictDetector`,
  `DocumentParseService`, `neo4j/graph_model`, `GenerateDraft`,
  `LakebaseConnectionPool`, `LakebaseAuth`, `RegistryService`,
  `scheduler_tasks/context`, `lakebase/provisioner`; renamed `provisioner.py`
  → `LakebaseGraphProvisioner.py`, `context.py` → `TaskContext.py`; all
  `back/objects` re-export gaps closed. Remaining: `registry/store/base.py`
  records split, `graph_analysis/models.py` regrouping, the other 30
  filename renames, and the layering fixes.
- Lesson: after extracting `X` into `pkg/X.py`, a patch target
  `"pkg.X.method"` now resolves to the *module*, not the class. Retarget to
  `"pkg.X.X.method"` (19 such targets for `RegistryCfg`; unpatched they hit
  live Lakebase and hung the run).
- [x] **Topic sub-packages** (2026-10-11) — `reasoning/{swrl,rules,results}`,
  `digitaltwin/{build,quality,cohort,explorer,node,query,cache}`,
  `domain/settings/graphengine`, `ontology/generate`, `databricks/document`.
  Rule recorded in `.cursor/07 §Package Organisation 5`. Lesson: a module
  named like its class (`GenerateWorkflow`) must be re-exported as the
  *module* when callers monkeypatch module attributes.

## Decisions (2026-10-11)

- **D1 — Data records: one file per aggregate root.** A record file holds
  the root plus value objects that only exist inside it (e.g.
  `ConflictReport` + `ConflictItem`). Independent records get their own file.
- **D2 — Facades are removed outright.** No deprecation shims. The same
  applies to moved/renamed modules: all importers (src and tests) are
  updated in the same commit, and the old module is deleted. Exception:
  string-referenced entry points (`app.yaml`, DAB job files) are not moved.
- **D3 — Start with phase 0 + 1.** Scope of phase 1 is `back/` and
  `shared/`; `agents/` multi-class files move with phase 2, router schemas
  with phase 5, `mcp-server/` stays under its own plan.
