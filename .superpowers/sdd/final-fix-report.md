# Final-fix report — Domain version review blockers (2026-09-25)

## Verified findings and resolutions

1. **Persisted deletion identity — confirmed.** The Domain endpoint used
   `uc_domain_folder`, whose name fallback could select an existing registry
   domain from an unsaved same-name session. It now accepts only the persisted
   `domain_folder`; missing identity is rejected before the deletion service.
   The API regression proves the fallback cannot trigger a delete.
2. **Deletion concurrency — confirmed.** Policy validation read status before
   a later unconditional delete. Lakebase now executes one PostgreSQL
   `DELETE ... USING` statement with `v.status = 'DRAFT'`. `rowcount == 0`
   raises the established `ConflictError` (HTTP 409). The Registry service
   therefore never starts Knowledge Store or legacy Volume cleanup after a
   state-change conflict. This uses the existing psycopg connection context;
   no unsupported transaction API was introduced.
3. **Global lifecycle state — confirmed.** Inline card transitions refreshed
   only the card list. Successful transitions now refresh the authoritative
   navbar state, and transitions of the loaded version invalidate and re-fetch
   version status to synchronize badges, globals, role tooltip, and the
   `read-only-version` body class without a reload.
4. **Confirmation copy — confirmed.** Domain-card version deletion names the
   escaped domain and version and explicitly warns about permanent Knowledge
   Store removal. Registry version and whole-domain confirmations provide the
   same warning and escape raw-HTML-bound values.
5. **Cancellation focus — confirmed.** Transition, Load, and Delete share one
   connected/enabled trigger-focus restore helper. Real Bootstrap-modal
   browser tests cover Transition and Load cancellation.

## Related minors

- Cards now use `aria-labelledby` pointing to their visible version heading.
- Removed the unused `?refresh=true` parameter from card-list requests while
  retaining awaited post-action refreshes.
- README and user documentation now describe guarded deletion and immediate
  lifecycle synchronization.
- Message-substring capability normalization remains deferred. Converting it
  would touch shared lifecycle presentation outside these blockers; the new
  zero-row deletion conflict uses the existing structured `ConflictError`
  path instead.

## RED/GREEN evidence

- RED: focused regressions initially produced 11 expected failures (persisted
  identity, guarded SQL/zero-row conflict, accessible names, confirmation
  copy, focus restoration, and global state refresh).
- GREEN: the same focused set completed with 34 passed.
- Mocked Chromium: 7 passed, including real-modal Transition/Load focus and
  loaded Draft → In Review global read-only synchronization.

## Commands and results

- `uv run --frozen pytest -q tests/units/domain/test_version_capabilities.py tests/units/registry/test_version_lifecycle.py tests/units/registry/test_registry.py tests/units/registry/test_lakebase_guarded_version_delete.py tests/units/settings/test_settings_version_deletion.py tests/units/api/test_delete_version_endpoints.py tests/units/front/test_domain_versions_cards.py`
  → 130 passed, 1 warning.
- `ONTOBRICKS_E2E_FAKE_CREDS=1 uv run --frozen pytest -q tests/e2e/domain/test_domain_versions_cards.py`
  → 7 passed, 1 warning.
- `node --check` on all four changed JavaScript files → passed.
- `uv run --frozen ruff check --select F,E9 ...` → all checks passed.
  The repository-wide/default Ruff invocation reports 155 pre-existing
  modernization/import findings in the already non-compliant large modules;
  no IDE diagnostics were reported for changed files.
- `git diff --check` → passed.
- `uv run --frozen pytest -q -m "not scenario"` → 6950 passed,
  315 skipped, 6 deselected, 1 xfailed, 32 warnings in 54.18s.

## Commit and self-review

Commit: this report is part of the final-fix commit; its immutable SHA is
reported in the final handoff because a commit cannot embed its own SHA.

Self-review found no unguarded deletion path in the changed flow. The SQL
status predicate is authoritative because lifecycle transitions update the
same denormalized `domain_versions.status` column. Knowledge Store cleanup
remains after, and conditional on, successful row deletion. No user changes
were overwritten, and `.superpowers/sdd/progress.md` was not edited.

## Follow-up — remaining Important findings

### Persistent context-menu blocker

Confirmed: the capture listener was installed once and permanently prevented
context menus even after `read-only-version` was removed. The listener remains
deduplicated, but now checks the current `read-only-version` or `role-viewer`
body class for every event. Inline Draft → In Review blocks context menus;
inline In Review → Draft restores context menus and `canEditOntology()` without
a reload.

### Knowledge Store cleanup failures

Confirmed: returned `delete_documents()` errors and raised exceptions were
logged and swallowed. Cleanup now raises `InfrastructureError` with the
truthful message that registry metadata was deleted but Knowledge Store cleanup
failed. Both Domain and Settings routes propagate the shared 5xx error. The
atomic Draft-guarded registry-row deletion still runs first; cleanup was not
moved before it. Since the registry and cleanup APIs do not support one shared
transaction, this can be a partial deletion. Version-status caches are cleared
on this partial-failure path so the removed registry row is not presented as
live.

### Follow-up RED/GREEN and verification

- RED backend: 3 expected failures proved returned cleanup errors, raised
  cleanup exceptions, and missing partial-failure cache invalidation.
- RED browser: In Review → Draft left a synthetic OntoViz context menu blocked.
- GREEN targeted backend/API/frontend: 135 passed, 1 warning.
- GREEN mocked Chromium: 8 passed, 1 warning, including both lifecycle
  directions through the real inline confirmation flow.
- Node syntax checks: passed.
- Ruff correctness checks and IDE diagnostics: passed for changed code. A
  broader touched-module `F,E9` run surfaced three pre-existing findings in
  `SettingsService.py` and `RegistryService.py`; none is in a changed hunk.
- `git diff --check`: passed.
- Full `uv run --frozen pytest -q -m "not scenario"`: 6955 passed,
  316 skipped, 6 deselected, 1 xfailed, 32 warnings in 53.64s.

Follow-up commit: reported in the final handoff; a commit cannot contain its
own immutable SHA. Remaining concern: cross-store rollback remains unsupported,
so cleanup failure is explicitly reported as partial deletion rather than
atomic success.

## Concrete-store cleanup follow-up

### Verified gap and implementation contract

Confirmed: `LakebaseRegistryStore.list_documents()` returned `[]` when the
document table could not be ensured or its query failed. The destructive
workflow therefore could not distinguish infrastructure failure from a
genuinely empty corpus.

`RegistryStore.list_documents()` now accepts the opt-in keyword
`strict=False`. Existing API listing, parsing, generation, mapping, and agent
tool callers retain tolerant behavior. `RegistryService.delete_version()` is
the only strict caller. In strict mode, an unavailable document table or query
failure raises `StoreError`; a successful query with no rows still returns
`[]`. The service converts the raised failure into the existing truthful 5xx
partial-deletion `InfrastructureError`.

The ordering is unchanged: the atomic Draft-guarded registry-row deletion
still completes before strict Knowledge Store inspection. No unsupported
cross-store transaction or rollback was introduced.

### TDD and verification evidence

- RED: the new focused run produced 5 expected failures and 1 pass. Lakebase
  rejected the `strict` keyword, and the service still called tolerant
  listing.
- GREEN strict contract: 6 passed, 1 warning.
- Focused store/service/Domain API/Settings API run: 77 passed, 1 warning.
- New-test default Ruff run: passed.
- Touched-file Ruff `F,E9`: one pre-existing unused import remains in
  `RegistryService.py`; no changed hunk introduced it. IDE diagnostics found
  no errors.
- `git diff --check`: passed.
- Full `uv run --frozen pytest -q -m "not scenario"`: 6959 passed,
  316 skipped, 6 deselected, 1 xfailed, 32 warnings in 55.29s.

Concrete-store follow-up commit: reported in the final handoff because the
commit cannot contain its own immutable SHA.

### Self-review and remaining concern

The concrete tests execute the Lakebase implementation against controlled
table-availability and cursor outcomes, including a genuine zero-row query.
All non-destructive callers keep the legacy tolerant contract. Existing docs
already describe the correct partial-deletion boundary and were not
strengthened.

The unavoidable concern remains: metadata is already deleted when strict
listing or Knowledge Store cleanup fails. The shared 5xx response reports that
partial deletion truthfully, but cross-store rollback is unavailable.
