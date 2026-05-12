# OntoBricks — Release Notes V3.3.1

**Release window:** May 2026
**Type:** Hotfix
**Test status:** 141 cohort tests passed, 0 failed (49 `test_cohort_builder.py`, 31 `test_dtwin_cohort.py`, 34 `test_cohort_models.py`, 24 `test_agent_cohort_tools.py`, 3 `test_agent_cohort_engine.py`).

---

## Highlights

- **Cohort Discovery: predicate namespace fix** — `hasClaim` (and any predicate loaded outside R2RML) now resolves correctly. The engine no longer silently misses triples whose predicate is in ontology-namespace form (`#`) when the lookup key is in data-namespace form (`/`).
- **Cohort Discovery: cross-namespace predicate fallback** — `CohortBuilder` gains a local-name alias map mirroring the `SparqlTranslator` approach, so predicates from a completely foreign namespace (e.g. `ontobricks.com/ontology#hasclaim` vs. `databricks-ontology.com/Cust360Auto/hasclaim`) resolve via local-name matching.
- **Cohort designer UX** — attribute dropdowns in the Path "where" filter and the Compatibility section are now scoped to the entity being filtered, not the full ontology property list.

---

## Cohort Discovery — Bug Fixes

### Fix 1: ontology-form predicate silent miss (`CohortBuilder._outgoing_edge_index`)

When data triples were inserted outside the R2RML pipeline (direct insert, W3C OWL round-trip, manual load), their predicates were stored in ontology-namespace form (`…#hasClaim`) while the lookup key produced by `_normalized_links` was in data-namespace form (`…/hasClaim`). This caused a silent `neighbours_raw = 0` and an empty cohort.

**Changes:**

- `src/back/core/graph_analysis/CohortBuilder.py` — `_outgoing_edge_index` promoted from `@staticmethod` to instance method so it can call `self._to_data_uri(pred)`. Every triple predicate is now normalised to data-namespace form when the index is built.
- `src/front/static/query/js/query-cohorts.js` — `_renderTraceLink` now guards on `in_frontier === 0` before `neighbours_raw === 0`. When the starting frontier is empty the diagnostic message now reads *"the starting frontier for this hop is empty — all members were eliminated before reaching it. Check the compatibility (Stage 3a) filters or the previous hop's target_class."* instead of misleadingly blaming the predicate URI.
- `tests/test_cohort_builder.py` — 2 new tests: `test_data_with_ontology_form_predicate_is_indexed_correctly`, `test_trace_shows_nonzero_raw_for_ontology_form_predicate`.

### Fix 2: cross-namespace predicate — local-name alias fallback

`_to_data_uri` can only bridge `#` ↔ `/` within the **same** base namespace. When the domain's object property URIs live in a completely different namespace (e.g. inherited shared namespace `ontobricks.com/ontology#`) the first fix was not sufficient.

**Changes:**

- `src/back/core/graph_analysis/CohortBuilder.py`:
  - `_predicate_alias_map()` — scans loaded triples, builds `{local_name → canonical_data_namespace_uri}`, cached in `self._cache["predicate_alias"]` and invalidated on triple reload.
  - `_resolve_predicate(uri)` — tries `_to_data_uri` first; if the URI is unchanged (foreign namespace) falls back to the alias map by local name.
  - `_normalized_links` and `_normalized_compat` updated to use `_resolve_predicate` instead of `_to_data_uri`.
- `tests/test_cohort_builder.py` — 1 new test: `test_via_from_foreign_namespace_resolved_by_local_name` (exact replica of the `ElectricitySuspended` / `Cust360Auto` production scenario).

---

## Cohort Designer — UX

### Attribute dropdowns scoped to entity

Property dropdowns in the Path "where" filter and the Compatibility section previously listed every property in the ontology regardless of the entity in scope. Users had to scroll through unrelated properties when filtering a specific hop.

**Changes:**

- `src/front/static/query/js/query-cohorts.js`:
  - New `_dataPropsForClass(classUri)` helper — filters to data properties whose `domain` matches the class, with a full-list fallback when ontology metadata is incomplete.
  - `_renderHopWhereRow` now calls `_dataPropsForClass(targetClassUri)`.
  - `_renderCompat` now calls `_dataPropsForClass(this.rule.class_uri)`.

---

## Modified files

| File | Change |
|------|--------|
| `src/back/core/graph_analysis/CohortBuilder.py` | Predicate normalisation fixes + alias map |
| `src/front/static/query/js/query-cohorts.js` | Diagnostic guard + scoped attribute dropdowns |
| `tests/test_cohort_builder.py` | 3 new regression tests |

---

## Upgrade notes

No schema, API, or configuration changes. Drop-in replacement for v3.3.0.
If a cohort was returning empty results due to the `hasClaim` predicate mismatch, re-run **Materialise** — no manual data migration required.
