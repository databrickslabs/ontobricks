# OntoBricks — Release Notes V0.9.3

**Release window:** November 2026<br>
**Type:** Patch release (v0.9.2 → v0.9.3)<br>
**Test status:** `uv run --frozen pytest -q -m "not scenario"`: 9712 passed, 337 skipped, 8 deselected.
Live validation on the deployed `ontobricks-09x` instance: app smoke 11 passed, MCP smoke 6 passed,
scenario campaign 7 passed (validation report 13 passed, 0 failed).

This release is a patch on v0.9.2. It tightens registry promotion semantics,
adds an opt-in hierarchy instance IRI scope for multi-typed entities, raises
Graph Chat output budgets for reasoning models, and corrects documentation that
described a never-shipped registry transfer CLI. MCP identity, SPARQL value
filters, multi-dataset classes, and the 0.9.2 security floors remain as
documented in `releases/ReleaseNotes_V0.9.2.md`.

---

## Highlights

- **Opt-in hierarchy instance IRI scope** — a parent class and a subclass mapped
  with the same business id can mint one Knowledge Graph node (root-class URI)
  that carries every `rdf:type` and inherited relationship, instead of two
  unlinked nodes. ([#228](https://github.com/databrickslabs/ontobricks/issues/228))
- **Registry import consistency** — renamed domains rewrite ontology URIs;
  existing local versions are never overwritten; missing versions can be added;
  the highest newly written version starts as DRAFT.
  ([#229](https://github.com/databrickslabs/ontobricks/issues/229))
- **Graph Chat `max_tokens` is configurable** — default raised from 2048 to
  8192 (`ONTOBRICKS_DTWIN_CHAT_MAX_TOKENS`) so reasoning models stop exhausting
  the budget before emitting an answer.
- **Docs: registry transfer CLI removed** — `docs/advanced_features.md` no
  longer describes `registry_transfer.sh` / `src/cli/` (never shipped). Shipped
  paths only: OBX UI and GitHub mirror.
  ([#227](https://github.com/databrickslabs/ontobricks/issues/227))
- **Dependency security floors** — Dependabot alerts #318, #325–#329 closed:
  `oauthlib` 4.0.1, `opentelemetry-api` 1.45.2, `dnspython` 2.9.2, with
  `databricks-sql-connector` ≥ 4.7.0 (the connector release that unblocks oauthlib 4.x).

---

## Instance IRI scope (hierarchy)

By default, OntoBricks still mints instance URIs as `{base}{MappedClass}/{id}`.
When a subclass and its parent are both mapped to the same business key, that
produces two nodes; inherited relations stay on the declaring class URI.

A new domain setting (**Domain → General → Instance IRI scope**) offers
**Hierarchy** mode: R2RML templates and TwinMapping fallbacks use the
**root-class** segment so both mappings land on one node. Mapping diagnostics
warn when unrelated mapped classes share a root. Graph Explorer prefers the
most specific `rdf:type` on multi-typed nodes.

- Opt-in only; existing domains keep class-scoped IRIs until an operator
  switches and rebuilds.
- Documented in the User Guide, Architecture, and Advanced features pages.

---

## Registry export / import

### Import behaviour (#229)

Registry → Browse → Import (OBX or Load from GitHub) now matches the product
intent for cross-environment promotion:

- **Rename** rewrites every ontology URI from the source base URI to the new
  auto base URI (classes, properties, SHACL, mappings).
- **Add missing versions** writes only version numbers that do not exist
  locally; existing versions are never replaced.
- The **highest newly written** version starts as **DRAFT** (Published copies
  are not re-published blindly).
- The UI surfaces skipped versions and documents Skip / Add missing / Rename.
- Domain → General save fails loudly when the configured default base URI
  cannot resolve (no silent flip to a built-in host).

### Documentation correction (#227)

The Advanced features page previously documented a CLI zip workflow
(`scripts/registry_transfer.sh`, `src/cli/registry_transfer.py`) that was
**never implemented** (docs-only since April 2026). That section is removed.

Documented paths for moving domains between environments:

| Method | When to use |
|--------|-------------|
| **OBX UI** | Ad-hoc / cross-tenant copy from Registry → Browse → Export / Import |
| **GitHub mirror** | Continuous backup; restore via Import → Load from GitHub repository |

Large registries: batch OBX export by domain (50 MB upload cap) or restore via
GitHub. After import, **rebuild** the knowledge graph (triples and Knowledge
Store documents are not carried in OBX).

---

## Graph Chat

Reasoning models could burn a hardcoded `max_tokens=2048` budget on internal
reasoning and return the fallback “I couldn't display that answer.”

- New setting: `ONTOBRICKS_DTWIN_CHAT_MAX_TOKENS` (default **8192**, bounded
  `1…8192`).
- Blocking and streaming chat routes pass the setting into the agent engine.
- Exposed in `app.yaml.template` and documented under Getting Started → Graph
  Chat (Digital Twin Assistant).

---

## Dependencies and maintenance

- **Security floors:** `pyproject.toml` and `src/mcp-server/pyproject.toml` raise
  `databricks-sql-connector` to ≥ 4.7.0, `oauthlib` to ≥ 4.0.1,
  `opentelemetry-api` to ≥ 1.45.2 and `dnspython` to ≥ 2.9.2. Both `uv.lock`
  files are locked to these versions.
- **Public lockfile URLs:** `uv.lock` and `src/mcp-server/uv.lock` point at
  `files.pythonhosted.org` only, so the container's `uv run --frozen` installs
  from the public CDN.
- **Test hygiene:** `pytest.ini` and `pyproject.toml` ignore two upstream
  `Dataset.default_context` deprecation warnings (pyshacl 0.31 / rdflib 7.6).
- **Timestamps:** digital-twin reasoning runs use timezone-aware UTC instead of
  the deprecated `datetime.utcnow()`. The stored `last_run` format is unchanged.

---

## Documentation

- **`docs/advanced_features.md`**: hierarchy IRI scope; registry import
  Skip / Add missing / Rename; Listing domains; Large registries; no
  `registry_transfer` CLI.
- **`docs/user-guide.md`**, **`docs/architecture.md`**: instance IRI scope;
  import semantics.
- **`docs/getting-started.md`**: Graph Chat `max_tokens` env var and bounds.
- **`README.md`**, **`docs/README.md`**: pointers to OBX + GitHub transfer
  paths and this release.

---

## Upgrade notes

- **Hierarchy IRI scope:** leave the setting on the default (class-scoped)
  unless you intentionally need parent/subclass mappings to share one node.
  After switching to Hierarchy, regenerate R2RML / rebuild the graph so URIs
  remint.
- **Registry import:** workflows that relied on Overwrite replacing an
  existing version must switch to Add missing versions (or rename). Existing
  local versions are never replaced.
- **Graph Chat:** if a deployed app still shows truncated / empty answers on
  reasoning models, set `ONTOBRICKS_DTWIN_CHAT_MAX_TOKENS` (accepted range
  1…8192, default 8192).
- **Dependencies:** `databricks-sql-connector` now requires ≥ 4.7.0. Redeploy
  with `make deploy` to pick up the locked versions. No configuration change.
- **No registry schema migration.** No breaking MCP tool names.
- **Cross-env promotion:** use OBX or GitHub; there is still no shipped CLI
  for scripted registry transfer (product decision tracked separately from
  the #227 doc fix).
