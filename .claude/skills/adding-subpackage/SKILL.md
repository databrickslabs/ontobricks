---
name: adding-subpackage
description: Use when adding a new subpackage under back/core/, back/objects/, or agents/ — e.g. a new graph DB engine, W3C parser, industry importer, reasoning module, or domain class. Enforces the checklist defined in .cursor/07-project-conventions.mdc.
---

# Adding a new subpackage

The canonical 8-step checklist lives in **`.cursor/07-project-conventions.mdc
§Adding a New Subpackage Checklist`**. The reference patterns to imitate live
in the same file under **§Subpackage Patterns (reference)**. Read both
first; this skill only sequences the work and acts as a parent-selector.

## Choose the parent

| Need | Parent |
|------|--------|
| Talks to Databricks/Delta/UC, parses W3C, runs reasoning, etc. | `back/core/` |
| Owns business logic for a domain concept (Ontology, Mapping, …) | `back/objects/` |
| Wraps an LLM-driven engine | `agents/` |

If unsure, default to `back/core/` and lift to `back/objects/` only when
state or session-awareness justifies it.

## Procedure

1. Pick the parent (table above).
2. Walk the 8 steps from `.cursor/07 §Adding a New Subpackage Checklist`. Tick each off in a TodoWrite.
3. Pick the closest reference pattern from `.cursor/07 §Subpackage Patterns` and imitate its layout (e.g. graph DB engine → `back/core/graphdb/`).
4. Update Sphinx (`docs/sphinx/api/<pkg>.rst` + parent toctree, then `scripts/build_docs.sh`).
5. Run tests.
6. Run the `changelog` skill.

## Don't

- Don't split a class across files.
- Don't put HTTP/FastAPI types in `back/core/`.
- Don't put `Request`/`Response` in `back/objects/`.
- Don't import from internal modules outside the package — always import from the package (`.cursor/07 §__init__.py Conventions`).
- Don't skip the Sphinx page — `make docs` warnings will fail review.
