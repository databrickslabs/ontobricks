# v0.6 Neo4j integration — end-to-end demo artefacts

This folder collects the proof artefacts for **PR #47 — Neo4j as a selectable
graph DB engine**. The demo was run on the `fevm-mjolnir` Databricks workspace
on 2026-06-12 using a real PFAS research-paper ontology.

## Files

- **`OntoBricks-PR47-Neo4j.pdf`** (4.9 MB, 21 slides) — full deck walking through
  the end-to-end flow: Settings → Documents → Generate Ontology →
  Data Source → Auto-Map → Build → Neo4j Browser → Inference → Cockpit →
  GraphQL Playground → GraphQL→Cypher behind-the-scenes → SHACL Data Quality.
- **`deck.html`** — same content as a single-file HTML deck (keyboard
  ← / → / `P` to print; click left/right halves to navigate).
- **`screenshots/`** — the 13 source PNGs referenced by the deck.

## Demo numbers

| | |
|---|---|
| AI-generated classes | **32** |
| AI-generated relations | **13** |
| Entities mapped via Auto-Map | **25 / 25** |
| Relations mapped via Auto-Map | **12 / 12** |
| Triples written to Neo4j | **303** |
| Build duration (cold) | **10.3 s** |
| Build duration (cached) | **5.3 s** |
| Inferred triples (T-Box OWL 2 RL) | **99** in 0.102 s |
| SHACL Consistency rules auto-generated | **13** |
| SHACL Graph-mode pass rate against Neo4j | **92.3 %** |
| Total nodes in Neo4j after cleanup | **0** (label dropped) |

## What was tested live

- ✅ Settings → Triple store → Global engine swap to Neo4j
- ✅ Settings → Neo4j config form (URI / database / basic-auth / encrypted)
- ✅ Domain → Documents PDF upload → Ontology → Generate (AI)
- ✅ Ontology Designer (with auto-generated icons)
- ✅ Domain → Data Sources (UC table import)
- ✅ Mapping → Auto-Map (batch + per-entity)
- ✅ Mapping → Diagnostics (0 errors after exclude pass)
- ✅ Domain → Build (writes triples to Neo4j over Bolt)
- ✅ Domain → Cockpit (3-card arch shows Bolt + Graph DB Neo4j)
- ✅ Digital Twin → Knowledge Graph header (engine-aware)
- ✅ Digital Twin → GraphQL Playground (real query against Neo4j)
- ✅ Digital Twin → Inference (OWL 2 RL produced 99 inferred)
- ✅ Digital Twin → Data Quality → Graph mode (SHACL against Neo4j)
- ✅ Neo4j Browser external verification (303 nodes, single label)

## How to reproduce on your own Neo4j endpoint

```bash
export NEO4J_URI=neo4j+s://<your-endpoint>
export NEO4J_USER=neo4j
export NEO4J_PASS=<password>
make deploy            # to a fevm-* workspace with --extra neo4j
# then in the deployed app:
# Settings → Triple Store → Global → Neo4j (Bolt) → fill creds → Save
# Domain → Build  (writes triples via Bolt)
# Verify: tests/integration/neo4j_e2e_smoke.py  — 9 / 9 assertions
```
