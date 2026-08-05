# Neo4j Backend — Requirements & Compatibility

OntoBricks can store a domain's Knowledge Graph in **Neo4j** as a **typed
property graph** (as opposed to the flat triple tables used by the Lakebase and
Delta backends). This document states what a Neo4j server must provide, which
Neo4j *flavors* are supported for the first release, and the limitations users
should know before pointing a production domain at Neo4j.

For the connection/setup walk-through see `documentation/pr47-neo4j-demo/`. For the
graph model internals see the module docstrings in
`src/back/core/graphdb/neo4j/`.

---

## 1. What the typed model stores

When a domain's graph backend is **Neo4j** (Domain → Information → Knowledge
Graph → *Neo4j*), the build pipeline writes a property graph, not flat triples:

| RDF construct | Neo4j representation |
|---|---|
| subject / object URI | a **node**, MERGE-keyed on its full URI (`uri` property) |
| `rdf:type` | a Neo4j **label** on the node |
| `rdfs:label` | the node's `name` property |
| predicate with a **literal** object | a **property** on the subject node |
| predicate with a **URI** object | a **relationship** `(s)-[:reltype]->(o)` |

Every node also carries a per-graph **marker label** (the sanitised
`<Domain>_V<version>` name) and is covered by a `uri` uniqueness constraint, so
one domain graph is a single `MATCH (n:\`<marker>\`)` away for counting,
isolation, and dropping.

Reads reconstruct the exact original `{subject, predicate, object}` triples from
the graph (using a small per-graph reverse-map stored on a `:__GraphSchema`
node), so the Knowledge-Graph view, GraphQL, and reasoning layers behave
identically to the SQL backends — while traversal and reasoning run as **native
Cypher** relationship patterns.

---

## 2. Server requirements

**A Neo4j server running version 5.x is required.** This is a *server* version
requirement, not just a driver requirement. The backend uses 5.x-only syntax:

- `CREATE CONSTRAINT … FOR (n:Label) REQUIRE n.uri IS UNIQUE`
  (Neo4j 4.x used the older `ASSERT` form)
- `SHOW CONSTRAINTS YIELD name, labelsOrTypes`
- `SHOW DATABASES YIELD name`

Neo4j **4.x will not work** and is not supported.

**APOC is not required.** The write path deliberately inlines sanitised labels
and groups rows with `UNWIND` rather than calling `apoc.create.*`, so the
backend runs on Aura Free and Community with no plugins installed.

**Connection / auth.** The Bolt URI, database, username, and encryption flag are
configured in Settings → Neo4j. In a deployed Databricks App the password comes
from the `NEO4J_PASSWORD` env var (bound via an Apps secret resource); the
persisted config password is a local-dev fallback only and is stripped at
save-time in production. `auth_method=databricks_secret` is reserved for a
future release (currently raises `NotImplementedError`). Self-hosted servers may
use `bolt://` / `neo4j://` (unencrypted) or `neo4j+s://` / `bolt+s://` (TLS
embedded).

---

## 3. Neo4j flavor compatibility

OntoBricks is developed and tested against **Neo4j Aura** (managed, v5). Other
flavors work with the caveats below.

| Flavor | Typed graph write/read | Multi-database (DB selector) | Notes |
|---|---|---|---|
| **Aura** (managed) | ✅ | ✅ (paid tiers) | Primary dev/test target. Aura Free is single-DB. |
| **Enterprise** (self-hosted) | ✅ | ✅ | Full support incl. per-domain database routing. |
| **Community** (self-hosted) | ✅ | ❌ single `neo4j` DB only | Typed graph works; DB selector degrades gracefully (see §4). |
| **AuraDS / Neo4j 4.x** | ❌ (4.x) / ⚠️ (AuraDS = v5, works) | — | 4.x unsupported; AuraDS follows the v5 rules above. |

---

## 4. The per-domain database selector (multi-database)

Domain → Information → Knowledge Graph exposes a **Neo4j database** selector
(parity with Lakebase's database picker). This lets one OntoBricks deployment
target a different Neo4j *database* per domain.

**Multi-database is an Enterprise / Aura feature.** Neo4j **Community Edition is
single-database** — it only has the `neo4j` database and cannot create or route
to others. On flavors where `SHOW DATABASES` is unavailable or restricted, the
selector **degrades gracefully**: the backend catches the error, returns an
empty list, and the connection falls back to the configured default database.
So the *feature* is only useful on Enterprise / Aura, but selecting Neo4j as a
backend never fails on Community because of it.

> **If you pick a database that does not exist on your server**, queries will
> fail at runtime when the driver opens a session against it. Only choose a
> database the server actually has (use the selector's *Refresh* button, which
> lists real databases via `SHOW DATABASES`).

The per-domain choice is stored in `DomainSession.info['neo4j_database']`, is
versioned and survives UC export/import, and overrides the workspace-global
`graph_engine_config.neo4j.database` at build time (empty → keep the global
default).

---

## 5. OntoBricks assumes it owns the connected database

The admin **Objects** tab (Settings → Neo4j → Objects) lists **every OntoBricks
graph** in the connected Neo4j database — that is, every node label backed by a
`node_<label>_uri` uniqueness constraint — with node and relationship counts,
and lets an admin **drop** any of them.

Because this operates on the whole connected database:

- **Give each OntoBricks deployment its own Neo4j database** (Enterprise / Aura)
  **or its own instance.** Two deployments pointed at the same database will see
  and can drop each other's graphs.
- Non-OntoBricks data in the same database is *not* listed (it has no
  `node_*_uri` marker constraint) and is not touched by graph drops, but you are
  still sharing an instance — plan capacity and access accordingly.

---

## 6. Migration note (pre-release flat graphs)

Earlier v0.7 pre-release builds wrote Neo4j as a **flat triple store**
(`(:Label {subject, predicate, object})` nodes with no relationships). The typed
model is a **breaking storage change**: old flat graphs are **not** migrated
automatically and must be **rebuilt**. Old flat nodes and new typed nodes cannot
coexist meaningfully under the same marker label — drop the old graph (Objects
tab) and rebuild the domain.

---

## 7. Operational notes

- **Bulk-build memory.** On heap-constrained instances (e.g. Aura Free), very
  large ontologies may need batch tuning; the default insert batch size is
  2000 triples.
- **No raw Cypher entry point.** All writes go through the build pipeline after
  ontology validation (the C2 safeguard); there is no user-facing Cypher console.
- **Health tab.** Settings → Neo4j → Health runs a Bolt handshake + trivial
  `RETURN 1` against the saved connection so you can confirm reachability
  without running a build.
