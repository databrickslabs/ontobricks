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

**Named connection profiles.** Settings → Neo4j stores one or more **named
connections** (`graph_engine_config.neo4j.connections[]`), each with its own
Bolt URI, database, username, encryption flag, and Databricks secret
scope/key. There is no auto-migration from the older flat single-profile keys —
admins re-enter connections in the master–detail UI.

**Auth.** Every connection must use a Databricks secret (`auth_method:
databricks_secret`). Clear-text passwords are stripped on save. In a deployed
Databricks App the runtime still accepts a bound `NEO4J_PASSWORD` env var as a
legacy fallback for older configs, but the Settings UI only exposes the
secret-scope path. Self-hosted servers may use `bolt://` / `neo4j://`
(unencrypted) or `neo4j+s://` / `bolt+s://` (TLS embedded).

---

## 3. Neo4j flavor compatibility

OntoBricks is developed and tested against **Neo4j Aura** (managed, v5). Other
flavors work with the caveats below.

| Flavor | Typed graph write/read | Multi-database (per connection) | Notes |
|---|---|---|---|
| **Aura** (managed) | ✅ | ✅ (paid tiers) | Primary dev/test target. Aura Free is single-DB. |
| **Enterprise** (self-hosted) | ✅ | ✅ | Full support; put the target DB name on the connection profile. |
| **Community** (self-hosted) | ✅ | ❌ single `neo4j` DB only | Typed graph works; leave the connection's database at the Community default. |
| **AuraDS / Neo4j 4.x** | ❌ (4.x) / ⚠️ (AuraDS = v5, works) | — | 4.x unsupported; AuraDS follows the v5 rules above. |

---

## 4. Domain binding — named connection (required)

When a domain's Graph Backend is **Neo4j**, Domain → Information → Knowledge
Graph requires a **Neo4j connection** dropdown value (`domain.info.neo4j_connection`).
That name resolves to one Settings profile at build / query time. There is no
blank “use default” and no per-domain database override — the database lives on
the connection profile.

> **If the profile points at a database that does not exist on the server**,
> queries fail when the driver opens a session. Prefer **Test connection** on
> the selected profile in Settings → Neo4j before binding domains to it.

Deleting or renaming a connection that domains still reference is rejected with
the list of affected domains.

---

## 5. OntoBricks assumes it owns the connected database

The admin **Objects** tab (Settings → Neo4j → Objects) lists **every OntoBricks
graph** in the **selected** connection's Neo4j database — that is, every node
label backed by a `node_<label>_uri` uniqueness constraint — with node and
relationship counts, and lets an admin **drop** any of them. Pick the
connection in the Objects picker first.

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

Domains that still have a flat workspace Neo4j config (no `connections[]`) must
be reconfigured: add named connection(s) in Settings → Neo4j, then set each
Neo4j domain's **Neo4j connection** on Domain → Information → Knowledge Graph.

---

## 7. Operational notes

- **Bulk-build memory.** On heap-constrained instances (e.g. Aura Free), very
  large ontologies may need batch tuning; the default insert batch size is
  2000 triples.
- **No raw Cypher entry point.** All writes go through the build pipeline after
  ontology validation (the C2 safeguard); there is no user-facing Cypher console.
- **Test connection.** Settings → Neo4j → **Test connection** (per selected
  profile) runs a Bolt handshake plus a trivial `RETURN 1` so you can confirm
  reachability without running a build. The former dedicated Health tab was
  removed; Test connection covers the same probe.
