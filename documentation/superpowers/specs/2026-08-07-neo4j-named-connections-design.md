# Neo4j Named Connections Design

## Context

Settings → Neo4j currently stores a single flat Bolt profile (`uri`,
`database`, `username`, Databricks secret scope/key, `encrypted`). Domain →
Knowledge Graph exposes an optional “Neo4j Database” dropdown that lists
databases on that one server (`SHOW DATABASES` style), as an override of the
global default database name.

Admins need several named connection profiles (different Aura / self-hosted
endpoints, databases, and secrets) and must bind each Neo4j domain to one of
those profiles explicitly.

## Goal

- Manage multiple Neo4j connection tuples in Settings (add / edit / delete),
  each identified by a unique **Connection name**.
- When a domain’s Graph Backend is Neo4j, require choosing one named connection
  via the existing Domain dropdown (repurposed to list Settings connections).
- Block delete or rename of a connection while any domain still references it.
- Do **not** auto-migrate the legacy single-profile blob; admins recreate
  connections manually. Runtime and UI read only `connections[]`.

## Decisions (locked)

| Topic | Choice |
|-------|--------|
| Domain binding | Required named connection (no “use default”) |
| Delete / rename in use | Block; list referencing domains |
| Legacy flat config | No auto-migration; readers use only `connections[]` (flat keys unused) |
| Objects / Health tabs | Operate on the connection selected in the Settings list |
| Settings UI | Master–detail: list left, edit form right |
| Domain field | New `neo4j_connection` (connection name); retire domain DB override |

## Design

### Data model

**Settings** — `graph_engine_config.neo4j`:

```json
{
  "connections": [
    {
      "name": "Aura Prod",
      "uri": "neo4j+s://….databases.neo4j.io",
      "database": "neo4j",
      "username": "neo4j",
      "secret_scope": "ontobricks",
      "secret_key": "neo4j-password",
      "encrypted": true,
      "auth_method": "databricks_secret"
    }
  ]
}
```

- `name` is unique (case-sensitive), required, and is the stable key domains
  store.
- Per-connection fields match today’s Settings form (URI, database, username,
  secret scope, secret name, encrypted). Password is never persisted.
- Flat legacy keys on the Neo4j bucket (`uri`, `username`, …) are not used by
  readers after this feature. No conversion of old flat config into a
  `"default"` connection.

**Domain** — `domain.info`:

- When `graph_backend == "neo4j"`, `neo4j_connection` is required (string =
  connection `name`).
- Stop writing `neo4j_database` as a domain-level DB override; the Neo4j
  database name lives on the connection. Existing `neo4j_database` values may
  remain on disk until next save but are not used for resolution.

### Runtime resolution

1. Domain graph backend is Neo4j → require non-empty `neo4j_connection`.
2. Load `graph_engine_config.neo4j.connections` and find matching `name`.
3. Pass that profile’s URI / database / username / secret / encrypted into
   `Neo4jConnection` / `GraphDBFactory`.
4. Missing name or empty list → clear validation / connection error
   (“connection X not found in Settings”).

`GraphDBFactory` / Neo4j store paths that today take a single neo4j config
object must resolve via connection name + Settings list instead of treating
the Neo4j bucket as one flat profile.

### Referential integrity

Before deleting or renaming a connection in Settings:

1. Scan domains for `info.neo4j_connection == <name>` (or pending rename old
   name).
2. If any domain references it, reject the operation and return the domain
   identifiers/names so the admin can re-point them first.

Rename that would collide with an existing name is also rejected.

### Settings UI (master–detail)

**Connection** tab:

- Left: list of connections + **Add**. Selecting a row loads the form on the
  right.
- Right: Connection name, Bolt URI, Database, Username, Secret scope, Secret
  name, Encrypted; actions **Test connection**, **Delete**, **Save**.
- Empty state: “No Neo4j connections yet” with Add.
- Secret scope/key dropdowns keep the existing live Databricks Secrets API
  behaviour.

**Objects** and **Health** tabs:

- Use the currently selected list row’s connection.
- If none selected, disable actions and show a short hint to select a
  connection first.

Persist still goes through the graph-engine config save path, writing
`neo4j.connections[]` (not the old flat fields).

### Domain UI

When Graph Backend = Neo4j, show **Neo4j Connection** (required):

- Same control as today’s `#domainNeo4jDatabase` dropdown, relabeled and
  populated from Settings connection names (refresh retained).
- No blank / “use global default” option.
- Help text points to Settings → Neo4j.
- Save with Neo4j and empty/missing `neo4j_connection` → validation error.
- If the saved name is no longer in the list, keep it visible as an invalid
  option so the admin can see and fix it.

### APIs

Admin-gated under existing `/settings/graph-engine/…`:

| Endpoint | Role |
|----------|------|
| `GET …/neo4j-connections` | List connection profiles (no passwords). Domain dropdown uses this. |
| Existing graph-engine config save | Persist `neo4j.connections[]`. |
| `POST …/neo4j-test` | Accept `connection_name` and/or inline draft fields for unsaved edits. |
| Objects / Health / drop-label | Accept `connection_name` for the selected row. |

Retire Domain use of `GET …/neo4j-databases` (server `SHOW DATABASES`). The
endpoint may remain unused or be removed in the same change set if nothing
else calls it.

### Errors

- Duplicate or empty connection name → reject save.
- Missing required fields (URI, username, secret scope/key) → reject save /
  test.
- Domain Neo4j without a valid `neo4j_connection` → reject domain save.
- Delete/rename while referenced → reject with domain list.
- Runtime lookup miss → clear error pointing at Settings.

### Testing

- Unit: normalize/persist `connections[]`; resolve by name; block
  delete/rename when referenced; domain validation for required connection.
- Settings / frontend tests: master–detail merge into config; Domain dropdown
  loads names and has no empty default option.
- Default test command: `uv run --frozen pytest -q -m "not scenario"`.
- No scenario e2e unless explicitly requested.

### Out of scope

- Auto-migration of legacy flat Neo4j config into a named connection.
- Per-domain override of database name independent of the connection.
- Storing passwords in Settings (Databricks secret remains the only UI path).
- Changing Lakebase / Lakehouse multi-connection patterns.

## Success criteria

1. Admin can add, edit, and delete multiple named Neo4j connections in
   Settings with a master–detail UI.
2. Neo4j domains must select a connection by name; build/query uses that
   profile.
3. Delete/rename of an in-use connection is blocked with a clear domain list.
4. Objects/Health operate on the selected Settings connection.
5. Tests cover persistence, resolution, referential checks, and Domain
   validation.
