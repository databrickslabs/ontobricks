# Adding a New Graph DB Engine

This guide walks a developer through adding support for a new graph database
engine to OntoBricks.  It covers the architecture, the abstract contracts,
registration in the factory and global config, and a ready-to-use starter kit.

---

OntoBricks currently ships with one **runtime** graph engine, selectable under **Settings → Graph DB**
(admin). The list is intentionally kept short — the abstraction is what matters here.

| Engine | Storage | Notes |
|--------|---------|--------|
| ``lakebase`` (default and only built-in) | Flat triple tables on **Lakebase Postgres** | Uses the App-bound Postgres instance (``PGHOST`` / ``PGDATABASE``…). Configure JSON ``graph_engine_config`` with optional ``database`` (Postgres DB name on that instance) and ``schema`` (default ``ontobricks_graph``), and ``mode`` (``app_managed`` or ``managed_synced``). SQL-only (no Cypher); reasoning uses the existing SQL translators. |

``GraphDBFactory.create(engine=...)`` is the single decision point: only the selected engine is instantiated. The capability flags on ``GraphDBBackend`` (``supports_cypher``, ``is_cypher_backend``, ``query_dialect``) are kept as architectural seams so a future Cypher / Gremlin engine can be added without rewiring reasoning.

---

## 1. Architecture Overview

OntoBricks has two storage layers for graph viewer data:

| Layer | Package | Purpose |
|-------|---------|---------|
| **Triple Store** | `back.core.triplestore` | Permanent storage in Unity Catalog (Delta views, SQL queries). |
| **Graph DB** | `back.core.graphdb` | Local/embedded graph engine for Cypher queries, traversal, reasoning, and analytics. |

The `TripleStoreFactory` delegates to `GraphDBFactory` when `backend="graph"`.
The factory reads the configured engine name from `GlobalConfigService` and
passes it to `GraphDBFactory.create(engine=...)`.

```
TripleStoreFactory.create(domain, settings, backend="graph")
    │
    ├─ _resolve_graph_engine()  →  GlobalConfigService.get_graph_engine()
    │                                returns e.g. "kuzu"
    └─ get_graphdb(domain, settings, engine="kuzu")
           │
           └─ GraphDBFactory.create(domain, settings, engine="kuzu")
                  │
                  └─ _create_kuzu(domain, settings)  →  KuzuStore(...)
```

### Key files

| File | Role |
|------|------|
| `src/back/core/triplestore/TripleStoreBackend.py` | Abstract base — triple CRUD + named query methods (SQL defaults). |
| `src/back/core/graphdb/GraphDBBackend.py` | Graph DB abstract base — extends `TripleStoreBackend` with capability flags, connection management, sync, reasoning. |
| `src/back/core/graphdb/GraphDBFactory.py` | Factory — maps engine names to constructor methods. |
| `src/back/core/graphdb/__init__.py` | Package exports (`get_graphdb`, `GRAPHDB_AVAILABLE`). |
| `src/back/objects/session/GlobalConfigService.py` | Persists the selected engine name in `.global_config.json`. |

---

## 2. The Contract

A new engine must implement **two levels of abstraction**:

### 2.1 `TripleStoreBackend` (core CRUD)

These abstract methods **must** be implemented:

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_table` | `(table_name: str) -> None` | Create the `(subject, predicate, object)` storage. |
| `drop_table` | `(table_name: str) -> None` | Drop the table if it exists. |
| `insert_triples` | `(table_name, triples, batch_size, on_progress) -> int` | Batch insert triples. Return count inserted. |
| `query_triples` | `(table_name: str) -> List[Dict[str, str]]` | Return all triples as `{subject, predicate, object}` dicts. |
| `count_triples` | `(table_name: str) -> int` | Return the number of triples. |
| `table_exists` | `(table_name: str) -> bool` | Check if the triple table exists. |
| `get_status` | `(table_name: str) -> Dict[str, Any]` | Return `{count, last_modified, path, format}`. |
| `execute_query` | `(query: str) -> List[Dict[str, Any]]` | Execute a raw query (SQL or native). Raise `NotImplementedError` if not applicable. |

These methods have **SQL default implementations** that you should **override**
if your engine does not speak SQL:

- `get_aggregate_stats`
- `get_type_distribution` / `get_predicate_distribution`
- `find_subjects_by_type` / `resolve_subject_by_id`
- `get_entity_metadata` / `get_triples_for_subjects`
- `get_predicates_for_type`
- `paginated_triples` / `paginated_count`
- `bfs_traversal`
- `find_seed_subjects` / `find_subjects_by_patterns`
- `transitive_closure` / `symmetric_expand` / `shortest_path`
- `expand_entity_neighbors`
- `delete_triples` (raises `NotImplementedError` by default)
- `optimize_table` (no-op by default)

### 2.2 `GraphDBBackend` (graph-specific)

**Constructor parameter** — every engine receives `engine_config: Dict[str, Any]`
(default `{}`) from the factory.  This is a free-form JSON dict set by the
admin in **Settings > Graph DB > Engine Configuration**.  Each engine defines
its own keys.  For Lakebase, recognised keys include ``database``, ``schema``,
and ``mode`` (``app_managed`` or ``managed_synced``).

These abstract methods **must** be implemented:

| Method | Signature | Description |
|--------|-----------|-------------|
| `get_connection` | `() -> Any` | Return (and lazily open) the native database connection. |
| `close` | `() -> None` | Release the connection and any related resources. |

These have sensible **defaults** that you should **override** as needed:

| Method | Default | Override when... |
|--------|---------|-----------------|
| `supports_cypher` | `False` | Your engine speaks Cypher. |
| `supports_graph_model` | `False` | Your engine uses typed node/relationship tables. |
| `query_dialect` | `"sql"` | Your engine uses a different dialect (e.g. `"cypher"`, `"gremlin"`). |
| `get_node_table(name)` | Returns `name` unchanged | Your engine has naming constraints (e.g. identifier sanitisation). |
| `get_graph_schema()` | `None` | Your engine builds a graph schema from the ontology. |
| `sync_to_remote(uc_path, volume_service)` | No-op | Your engine stores files that should be synced to UC Volumes. |
| `sync_from_remote(uc_path, volume_service)` | No-op | Same, for restore on cold start. |
| `local_path()` | `None` | Your engine stores data locally. |
| `remote_archive_path(uc_domain_path)` | `None` | Your engine has a remote archive naming convention. |
| `get_query_translator(table_name)` | `SWRLSQLTranslator()` | Your engine needs a custom SWRL/rule translator for reasoning. |

---

## 3. Step-by-Step Integration

### Step 1 — Create the engine subpackage

```
src/back/core/graphdb/
├── __init__.py
├── GraphDBBackend.py
├── GraphDBFactory.py
├── lakebase/           ← existing (Postgres flat-store reference impl)
├── _starter_kit/       ← copy-paste template (ExampleStore.py)
└── kuzu/               ← NEW
    ├── __init__.py
    └── KuzuStore.py
```

Per coding rules: **one public class per file**, file named after the class
in PascalCase.

### Step 2 — Implement the store class

Create `src/back/core/graphdb/kuzu/KuzuStore.py`.  Copy it from the starter
kit at `src/back/core/graphdb/_starter_kit/ExampleStore.py` and rename.
See [Section 5](#5-starter-kit) for details.

Key decisions:

1. **Query dialect**: If your engine speaks Cypher, set `supports_cypher = True`
   and `query_dialect = "cypher"`. Override the named query methods with native
   Cypher implementations and ship a matching `SWRLCypherTranslator` (the SQL
   translator stays the default for SQL engines).

2. **Graph model**: If your engine uses typed node/relationship tables, set
   `supports_graph_model = True` and implement `get_graph_schema()`. If it uses
   a flat triple table (like the shipped `LakebaseFlatStore`), leave it `False`.

3. **Reasoning translator**: Return the appropriate `SWRL*Translator` from
   `get_query_translator()`. For SQL engines, the default `SWRLSQLTranslator`
   works.

4. **Sync**: If your engine stores data as local files, implement
   `sync_to_remote()` and `sync_from_remote()` to archive/restore via
   `VolumeFileService`. Lakebase does not need this — the data lives in Postgres.

### Step 3 — Create the package `__init__.py`

```python
# src/back/core/graphdb/kuzu/__init__.py
"""KuzuDB graph database backend."""
from back.core.graphdb.kuzu.KuzuStore import KuzuStore  # noqa: F401

__all__ = ["KuzuStore"]
```

### Step 4 — Register the engine in `GraphDBFactory`

Edit `src/back/core/graphdb/GraphDBFactory.py`:

```python
def create(self, domain, settings=None, engine=None, engine_config=None):
    if engine is None:
        engine = "lakebase"
    if engine_config is None:
        engine_config = {}

    if engine == "lakebase":
        return self._create_lakebase(domain, settings, engine_config=engine_config)

    if engine == "kuzu":                      # ← NEW
        return self._create_kuzu(domain, settings, engine_config=engine_config)

    logger.warning("Unknown graph DB engine: %s", engine)
    return None

def _create_kuzu(self, domain, settings=None, *, engine_config=None):   # ← NEW
    """Instantiate a KuzuDB store."""
    try:
        from back.core.graphdb.kuzu.KuzuStore import KuzuStore
        base_name = (domain.info or {}).get("name", DEFAULT_GRAPH_NAME)
        version = getattr(domain, 'current_version', '1') or '1'
        db_name = f"{base_name}_V{version}"
        return KuzuStore(db_name=db_name, engine_config=engine_config)
    except ImportError as e:
        logger.warning("KuzuDB requires kuzu: %s", e)
        return None
    except Exception as e:
        logger.exception("Failed to create KuzuStore: %s", e)
        return None
```

> **`engine_config`** is a free-form JSON dict set by the admin in
> **Settings > Graph DB > Engine Configuration**. The factory reads it
> from `GlobalConfigService` and passes it to every engine constructor.
> Each engine defines its own keys (e.g. `host`, `port`, `credentials_path`).
> For Lakebase, recognised keys are `database`, `schema`, and `mode`.

Then update the availability check at the bottom of the file:

```python
try:
    from back.core.graphdb.kuzu.KuzuStore import KuzuStore  # noqa: F401
    GraphDBFactory.KUZU_AVAILABLE = True
except ImportError:
    GraphDBFactory.KUZU_AVAILABLE = False
```

### Step 5 — Register the engine name in `GlobalConfigService`

Edit `src/back/objects/session/GlobalConfigService.py`:

```python
ALLOWED_GRAPH_ENGINES = ("lakebase", "kuzu")  # ← add here
```

That single change makes the engine selectable from the Settings UI and
validates it on save.

### Step 6 — Update the Settings UI dropdown

Edit `src/front/templates/settings.html` — add an `<option>` to the
`#graphEngineSelect` dropdown:

```html
<select class="form-select form-select-sm" id="graphEngineSelect" style="max-width:20rem;">
    <option value="lakebase">Lakebase Postgres</option>
    <option value="kuzu">KuzuDB</option>        <!-- NEW -->
</select>
```

### Step 7 — Add the dependency

Add the engine's Python package to `pyproject.toml` as an optional dependency:

```toml
[project.optional-dependencies]
kuzu = ["kuzu>=0.4"]
```

Update `docs/development.md` with the new dependency (name, link, license).

### Step 8 — Add tests

Create `tests/test_kuzu_store.py` following the patterns in
`tests/test_lakebase_flat_store.py`. At minimum, test:

- Store instantiation (with and without the library installed)
- `create_table` / `drop_table`
- `insert_triples` / `query_triples` / `count_triples`
- `table_exists` / `get_status`
- Capability flags (`supports_cypher`, `query_dialect`)

### Step 9 — Update documentation

- Update this file if the architecture changes.
- Add an entry to `docs/development.md` in the Dependencies section.
- Add a Sphinx `.rst` file under `docs/sphinx/api/` for the new subpackage.
- Update the changelog.

---

## 4. Reference: Lakebase Engine Structure

The built-in Lakebase Postgres engine is the reference implementation:

```
graphdb/lakebase/
├── __init__.py           ← re-exports
├── LakebaseBase.py       ← GraphDBBackend subclass (connection pool, capabilities)
├── LakebaseFlatStore.py  ← Flat triple table (subject, predicate, object) on Postgres
├── SyncedTableManager.py ← Lakeflow synced-table orchestration (managed_synced mode)
└── models.py             ← Internal dataclasses
```

The flat store keeps the contract simple: a single Postgres table per
`(domain, version)` with a primary key on `(subject, predicate, object)` and
two write modes (`app_managed` via `COPY FROM STDIN`, `managed_synced` via
Lakeflow). A simpler engine can use a single store class and skip
`SyncedTableManager`.

---

## 5. Starter Kit

A ready-to-use starter kit lives at:

```
src/back/core/graphdb/_starter_kit/
├── README.md          ← usage instructions
├── __init__.py        ← package re-exports (template)
└── ExampleStore.py    ← full store class with every method stubbed
```

### How to use

1. **Copy** the `_starter_kit/` directory into a new subpackage:

   ```bash
   cp -r src/back/core/graphdb/_starter_kit src/back/core/graphdb/kuzu
   ```

2. **Rename** `ExampleStore.py` to `KuzuStore.py` (matching your engine class).

3. **Find and replace** these placeholders throughout the copied files:

   | Placeholder | Replace with | Example |
   |-------------|-------------|---------|
   | `ExampleStore` | Your class name | `KuzuStore` |
   | `example_store` | Your module name (snake_case) | `kuzu_store` |
   | `example` | Your engine identifier (lowercase) | `kuzu` |
   | `Example` | Your engine display name | `Kuzu` |
   | `example_library` | The Python package to import | `kuzu` |

4. **Fill in** every `TODO` marker with your engine's native API calls.

5. **Continue from [Step 3](#step-3--create-the-package-__init__py)** above
   to register the engine in the factory, global config, and UI.

The `ExampleStore.py` template contains the full method contract with
detailed docstrings, grouped into sections:
- Capability flags (`supports_cypher`, `query_dialect`, …)
- Connection management (`get_connection`, `close`)
- Schema helpers (`get_node_table`, `get_graph_schema`)
- Sync to/from UC Volume (`sync_to_remote`, `sync_from_remote`)
- Reasoning support (`get_query_translator`)
- Core CRUD (`create_table`, `insert_triples`, `query_triples`, …)
- Named query overrides (commented stubs for non-SQL engines)

---

## 6. Checklist

Use this checklist to track your progress:

- [ ] Create `src/back/core/graphdb/<engine>/` package with `__init__.py`
- [ ] Implement `<EngineName>Store(GraphDBBackend)` with all abstract methods
- [ ] Override named query methods if your engine is non-SQL
- [ ] Register engine in `GraphDBFactory.create()` + add `_create_<engine>()` method
- [ ] Add engine name to `GlobalConfigService.ALLOWED_GRAPH_ENGINES`
- [ ] Add `<option>` to `#graphEngineSelect` in `settings.html`
- [ ] Add optional dependency to `pyproject.toml`
- [ ] Add tests in `tests/test_<engine>_store.py`
- [ ] Update `docs/development.md` (dependency table)
- [ ] Add Sphinx `.rst` under `docs/sphinx/api/`
- [ ] Update changelog

---

## 7. FAQ

**Q: Can I support both flat and graph models?**
Yes. Create a base class extending `GraphDBBackend`, then two subclasses
(flat and graph). Register the graph variant in the factory and have it
fall back to flat when the ontology is not available.

**Q: What if my engine is remote (e.g. Neo4j Aura)?**
The architecture supports it.  `get_connection()` can return a driver
connected to a remote endpoint.  `sync_to_remote` / `sync_from_remote` may
be no-ops if data is already remote.  `local_path()` should return `None`.

**Q: What about the reasoning engines?**
Reasoning engines use `GraphDBBackend.is_cypher_backend(store)` and the
capability flags to decide which translator to use.  If your engine speaks
Cypher, set the flag and return the appropriate translator from
`get_query_translator()`.  If SQL, the defaults work.

**Q: Do I need to touch `TripleStoreFactory`?**
No.  `TripleStoreFactory` reads the engine from `GlobalConfigService` and
passes it to `GraphDBFactory`.  You only edit `GraphDBFactory`.

---

## 8. Lakebase build performance

When the active engine is **Lakebase**, the Knowledge Graph build keeps heavy
data on the Databricks side and never holds the full triple set inside the
FastAPI process.

### Read side (Databricks SQL → app)

`SQLWarehouse.iter_rows(query, batch_size=5000)` opens a cursor on the
warehouse and yields dict rows in `fetchmany` batches. The build pipeline
uses it for the full rebuild (`SELECT subject, predicate, object FROM view`)
without ever materializing the full triple set inside the FastAPI process.

### Write side (app → Lakebase Postgres)

`LakebaseFlatStore` exposes two streaming bulk paths used by the pipeline:

- `bulk_insert_iter(table, triple_iter, batch_size=5000)` — per batch:
  `CREATE TEMP TABLE _ob_copy_stage … ON COMMIT DROP`, `COPY FROM STDIN`
  (binary), then `INSERT INTO {phy} … SELECT FROM _ob_copy_stage ON CONFLICT
  DO NOTHING`. The temp table lives only inside the per-batch transaction
  (`conn.transaction()` is needed because the pool runs `autocommit=True`).
- `bulk_delete_iter(table, triple_iter, batch_size=5000)` — symmetrical
  `COPY` into `_ob_del_stage` followed by `DELETE FROM {phy} USING
  _ob_del_stage d WHERE …`.

`insert_triples` / `delete_triples` keep their public signatures and
delegate to the bulk iterator paths once the payload crosses
`_BULK_INSERT_THRESHOLD` / `_BULK_DELETE_THRESHOLD` (50 rows).

### Pipeline gating

`_BuildPipeline._stream_triples_into_store` and
`_stream_triples_out_of_store` call `bulk_insert_iter` /
`bulk_delete_iter` when the store exposes them (Lakebase) and fall back to
materializing the iterator into a list for backends without a streaming
write path. `_start_background_archive` is a no-op for SQL-backed engines:
the Delta view + Postgres tables are the system of record, no archive is
pushed to the Volume.

---

## 9. Lakebase managed-synced mode (data plane only)

The default Lakebase mode (`sync_mode = "app_managed"`) still flows R2RML
triples through the FastAPI process via `iter_rows` + `COPY FROM STDIN`.
Bounded memory, but the app is on the hot path.

`sync_mode = "managed_synced"` moves the bulk movement out of the app
entirely: a Databricks **Lakeflow snapshot pipeline** keeps a Postgres
**synced table** in lock-step with the R2RML view, and the app only
orchestrates. Reasoning + cohort writes (small volumes) keep their direct
PG path through a writable **companion table**; readers see both via a
**UNION view** with the legacy table name, so SPARQL / KG search code is
unchanged.

### Postgres layout per graph version

| Object | Owner | Purpose |
|--------|-------|---------|
| `g_<dom>_v<n>_sync` | Lakeflow (read-only) | Mirrors the source view via snapshot. |
| `g_<dom>_v<n>__app`  | App (read/write)     | Reasoning + cohort triples (datatype/lang aware). |
| `g_<dom>_v<n>`       | App DDL (`CREATE OR REPLACE VIEW`) | UNION view readers query (back-compat name). |

The synced side is restricted to `(subject, predicate, object)` — the union
view NULL-pads `datatype` / `lang` for those rows so the view exposes a
uniform 5-column shape.

### Configuration

`graph_engine_config` accepts the following extra keys (all optional):

```jsonc
{
  "schema": "ontobricks_graph",         // fallback PG schema only when Registry has no Volume schema
  "database": "appdb",                   // PG database (overrides PGDATABASE)
  "sync_mode": "managed_synced",         // default: "app_managed"
  "sync_table_mode": "snapshot",         // snapshot | triggered | continuous
  "sync_timeout_s": 600,                  // wait deadline for a sync run
  "sync_uc_catalog": "main"              // UC catalog for synced table registration (optional override)
}
```

Sync UC naming is `<sync_uc_catalog or fallback>.<schema>.<table>` where **schema**
is resolved by ``resolve_lakebase_graph_schema``: **Registry Volume schema**
(``RegistryCfg.schema``) **always wins** when Settings → Registry resolves to a
non-empty triplet; otherwise ``graph_engine_config.schema`` (default
``ontobricks_graph``). Together with catalog fallback from the same Registry,
managed-synced tables register under the **same ``catalog.schema`` as the Volume**.

### Unity Catalog Explorer — graph triples + synced table

Open **Catalog Explorer** at ``<catalog>.<registry_volume_schema>``: graph triple
tables, companion, union view, and the UC synced-table registration share that
schema segment once the store is constructed (see build log
``Managed-sync registers UC synced table at …``).

`validate_engine_config_keys` enforces the type and value constraints.

### Build pipeline branch

`_BuildPipeline._apply_via_synced_pipeline(full=...)` replaces the row-level
ingest in synced mode:

1. Resolve the synced UC FQN as `<catalog>.<schema>.<base>_sync` where
   *catalog* is: ``graph_engine_config.sync_uc_catalog`` if set; otherwise
   ``resolve_sync_uc_fallback_catalog`` — optional deployment env
   ``ONTOBRICKS_SYNC_UC_CATALOG`` (legacy ``ONTBRICKS_*`` still honoured),
   then **Settings → Registry** UC catalog,
   then ``domain.delta.catalog`` (per-domain Delta catalog). This avoids
   registering the synced table under a personal/home UC catalog when the
   registry triplet points at the team catalog.
2. ``CREATE SCHEMA IF NOT EXISTS`` for that **Unity Catalog** ``catalog.schema``
   (SQL warehouse DDL). The synced-table API requires this metastore object;
   Postgres schema alone on Lakebase is not enough.
   See ``_sync_uc_schema.ensure_uc_schema_for_synced_table_fqn``.
3. `SyncedTableManager.ensure(...)` -- idempotent
   `WorkspaceClient.database.create_synced_database_table` call.
4. `LakebaseFlatStore.ensure_synced_companion(name)` — companion table only
   (must run before Lakeflow materializes the ``_sync`` table).
5. `SyncedTableManager.trigger_and_wait(...)` — calls `trigger_refresh`
   (`pipelines.start_update` with ``full_refresh=True``), then waits on the
   returned **update id** via ``pipelines.get_update`` until that Lakeflow run
   finishes (so we do not mistake a stale ``ONLINE`` synced-table status for the
   new build). If ``start_update`` was skipped because another update was already
   active, it falls back to ``wait_get_pipeline_idle`` plus synced-table polling.
6. `LakebaseFlatStore.ensure_synced_union_view(name)` — union view after the ``_sync`` table
   exists in Postgres (``CREATE OR REPLACE VIEW`` references the synced table).
7. On full rebuild, `TRUNCATE` the companion so reasoning + cohort start
   from a clean slate.

`_compute_diff_or_fall_through` short-circuits to `actual_mode = "full"` in
synced mode -- snapshot pipelines always rewrite the table, so a row-level
diff is wasted work. `_refresh_snapshot` is also skipped (Lakeflow is the
truth).

The scheduler mirrors this logic via `_apply_synced_pipeline` in
`back/objects/registry/scheduler.py`.

### Read paths

`LakebaseFlatStore` separates the resolvers:

- `_writable_table_id(name)` -- companion in synced mode, legacy phy in
  app-managed mode (used by `insert_triples`, COPY insert, COPY delete,
  `delete_triples`).
- `_readable_table_id(name)` -- union view in synced mode (same identifier
  as the legacy phy in app-managed mode), used by `query_triples`,
  `iter_triples`, `count_triples`, `table_exists`, `get_status`.
- `optimize_table` vacuums only the writable companion in synced mode (the
  synced side is Lakeflow-managed).

### Lifecycle

`LakebaseFlatStore.drop_table(name)` cascades in synced mode:
1. `DROP VIEW IF EXISTS` for the union view.
2. `DROP TABLE IF EXISTS` for the companion.
3. `SyncedTableManager.delete(uc_name, purge_data=True)` to remove the
   synced table from UC and its underlying PG table.

If the SDK or UC catalog is unavailable, the cascade still drops the PG
view + companion and logs a warning rather than aborting.
