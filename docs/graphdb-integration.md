# Adding a New Graph DB Engine

This guide walks a developer through adding support for a new graph database
engine to OntoBricks.  It covers the architecture, the abstract contracts,
registration in the factory and global config, and a ready-to-use starter kit.

---

## 1. Architecture Overview

OntoBricks has two storage layers for knowledge graph data:

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
its own keys.  For LadybugDB, an empty `{}` is sufficient.

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
├── ladybugdb/          ← existing
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
   and `query_dialect = "cypher"`.  Override the named query methods with native
   Cypher implementations (see `LadybugFlatStore` for reference).  If SQL, the
   inherited defaults work.

2. **Graph model**: If your engine uses typed node/relationship tables (like
   LadybugDB's graph model), set `supports_graph_model = True` and implement
   `get_graph_schema()`.  If it uses a flat triple table, leave it `False`.

3. **Reasoning translator**: Return the appropriate `SWRL*Translator` from
   `get_query_translator()`.  For SQL engines, the default `SWRLSQLTranslator`
   works.  For Cypher, return `SWRLFlatCypherTranslator` or
   `SWRLCypherTranslator`.

4. **Sync**: If your engine stores data as local files, implement
   `sync_to_remote()` and `sync_from_remote()` to archive/restore via
   `VolumeFileService`.

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
        engine = "ladybug"
    if engine_config is None:
        engine_config = {}

    if engine == "ladybug":
        return self._create_ladybug(domain, settings, engine_config=engine_config)

    if engine == "kuzu":                      # ← NEW
        return self._create_kuzu(domain, settings, engine_config=engine_config)

    logger.warning("Unknown graph DB engine: %s", engine)
    return None

def _create_kuzu(self, domain, settings=None, *, engine_config=None):   # ← NEW
    """Instantiate a KuzuDB store."""
    try:
        from back.core.graphdb.kuzu.KuzuStore import KuzuStore
        db_path = DEFAULT_LADYBUG_PATH  # or a kuzu-specific path
        base_name = (domain.info or {}).get("name", DEFAULT_GRAPH_NAME)
        version = getattr(domain, 'current_version', '1') or '1'
        db_name = f"{base_name}_V{version}"
        return KuzuStore(db_path=db_path, db_name=db_name, engine_config=engine_config)
    except ImportError as e:
        logger.warning("KuzuDB requires kuzu: %s", e)
        return None
    except Exception as e:
        logger.exception("Failed to create KuzuStore: %s", e)
        return None
```

> **`engine_config`** is a free-form JSON dict set by the admin in
> **Settings > Graph DB > Engine Configuration**.  The factory reads it
> from `GlobalConfigService` and passes it to every engine constructor.
> Each engine defines its own keys (e.g. `host`, `port`, `credentials_path`).
> For LadybugDB an empty `{}` is sufficient.

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
ALLOWED_GRAPH_ENGINES = ("ladybug", "kuzu")  # ← add here
```

That single change makes the engine selectable from the Settings UI and
validates it on save.

### Step 6 — Update the Settings UI dropdown

Edit `src/front/templates/settings.html` — add an `<option>` to the
`#graphEngineSelect` dropdown:

```html
<select class="form-select form-select-sm" id="graphEngineSelect" style="max-width:20rem;">
    <option value="ladybug">Internal (LadybugDB)</option>
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
`tests/test_ladybug.py`.  At minimum, test:

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

## 4. Reference: LadybugDB Engine Structure

The built-in LadybugDB engine is the reference implementation:

```
graphdb/ladybugdb/
├── __init__.py           ← re-exports, backward-compat wrappers
├── LadybugBase.py        ← GraphDBBackend subclass (connection, sync, capabilities)
├── LadybugFlatStore.py   ← Flat triple table (single node table, Cypher queries)
├── LadybugGraphStore.py  ← Typed graph model (node/rel tables from ontology)
├── GraphSchema.py        ← Schema model (node table defs, rel table defs)
├── GraphSchemaBuilder.py ← Builds GraphSchema from ontology classes/properties
├── GraphSyncService.py   ← Upload/download .lbug files to/from UC Volume
└── models.py             ← NodeTableDef, RelTableDef dataclasses
```

**Two store variants** share a common base:

- `LadybugFlatStore` — all triples in a single `Triple(id, subject, predicate, object)` node table.
- `LadybugGraphStore` — OWL classes become node tables, object properties become relationship tables.  Falls back to flat when schema is unavailable.

A simpler engine can use a single store class.

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

**Q: Can I support both flat and graph models like LadybugDB?**
Yes.  Create a base class extending `GraphDBBackend`, then two subclasses
(flat and graph).  Register the graph variant in the factory and have it
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
