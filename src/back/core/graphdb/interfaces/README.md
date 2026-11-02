# Graph DB Engine — Starter Kit

This directory contains template files for adding a new graph database engine
to OntoBricks.

## How to use

1. **Copy** the entire `_starter_kit/` directory to a new subdirectory named
   after your engine (e.g. `kuzu/`, `neo4j/`).

2. **Rename** `ExampleStore.py` to match your engine class (e.g. `KuzuStore.py`).

3. **Find and replace** these placeholders throughout the copied files:

   | Placeholder | Replace with | Example |
   |-------------|-------------|---------|
   | `ExampleStore` | Your class name | `KuzuStore` |
   | `example_store` | Your module name (snake_case) | `kuzu_store` |
   | `example` | Your engine identifier (lowercase) | `kuzu` |
   | `Example` | Your engine display name | `Kuzu` |
   | `example_library` | The Python package to import | `kuzu` |

4. **Fill in** every `TODO` marker with your engine's native API calls.

5. **Use `engine_config`** — the constructor receives a free-form dict from
   Settings > Graph DB > Engine Configuration.  Define the keys your engine
   needs (e.g. `host`, `port`, `credentials_path`) and document them.  For
   engines that need no configuration, an empty `{}` is fine.

6. **Pick the hat.** `ExampleStore` subclasses `MaterializedGraphBackend` (the
   engine owns a copy of the triples). For a read-only, no-copy pass-through
   engine, subclass `VirtualBackend` instead and drop the write methods.

7. **Add the capability impls your engine needs** and wire them into the factory
   accessors — see "Capabilities & the import boundary" below.

8. **Follow the remaining steps** in the Engine integration section of
   `docs/backend.md` (register in factory, add to allowed engines, update UI,
   add tests).

## Capabilities & the import boundary

The store class is only half the contract. **No code outside
`back/core/graphdb/` may import a backend package** — build, health, provision
and simulation logic is reached through four factory-vended capability
interfaces instead. This is enforced by
`tests/units/architecture/test_backend_import_boundary.py`.

For each capability your engine needs, add a small facade subclass and wire it
into `GraphDBFactory`:

| Capability | Factory accessor | Covers |
|------------|------------------|--------|
| `GraphBuilder` | `get_builder()` | build / materialize / naming helpers |
| `GraphHealthProbe` | `get_health_probe()` | health / quality / object-listing probes |
| `GraphProvisioner` | `get_provisioner()` | config validation / provisioning / credentials |
| `GraphSimulation` | `get_simulation()` | sandbox store, sim naming, union-view lifecycle |

A capability the engine does not need simply has no impl (the accessor returns
`None`). See the Delta / Lakebase / Neo4j impls for the shape.

> If you are about to write `from back.core.graphdb.<engine> import …` outside
> `graphdb/`, stop — add or use a capability accessor instead.

## Files

| File | Purpose |
|------|---------|
| `__init__.py` | Package re-exports (ready to use after renaming) |
| `ExampleStore.py` | Full store class with every abstract method stubbed out |
