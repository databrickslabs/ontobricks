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

6. **Follow the remaining steps** in `docs/graphdb-integration.md` (register
   in factory, add to allowed engines, update UI, add tests).

## Files

| File | Purpose |
|------|---------|
| `__init__.py` | Package re-exports (ready to use after renaming) |
| `ExampleStore.py` | Full store class with every abstract method stubbed out |
