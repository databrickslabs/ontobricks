# Registry Import / Export

OntoBricks provides two complementary ways to move domains between environments or
share them with teammates:

| Method | When to use | Where |
|--------|-------------|-------|
| **OBX UI** (`.obx` file) | Ad-hoc share, cross-tenant copy, quick backup from the browser | **Registry → Browse** → Export / Import buttons |
| **CLI zip** (`registry_transfer.sh`) | Automated CI/CD promotion, full registry migration | Terminal / shell scripts |

---

## OBX Export / Import (Browser UI)

Available from **Registry → Browse** via the **Export** and **Import** buttons
(Admin role required for Import).

### Export

1. Click **Export** on the Registry → Browse page.
2. Check the domains you want to include.
3. For each domain choose a version mode:
   - **Latest** — only the most recent version
   - **Active** — only the version currently marked as the API/MCP active version
   - **All** — every version
   - **Choose…** — pick individual versions by checkbox
4. Click **Download** — the browser downloads `ontobricks-YYYY-MM-DD.obx`.

The `.obx` file is a JSON envelope carrying an integer `format_version` field
(current: 1) plus the `ontobricks_version` that produced it, enabling a safe
upgrade path if the format changes in future releases.

### Import

1. Click **Import** on the Registry → Browse page.
2. Select the `.obx` file.
3. The preview step shows per-domain conflict flags and a suggested rename.
4. For each domain choose a resolution:
   - **Skip** — leave the existing domain untouched
   - **Overwrite** — replace all versions in the target
   - **Rename** — write under the suggested alternative name
5. Click **Apply**.

> **Note:** The 50 MB upload cap protects the in-memory parse. For larger
> registries use the CLI tool below.

---

## CLI Import / Export (`registry_transfer.sh`)

For automated, scripted, or large-scale migrations an operator runs
`scripts/registry_transfer.sh` directly. This tool has access to both the
source and the target Unity Catalog Volumes and is the recommended path for
CI/CD pipeline promotion.

Typical use cases:

- Promote a reviewed domain from `dev` → `staging` → `prod`.
- Archive a snapshot of the registry before a risky change.
- Seed a brand-new environment with a curated set of domains.
- Copy a single domain version between teams for collaboration.

## What the archive contains

The CLI produces a single `.zip` file that carries the UC-Volume files
needed to reproduce a domain in another environment:

```
manifest.json
domains/<folder>/.domain_permissions.json        (optional, if --include-permissions)
domains/<folder>/V1/V1.json
domains/<folder>/V1/documents/<files...>
domains/<folder>/V2/V2.json
...
```

| File | Included? | Reason |
|------|-----------|--------|
| `V{n}.json` | Yes | Ontology, mappings, design layout, metadata |
| `V{n}/documents/**` | Yes | User-uploaded files attached to the version |
| `.domain_permissions.json` | Optional (`--include-permissions`) | Role assignments for the domain |
| `manifest.json` | Yes | Schema version, source env, per-domain/version inventory |
| `.schedule_history.json` | **Never** | Per-env scheduling history, not portable |
| `.registry` marker, `.global_config.json`, cached files | **Never** | Env-specific |

The resulting archive is named:

```
ontobricks-registry-<source_catalog>.<source_schema>.<source_volume>-<YYYYMMDD-HHMMSS>.zip
```

## Authentication

The shell wrapper honours the standard Databricks SDK environment variables
and profile selection:

| Variable | Purpose |
|----------|---------|
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | Direct PAT authentication |
| `DATABRICKS_CONFIG_PROFILE` | Pick a profile from `~/.databrickscfg` |
| `ONTOBRICKS_PROFILE` | Convenience alias — the wrapper exports it as `DATABRICKS_CONFIG_PROFILE` |

The registry catalog / schema / volume come from `global_config.json` (the
same file the web app uses). Override any of them on the command line with
`--catalog`, `--schema`, `--volume`.

## Script layout

```
scripts/registry_transfer.sh       # thin wrapper — activates .venv, forwards args
src/cli/registry_transfer.py       # argparse entrypoint (actual CLI)
src/back/objects/registry/transfer.py  # pack/unpack library used by the CLI
```

Run `scripts/registry_transfer.sh --help` at any time to see the available
subcommands and flags.

## Subcommands

| Subcommand | Purpose |
|------------|---------|
| `inventory` | List domains and their versions in the configured registry |
| `export` | Pack selected domains/versions into a local `.zip` |
| `import-preview` | Read a `.zip` and show what would happen, without writing anything |
| `import-commit` | Apply a `.zip` into the target registry |

Every subcommand supports `--help`, `--verbose`, `--json`, and the config
overrides `--catalog`, `--schema`, `--volume`.

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `2` | Validation error (bad CLI args, unknown domain, invalid manifest) |
| `3` | Conflict detected on `import-preview` / `import-commit` without a resolution mode |
| `4` | I/O error (UC Volume read/write failed) |

## Examples

All examples below assume you have two Databricks profiles configured in
`~/.databrickscfg` named `src` (source environment) and `dst` (target).

### 1. List what is available in the source registry

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh inventory
```

Example output:

```
Catalog/Schema/Volume: main.ontobricks.registry
Source host: https://src-workspace.cloud.databricks.com

Domain               Versions     Documents
-------------------- ------------ ---------
CustomerAnalytics    V1, V2, V3   12
EnergyOps            V1           3
Finance360           V1, V2       0
```

Add `--json` to get a machine-readable payload suitable for piping into
`jq` or another script.

### 2. Export every domain, every version

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --all \
  --include-permissions \
  --output /tmp/ontobricks-full.zip
```

### 3. Export a specific domain, all versions

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --domain CustomerAnalytics:all \
  --output /tmp/customer-analytics.zip
```

### 4. Export a specific domain, only selected versions

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --domain CustomerAnalytics:V2,V3 \
  --domain EnergyOps:V1 \
  --output /tmp/promotion-bundle.zip
```

`--domain` can be passed multiple times. For each one the syntax is
`NAME:all` or `NAME:V1,V2,...`.

### 5. Preview an import in the target environment

Always preview before committing — this shows the manifest, the per-version
status (`new` vs `conflict`), and the list of documents that would be
written. Nothing is written yet.

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-preview \
  --input /tmp/promotion-bundle.zip
```

Example output:

```
Source:   main.ontobricks.registry @ src-workspace.cloud.databricks.com
Target:   main.ontobricks.registry @ dst-workspace.cloud.databricks.com
Created:  2026-04-22T08:14:03Z by alice@example.com
Schema:   registry-export/v1

Domain               Version  Status     Documents
-------------------- -------- ---------- ---------
CustomerAnalytics    V2       conflict   7
CustomerAnalytics    V3       new        5
EnergyOps            V1       new        3

3 versions will be written (2 new, 1 conflict). Re-run with --conflict to commit.
```

### 6. Commit an import — overwrite on conflict

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-bundle.zip \
  --conflict overwrite \
  --include-permissions \
  --yes
```

The `--yes` flag skips the interactive confirmation prompt (useful in CI).

### 7. Commit an import — keep existing versions, rename incoming ones

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-bundle.zip \
  --conflict rename \
  --yes
```

With `--conflict rename`, an incoming `V2` that collides with an existing
target `V2` is written as `V2_imported_<epoch>` so the source version is
still traceable while the target's original version is untouched.

### 8. Commit an import — skip anything that already exists

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-bundle.zip \
  --conflict skip \
  --yes
```

## Conflict modes

| Mode | Behavior |
|------|----------|
| `skip` | If the target already has `domain/V{n}`, leave it alone and skip the incoming copy |
| `overwrite` | Replace the target `domain/V{n}` (and its `documents/`) with the incoming copy |
| `rename` | Write the incoming version as `V{n}_imported_<epoch>`, preserving the target's original |

If you run `import-commit` without `--conflict` and the archive has any
conflicts, the CLI exits with code `3` and prints the conflict list instead
of writing anything. This is deliberate — there is no implicit default.

## End-to-end promotion workflow

```bash
# 1. Inventory source
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh inventory --json > /tmp/src-inventory.json

# 2. Export what you want to promote
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --domain CustomerAnalytics:V3 \
  --domain Finance360:V2 \
  --output /tmp/promotion-$(date +%Y%m%d).zip

# 3. Move the archive to a machine that can reach the target env
scp /tmp/promotion-*.zip user@target-host:/tmp/

# 4. Preview in the target env
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-preview \
  --input /tmp/promotion-20260422.zip

# 5. Commit once the preview looks right
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-20260422.zip \
  --conflict rename \
  --yes

# 6. On the target env, rebuild the Knowledge Graph for each imported domain
#    so the Delta view + Lakebase Graph DB tables (which are NOT transferred)
#    get regenerated.
```

## Things to watch out for

- **Triple-store materializations are not transferred.** Neither the Delta
  view nor the Lakebase Postgres flat table is part of the archive — they
  are re-created on the next synchronize in the target env, using the
  target's SQL Warehouse and Lakebase instance.
- **The archive carries no secrets.** Databricks host, PAT, and query
  results are never serialized. Authentication for both sides is handled
  via your Databricks CLI profiles.
- **Schema evolution.** `manifest.json` has a `schema_version` field. If
  you try to import an archive produced by a newer OntoBricks release than
  your target, the CLI refuses to write anything and exits with code `2`.
- **Large document payloads.** The zip is built in-memory. Registries with
  hundreds of MB of attachments may need more RAM on the host running the
  CLI.

## Comparison of export/import methods

| Capability | OBX UI (Registry → Browse) | CLI (`registry_transfer.sh`) |
|------------|----------------------------|------------------------------|
| No terminal required | **Yes** | No |
| Per-domain conflict resolution UI | **Yes** | `--conflict` flag |
| Version-mode selector | **Yes** (Latest/Active/All/Choose) | `--domain NAME:all\|Vx,Vy` |
| Handles large registries (>50 MB) | No (50 MB cap) | **Yes** |
| Suitable for CI/CD pipelines | No | **Yes** |
| Includes document attachments | Yes | Yes |
| Transfers permissions | No | Optional (`--include-permissions`) |

The OBX UI is the easiest option for ad-hoc or cross-tenant transfers. The CLI
is the right choice for automated promotion pipelines and full-registry migrations.
