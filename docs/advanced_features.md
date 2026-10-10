# Advanced features

Deeper topics beyond the day-to-day flow in the [User Guide](user-guide.md):
how OntoBricks models an ontology, how domains move between environments,
and constraint-based cohort discovery.

<!-- toc -->
**Contents**

- [Ontology specifics](#ontology-specifics)
  - [1. Dual identity: name (ID) vs label](#1-dual-identity-name-id-vs-label)
  - [2. URI minting (TBox)](#2-uri-minting-tbox)
  - [3. Studio node / resource types (TBox)](#3-studio-node--resource-types-tbox)
  - [4. Custom vocabulary http://ontobricks.com/schema#](#4-custom-vocabulary-httpontobrickscomschema)
  - [5. Attributes vs relationships (asymmetric model)](#5-attributes-vs-relationships-asymmetric-model)
  - [6. Instance identity (ABox / knowledge graph) — ID vs label again](#6-instance-identity-abox--knowledge-graph--id-vs-label-again)
  - [7. Graph node types (runtime, not OWL)](#7-graph-node-types-runtime-not-owl)
  - [8. What the parser and generator consume](#8-what-the-parser-and-generator-consume)
  - [9. Other product rules that shape the ontology](#9-other-product-rules-that-shape-the-ontology)
  - [10. Code map](#10-code-map)
- [Registry import and export](#registry-import-and-export)
  - [OBX Export / Import (Browser UI)](#obx-export--import-browser-ui)
  - [CLI Import / Export (registry_transfer.sh)](#cli-import--export-registry_transfersh)
  - [What the archive contains](#what-the-archive-contains)
  - [Authentication](#authentication)
  - [Script layout](#script-layout)
  - [Subcommands](#subcommands)
  - [Examples](#examples)
  - [Conflict modes](#conflict-modes)
  - [End-to-end promotion workflow](#end-to-end-promotion-workflow)
  - [Things to watch out for](#things-to-watch-out-for)
  - [Comparison of export/import methods](#comparison-of-exportimport-methods)
- [Cohort discovery](#cohort-discovery)
  - [1. Mental model](#1-mental-model)
  - [2. Authoring a rule (UX)](#2-authoring-a-rule-ux)
  - [3. Output destinations](#3-output-destinations)
  - [4. Persistence (where rules live)](#4-persistence-where-rules-live)
  - [5. The algorithm](#5-the-algorithm)
  - [6. Idempotency guarantees](#6-idempotency-guarantees)
  - [7. Worked examples](#7-worked-examples)
  - [8. API summary](#8-api-summary)
  - [9. Stage 2 — natural-language rule generation](#9-stage-2--natural-language-rule-generation)
<!-- /toc -->

---

## Ontology specifics


OntoBricks is **not a general RDF editor**. Studio keeps a **class/property OWL
model** plus a **Databricks-facing annotation layer**. Anything else is either
compiled into that model or parked as leftover triples (`rdf_extras`).

This page is the catalog of those product-specific conventions: identifiers,
labels, node types, the `ontobricks:` vocabulary, instance URIs, and what
import/export actually consume.

Related: [User Guide — Studio](user-guide.md), [Import / Export](#registry-import-and-export),
[Architecture — OWL generation](architecture.md#owl-generation).

---

### 1. Dual identity: `name` (ID) vs `label`

Every class, property, and group has two identities:

| Field | Role | RDF |
|---|---|---|
| `name` / `localName` | Stable identifier, URI fragment, GraphQL/SQL key | last segment of the resource IRI |
| `label` | Human-readable display name | `rdfs:label` |
| `alternate_labels` | Synonyms | `skos:altLabel` |

- Designer and the generate wizard mint `name` from the label: **PascalCase**
  for classes, **lowerCamelCase** for properties and relationships.
- If there is no label, the UI falls back to `name`.
- Import can keep a file’s `rdfs:label` (and optional `label_lang`) separate
  from the domain/project name.
- Renaming `name` remints the IRI (`base_uri + name`) when the stored URI’s
  local name no longer matches.

Recommended naming (enforced more by convention and sanitization than by OWL):

- Entities: `Person`, `CustomerOrder`
- Relationships: `worksIn`, `hasOrder`
- Attributes: `firstName`, `orderDate`
- No spaces in IDs (spaces become `_` on export)

Allowed identifier characters: letters, numbers, underscores, hyphens. Names
are case-sensitive (`Person` and `person` are different).

---

### 2. URI minting (TBox)

- Default namespace: `https://databricks-ontology.com/{DomainName}#`
  (Settings → Default Base URI Domain; Custom toggle allowed).
- Ontology resource IRI is the **base without `#`**:
  `https://databricks-ontology.com/MyOrg`.
- Class/property IRI is **`{base_uri}{name}`**, unless a stored `uri` still
  matches that local name.
- Import also stores full IRIs so external parents survive export:

  | JSON field | Meaning |
  |---|---|
  | `cls["uri"]` / `prop["uri"]` | Resource IRI; generator prefers this over `base_uri + name` |
  | `cls["parent_uri"]` | Full `rdfs:subClassOf` IRI when the parent is named (not a restriction) |
  | `prop["domain_uri"]` / `prop["range_uri"]` | Full domain/range IRIs |

- Turtle export binds a lowercase sanitized ontology-name prefix plus
  `ontobricks:`.

Implementation: `OntologyClassModel.ensure_uris`,
`OntologyGenerator._effective_uri`.

---

### 3. Studio node / resource types (TBox)

These are the first-class things Studio edits. Everything else is either a
blank-node restriction or leftover RDF.

| Kind | OWL / custom | Notes |
|---|---|---|
| **Class / entity** | `owl:Class` | Skip `owl:Thing`; skip blank nodes |
| **Attribute** | `owl:DatatypeProperty` | Owned by a class (`classes[].dataProperties` is source of truth) |
| **Relationship** | `owl:ObjectProperty` | Domain/range are classes; no nested attributes on the edge |
| **Group** | `owl:Class` + `ontobricks:isGroup true` | `owl:equivalentClass` / `owl:unionOf` members; **not** shown as a normal entity |
| **Inheritance** | `rdfs:subClassOf` | Named parent only; restriction blank nodes go to constraints |
| **Axiom / expression** | `equivalentClass`, `disjointWith`, `disjointUnionOf`, `unionOf`, `intersectionOf`, `complementOf`, `oneOf`, `inverseOf`, property chain, `disjointProperties` | Separate Expr. & Axioms store |
| **Cardinality / value restriction** | `owl:Restriction` on `subClassOf` | `min`/`max`/`exactCardinality`, `allValuesFrom`, `someValuesFrom`, `hasValue` |
| **Property characteristic** | extra `rdf:type` on the property | functional, inverseFunctional, transitive, symmetric, asymmetric, reflexive, irreflexive |
| **Value constraint** | `ontobricks:ValueConstraint` | String/regex-style checks, **not** OWL |
| **SWRL rule** | `ontobricks:SWRLRule` | Stored as text annotations, compiled to SQL |
| **Global DQ rule** | `ontobricks:GlobalRule` | e.g. `noOrphans`, `requireLabels` |
| **SHACL shape** | separate SHACL graph | Data quality, not OWL TBox |
| **Ontology header** | `owl:Ontology` | label, optional lang, leftover DCTERMS/PROV/`owl:versionInfo` |

Blank nodes (anonymous unions, restrictions) are **never** classes in the
designer.

---

### 4. Custom vocabulary `http://ontobricks.com/schema#`

Namespace constant: `ONTOBRICKS_NS` in `src/shared/config/constants.py`.
These predicates are **consumed** (never leftover) and are the
Databricks/product layer.

| Predicate | On | Meaning |
|---|---|---|
| `ontobricks:icon` | class, group | Emoji / visualization icon |
| `ontobricks:isGroup` | class | Marks a group class; parser excludes it from `classes` |
| `ontobricks:groupColor` | group | Designer color |
| `ontobricks:direction` | object property | `forward` / `reverse` / bidirectional (UI + graph, not OWL) |
| `ontobricks:dashboard` | class | Databricks dashboard URL |
| `ontobricks:dashboardParams` | class | JSON: map dashboard filters → `__ID__` or an attribute |
| `ontobricks:dataset` | class | JSON: linked Unity Catalog table/view |
| `ontobricks:actions` | class | JSON: UC functions, **exactly one arg = entity local ID** |
| `ontobricks:virtualAttributes` | class | JSON: computed attrs via UC functions (same 1-arg ID contract); **not** emitted as `owl:DatatypeProperty` |
| `ontobricks:bridges` | class | JSON: cross-project bridges |
| `ontobricks:businessRules` | class | JSON: SWRL rule names attached to the class |
| `ontobricks:ValueConstraint` + `appliesTo` / `onAttribute` / `checkType` / `checkValue` / `caseSensitive` / `hasValueConstraint` | class | Product value checks |
| `ontobricks:GlobalRule` + `ruleName` | ontology | Graph-wide DQ (`requireLabels`, `noOrphans`) |
| `ontobricks:SWRLRule` + `antecedent` / `consequent` | ontology | Rule text |

Session-only (not always OWL): `importedFrom` on entities copied from another
domain.

Default ontology base URI (when Settings has not overridden it):
`https://databricks-ontology.com/`.

---

### 5. Attributes vs relationships (asymmetric model)

- **Attributes** live on the class (`dataProperties`). Export writes one
  `owl:DatatypeProperty` per attribute, domain = that class, range = XSD
  (`string` default; also int/decimal/bool/date/datetime/anyURI, …).
- A parallel `properties[]` entry of type `DatatypeProperty` is a **shadow**.
  If the attribute is deleted from the class, the shadow is dropped on
  save/export (the class list wins).
- **Relationships** are flat `owl:ObjectProperty`s. No relationship-level
  attributes.
- Domain/range on properties are **canonical class `name`s**
  (case-insensitive normalize). Inherited outgoing relations are computed
  (`inherited` / `inheritedFrom`).
- Datatype properties are merged onto subclasses on load
  (`OntologyParser._propagate_inherited_properties`).

XSD range aliases accepted by the generator include `string`/`text`,
`integer`/`int`, `number`/`decimal`, `float`/`double`, `boolean`/`bool`,
`date`, `datetime`, `time`, `uri`/`url`.

---

### 6. Instance identity (ABox / knowledge graph) — ID vs label again

Mapped individuals are **not** the class IRI. R2RML always mints:

```text
{base_uri}{ClassLocalName}/{id_column_value}
```

Example: `https://databricks-ontology.com/MyOrg#Customer/C-42`

| Piece | Source | Used for |
|---|---|---|
| **URI** | class local name + mapped **ID column** | graph subject, joins, uniqueness |
| **Local ID** (`__ID__`) | last path segment after `Class/` | dashboards, UC actions, virtual attributes, “open in source table” |
| **`rdfs:label`** | mapped **label column** | graph node caption, search, `requireLabels` |
| **`rdf:type`** | class IRI | typing, GraphQL, filters |

**ID is the business key in the URI; label is display.** They are independent.
A node can have a URI without a label (data quality flags it if
`requireLabels` is on).

Relationship subject/object templates **must** use the same class-local-name
segment as the entity map (the class label is not used in the URI).

`DigitalTwin.extract_local_id` strips the `Class/` prefix from the fragment
because `base_uri` normally ends in `#`, so the fragment is `Class/id`, not
the bare id.

---

### 7. Graph node types (runtime, not OWL)

The explorer is a **projection** of triples:

| Runtime node | How it is recognized |
|---|---|
| **Typed entity** | Has `rdf:type` to an ontology class; icon/color/group from that class |
| **Orphan** | Subject with no `rdf:type` (`noOrphans` rule) |
| **Literal** | Not a node; attributes hang off the entity |
| **Inferred** | Triples from OWL-RL / SWRL / decision tables; shown with provenance, can be purged |
| **Group collapse** | UI grouping of types that share an `ontobricks:isGroup` class |
| **External / imported class** | Badge when the class IRI is outside the domain namespace |

`rdf:type` and `rdfs:label` are treated as **infra predicates**, not business
relationships.

---

### 8. What the parser and generator consume

**First-class (edited in Studio):**

- `rdf:type`
- `rdfs:label` (optional language tag → `label_lang`)
- `rdfs:comment` (optional language tag → `comment_lang`)
- `rdfs:subClassOf`, `rdfs:domain`, `rdfs:range`
- `skos:altLabel`
- every predicate under `http://ontobricks.com/schema#`

**Parked as `ontology.rdf_extras` (round-trip, not edited in Studio):**

`skos:definition`, `skos:editorialNote`, DCTERMS, PROV, `owl:versionInfo`,
and other named-subject annotations. Dropped if the subject is deleted or
its IRI changes.

Shape of `rdf_extras`:

```json
{
  "prefixes": {
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dcterms": "http://purl.org/dc/terms/"
  },
  "triples": [
    {
      "s": "http://example.org/onto#Customer",
      "p": "http://www.w3.org/2004/02/skos/core#definition",
      "o": "A party that buys goods.",
      "o_kind": "literal",
      "lang": "en",
      "datatype": null
    }
  ]
}
```

**Not modeled as classes:** individuals, unused named resources, blank-node
objects, bit-exact Turtle (prefix order, comments).

Implementation: `OntologyRdfExtras` (`CONSUMED_PREDICATES` +
`http://ontobricks.com/schema#` prefix).

---

### 9. Other product rules that shape the ontology

- **Class-first editor.** You design entities; OWL is a serialization of
  session JSON (`classes`, `properties`, `constraints`, `axioms`,
  `expressions`, `groups`, `swrl_rules`, `rdf_extras`).
- **Design layout** is separate from OWL (positions, views). It is
  reconciled when classes, attributes, or relationships change.
- **One parent in the designer** (`parent` / first named `subClassOf`). Extra
  named parents or annotations can survive only via extras or axioms.
- **Groups are union classes**, not SKOS collections or RDF named graphs.
- **SWRL is annotation + SQL**, not a full W3C SWRL/RIF document.
- **SHACL is a sibling artifact** (shapes file / session list), compiled to
  SQL against the triple VIEW.
- **Generate wizard** keeps a draft `id` (UUID-like) distinct from the
  minted OWL `name`, and unique normalized labels.
- **Industry import** (FIBO / CDISC / IOF / FHIR) goes through the same
  parser; conflict merge keys are **URI then name**.
- **Cross-domain import** rewrites IRIs to the target `base_uri`, keeps
  icon/description/parent, records `importedFrom`. Mappings, rules,
  constraints, and groups are **not** copied.
- **Class actions and virtual attributes** take exactly one parameter: the
  entity local ID extracted from the instance URI.
- **Dashboard parameters** may bind to `__ID__` (local ID) or to a class
  attribute.

---

### 10. Code map

| Concern | Primary types |
|---|---|
| Session / CRUD | `back.objects.ontology.Ontology`, `OntologyEditor`, `OntologyClassModel` |
| Parse / generate OWL | `back.core.w3c.owl.OntologyParser`, `OntologyGenerator`, `OntologyRdfExtras` |
| Import apply | `back.objects.ontology.OntologyOwl`, `OntologyImport` |
| Groups | `back.objects.ontology.OntologyGroups` |
| Instance URIs (R2RML) | `back.core.w3c.r2rml.R2RMLGenerator` |
| Local ID from URI | `back.objects.digitaltwin.TwinResolve.extract_local_id` |
| Namespace constants | `shared.config.constants.ONTOBRICKS_NS`, `DEFAULT_BASE_URI` |

---

## Registry import and export


OntoBricks provides two complementary ways to move domains between environments or
share them with teammates:

| Method | When to use | Where |
|--------|-------------|-------|
| **OBX UI** (`.obx` file) | Ad-hoc share, cross-tenant copy, quick backup from the browser | **Registry → Browse** → Export / Import buttons |
| **CLI zip** (`registry_transfer.sh`) | Automated CI/CD promotion, full registry migration | Terminal / shell scripts |

---

### OBX Export / Import (Browser UI)

Available from **Registry → Browse** via the **Export** and **Import** buttons
(Admin role required for Import).

#### Export

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

#### Import

1. Click **Import** on the Registry → Browse page.
2. Select the `.obx` file. A spinner shows while OntoBricks reads the archive.
3. In the preview, uncheck any domain you do not want to import. Checked
   domains are selected by default; the header checkbox toggles them all.
4. Review or change **Import as** for each selected domain. Names use
   CamelCase alphanumeric syntax (for example, `ClaimsArchive`).
5. When **Import as** targets an existing domain, choose **Skip** or
   **Overwrite**. When it differs from the source folder, Import creates a
   separate domain under the new name.
6. Click **Import**. A spinner shows on the button until the write finishes.

Renamed imports regenerate the ontology base URI from the configured default
and the new name. Ontology, mappings, settings, and the versions present in the
file are copied, but graph data, build state, and uploaded documents are not.
Build the new domain before using its graph. Overwrite retains the imported
identity and build metadata unchanged.

> **Note:** The 50 MB upload cap protects the in-memory parse. For larger
> registries use the CLI tool below.

---

### CLI Import / Export (`registry_transfer.sh`)

For automated, scripted, or large-scale migrations an operator runs
`scripts/registry_transfer.sh` directly. This tool has access to both the
source and the target Unity Catalog Volumes and is the recommended path for
CI/CD pipeline promotion.

Typical use cases:

- Promote a reviewed domain from `dev` → `staging` → `prod`.
- Archive a snapshot of the registry before a risky change.
- Seed a brand-new environment with a curated set of domains.
- Copy a single domain version between teams for collaboration.

### What the archive contains

The CLI produces a single `.zip` file that carries the UC-Volume files
needed to reproduce a domain in another environment:

```
manifest.json
domains/<folder>/.domain_permissions.json        (optional, if --include-permissions)
domains/<folder>/V1/V1.json
domains/<folder>/V2/V2.json
...
```

| File | Included? | Reason |
|------|-----------|--------|
| `V{n}.json` | Yes | Ontology, mappings, design layout, metadata — including the domain's `mcp_policy` (see below) |
| Knowledge Store documents | **No** | Since v0.9.0 uploaded documents are parsed text rows in the Lakebase `domain_documents` table, not files on the Volume. They are **not** carried in the `.zip` bundle; re-upload them in the target environment (their parsed text is copied forward within an environment when a new version is created) |
| `.domain_permissions.json` | Optional (`--include-permissions`) | Role assignments for the domain |
| `manifest.json` | Yes | Schema version, source env, per-domain/version inventory |
| `.schedule_history.json` | **Never** | Per-env scheduling history, not portable |
| `.registry` marker, `.global_config.json`, cached files | **Never** | Env-specific |

The resulting archive is named:

```
ontobricks-registry-<source_catalog>.<source_schema>.<source_volume>-<YYYYMMDD-HHMMSS>.zip
```

> **MCP policy travels with the domain.** The `info` block of each `V{n}.json`
> carries `mcp_policy`, so a domain keeps the tools and ontology attachments it
> publishes when promoted to another environment. It is **domain-level**, not
> per-version — it is written to the `domains` row, so importing several
> versions of one domain leaves the last one written in effect. It is
> re-validated on both export and import: unknown tool names, registry-level
> tools and unknown context features are dropped rather than rejected, so a
> bundle produced by a newer OntoBricks still imports cleanly into an older
> one. A bundle from before 0.8 has no `mcp_policy` and imports as `{}` —
> every tool exposed, every attachment normal. See
> [Per-domain MCP policy](mcp.md#per-domain-mcp-policy).

### Authentication

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

### Script layout

```
scripts/registry_transfer.sh       # thin wrapper — activates .venv, forwards args
src/cli/registry_transfer.py       # argparse entrypoint (actual CLI)
src/back/objects/registry/transfer.py  # pack/unpack library used by the CLI
```

Run `scripts/registry_transfer.sh --help` at any time to see the available
subcommands and flags.

### Subcommands

| Subcommand | Purpose |
|------------|---------|
| `inventory` | List domains and their versions in the configured registry |
| `export` | Pack selected domains/versions into a local `.zip` |
| `import-preview` | Read a `.zip` and show what would happen, without writing anything |
| `import-commit` | Apply a `.zip` into the target registry |

Every subcommand supports `--help`, `--verbose`, `--json`, and the config
overrides `--catalog`, `--schema`, `--volume`.

#### Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `2` | Validation error (bad CLI args, unknown domain, invalid manifest) |
| `3` | Conflict detected on `import-preview` / `import-commit` without a resolution mode |
| `4` | I/O error (UC Volume read/write failed) |

### Examples

All examples below assume you have two Databricks profiles configured in
`~/.databrickscfg` named `src` (source environment) and `dst` (target).

#### 1. List what is available in the source registry

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
`jq` or another script. The `Documents` column is an informational count of each
domain's Knowledge Store documents (Lakebase `domain_documents` rows); those
parsed documents are not included in export bundles.

#### 2. Export every domain, every version

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --all \
  --include-permissions \
  --output /tmp/ontobricks-full.zip
```

#### 3. Export a specific domain, all versions

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --domain CustomerAnalytics:all \
  --output /tmp/customer-analytics.zip
```

#### 4. Export a specific domain, only selected versions

```bash
ONTOBRICKS_PROFILE=src scripts/registry_transfer.sh export \
  --domain CustomerAnalytics:V2,V3 \
  --domain EnergyOps:V1 \
  --output /tmp/promotion-bundle.zip
```

`--domain` can be passed multiple times. For each one the syntax is
`NAME:all` or `NAME:V1,V2,...`.

#### 5. Preview an import in the target environment

Always preview before committing — this shows the manifest and the per-version
status (`new` vs `conflict`). Nothing is written yet. The `Documents` column is
an informational count of each version's Knowledge Store documents in the source;
the parsed documents themselves live in Lakebase and are **not** carried in the
bundle.

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

#### 6. Commit an import — overwrite on conflict

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-bundle.zip \
  --conflict overwrite \
  --include-permissions \
  --yes
```

The `--yes` flag skips the interactive confirmation prompt (useful in CI).

#### 7. Commit an import — keep existing versions, rename incoming ones

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-bundle.zip \
  --conflict rename \
  --yes
```

With `--conflict rename`, an incoming `V2` that collides with an existing
target `V2` is written as `V2_imported_<epoch>` so the source version is
still traceable while the target's original version is untouched.

#### 8. Commit an import — skip anything that already exists

```bash
ONTOBRICKS_PROFILE=dst scripts/registry_transfer.sh import-commit \
  --input /tmp/promotion-bundle.zip \
  --conflict skip \
  --yes
```

### Conflict modes

| Mode | Behavior |
|------|----------|
| `skip` | If the target already has `domain/V{n}`, leave it alone and skip the incoming copy |
| `overwrite` | Replace the target `domain/V{n}` with the incoming copy |
| `rename` | Write the incoming version as `V{n}_imported_<epoch>`, preserving the target's original |

If you run `import-commit` without `--conflict` and the archive has any
conflicts, the CLI exits with code `3` and prints the conflict list instead
of writing anything. This is deliberate — there is no implicit default.

### End-to-end promotion workflow

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

### Things to watch out for

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

### Comparison of export/import methods

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

---

## Cohort discovery


Cohort Discovery turns the question *"which entities travel together?"* into a
deterministic, explainable, idempotent rule that any business user can author,
preview, materialise, and re-run.  It complements — not replaces — the
existing reasoning engines (OWL 2 RL, SWRL, SPARQL CONSTRUCT, Decision
Tables, Aggregate Rules); cohort rules live in their own slot
(`ontology.cohort_rules`) and never interfere with W3C standard reasoning.

This document is the canonical user/developer reference.  The release-
requirement specification that drove the implementation lives at
[`releasereq/cohort_design.md`](../releasereq/cohort_design.md).

---

### 1. Mental model

A cohort is a set of entities that:

1. belong to the same **target class** (e.g. `:Person`),
2. are **linked together** through some bridging entity (e.g. they share a
   `:Project` reachable through `:assignedTo`), and
3. all satisfy the same **compatibility policies** (e.g. their `:status`
   equals `Exempt`, or every member shares the same `:department`).

The user writes this as a **CohortRule** in five small sections.  The engine
runs it, produces deterministic cohort URIs, and writes the result either
into the graph viewer (as `:inCohort` triples) or into a Unity Catalog
Delta table — or both.

---

### 2. Authoring a rule (UX)

In the Knowledge Graph, open **Advanced → Cohorts**.  The form has five
sections, each with live feedback:

1. **Identity** — *Rule name* + optional *Description*.  The internal id is
   slug-derived from the name (e.g. `Exempt staffing pool` →
   `exempt_staffing_pool`).
2. **What are we grouping?** — pick a target class from the ontology.  The
   counter to the right shows live instance counts in the graph.
3. **When are two members linked?** — zero-or-more *shared-resource
   paths*.  Each path is an ordered chain of hops starting from the
   source class; the **last hop's target** is the entity two members
   must reach to be considered linked.  Most cohorts only need a 1-hop
   path (e.g. `Person —assignedTo→ Project`), but you can click
   *Add hop* to chain hops — for example
   `Person —assignedTo→ Project —governedBy→ ComplianceType` lets you
   say *"two persons are linked when they work on projects governed by
   the same compliance type"* without materialising an intermediate
   predicate.

   The dropdowns are **dependent at every hop**: each hop's source is
   locked (it's either the rule's source class or the previous hop's
   target).  The `via` list shows only object properties whose
   `domain = <hop source>`, and is further narrowed to those with
   `range = <target_class>` once a target is picked.  The
   `target_class` list is disabled until `via` is picked, then filtered
   to that property's range.  Editing a hop propagates downstream: the
   next hop's source pill updates and its dropdowns re-filter.

   When you add more than one path, choose **ANY** (union) or **ALL**
   (intersection) to combine them.  The link-edge counter shows how
   many candidate edges are produced live.

   Each hop also carries an optional **where filter** (the funnel icon
   next to *shared* shows the count). Use it to constrain a hop's
   *target* node by its own attributes — *"… → ComplianceType where
   complianceTypeId = 'Individual'"* attaches the constraint to the
   compliance type itself, instead of misusing rule-level
   compatibility (which only filters the source class). This is the
   correct fix when a multi-hop preview returns *"0 cohorts"* despite a
   path that would clearly match data: the constraint usually lives on
   a node along the path, not on the source. The same four primitives
   are available — `equals`, `in any`, `between` — minus `same value`,
   which is a pairwise edge constraint and meaningless on a single
   node. Missing values still drop the candidate by default; use the
   filter's `Allow missing` toggle (in the JSON payload, not yet on
   the form) to opt out.
4. **Compatibility policies** — zero-or-more constraints from this menu:
   * **Same value of `<property>`** (every member shares the same value).
   * **`<property>` equals `<value>`** (per-member literal).
   * **`<property>` in any of `<list>`** (per-member set membership).
   * **`<property>` between `<min>` and `<max>`** (per-member numeric range).
   The match-count badge shows surviving members live.  The picker next to
   `value_equals` calls `/dtwin/cohorts/sample-values` so users can pick
   actual values from the graph instead of guessing.
5. **Group type** — *Connected* (transitive: A↔B and B↔C ⇒ {A,B,C}) or
   *Strict* (clique: every pair must be linked directly).  Plus a
   *Minimum cohort size* knob (default 2).

The sticky **action bar** at the bottom of the form lets you:

| Button | What it does |
|---|---|
| **Preview cohorts** | Runs `/dtwin/cohorts/dry-run` and switches to the **Preview** tab (no writes). |
| **Save rule** | `POST /dtwin/cohorts/rules` — versioned with the domain (works in both Volume and Lakebase modes). |
| **Materialise** | Opens a small modal confirming what gets written. Idempotent per rule. |
| **Configure outputs** | Toggle graph triples on/off and configure the optional Unity Catalog Delta target. |
| **View JSON** | Inspect the canonical `CohortRule` payload. |

The Preview tab's **Why? / Why not?** explainer accepts a member URI and
returns a per-stage breakdown — class membership, surviving compatibility
constraints, edge presence, final cohort.  Perfect when a stakeholder
asks *"why isn't Alice in the pool?"*.

The **Trace path** button on the Preview tab is the corresponding
*"why are there 0 cohorts?"* tool. Click it and the engine instruments
each link's path with per-hop counters:

| Column | Meaning |
|---|---|
| `in` | distinct nodes at hop entry (after Stage 3a survivors) |
| `raw` | outbound edges traversed via the hop's `via` predicate |
| `drop` | neighbours rejected — split between `target_class` (type filter) and `where` (hop filter) on hover |
| `out` | distinct surviving neighbours fed into the next hop |

The first hop where `out` collapses to 0 highlights itself in red,
and the diagnostic line below the table reads off the most likely
cause — wrong predicate URI, wrong target class URI, or a misconfigured
hop `where` filter (case-sensitive value, missing `allow_missing`,
etc.). This turns the silent *"0 cohorts, 0 of N members grouped"*
symptom into a one-glance pinpoint.

---

### 3. Output destinations

A cohort run can write to **graph triples**, a **Unity Catalog table**, or
both — the two outputs are independent and idempotent.

#### 3.1 Graph triples (always available)

For each cohort `c` produced by rule `r`:

```
<cohort_uri>  rdf:type             :Cohort
<cohort_uri>  rdfs:label           "<rule.label> — cohort #N"
<cohort_uri>  :fromRule            "<rule.id>"
<cohort_uri>  :cohortSize          "<size>"
<member_uri>  :inCohort            <cohort_uri>     # one per member
```

`<cohort_uri>` is `<base_uri>/cohort/<rule_id>/c-<sha256(sorted(members))[:8]>` —
a content-hash URI.  Same membership ⇒ same URI across runs (stable join
key in BI tools, Sigma, GraphQL).  Different membership ⇒ new URI; old
ones are deleted on re-materialise.

#### 3.2 Unity Catalog Delta table (optional)

When the rule's `output.uc_table` is set, materialisation creates (if
needed) and populates a Delta table with this schema:

| column | type | notes |
|---|---|---|
| `rule_id` | STRING | partition key |
| `rule_label` | STRING | |
| `cohort_id` | STRING | local fragment, e.g. `c-3f2a91b6` |
| `cohort_uri` | STRING | full URI |
| `cohort_idx` | INT | sequence within the run |
| `cohort_size` | INT | |
| `member_uri` | STRING | |
| `member_id` | STRING | local name |
| `member_label` | STRING | best-effort `rdfs:label` |
| `domain_name` | STRING | |
| `domain_version` | STRING | |
| `materialised_at` | TIMESTAMP | |

Re-runs are idempotent: `DELETE FROM <fq> WHERE rule_id = ?` then
`INSERT`.  The table is partitioned by `rule_id` so multiple rules can
share one table cheaply.

The **Configure outputs** modal exposes two safety nets:

* **Auto-pick** (`/dtwin/cohorts/uc/suggest-target?rule_name=…`) —
  proposes catalog/schema from the domain settings, source-table
  metadata, or registry (falling back to a literal `cohorts` schema),
  and `table_name = cohorts_<snake_rule_name>` so the table reads as
  `cohorts_exempt_staffing_pool` for a rule named `ExemptStaffingPool`.
  When `rule_name` is omitted (legacy callers) the table falls back to
  `cohorts_<domain_slug>`.
* **Test write access** (`/dtwin/cohorts/uc/probe-write`) — runs a
  three-step read-only probe (catalog → schema → table) so users find
  out about a missing privilege *before* clicking Materialise.

---

### 4. Persistence (where rules live)

Cohort rules live alongside SWRL/SPARQL/aggregate rules in
`session.ontology.cohort_rules`.  They are versioned and persisted by the
existing registry layer:

* **Volume mode** — written into `versions/V<N>.json` on the Unity Catalog
  Volume next to the rest of the ontology payload.
* **Lakebase mode** — shredded into the `ontology` JSONB column of the
  `<schema>.domain_versions` Postgres table; the registry layer is
  rule-agnostic, so no schema migration was needed.

In-memory access goes through `DomainSession.cohort_rules` (property +
setter, mirroring `aggregate_rules`).  `export_for_save()` includes the
list automatically.  Activating an older domain version reloads its
historical rules transparently.

---

### 5. The algorithm

The engine (`back/core/graph_analysis/CohortBuilder.py`) is **backend-
agnostic**: it talks to the triplestore exclusively through
`store.query_triples(graph_name)` and `store.insert_triples(...)`, which
work on every supported backend (Delta + Spark SQL, Lakebase Postgres SQL,
or any future Cypher / Gremlin engine added through `GraphDBFactory`). All
higher-level filtering, edge construction, and grouping happens in pure
Python — same approach as `CommunityDetector`.

The pipeline has six stages, in order:

```
1. List class members         (subjects with rdf:type = class_uri)
2. Fetch attribute values     (one pass over triples, indexed per property)
3a. Apply node filters         (value_equals / value_in / value_range)
3b. Build candidate edges      (members sharing a bridging entity, per link)
4. Apply edge filters         (same_value)
5. Run NetworkX grouping      (connected_components OR find_cliques)
6. Rank, hash, materialise    (sort by size, content-hash URIs, write)
```

`CohortBuilder` exposes **per-stage helpers** consumed by the live
counters and the Why? explainer:

| Helper | Purpose |
|---|---|
| `count_class_members(class_uri)` | Section 2 counter. |
| `count_link_edges(class_uri, links, combine)` | Section 3 counter. |
| `count_matching_nodes(class_uri, compatibility)` | Section 4 counter. |
| `sample_property_values(class_uri, property_uri, limit)` | `value_equals` picker. |
| `explain_membership(rule, target_uri)` | Why? / Why not? per-stage trace. |

For algorithmic detail (SQL dispatch, complexity, schema-drift handling,
worked example), see
[`releasereq/cohort_design.md` §9](../releasereq/cohort_design.md).

---

### 6. Idempotency guarantees

A re-materialise of a saved rule:

1. Wipes the rule's old graph triples via
   `store.delete_cohort_triples(table, prefix, in_cohort)` — the cohort
   URI prefix is `<base_uri>/cohort/<rule_id>/`, and the predicate is
   `:inCohort<RuleId>` (rule-scoped, so multiple rules can co-exist in
   the same graph without sharing a predicate column).  SQL backends use
   `DELETE FROM ... WHERE subject LIKE 'prefix%' OR (predicate =
   '<inCohort<RuleId>>' AND object LIKE 'prefix%')` on every shipped engine
   (Spark SQL on Delta, Postgres SQL on Lakebase). A future Cypher /
   Gremlin engine can override `delete_cohort_triples` to provide its own
   native pass.
2. Wipes the rule's old Delta-table partition via
   `DELETE FROM <fq> WHERE rule_id = ?`.
3. Re-inserts fresh rows — content-hash URIs are stable, so unchanged
   cohorts keep their identity even though they were deleted/re-inserted.

Multiple concurrent runs of *different* rules are safe (they touch
disjoint URI prefixes / partitions).

---

### 7. Worked examples

#### 7.1 Consulting — Exempt staffing pool

> "Find people who can be staffed together: same project AND all
> Exempt." — Acme Consulting

* **Class**: `:Person`.
* **Linked when**: share a `:Project` via `:assignedTo`.
* **Compatibility**: `:status` *equals* `Exempt`.
* **Group type**: Connected; min size 2.

A graph with Alice/Bob (P1, Exempt), Carol (P1, Non-Exempt),
Dave/Eve (P2, Exempt), Frank (P3, Exempt), and Bob bridging P1 and P3
yields **two cohorts**: `{Alice, Bob, Frank}` (3 members, connected via
P1↔P3 through Bob) and `{Dave, Eve}` (2 members).  Carol is dropped at
Stage 3a because her `:status` is `Non-Exempt`.

#### 7.2 Healthcare — Co-treated patient cohort

> "Patients seen by the same doctor in the same period, with the same
> primary diagnosis."

* **Class**: `:Patient`.
* **Linked when**: share a `:Doctor` via `:treatedBy` AND share a
  `:Visit` via `:hasVisit` (combine = ALL).
* **Compatibility**: `:primaryDiagnosis` *same value*; `:visitDate`
  *between* (clinic study window).
* **Group type**: Strict; min size 3.

Output to a `cohorts.<study_name>` UC table for downstream BI / cohort
matching analysis.

#### 7.3 Manufacturing — Co-located machine cluster

> "Machines on the same shop floor with the same firmware band."

* **Class**: `:Machine`.
* **Linked when**: share a `:ShopFloor` via `:locatedIn`.
* **Compatibility**: `:firmwareVersion` *in any of* `["v3.4", "v3.5"]`.
* **Group type**: Connected; min size 5.

Re-runs nightly into both the graph (so MES dashboards can pivot via
`:inCohort`) and a UC Delta table partitioned by `rule_id`.

#### 7.4 Education — Course-cohort recommender

> "Students who can take the same elective track."

* **Class**: `:Student`.
* **Linked when**: share a `:Course` via `:enrolledIn`.
* **Compatibility**: `:program` *same value*; `:yearOfStudy` *value
  range* min 2 max 4.
* **Group type**: Connected; min size 4.

Materialise into the graph only — the recommender simply queries
`:inCohort` to suggest electives.

#### 7.5 Compliance — Same-policy people (multi-hop path + hop where)

> "People who work on Individual-compliance projects."

* **Class**: `:Person`.
* **Linked when**: 2-hop path
  `Person —assignedTo→ Project —governedBy→ ComplianceType`
  (terminal = `ComplianceType`), with a **per-hop where** on the
  terminal: `complianceTypeId equal to "Individual"`.
* **Compatibility**: none (the hop where already segments — putting
  `complianceTypeId = "Individual"` in rule-level compatibility would
  silently filter every Person, since `complianceTypeId` lives on
  `ComplianceType`, not `Person`).
* **Group type**: Connected; min size 2.

Two persons land in the same cohort whenever any pair of their projects
is governed by the same Individual-typed `ComplianceType`. Drop the
`where` to get the broader "same compliance type, whatever it is"
variant. Useful for building training-cohort lists or quarterly review
groups without having to materialise a `:hasCompliance` predicate on
`:Person`.

---

### 8. API summary

Endpoints (all under `/dtwin/cohorts/*`, session-scoped):

| Method + path | Purpose |
|---|---|
| `GET  /rules` | List saved rules. |
| `POST /rules` | Validate and upsert a rule (BUILDER role). |
| `DELETE /rules/{rule_id}` | Delete a rule (BUILDER role). |
| `POST /dry-run` | Run engine without writing. |
| `POST /materialize` | Re-run + write outputs (BUILDER role). |
| `GET  /preview/class-stats?class_uri=…` | Live class-instance count. |
| `POST /preview/edge-count` | Live link-edge count. |
| `POST /preview/node-count` | Live matching-member count. |
| `POST /preview/path-trace` | Per-hop frontier diagnostic (powers the *Trace path* button on the Preview tab). |
| `POST /sample-values` | Distinct property values for a `value_equals` picker. |
| `POST /explain` | Why? / Why not? for one member URI. |
| `GET  /uc/suggest-target` | Auto-pick UC catalog/schema/table_name. |
| `POST /uc/probe-write` | Read-only 3-step permission probe. |

The JSON contract is documented inline in
`api/routers/internal/dtwin.py` and tested in
`tests/test_dtwin_cohort.py`.

---

### 9. Stage 2 — natural-language rule generation

A dedicated agent (`agents/agent_cohort/`) translates prompts like
*"find consultants who can be staffed together — exempts only with
exempts"* into a validated `CohortRule` JSON via OpenAI-compatible
tool-calling against the active session's ontology + graph.

#### 9.1 Tools (read-only except `propose_rule`, which only validates)

| Tool | Wraps |
|---|---|
| `list_classes()` | `GET /ontology/get-loaded-ontology` (compact: uri, label, n data props) |
| `list_properties_of(class_uri)` | same endpoint, sliced to one class (data + object properties) |
| `count_class_members(class_uri)` | `GET /dtwin/cohorts/preview/class-stats` |
| `sample_values_of(class_uri, property_uri, limit)` | `POST /dtwin/cohorts/sample-values` |
| `propose_rule(rule)` | client-side `CohortRule.validate()`; on success, parks the canonical dict on the engine context |
| `dry_run(rule)` | `POST /dtwin/cohorts/dry-run` (cluster body trimmed to top 5 sizes) |

Stage 2 reuses every Stage 1 endpoint as the agent's toolbox — there is
no parallel pipeline. The agent **never writes**; the user reviews and
edits the proposed rule in the same form before clicking *Save* or
*Materialise*.

#### 9.2 Workflow

The system prompt constrains the agent to:

1. `list_classes()` to anchor on a real class URI.
2. `count_class_members(class_uri)` to confirm the class has data.
3. `list_properties_of(class_uri)` to discover datatype properties (for
   compatibility) and object properties (for `links[].via`).
4. `sample_values_of(...)` for each `value_equals` / `value_in` literal
   so the constants match the data's casing/spelling exactly.
5. `propose_rule(rule)` to validate and register the candidate. On
   `valid=false`, the agent reads the errors and re-proposes.
6. (Optional) `dry_run(rule)` exactly once to surface cluster stats.
7. Reply with a short markdown explanation. The form is the interface;
   the JSON is hydrated into it automatically.

If the prompt is too vague to pick a class, the agent asks one short
clarifying question instead of guessing.

#### 9.3 API

```
POST /dtwin/cohorts/agent
{
  "prompt":  "find consultants who can be staffed together",
  "history": []
}

→ {
    "success": true,
    "rule":    { ... validated CohortRule ... } | null,
    "reply":   "...short markdown explanation...",
    "tools":   [{"name": "list_classes", "duration_ms": 12}, ...],
    "iterations": 4,
    "usage":   {"prompt_tokens": ..., "completion_tokens": ...}
}
```

When `rule` is `null` the agent could not assemble a valid rule and the
`reply` carries the (likely clarifying) follow-up question.

#### 9.4 UX

The Cohorts page exposes a single-line prompt input above the form
(*"Describe the cohort you want"*) plus a *Generate rule* button. On
success:

* the form is hydrated with the proposed rule (class, links,
  compatibility, group type, min size),
* the rule lands as a **draft** (no `activeRuleId`) — saving is an
  explicit user click,
* a collapsible *Agent trace* shows the tool-call order, durations,
  iterations, and token usage so users can audit what the agent did.

#### 9.5 Safety

* No write-side tools — the agent cannot save, materialise, or modify
  the ontology/graph.
* All validation runs server-side via `CohortRule.from_dict()` +
  `CohortRule.validate()`. Invalid output never reaches the form.
* The agent reuses the same session cookies + Databricks-Apps headers
  as the request, so loopback tool calls go through
  `PermissionMiddleware` as the same user (no privilege escalation).
* Iteration cap (10) protects against infinite loops; tools that fail
  return a JSON `{error}` payload so the LLM can self-correct.

See `releasereq/cohort_design.md` §12 for the full design and
`agents/agent_cohort/` for the implementation.
