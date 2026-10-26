# Parsed Document Corpus Design

> Asana: [Parse documents once after upload (shared corpus for Generate and Mapping)](https://app.asana.com/1/8808412813448/project/1217637563677287/task/1217968617394680)
> (OntoBricks-Product → Version 0.9.0).

## Goal

Parse every uploaded domain document once, persist a durable text
representation beside the source document in its Unity Catalog volume, and
make that representation the only document corpus consumed by Generate and
Mapping.

This removes repeated `ai_parse_document` warehouse calls across agent runs,
retries, and consumers while preserving the original uploaded file.

## Scope

This design covers:

- parsing immediately after a successful document upload;
- durable parsed text and parse-state metadata;
- content-hash no-op and invalidation behavior;
- pending, ready, and failed states with manual retry;
- the shared reader contract used by Generate and Mapping;
- status and retry controls in the document-management UI;
- document deletion and domain-version copy behavior.

It does not cover:

- Genie Pages or Databricks Domains as additional sources;
- semantic retrieval, embeddings, or a Lakebase search index;
- chunk sidecars or entity-scoped chunk retrieval;
- changes to ontology-generation stages beyond adopting the corpus;
- parsing initiated by an agent.

## Current State

The upload endpoint in `src/api/routers/internal/domain.py` writes each source
file to:

```text
/Volumes/{catalog}/{schema}/{volume}/domains/{domain}/V{version}/documents/{filename}
```

Binary files are parsed on demand by `src/agents/tools/documents.py`.
`tool_read_document` creates a `DocumentExtractor`, invokes
`ai_parse_document`, and caches the result only on the current `ToolContext`.
A new Generate run, Mapping run, or retry therefore pays the warehouse cost
again.

Mapping follows a separate path in
`Mapping.fetch_documents_for_agent`. It reads document files as text, truncates
them to the prompt limit, and silently excludes binary files it cannot decode.
Consequently, Generate and Mapping do not currently share a durable corpus.

## Considered Approaches

### Volume sidecars

Store parsed markdown and a JSON manifest under the document directory.

Advantages:

- matches the existing `VolumeFileService` persistence model;
- keeps each source and its derived artifact in the same domain version;
- requires no database schema or data migration;
- can be copied and deleted with the source document.

Limitations:

- directory listing is less efficient than indexed queries at large scale;
- semantic retrieval would require a later index.

### Lakebase table

Store source identity, parse state, and extracted text in a
`document_parse` table.

Advantages:

- efficient status queries and filtering;
- natural foundation for indexed retrieval.

Limitations:

- introduces schema management and dual persistence;
- creates additional lifecycle and permission concerns;
- is unnecessary for the current read-by-filename access pattern.

### Hybrid sidecar and Lakebase index

Keep markdown in the volume and index metadata or chunks in Lakebase.

This is the likely future direction for semantic or entity-scoped retrieval,
but it adds complexity before a retrieval requirement exists.

## Decision

Use volume sidecars. Keep the original at its current path and store derived
artifacts under:

```text
documents/
├── Specification.pdf
└── _parsed/
    ├── Specification.pdf.md
    └── Specification.pdf.json
```

The filename, including its original extension, is retained before the sidecar
extension. `_parsed` remains an implementation directory and is not returned
as a user document.

Chunk sidecars are explicitly deferred. Existing character limits remain
prompt-safety limits applied after reading the durable parsed text; they are no
longer parsing or caching mechanisms.

## Components

### `DocumentExtractor`

`src/back/core/databricks/document/DocumentExtractor.py` remains a generic adapter for
`ai_parse_document`. It accepts a volume source path and returns extracted
text. It does not decide whether parsing is necessary and does not persist
state.

### `DocumentParseService`

A new focused service under `src/back/core/databricks/` owns the parsed corpus
lifecycle. It:

- computes SHA-256 from uploaded source bytes;
- resolves manifest and markdown paths safely;
- reads and validates manifests;
- records `pending`, `ready`, and `failed`;
- performs content-hash no-op checks;
- invokes `DocumentExtractor` for supported binary formats;
- exposes parsed content to application and agent readers;
- removes and copies sidecars with their source.

The upload route coordinates HTTP concerns only. Generate and Mapping depend
on this service's reader contract and never instantiate `DocumentExtractor`.

### Background execution

After the source write succeeds, the upload path writes a `pending` manifest
before starting binary extraction. Extraction runs outside the upload request
using the existing task infrastructure so a warehouse call cannot exhaust the
request timeout.

`TaskManager` may expose transient progress, but the manifest is the status
source of truth because task state is process-local and can disappear after a
restart.

If the process stops while parsing, the manifest remains `pending`. A manual
retry can restart it. Listing may identify stale pending manifests for display,
but it must not silently start parsing.

## Manifest Contract

Each source has a JSON manifest with this shape:

```json
{
  "schema_version": 1,
  "filename": "Specification.pdf",
  "source_hash": "<lowercase sha256 hex>",
  "parser": "ai_parse_document",
  "output_schema": "2.0",
  "status": "ready",
  "parsed_at": "2026-09-18T15:00:00+00:00",
  "error": null,
  "sidecar_path": "_parsed/Specification.pdf.md"
}
```

Contract rules:

- `schema_version` is the manifest schema and starts at integer `1`.
- `source_hash` is SHA-256 of the uploaded bytes.
- `parser` is `ai_parse_document` for supported binary files and `plaintext`
  for directly readable text.
- `output_schema` is `DocumentExtractor.OUTPUT_SCHEMA_VERSION` for parsed
  binaries and `null` for plaintext.
- `status` is exactly `pending`, `ready`, or `failed`.
- `parsed_at` is UTC ISO 8601 and is `null` until the attempt completes.
- `error` is `null` except for failed attempts and contains a safe,
  user-displayable message without credentials or query text.
- `sidecar_path` is relative to `documents/`; it is `null` for plaintext.

Manifests are written atomically where supported. A ready manifest is written
only after its markdown sidecar has been written successfully.

## Plaintext Documents

Known UTF-8 text formats are not copied. After upload they receive a ready
manifest with `parser=plaintext`, `output_schema=null`, and
`sidecar_path=null`. Readers load the unchanged original.

If decoding fails, the parse attempt is recorded as failed. Unknown extensions
are not opportunistically sent to `ai_parse_document`; supported parser input
types remain explicit.

## Upload and Invalidation Flow

For each uploaded file:

1. Sanitize the filename using the existing upload rules.
2. Read its bytes and compute SHA-256.
3. Read the existing manifest, if any.
4. If the hash matches and status is `ready`, return a no-op result without
   rewriting the source or invoking the warehouse.
5. Otherwise write or overwrite the original source.
6. For plaintext, validate UTF-8 and write a ready or failed manifest.
7. For a supported binary, write a pending manifest and schedule one parse.
8. On parse success, write markdown and then a ready manifest.
9. On parse failure, remove any incomplete markdown and write a failed
   manifest. The uploaded source remains available.

A changed hash always invalidates the old artifact. A matching failed or
pending manifest is not treated as ready and does not qualify for the ready
no-op.

Concurrent uploads of identical bytes must not schedule duplicate extraction
within one application process. The service uses a lock keyed by the complete
domain-version source path and rechecks the manifest after acquiring it.

Cross-process exactly-once execution is not guaranteed by the current
in-memory task infrastructure. Durable sidecar idempotency prevents duplicate
results, but multiple app replicas could still make duplicate warehouse calls.
If multi-replica deployment becomes a requirement, a durable lease or job
queue is required before claiming globally exactly-once execution.

## Failure and Retry Behavior

Parsing failure never rolls back or deletes the successful source upload.

The failed manifest records the source hash, completion time, and safe error.
The UI offers Retry parse for failed binary documents. Retry:

- keeps the original unchanged;
- changes status to pending;
- invokes the same parse service;
- writes a new ready or failed result;
- does not run from Generate or Mapping.

Generate and Mapping treat both pending and failed documents as unavailable.
They surface structured status rather than falling back to an extractor.

## Shared Reader Contract

Application and agent readers return the source filename, content, content
length, truncation information, parser, and parse status.

For a ready binary document, readers load `_parsed/{filename}.md`.
For a ready plaintext document, readers load the source. For pending or failed
documents, they return a structured unavailable result:

```json
{
  "filename": "Specification.pdf",
  "parse_status": "pending",
  "error": "Document parsing is not ready"
}
```

A failed result may include the safe manifest error. It must not trigger a
parse.

`list_documents` adds parse status to each source entry. It does not expose
files inside `_parsed`.

`tool_read_document` and Mapping's preload path use the same service contract.
All direct `DocumentExtractor` calls and unknown-binary extraction fallback are
removed from agent tools. Existing per-document and aggregate prompt limits
remain in place after the text is loaded.

## Generate and Mapping Behavior

Generate lists source documents and reads only ready corpus entries. A pending
or failed entry produces a clear tool result that the agent can report rather
than a first-time parse.

Mapping preloads ready corpus entries through the same reader. Pending and
failed entries are represented as unavailable rather than silently parsed or
decoded. Mapping may continue with metadata and ontology context, but the UI
must disclose that document context was unavailable.

Changing tool descriptions or structured payloads is a material agent-tool
contract change. The implementation must satisfy the AI Feature Lifecycle:

- update the relevant `.planning/<agent>/SPEC.md` tool surfaces and failure
  modes;
- add at least ten evaluation examples for each materially changed existing
  agent;
- run before-and-after evaluations;
- link the MLflow evaluation run in the pull request.

## UI and API

`GET /domain/documents/list` augments each source file with `parse_status` and,
for failures, a safe summary.

`POST /domain/documents/upload` returns per-file upload and parse state. A
binary upload can succeed with `parse_status=pending`; successful upload no
longer implies parsing is complete.

`POST /domain/documents/retry-parse` accepts one sanitized filename and is
valid only for an existing supported binary source whose manifest is failed or
stale pending.

The document list displays pending, ready, and failed badges. Failed rows show
a Retry action. The page refreshes while any item is pending and stops polling
when all items are terminal.

Generate and Mapping entry points must block with “Document parsing is not
ready” when their selected workflow requires document context and no selected
document is ready. Where document context is optional, they may continue only
after visibly reporting unavailable documents.

## Delete and Version Lifecycle

Deleting a document removes:

- the source;
- `_parsed/{filename}.md`, if present;
- `_parsed/{filename}.json`, if present.

Missing sidecars do not make source deletion fail.

Creating a new domain version copies both originals and `_parsed`. Because the
source bytes are unchanged, ready manifests remain valid in the new version.
The copy operation must preserve `_parsed` as an internal directory and keep
document counts limited to source files.

## Security and Path Safety

All source and sidecar operations use the existing filename sanitization and
volume service. The parse service derives sidecar paths from the sanitized
basename and never accepts a caller-supplied sidecar path.

Manifest errors never expose bearer tokens, SQL statements, warehouse
responses, or full stack traces. Authorization remains the same as for the
owning domain-version document directory.

## Testing Strategy

Unit tests use an injected fake `DocumentExtractor` and in-memory or mocked
volume service. Required cases:

- upload binary → pending → ready → reader returns markdown;
- upload identical bytes when ready → no source rewrite and zero additional
  extractor calls;
- upload changed bytes → old artifact invalidated and one new extraction;
- extractor failure → source retained and failed manifest written;
- retry failed parse → pending then ready;
- ready plaintext → original content read with no markdown copy;
- pending and failed readers → structured unavailable result with no extractor
  call;
- Generate `read_document` reads a ready sidecar without invoking the
  extractor;
- Mapping preload reads the same ready sidecar without invoking the extractor;
- delete removes sidecars;
- domain-version copy preserves valid sidecars;
- document list hides `_parsed` and exposes status;
- filenames cannot escape the document directory.

Route tests cover upload response shape, retry validation, and list status.
Frontend tests cover badge rendering, pending polling, and retry behavior.

The mandatory repository test command remains:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Scenario suites are out of scope unless explicitly requested.

## Acceptance Criteria

The feature is complete when:

1. A supported binary upload schedules one parse and persists a durable ready
   or failed manifest without changing the source.
2. Re-uploading identical bytes for a ready document does not invoke
   `DocumentExtractor`; changed bytes invalidate and re-parse.
3. Ready parsed markdown is shared by Generate and Mapping.
4. No Generate or Mapping path can initiate first-time parsing.
5. Pending and failed status is visible and failed parsing is retryable.
6. Source deletion and version copying maintain sidecar lifecycle.
7. Unit, route, frontend, and required repository tests pass.
8. Agent-tool contract changes include the required SPEC, dataset, evaluation
   delta, and linked MLflow run.
