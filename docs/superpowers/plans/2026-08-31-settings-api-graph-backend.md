# Settings API Graph Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Settings → API and Swagger accurately represent published
domains, configured graph backends, graph availability, and ontology-only
behavior.

**Architecture:** Keep the curated Jinja endpoint catalog and its existing
Try-it JavaScript. Source its selector from the public domain API, attach
capability metadata to options and graph-dependent controls, and centralize
status/gating in `query-api.js`. Add a typed response model for the legacy
stateless domain-info endpoint so Swagger exposes its real payload.

**Tech Stack:** FastAPI, Pydantic v2, Jinja2, Bootstrap 5.3, vanilla
JavaScript, pytest static/OpenAPI contracts, Playwright browser verification.

## Global Constraints

- Keep Settings → API as a curated reference; do not generate cards from
  OpenAPI.
- Preserve `/api/docs`, `/api/redoc`, and `/api/openapi.json`.
- Preserve every endpoint path and the `domain_path` / `project_path`
  compatibility alias.
- Use only PUBLISHED domains and versions in the Settings Try-it selectors.
- Keep ontology endpoints available for ontology-only domains.
- Do not add a new card or page-level visual language.
- Do not commit unless the user explicitly requests it.

---

### Task 1: Type the stateless domain-info Swagger response

**Files:**
- Modify: `src/api/routers/v1.py`
- Modify: `src/api/constants.py`
- Modify: `src/api/external_app.py`
- Modify: `tests/contract/test_openapi_contract.py`
- Test: `tests/units/api/test_api_service.py`

**Interfaces:**
- Consumes: `service.get_domain_info(domain_data) -> dict`.
- Produces: `DomainFileInfoResponse` and
  `DomainInfoSuccessResponse.data: DomainFileInfoResponse`.

- [ ] **Step 1: Add the failing OpenAPI contract**

Add this test to `TestMCPContractPaths`:

```python
def test_stateless_domain_info_declares_its_backend_payload(self, client):
    spec = client.get("/api/openapi.json").json()
    schemas = spec["components"]["schemas"]
    payload = schemas["DomainFileInfoResponse"]["properties"]
    assert payload["graph_backend"]["enum"] == [
        "none",
        "lakebase",
        "databricks",
        "neo4j",
    ]
    assert payload["statistics"]["$ref"].endswith(
        "/DomainStatisticsResponse"
    )
    operation = spec["paths"]["/api/v1/domain/info"]["post"]
    response_schema = operation["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert response_schema["$ref"].endswith("/DomainInfoSuccessResponse")


def test_external_openapi_preserves_descriptive_metadata(self, client):
    spec = client.get("/api/openapi.json").json()
    tags = {tag["name"]: tag["description"] for tag in spec["tags"]}
    assert "graph_backend" in tags["Domain"]
    assert "materialized graph" in tags["GraphQL"]
    assert spec["info"]["contact"]["name"] == "OntoBricks Support"
    assert spec["info"]["license"]["name"] == "Apache 2.0"
```

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --frozen pytest -q \
  tests/contract/test_openapi_contract.py::TestMCPContractPaths::test_stateless_domain_info_declares_its_backend_payload \
  tests/contract/test_openapi_contract.py::TestMCPContractPaths::test_external_openapi_preserves_descriptive_metadata
```

Expected: failure because the endpoint still declares generic
`SuccessResponse`.

- [ ] **Step 3: Add precise Pydantic response models**

In `src/api/routers/v1.py`, import `Literal`, then add after
`SuccessResponse`:

```python
class DomainStatisticsResponse(BaseModel):
    classes: int = 0
    properties: int = 0
    entities: int = 0
    relationships: int = 0
    has_r2rml: bool = False


class DomainFileInfoResponse(BaseModel):
    name: str
    description: str = ""
    uri: str = ""
    version: str
    status: str = "DRAFT"
    author: str = ""
    graph_backend: Literal["none", "lakebase", "databricks", "neo4j"] = Field(
        default="lakebase",
        description="Normalized backend; 'none' denotes an ontology-only domain.",
    )
    statistics: DomainStatisticsResponse


class DomainInfoSuccessResponse(BaseModel):
    success: bool = True
    data: DomainFileInfoResponse
    message: Optional[str] = None
```

Change the route declaration to:

```python
@router.post("/domain/info", response_model=DomainInfoSuccessResponse)
```

- [ ] **Step 4: Clarify Swagger overview and tags**

In `src/api/constants.py`, add to `EXTERNAL_API_DESCRIPTION`:

```markdown
## Domain backends

Published domain summaries expose `graph_backend` (`none`, `lakebase`,
`databricks`, or `neo4j`) and `has_graph`. `none` is ontology-only;
`has_graph` becomes true after a successful graph build.
```

Change the Domain tag description to mention backend/availability metadata,
and the GraphQL tag description to state that only domains with a materialized
graph are listed.

The custom schema builder currently drops tag, contact, and license metadata.
In `src/api/external_app.py`, pass the existing constants to `get_openapi()`:

```python
openapi_schema = get_openapi(
    title=app.title,
    version=app.version,
    openapi_version=app.openapi_version,
    description=app.description,
    routes=app.routes,
    tags=EXTERNAL_OPENAPI_TAGS,
    contact=EXTERNAL_API_CONTACT,
    license_info=EXTERNAL_API_LICENSE_INFO,
)
```

- [ ] **Step 5: Verify GREEN**

Run:

```bash
uv run --frozen pytest -q \
  tests/contract/test_openapi_contract.py \
  tests/units/api/test_api_service.py \
  tests/units/api/test_external_api.py
```

Expected: all selected tests pass.

### Task 2: Update the curated Settings API reference

**Files:**
- Create: `tests/units/front/test_settings_api_contract.py`
- Modify: `src/front/templates/partials/dtwin/_query_api.html`

**Interfaces:**
- Consumes: `GET /api/v1/domains` fields `name`, `description`,
  `graph_backend`, `has_graph`, and `mcp_policy`.
- Produces: `#apiDomainStatus` plus `data-requires-graph="ready"` markers.

- [ ] **Step 1: Add failing template contracts**

Create `tests/units/front/test_settings_api_contract.py`:

```python
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_api.html"
)
SCRIPT = REPO_ROOT / "src/front/static/query/js/query-api.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_domains_card_documents_backend_and_graph_availability():
    template = _read(TEMPLATE)
    domains_anchor = template.index('data-try-endpoint="/api/v1/domains"')
    domains_card = template[template.rindex("<div class=\"card", 0, domains_anchor):domains_anchor]
    assert '"graph_backend": "lakebase"' in domains_card
    assert '"graph_backend": "none"' in domains_card
    assert '"has_graph": true' in domains_card
    assert '"has_graph": false' in domains_card
    assert '"mcp_policy":' in domains_card


def test_page_has_domain_status_and_graph_capability_markers():
    template = _read(TEMPLATE)
    assert 'id="apiDomainStatus"' in template
    assert template.count('data-requires-graph="ready"') >= 7


def test_graphql_copy_requires_a_materialized_graph():
    template = _read(TEMPLATE)
    assert "materialized graph" in template
    assert "marked <em>Active</em>" not in template
```

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_settings_api_contract.py
```

Expected: all three tests fail against the stale curated reference.

- [ ] **Step 3: Add the selected-domain status surface**

After the selector row in `_query_api.html`, add:

```html
<div id="apiDomainStatus"
     class="small text-muted mt-2"
     role="status"
     aria-live="polite">
    Select an API-exposed domain to inspect its backend availability.
</div>
```

- [ ] **Step 4: Update domain and GraphQL copy**

Update the `/api/v1/domains` card to explain:

- `graph_backend` is `none`, `lakebase`, `databricks`, or `neo4j`;
- `has_graph` means a successful graph build exists;
- `mcp_policy` is the per-domain publication policy.

Use this sample:

```json
{
  "success": true,
  "domains": [
    {
      "name": "customer_360",
      "description": "Customer 360 ontology",
      "graph_backend": "lakebase",
      "has_graph": true,
      "mcp_policy": {}
    },
    {
      "name": "finance_ontology",
      "description": "Contracts and payments",
      "graph_backend": "none",
      "has_graph": false,
      "mcp_policy": {}
    }
  ]
}
```

Replace both GraphQL Active-toggle statements with language requiring a
PUBLISHED, API-exposed domain with a populated ontology and materialized graph.

- [ ] **Step 5: Mark graph-dependent controls**

Add `data-requires-graph="ready"` to these interactive controls:

```html
data-try-endpoint="/api/v1/digitaltwin/stats"
data-action="find-run"
data-action="triples-run"
data-try-endpoint="/api/v1/digitaltwin/inference/results"
data-action="graphiql-open"
data-action="graphql-run"
data-action="graphql-schema"
```

Do not mark domain discovery, versions, design status, OWL, R2RML, Spark SQL,
registry, status, cohort-rule listing, or the global GraphQL domain listing.

- [ ] **Step 6: Verify GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_settings_api_contract.py
```

Expected: 3 passed.

### Task 3: Make the selector capability-aware

**Files:**
- Modify: `tests/units/front/test_settings_api_contract.py`
- Modify: `src/front/static/query/js/query-api.js`

**Interfaces:**
- Consumes: selected option datasets `graphBackend` and `hasGraph`.
- Produces: `syncApiDomainCapabilities()`, `clearApiResponses()`, and
  published-only version options.

- [ ] **Step 1: Add failing JavaScript contracts**

Append:

```python
def test_domain_selector_uses_the_external_contract():
    script = _read(SCRIPT)
    assert "fetch('/api/v1/domains'" in script
    assert "opt.dataset.graphBackend = p.graph_backend" in script
    assert "opt.dataset.hasGraph = String(Boolean(p.has_graph))" in script
    assert "fetch('/settings/registry/domains'" not in script


def test_versions_are_limited_to_published_versions():
    script = _read(SCRIPT)
    assert "data.versions.filter(v => v.is_published)" in script
    assert "latest PUBLISHED" in script


def test_domain_capabilities_control_graph_actions():
    script = _read(SCRIPT)
    assert "function syncApiDomainCapabilities()" in script
    assert "option.dataset.graphBackend === 'none'" in script
    assert "option.dataset.hasGraph === 'true'" in script
    assert "document.querySelectorAll('[data-requires-graph]')" in script
    assert "control.disabled = hasSelection && !hasGraph" in script
    assert "clearApiResponses();" in script
```

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_settings_api_contract.py
```

Expected: three new failures because the selector still uses the internal
registry endpoint and has no capability synchronization.

- [ ] **Step 3: Load API-exposed domains**

Replace the registry fetch in `loadApiDomains()` with:

```javascript
const resp = await fetch('/api/v1/domains', {
    credentials: 'same-origin'
});
const data = await resp.json();

select.innerHTML = '<option value="">Select a domain...</option>';
const domainRows = data.domains || [];
if (data.success && domainRows.length) {
    for (const p of domainRows) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = `${p.name} · ${formatGraphBackend(p.graph_backend)}`;
        opt.dataset.graphBackend = p.graph_backend || 'lakebase';
        opt.dataset.hasGraph = String(Boolean(p.has_graph));
        if (p.name === currentDomainSlug) opt.selected = true;
        select.appendChild(opt);
    }
}
syncApiDomainCapabilities();
```

The change listener becomes:

```javascript
select.addEventListener('change', async () => {
    clearApiResponses();
    syncApiDomainCapabilities();
    await loadApiVersions(select.value);
});
```

- [ ] **Step 4: Filter versions**

In `loadApiVersions()`:

```javascript
const publishedVersions = data.versions.filter(v => v.is_published);
if (!data.success || !publishedVersions.length) return;

const latestPublished = publishedVersions[0].version;
select.innerHTML =
    '<option value="">latest PUBLISHED (v' +
    escHtml(latestPublished) +
    ')</option>';
for (const v of publishedVersions) {
    const opt = document.createElement('option');
    opt.value = v.version;
    opt.textContent = 'v' + v.version + ' · PUBLISHED';
    select.appendChild(opt);
}
```

- [ ] **Step 5: Add capability/status helpers**

Add before the expand/collapse section:

```javascript
function formatGraphBackend(backend) {
    return {
        none: 'No Backend',
        lakebase: 'Lakebase',
        databricks: 'Lakehouse',
        neo4j: 'Neo4j'
    }[backend] || 'Lakebase';
}

function syncApiDomainCapabilities() {
    const select = document.getElementById('apiDomainName');
    const option = select?.selectedOptions?.[0];
    const status = document.getElementById('apiDomainStatus');
    const hasSelection = Boolean(select?.value && option);
    const graphless = hasSelection && option.dataset.graphBackend === 'none';
    const hasGraph = hasSelection && option.dataset.hasGraph === 'true';

    if (status) {
        if (!hasSelection) {
            status.textContent =
                'Select an API-exposed domain to inspect its backend availability.';
        } else if (graphless) {
            status.textContent = 'No Backend · Ontology only';
        } else if (hasGraph) {
            status.textContent =
                `${formatGraphBackend(option.dataset.graphBackend)} · Graph ready`;
        } else {
            status.textContent =
                `${formatGraphBackend(option.dataset.graphBackend)} · Awaiting first build`;
        }
    }

    document.querySelectorAll('[data-requires-graph]').forEach(control => {
        control.disabled = hasSelection && !hasGraph;
        control.title = control.disabled
            ? (graphless
                ? 'Unavailable for an ontology-only domain'
                : 'Build this domain before using graph operations')
            : '';
    });
}

function clearApiResponses() {
    document.querySelectorAll('.ob-api-response').forEach(response => {
        response.classList.add('d-none');
        response.innerHTML = '';
    });
}
```

Call `syncApiDomainCapabilities()` after initialization and domain loading.

- [ ] **Step 6: Verify GREEN and regression behavior**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_settings_api_contract.py \
  tests/units/front/test_sidebar_content_stretch_contract.py
```

Expected: all selected tests pass.

### Task 4: Browser verification, documentation, and mandatory suite

**Files:**
- Modify: `documentation/api.md`
- Modify: `documentation/user-guide.md`
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-31.log`

**Interfaces:**
- Consumes: completed Swagger and Settings API behavior.
- Produces: user documentation, browser evidence, and final test record.

- [ ] **Step 1: Update documentation**

In `documentation/api.md`, describe the Settings → API selector as
PUBLISHED-only and document the capability labels. In
`documentation/user-guide.md`, state that Settings → API disables graph Try-it
actions for ontology-only or unbuilt domains while keeping ontology actions
available.

- [ ] **Step 2: Browser-test Settings → API**

Open `http://localhost:8000/settings/?section=api` at 1440×900 and 375×667.
Verify:

- Swagger and ReDoc links resolve;
- domain options come from `/api/v1/domains` and include backend labels;
- only PUBLISHED versions appear;
- ontology-only, awaiting-build, and graph-ready labels render correctly when
  those states exist;
- OWL remains enabled for ontology-only domains;
- marked graph controls disable with the correct tooltip when no graph exists;
- endpoint cards expand and global `/domains` Try-it still works;
- no new console, page, or failed-asset errors;
- mobile content remains readable and horizontally scrollable where needed.

- [ ] **Step 3: Run diagnostics and diff hygiene**

Run IDE diagnostics on changed Python, JavaScript, template, and test files,
then:

```bash
git diff --check
```

Expected: no new diagnostics and exit code 0.

- [ ] **Step 4: Run the mandatory suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: complete non-scenario suite passes.

- [ ] **Step 5: Append the changelog**

Extend `Complete graphless external API contracts` in
`changelogs/v0.8.0/benoitcayladbx_2026-08-31.log` with the typed Swagger
response, Settings API template/JavaScript/tests, documentation files, exact
browser checks, and final pytest summary.
