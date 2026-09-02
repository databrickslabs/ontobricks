# Settings Lakehouse Health Permissions

## Goal

Make Settings → Lakehouse → Health report only whether the application
principal has the Unity Catalog permissions required to operate in the
configured Registry `catalog.schema`. The result must not depend on the domain
currently open in the session or inspect any domain assets.

Also align the Lakehouse, Lakebase, and Neo4j tab rails with the canonical
card-integrated `ob-tabs` design.

## Health data source

Keep the existing read-only endpoint:

`GET /settings/triple-store/databricks-health`

The endpoint resolves the application-level Registry catalog and schema. It
uses the Unity Catalog effective-permissions REST API for the active
application principal:

`GET /api/2.1/unity-catalog/effective-permissions/SCHEMA/<catalog.schema>`

In Databricks Apps, the principal is `DATABRICKS_CLIENT_ID`. Outside Apps, use
the authenticated Databricks identity returned by the current-user API. The
principal is supplied to the effective-permissions endpoint so assignments for
unrelated users or groups cannot make the check pass.

The permission probe does not require a SQL Warehouse and does not query,
describe, count, list, or derive any tables or views.

## Required permissions

The operational permission set is:

- `USE CATALOG`
- `USE SCHEMA`
- `CREATE TABLE`
- `CREATE VIEW`
- `SELECT`
- `MODIFY`

Names are normalized to uppercase with underscores converted to spaces.
`ALL PRIVILEGES` satisfies every required permission. Effective privileges
inherited from the catalog or metastore are accepted.

The response contains:

```json
{
  "success": true,
  "registry_configured": true,
  "registry_catalog": "main",
  "registry_schema": "ontobricks",
  "storage_location": "main.ontobricks",
  "principal": "application-client-id",
  "accessible": true,
  "operational": true,
  "permissions": [
    {
      "name": "USE CATALOG",
      "granted": true,
      "inherited_from": "main"
    }
  ],
  "error": null
}
```

`accessible` states whether the effective-permissions request succeeded.
`operational` is true only when all six required permissions are granted.
When Registry catalog/schema configuration is missing, return
`registry_configured: false`, an empty permission list, and do not call Unity
Catalog.

Authentication, authorization, API, and network failures use the established
error mapping. A successful API response with missing privileges remains an
HTTP 200 diagnostic result with `operational: false`.

## Health interface

The Health tab description states that it checks effective permissions on the
saved Registry `catalog.schema`.

The result displays:

- Registry `catalog.schema`
- checked principal
- one status row for each required permission
- inherited source when returned
- an overall Operational / Missing permissions state

The interface does not mention or display an active domain, R2RML view, data
table, inferred table, materialization mode, row count, object existence, or
SQL Warehouse.

The existing Refresh status action and branded loading indicator remain.

## Tab styling

Settings → Lakehouse, Lakebase, and Neo4j use the shared card-integrated tab
pattern:

- outer `card h-100`
- inner `card-body p-0 ob-tabs-wrap`
- rail `nav nav-tabs ob-tabs nav-fill`
- body `tab-content p-3`

The tabs retain Bootstrap tab behavior, existing IDs, icons, accessibility
attributes, event handlers, and content. Remove the independent
`ob-tab-content` surface from these three page-level tab groups. No local tab
CSS or alternate visual state is introduced.

## Testing

Backend tests cover:

- required permission normalization and evaluation;
- `ALL PRIVILEGES`;
- inherited privileges;
- principal filtering;
- missing Registry configuration avoiding the API call;
- no domain asset resolution or SQL Warehouse dependency;
- diagnostic response for missing privileges;
- infrastructure error propagation.

Frontend tests cover:

- Health copy describes Registry schema permissions only;
- Health renderer consumes only permission-oriented fields;
- removed domain asset labels and response fields;
- one row per required permission and overall state;
- Lakehouse, Lakebase, and Neo4j use the canonical card-integrated tab markup.

Browser validation covers desktop and 390 px layouts, tab behavior, keyboard
focus, permission rendering, refresh, network/console errors, and absence of
domain asset information.

## Out of scope

- Changing the Lakehouse Objects tab.
- Granting or revoking Unity Catalog permissions.
- Changing Lakebase or Neo4j health behavior.
- Changing graph build or query behavior.
