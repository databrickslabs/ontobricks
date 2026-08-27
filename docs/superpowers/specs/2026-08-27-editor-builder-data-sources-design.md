# Editor and Builder Data Sources Design

## Goal

Allow OntoBricks Editors and Builders to browse Unity Catalog locations and
import or manage domain data sources without granting Databricks App
`CAN_MANAGE`.

## Design

Keep the existing route structure and add the read-only catalog and schema
endpoints to the permission middleware's explicit non-admin allowlist:

- `GET /settings/catalogs`
- `GET /settings/schemas`
- `GET /settings/schemas/{catalog}`

The existing domain middleware remains authoritative for data-source
mutations under `/domain/metadata/*`: Editors and Builders can write, while
Viewers are blocked. Warehouse selection and every Settings write remain
admin-only.

## Testing

Add middleware regression cases proving the three browse routes are available
to non-admin users and proving the corresponding Settings writes remain
blocked. Retain existing domain-role tests for Editor writes and Viewer
write denial.

## Documentation

Update deployment permission guidance to state that Editors and Builders can
manage data sources, while administrators retain ownership of shared
configuration.
