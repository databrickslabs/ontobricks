# Build and Query Warehouse Split Design

## Context

OntoBricks currently routes Knowledge Graph builds through the Lakehouse query
warehouse. This fails when that warehouse is Lakehouse//RT because RT accepts
read queries only: it does not support `CREATE VIEW`, CTAS, or other DDL and
write operations.

## Decision

OntoBricks will maintain two explicit SQL warehouse roles:

- **Build SQL Warehouse**: a classic or serverless SQL warehouse used for
  mapping views, materialization, and all other build DDL/write operations.
- **Lakehouse Query Warehouse**: the existing Lakehouse warehouse used for
  graph reads, health probes, and low-latency serving. It may be
  Lakehouse//RT.

The existing global SQL warehouse setting becomes the dedicated build
warehouse. A separate build transport setting is persisted alongside it.
Lakehouse query settings remain under `graph_engine_config.lakehouse`.

## Configuration and migration

The Settings → Lakehouse → SQL Warehouse panel displays both roles. The Build
SQL Warehouse dropdown is editable by administrators and excludes
Lakehouse//RT warehouses. Applying persists it as the global build setting. By
default, the Query SQL Warehouse dropdown mirrors Build and is disabled.

Checking **Lakehouse//RT for queries** enables the Query dropdown. It uses the
same workspace warehouse list as Build, excluding the selected Build warehouse,
and requires the administrator to choose a distinct query warehouse. Clearing
the checkbox and applying clears the persisted query override, restores Build
as the effective query warehouse, and disables Query again.

Opening the SQL Warehouse tab immediately uses the existing **Loading
Lakehouse configuration…** state until the Build and Query selectors are
populated.

The build warehouse is mandatory and has no fallback to the Lakehouse query
warehouse. Existing registry configurations that already contain the global
`warehouse_id` retain that value as their build warehouse. Configurations
without one must select a build warehouse before a build can start.

The build selector must reject Lakehouse//RT (`REYDEN`) warehouses. The build
transport may use the Statement Execution API when enabled; this is independent
from the Lakehouse query transport setting.

## Runtime flow

Both Knowledge Graph build pipelines resolve build credentials and transport
from the dedicated build settings. If the warehouse is missing, the build
fails before submitting SQL with an actionable Settings message.

Lakehouse query clients continue resolving
`graph_engine_config.lakehouse.warehouse_id` and
`graph_engine_config.lakehouse.use_sea`. No Lakebase or Neo4j query routing
changes.

## Error handling

- Missing build warehouse: reject the build and direct the administrator to
  Settings → Databricks → Build SQL Warehouse.
- RT selected as build warehouse: reject the setting before persistence.
- SQL execution errors retain their underlying Databricks detail.

## Verification

Regression coverage must prove:

1. Build warehouse and transport settings persist and resolve independently.
2. Missing build configuration blocks both build entry paths.
3. Both pipelines initialize their SQL clients from build settings.
4. Lakehouse query clients continue using the RT warehouse and query
   transport.
5. Settings displays Build and Query dropdowns together, mirrors and disables
   Query by default, enables a distinct Query selection only for RT, and clears
   the override when RT is turned off.

The complete non-scenario test suite must pass.
