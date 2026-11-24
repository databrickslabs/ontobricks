# OntoBricks

Turn your Unity Catalog tables into a governed knowledge graph. OntoBricks lets you design an OWL ontology, map it to your lakehouse tables with R2RML, build the graph, and query and explore it, all from a Databricks App.

## Features
- Visual ontology designer (OWL), with industry starters (FHIR and more)
- Table-to-ontology mapping with AI-assisted auto-mapping
- Knowledge-graph build into Delta, with SPARQL querying
- SHACL data-quality shapes and validation
- Ontology assistant powered by Databricks Foundation Model APIs
- Runs as the app service principal; access governed by Databricks Apps permissions

## What you need at install time
| Resource | Used for |
|---|---|
| SQL Warehouse (`CAN_USE`) | Queries and metadata |

## After installing
1. **Bind a Lakebase database.** Compute → Apps → *your app* → Edit → Resources → add a **Database** resource named `postgres` (a Lakebase Autoscaling branch and database, permission *Can connect and create*), then redeploy. OntoBricks keeps its registry there.
2. **Give the app's service principal Can Manage on the app itself.** Apps → *your app* → Permissions → add the app's service principal (shown on the app's Overview) → Can Manage. OntoBricks reads its own permission list to decide who is an admin; until this is done every user, including you, sees an access-denied page.
3. Open the app and go to **Settings → Registry → Initialize**.
4. Grant colleagues `CAN_USE` on the app (Apps → Permissions). Users with `CAN_MANAGE` are OntoBricks admins.

## Learn more
- Source and docs: https://github.com/databrickslabs/ontobricks
- Databricks Labs projects are provided as-is, without formal Databricks support.
