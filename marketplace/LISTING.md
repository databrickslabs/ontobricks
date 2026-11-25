# OntoBricks

Turn your Unity Catalog tables into a governed knowledge graph. OntoBricks lets you design an OWL ontology, map it to your lakehouse tables with R2RML, build the graph, and query and explore it, all from a Databricks App.

## Features
- Visual ontology designer (OWL), with industry starters (FHIR and more)
- Table-to-ontology mapping with AI-assisted auto-mapping
- Knowledge-graph build into Delta, with SPARQL querying
- SHACL data-quality shapes and validation
- Ontology assistant powered by Databricks Foundation Model APIs
- Runs as the app service principal; access governed by Databricks Apps permissions

## Before installing
Create a **Lakebase Autoscaling project** (Compute → Lakebase → Create project) if you don't have one to dedicate to OntoBricks. The install dialog asks you to pick a branch and database from it.

## What you need at install time
| Resource | Used for |
|---|---|
| SQL Warehouse (`CAN_USE`) | Queries and metadata |
| Lakebase database (`Can connect and create`) | The OntoBricks registry (domains, versions, settings) |

## After installing
1. Open the app as the installer and go to **Settings → Registry → Set up the registry**. Pick a Unity Catalog catalog and a schema name (default `ontobricks`). Using your permissions, OntoBricks creates the schema, gives its service principal access to it and **Can Manage** on the app, and initializes the registry. You need `USE CATALOG` and `CREATE SCHEMA` on that catalog, or must own it.
2. Grant colleagues `CAN_USE` on the app (Apps → Permissions). Users with `CAN_MANAGE` are OntoBricks admins.

If setup reports a permission problem, click **Check prerequisites** on the same form. It runs every check as you and as the app, changes nothing, and shows the fix for each problem.

## Learn more
- Source and docs: https://github.com/databrickslabs/ontobricks
- Databricks Labs projects are provided as-is, without formal Databricks support.
