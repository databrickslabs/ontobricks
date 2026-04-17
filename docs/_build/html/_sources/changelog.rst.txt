Changelog
=========

v0.2.0 (Unreleased)
--------------------

- **Entity Groups**: group ontology classes and expand/collapse them in the
  Digital Twin Knowledge Graph.  Groups are stored as OWL defined classes
  (``owl:equivalentClass`` + ``owl:unionOf``) with ``ontobricks:isGroup``
  annotation for UI differentiation.
- Group CRUD API (``/ontology/groups/*``) and Digital Twin consumption
  endpoint (``GET /dtwin/groups``).
- Ontology UI: dedicated *Groups* section for creating, editing, and
  deleting groups with class member selection.
- Sigma.js graph: super-node rendering for collapsed groups, edge
  aggregation, and toolbar with collapse/expand controls.

v0.1.0
------

- Initial release of OntoBricks.
- OWL ontology design and import (FIBO, CDISC, IOF).
- R2RML mapping from Databricks tables to RDF.
- Delta and LadybugDB triple store backends.
- GraphQL typed API over the knowledge graph.
- SHACL data quality validation.
- OWL 2 RL and SWRL reasoning engine.
- LLM agents for ontology assistance.
- MCP server for external tool integration.
