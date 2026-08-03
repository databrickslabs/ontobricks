Changelog
=========

v0.6.0
------

Data Mapping — Attribute Include / Exclude
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Per-attribute include / exclude**: A checkbox on every attribute row in the
  Designer bottom-panel Status tab lets users exclude individual attributes from
  the mapping. Excluded attributes are shown with strikethrough, are omitted from
  gap-reporting, are not offered to the auto-mapping agent, and are not emitted
  as ``rr:predicateObjectMap`` triples in the R2RML export.
- **Auto-Exclude unmapped**: New button in the Status tab that bulk-excludes all
  attributes that currently have no column assignment.
- **Exclusions survive Unmap**: Right-clicking an entity and choosing *Unmap*
  now preserves ``excluded_attributes`` as a lightweight stub instead of
  removing the mapping entry entirely.
- **Auto-Map respects exclusions**: ``tool_submit_entity_mapping`` and
  ``tool_submit_relationship_mapping`` preserve existing ``excluded_attributes``
  across Auto-Map runs and filter out any agent-submitted mappings for excluded
  attributes. ``tool_get_ontology`` hides excluded attributes from the agent
  entirely.
- **Canvas node colour fix**: Entity nodes are coloured green when every
  *included* attribute is assigned — excluded attributes are not counted
  against completeness.
- **Auto-Map KPI fix**: The Attribute tile and gauge on the Auto-Map page count
  only included attributes. Excluded items are shown as ``· N excl.`` next to
  the tile values.
- **Always-enabled Save button**: The Save button in the Designer bottom panel
  is always enabled; it persists attribute exclusions (as a stub mapping) even
  when no SQL query has been written yet.
- **Mapping tab disabled when SQL is empty**: The *Mapping* tab is disabled
  whenever ``sql_query`` is absent, preventing premature column-assignment
  attempts.

Auto-Map — Metadata Quality Warning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- A collapsible warning is displayed on the Auto-Map page when one or more
  tables or columns lack ``COMMENT`` descriptions. The warning lists the
  problematic items and links directly to *Domain → Metadata*.

Auto-Exclude Canvas Logic
^^^^^^^^^^^^^^^^^^^^^^^^^^

- The **Auto-Exclude** button on the Designer canvas now excludes only
  entities that are orphans or are connected exclusively by inheritance
  (``rdfs:subClassOf``) — entities that participate in at least one
  ``ObjectProperty`` relationship are preserved. Relationships are never
  auto-excluded.

Column Name Quoting
^^^^^^^^^^^^^^^^^^^

- The auto-mapping agent (``agents/agent_auto_assignment/engine.py``) is
  instructed to backtick-quote unsafe SQL column names and alias them to
  safe ``snake_case`` identifiers before reporting them in
  ``attribute_mappings``, ``id_column``, and ``label_column``.
- ``R2RMLGenerator._quote_column()`` (new private helper) double-quotes
  column names in ``rr:column`` values and ``rr:template`` column references
  whenever the name is not a plain SQL identifier (contains spaces, hyphens,
  dots, or other non-alphanumeric characters).

Tests
^^^^^

- New test suite ``tests/units/mapping/test_attribute_exclusion.py`` (33
  tests across 7 classes) covers ``Mapping.build_entity_mapping``,
  ``Mapping.build_relationship_mapping``, ``Mapping.compute_mapping_gaps``,
  ``R2RMLGenerator`` excluded-attribute output, and all three agent tools
  (``tool_submit_entity_mapping``, ``tool_submit_relationship_mapping``,
  ``tool_get_ontology``).
- Extended ``tests/units/mapping/test_r2rml_generator.py`` with
  ``TestQuoteColumn`` (8 tests) and end-to-end column-quoting assertions.

Bug Fixes
^^^^^^^^^

- Fixed re-checking an excluded attribute showing a red cross in the
  *Mapped* column instead of preserving the previous green checkmark.
- Fixed the attributes table not appearing in the Status tab for entities
  whose ``dataProperties`` list is empty but that have existing
  ``attribute_mappings`` (e.g. Invoice, Payment).
- Fixed the auto-mapping agent inventing column mappings for attributes not
  declared in the ontology.
- Fixed right-click context menu items in the Designer canvas having no
  visible hover effect (``mapping-design.css`` divider and hover
  backgrounds).
- Fixed frontend ``_saveEntityAgentResult`` / ``_saveRelAgentResult``
  silently dropping ``excluded_attributes`` when saving an auto-map result.
- Fixed ``_buildAgentEntityItem`` sending all ontology attributes (including
  excluded ones) to the single-entity auto-assign endpoint.

v0.2.0 (Unreleased)
--------------------

- **Entity Groups**: group ontology classes and expand/collapse them in the
  Knowledge Graph Graph Viewer.  Groups are stored as OWL defined classes
  (``owl:equivalentClass`` + ``owl:unionOf``) with ``ontobricks:isGroup``
  annotation for UI differentiation.
- Group CRUD API (``/ontology/groups/*``) and Knowledge Graph consumption
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
- Delta-backed triple store mirrored on a Lakebase Postgres graph engine.
- GraphQL typed API over the graph viewer.
- SHACL data quality validation.
- OWL 2 RL and SWRL reasoning engine.
- LLM agents for ontology assistance.
- MCP server for external tool integration.
