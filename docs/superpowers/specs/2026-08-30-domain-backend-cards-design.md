# Domain Backend Cards Design

## Goal

Bring Domain → Information in line with the shared tab-content treatment and
replace the Knowledge Graph backend dropdown with clear, directly selectable
backend cards.

## Interface

The page uses the standard `ob-tabs` rail followed by an independent
`ob-tab-content` surface. The Knowledge Graph tab presents four responsive
radio cards:

- Lakebase, using the existing Lakebase/PostgreSQL brand asset.
- Lakehouse, using the existing Lakehouse brand asset.
- Neo4j, using the existing Neo4j brand asset.
- No Backend, using a neutral Bootstrap icon because no product logo applies.

Each card contains a logo, title, concise description, native radio semantics,
a visible keyboard focus state, and an indigo selected state with a checkmark.
Cards collapse from four columns to two and then one at narrower widths.

## Behaviour

The selected card continues to produce the existing `graph_backend` values:
`lakebase`, `databricks`, `neo4j`, or `none`. Existing persistence, validation,
late-hydration race protection, MCP constraints, and read-only lifecycle
behaviour remain unchanged.

Backend-specific controls retain their current rules:

- Lakehouse shows Materialization.
- Neo4j shows the connection selector.
- No Backend hides Materialization, the Neo4j selector, the backend migration
  warning, Dual Knowledge Graph, and Triple-Store Gateway.
- No Backend keeps the ontology-only informational notice visible.

## Testing

Static UI contracts verify the card values, brand assets, accessible radio
markup, standard tab surface, and graphless-only visibility targets. Existing
domain payload, Neo4j picker, and No Backend tests remain green. Browser
verification covers all four choices, responsive wrapping, keyboard selection,
read-only state, and console errors.
