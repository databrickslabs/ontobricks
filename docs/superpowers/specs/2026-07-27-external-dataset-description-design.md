# External Dataset Description

## Goal

Let ontology authors describe the purpose of a class-linked external Unity
Catalog dataset, defaulting the text from the selected table/view comment, and
surface that description to MCP clients, agents, and Graph Explorer users.

## Data Model

Extend the existing optional `ontology.classes[].dataset` object with:

```json
{
  "catalog": "main",
  "schema": "crm",
  "asset": "customers",
  "type": "TABLE",
  "fullName": "main.crm.customers",
  "key_column": "customer_id",
  "description": "Customer master records used for account enrichment."
}
```

`description` is an optional string. Existing domains without it remain valid.
The text is an ontology-authored purpose statement after initial selection; it
is not continuously synchronized with Unity Catalog.

## Ontology UI

The editable Dataset section adds a multiline **Description** field below Key
column. In view-only mode, a non-empty description is displayed as text below
the dataset metadata.

Selection behavior:

1. On the first dataset selection for an entity, initialize `description` from
   the selected asset's `comment` returned by `/settings/uc-assets`.
2. If the selected asset has no comment, initialize it to an empty string.
3. When replacing one linked dataset with another, preserve the existing
   description exactly, including an intentionally empty value. The new UC
   comment must not overwrite it.
4. Removing a dataset removes its description with the dataset object. A later
   selection is a new first selection and may default from that asset's UC
   comment.
5. User edits update `sharedPanelDataset.description` and mark the entity panel
   dirty.

No additional metadata endpoint is needed: the existing UC asset selector
already receives each asset's `comment`.

## REST and MCP

`GET /api/v1/digitaltwin/nodes/context` adds optional
`dataset.description` to `NodeContextDataset` and copies the persisted string
from the matched class dataset. Empty strings are omitted by the existing
`response_model_exclude_none` behavior only if normalized to `None`.

LLM-facing MCP paths include the description:

- `describe_entity`: `_format_class_context_block` adds an indented
  `Description: ...` line in the class Actions context.
- `get_entity_context`: `_format_node_context_response` adds the same
  description below the dataset name.
- `list_entity_types`: when a type has a linked dataset, appends
  `Dataset:` and `Description:` under that type.

The domain classes endpoint already returns the dataset object unchanged, so
its schema requires no new transport logic.

This is deterministic metadata formatting only. It does not change a prompt,
invoke a Foundation Model API, wrap an agent, or add an MLflow-traced LLM code
path; therefore the AI Feature Lifecycle eval gate is not triggered.

## Graph Explorer

The ontology class loader and entity mapping loader retain each class's
`dataset` object. The entity details panel adds a **Dataset** section when a
dataset exists, showing:

- table/view full name;
- configured key column when present;
- persisted description when present.

All values are HTML-escaped. The section is metadata-only and does not query
dataset rows.

## Error and Compatibility Behavior

- Missing `description` is treated as no description everywhere.
- UC comments are defaults only; opening an existing entity never fetches or
  overwrites the saved description.
- A saved description is still shown if the current UC asset cannot be queried.
- MCP and Graph Explorer continue showing dataset name/key when description is
  absent.
- No backend migration is required because class datasets are JSON objects.

## Testing

Add focused coverage for:

- first selection defaults from `asset.comment`;
- dataset replacement preserves both non-empty and intentionally empty text;
- description changes persist and mark the panel dirty;
- view-only ontology rendering includes the saved description;
- node-context REST response includes the description;
- both MCP formatters include `Purpose:`;
- Graph Explorer loaders retain `dataset` and details render its name, key, and
  escaped description;
- existing dataset objects without `description` remain supported.

Run focused tests first, then:

```bash
uv run pytest -q -m "not scenario"
```
