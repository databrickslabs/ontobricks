# Lakehouse and Databricks Icon Split Design

## Context

Lakehouse backend surfaces currently use the official Databricks logo through
the `ob-icon-databricks` modifier. The Settings → Configuration → Databricks
item instead uses the generic Bootstrap `bi-database` icon. The supplied orange
lakehouse artwork must identify Lakehouse everywhere, while the official
Databricks logo must be reserved for the Databricks configuration item.

## Goal

Give Lakehouse and Databricks distinct, semantically named local icon mappings
without changing the shared icon sizing, alignment, or menu rendering model.

## Design

### Assets and CSS

Copy the supplied PNG into `src/front/static/global/img/` as
`lakehouse-icon.png`. Keep `databricks-icon.svg` unchanged as the official
Databricks asset.

Add an `ob-icon-lakehouse` modifier to `components.css` that points to the new
PNG. Keep `ob-icon-databricks` pointing to `databricks-icon.svg`. Both modifiers
continue to use the shared `ob-brand-icon` component, preserving its `1em × 1em`
box, baseline alignment, and contained background rendering.

### Lakehouse mapping

Replace `ob-icon-databricks` with `ob-icon-lakehouse` anywhere the icon denotes
the Lakehouse backend:

- Settings Back end menu and Lakehouse section header.
- Query Sync backend selection and dynamic backend icon mapping.
- Domain Validation backend selection and dynamic backend icon mapping.
- Registry backend badges and any other Lakehouse backend card or label.

Backend aliases such as `databricks`, `delta`, and `lakehouse` still describe
the same Lakehouse storage backend in existing data flows; their displayed icon
maps to `ob-icon-lakehouse`.

### Databricks settings mapping

Change Settings → Configuration → Databricks from the generic `bi-database`
icon to `ob-brand-icon ob-icon-databricks`. No other item uses the official
Databricks logo after this change.

## Testing

Update `tests/units/front/test_backend_brand_icons.py` first so it fails against
the current mapping. The contract will verify:

- Lakehouse backend menu and headers use `ob-icon-lakehouse`.
- The supplied `lakehouse-icon.png` is a local asset.
- Dynamic Query Sync and Domain Validation mappings select
  `ob-icon-lakehouse` for Lakehouse aliases.
- Settings → Configuration → Databricks uses
  `ob-brand-icon ob-icon-databricks`.
- `ob-icon-databricks` still resolves to the official local Databricks SVG.
- Shared icon sizing remains unchanged.

After the focused test passes, run:

`uv run --frozen pytest -q -m "not scenario"`

## Success Criteria

- Every Lakehouse icon in the application uses the supplied orange artwork.
- Only Settings → Configuration → Databricks uses the official Databricks logo.
- Lakebase and Neo4j icon mappings are unchanged.
- Icons remain locally served, correctly aligned, and sized like surrounding
  Bootstrap icons.
- The required non-scenario test suite passes.
