# Knowledge Graph Icon Design

## Context

The Domain menu entry now uses `bi-box`, the icon the Knowledge Graph section
previously owned. Both entries currently resolve to the same glyph in
`menu_config.json`, so the navbar, the level-2 subnav and the Domain dropdown
give Domain and Knowledge Graph an identical visual identity.

Three further places hardcode `bi-box-fill` for Knowledge Graph — the filled
sibling of Domain's `bi-box`. These are the same collision in a second form:
the breadcrumb, the help modal walkthrough step, and the Domain validation
card header.

## Goal

Give Knowledge Graph its own icon, distinct from Domain (`bi-box`), Registry
(`bi-boxes`), Ontology (`bi-bezier2`), the Ontology Designer (`bi-diagram-3`)
and Mapping (`bi-shuffle`). Apply it consistently everywhere the section is
labelled, so the menu and the breadcrumb no longer disagree.

## Design

### Chosen icon

`bi-radar`. It leans on what the section is *for* — Explorer, Analytics, Query
and Chat are all exploration surfaces over the materialized graph — rather than
on structure, which is already claimed by `bi-bezier2` (Ontology) and
`bi-diagram-3` (Designer). It shares no silhouette with any icon listed in the
Goal and stays legible at the 15–20px sizes the navbar and breadcrumb use.

`bi-radar` exists in Bootstrap Icons 1.11.2, the version pinned by the CDN link
in `base.html` and `access_denied.html`. No new asset or dependency.

### Edits

Four occurrences change to `bi-radar`:

1. `src/front/config/menu_config.json` — the `digitaltwin` menu's top-level
   `icon`, currently `bi-box`.
2. `src/front/static/global/js/breadcrumb.js` — the `/dtwin/` entry's `icon`,
   currently `bi-box-fill`.
3. `src/front/templates/partials/layout/help_modal.html` — the `<i>` on
   walkthrough step 5, currently `bi-box-fill`.
4. `src/front/templates/partials/domain/_domain_validation.html` — the `<i>` in
   the Knowledge Graph validation card header, currently `bi-box-fill`.

The level-2 subnav in `base.html` and the navbar both read the icon from
`menu_config.json`, so they inherit the change with no edit. The `digitaltwin`
group and item icons inside `menu_config.json` (Management, Insight,
Navigation, Advanced and their children) describe sub-sections rather than the
section itself and are left untouched.

Every other `bi-box` and `bi-box-fill` occurrence in `src/front` refers to a
domain, an entity, or an unrelated `bi-box-arrow-*` glyph. None of them change.

### Icon variant consistency

Bootstrap Icons has no `bi-radar-fill`, so Knowledge Graph uses a single glyph
in every surface instead of the current outline-in-menu / filled-elsewhere
split. This matches Ontology and Mapping, which already use the same icon in
both `menu_config.json` and `breadcrumb.js`.

## Testing

- Add a static contract test asserting the `domain` and `digitaltwin` top-level
  menu entries in `menu_config.json` do not share an `icon` value, and that
  `digitaltwin` resolves to `bi-radar`. This locks out the regression that
  prompted the change.
- Add to the same contract that `breadcrumb.js` maps `/dtwin/` to the same icon
  as the `digitaltwin` menu entry, so the two cannot drift apart again.
- Run the mandated suite: `uv run --frozen pytest -q -m "not scenario"`.

No existing test or doc asserts any of the four values, so nothing else is
affected.

## Success criteria

- Knowledge Graph shows `bi-radar` in the navbar, the level-2 subnav, the
  breadcrumb, the help modal walkthrough and the Domain validation card.
- No surface renders Domain and Knowledge Graph with the same glyph.
- Domain, Registry, Ontology, Designer and Mapping icons are unchanged.
- Unrelated `bi-box`, `bi-box-fill` and `bi-box-arrow-*` usages are unchanged.
- The new contract test fails if the two menu icons collide again or if the
  menu and breadcrumb icons drift apart.
- A `changelogs/v0.7.0/benoitcayladbx_2026-08-05.log` section records the
  change.
