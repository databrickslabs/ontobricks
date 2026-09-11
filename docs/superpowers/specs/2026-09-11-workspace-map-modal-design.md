# Workspace map modal (L1 Domain badge)

Date: 2026-09-11  
Version: 0.9.0  
Status: approved

## Problem

When a domain is loaded, the L1 navbar shows a Domain + version badge (`#domainL1Link`). Clicking it currently navigates to `/domain/`. Users need a single overlay that shows every L2 workspace menu (Domain, Ontology, Mapping, Knowledge Graph) and its sub-menus at once, without hunting through L2 dropdowns.

## Decision

Clicking the Domain + version badge opens a Bootstrap modal “workspace map”: four columns, one per L2 workspace, with the same groups, labels, Bootstrap Icons, routes, and permission gates as the L2 dropdowns. L2 chrome is unchanged.

## Non-goals

- Do not add a third nav level.
- Do not replace or restyle the L2 segmented control or its dropdowns.
- Do not include Settings, Registry, Help, or domain actions (Save / Switch Version / Close).
- Do not show Domain sidebar-only groups (`sidebar_only: true`, currently W3C OWL / R2RML).
- Do not use `alert()` / `confirm()` / `prompt()`.
- Do not invent a second menu source of truth.

## Trigger and dismiss

- Affordance: `#domainL1Link` (the Domain + version badge), only when a domain is loaded (the L1 item is already hidden otherwise).
- Click: `preventDefault()`, open the modal. Do not navigate to `/domain/`.
- Keyboard: `#domainL1Link` stays an `<a>`. Native activation (click / Enter) opens the map; do not add a separate Space handler.
- Dismiss: Bootstrap defaults — × close, Esc, backdrop click.
- After dismiss, focus returns to `#domainL1Link`.
- The badge stays visually “active” whenever a domain is loaded (existing L1 behavior). Opening the map does not change that.

## Content

Four columns, left to right, matching L2 order:

1. Domain (`menu.id == domain`)
2. Ontology (`ontology`)
3. Mapping (`assignment`)
4. Knowledge Graph (`digitaltwin`)

Each column:

- Header: workspace icon + label from `menu_config.json` (Domain `bi-box`, Ontology `bi-bezier2`, Mapping `bi-shuffle`, Knowledge Graph `bi-radar`). Headers are static labels, not links.
- Body: the same groups and items as that workspace’s L2 dropdown:
  - skip `group.sidebar_only`
  - skip Domain group `id == domain-design` (already excluded from L2)
  - item href = `item.route` if set, else `{menu.route}?section={item.id}`
  - item icon + label from config
  - copy permission classes from L2: `dropdown-requires-domain` when `item.requires == domain_saved`; `view-mode-hidden` when `item.hidden_in_view_mode`; Mapping and Knowledge Graph column headers/items inherit graph gating (`nav-requires-graph` on Mapping/KG column, same as L2 toggles)
- Untitled first Domain group (`title: ""`) renders items with no group header, same as L2.

Title bar: current domain name + version (same string as `#currentDomainName`) and the lifecycle status badge already shown next to the L1 name. Close button on the right. No modal footer.

## Visual contract

Reuse app-shell tokens (`--db-*`) and existing menu chrome:

- Modal: Bootstrap `modal fade` + `modal-dialog-centered modal-xl`, `ob-*` classes for the grid. Warm white surface, 1px `--db-border`, `--db-radius-card`, elevation only on the floating dialog (`--db-shadow-lg`).
- Column headers: 0.82rem, weight 600, `--db-primary-darker`, workspace icon.
- Group titles: same treatment as `.dropdown-header` / `.sidebar-title` — uppercase 0.65rem, letter-spacing, muted, icon + title.
- Items: same hover as L1/L2 dropdown items and sidebar — `--db-hover-indigo` well, 9px radius, not a solid indigo invert.
- Current item (matching pathname prefix + `section` query or hash): `--db-primary-light` fill, `--db-primary` icon, weight 600.
- Disabled items stay visible, 0.5 opacity, `pointer-events: none`, existing `title` tooltips from `navbar.js` (`updateMenusForDomainStatus` / `updateMenusForGraphBackend`).
- Below 768px: two-column grid (Domain|Ontology, then Mapping|KG). Do not horizontally scroll the modal body; allow vertical scroll (`modal-dialog-scrollable` if needed).
- Icons: Bootstrap Icons only, from `menu_config.json`. No emoji.
- No inline CSS/JS in templates.

## Behaviour

- Item click: navigate to the href (full page load, same as L2). Modal may unmount with the page; no extra hide required beyond letting navigation proceed.
- Gated items: do not navigate (existing disabled handling).
- Graphless domains (`graph_backend === none`): Mapping and Knowledge Graph columns remain, but links stay `nav-disabled` with the existing tooltip.
- Settings pages: L1 Domain badge is still shown when a domain is loaded; the map still opens. L2 stays hidden on Settings (unchanged).
- Current-section highlight is computed in JS on show (path + `?section=` / hash), not hard-coded in Jinja.

## Architecture

| Piece | Owner |
| --- | --- |
| Markup | `src/front/templates/partials/layout/_workspace_map_modal.html`, `{% include %}` from `base.html` next to Help / Registry modals |
| Menu data | `src/front/config/menu_config.json` via existing `menu_config` Jinja context — no duplicate list |
| CSS | `src/front/static/global/css/components.css` (`.ob-workspace-map*`), `--db-*` tokens only |
| Open/close + current item | `src/front/static/global/js/navbar.js` |
| Permissions | existing classes; `permissions.js` / `navbar.js` already scan the DOM |

Open API:

```js
function openWorkspaceMap(event) {
  if (event) event.preventDefault();
  const el = document.getElementById('workspaceMapModal');
  if (!el || typeof bootstrap === 'undefined') return;
  highlightWorkspaceMapCurrent();
  bootstrap.Modal.getOrCreateInstance(el).show();
}
```

Wire `#domainL1Link` in `initNavbar()`: click → `openWorkspaceMap`. Do not use `data-bs-toggle="modal"` alone if that would still follow `href="/domain/"`.

## Error handling

- Missing modal node: no-op (do not navigate, do not throw).
- Bootstrap not loaded yet: no-op.
- No domain loaded: badge already disabled/hidden; do not bind an alternate path.

## Testing

File-level contracts in `tests/units/front/` (same style as `test_knowledge_graph_icon.py` / `test_clarity_design_contract.py`):

- Modal is included from `base.html`.
- Four columns in Domain / Ontology / Mapping / Knowledge Graph order, with `bi-box`, `bi-bezier2`, `bi-shuffle`, `bi-radar`.
- Representative L2 items present (`/ontology/?section=map`, `/dtwin/?section=sigmagraph`, Domain Information).
- Domain W3C sidebar-only items absent from the modal.
- `#domainL1Link` click handler in `navbar.js` calls `preventDefault` and `Modal.getOrCreateInstance`.
- CSS uses `--db-*` tokens; no hardcoded hex for hover/current.

Run: `uv run --frozen pytest -q -m "not scenario"`.

## Docs

Update `.cursor/11-frontend-design.mdc` Two-Level Navigation: document the L1 badge → workspace map modal. Do not change L1/L2 structure rules other than the badge click.

Changelog: `changelogs/v0.9.0/` per `.cursorrules`.
