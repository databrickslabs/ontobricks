# Level 2 Navbar Segmented Navigation — Design Specification

## Purpose

The level 2 navbar must feel consistent with OntoBricks' current Clarity
language while preserving its existing information architecture, routes,
dropdown contents, breadcrumb, and domain actions.

The current full-width warm strip and bottom divider make L2 read as a second
page header. The approved direction instead presents navigation and actions as
self-contained controls directly on the application canvas.

## Decisions

- Keep the existing order: Domain, Ontology, Mapping, Knowledge Graph,
  breadcrumb, Save, Switch Version, Close.
- Present the four workspace targets as one compact white segmented control.
- Use `1px solid var(--db-border)`, `var(--db-radius-control)`, and a small
  internal gutter for the segmented control.
- Use `--db-primary-light` and `--db-primary-darker` for the selected workspace.
- Remove the L2 full-width background and bottom border. The application canvas
  must remain visible behind the controls.
- Keep the one-pixel divider under L1 as the only full-width chrome separator.
- Preserve dropdown menus for non-selected workspace targets.
- Remove the dropdown affordance and dropdown behavior from the selected target.
  Its page sidebar remains responsible for navigation within that workspace.
- Keep dropdown styling aligned with the sidebar: white floating surface,
  warm one-pixel border, card radius, floating shadow, indigo-soft hover.
- Keep Save as the only filled primary action. Switch Version remains neutral,
  and Close remains neutral with danger-colored intent.
- Keep the inline breadcrumb between the segmented control and action group.
- Do not change routes, menu configuration, permission gates, or domain state.

## Visual Structure

```text
L1 navbar (white, existing bottom divider)

L2 transparent row
  [ Domain ▾ | Ontology ▾ | Mapping selected | Knowledge Graph ▾ ]
                                      Mapping / Designer
                                      [ Save ] [ Versions ] [ Close ]

Page canvas
```

The L2 row still contributes to `--ob-chrome-height`; only its surface and
control styling change.

## Interaction

| Event | Result |
|---|---|
| Hover a non-selected workspace | Indigo-soft hover well inside its segment. |
| Click a non-selected workspace | Open its existing Bootstrap dropdown. |
| Select a dropdown item | Navigate to the existing route and section. |
| View the selected workspace | Indigo-soft selected segment, no chevron. |
| Click the selected workspace | No dropdown opens; the page sidebar handles section navigation. |
| Focus any workspace target | Show the shared accessible primary focus ring. |
| Click Save / Switch / Close | Preserve all existing handlers and permission behavior. |

## Responsive Behavior

Preserve the existing mobile contract below `768px`:

- keep all four workspace targets and all three domain actions;
- hide only `.ob-subnav-label`, leaving icons and accessible names in the DOM;
- hide the breadcrumb and flex spacer;
- keep dropdowns able to escape the row without horizontal clipping;
- do not introduce horizontal scrolling.

The segmented control may compact to icon-only segments but must retain its
white surface, border, and rounded outer frame.

## Implementation

1. Update shared L2 styles in
   `src/front/static/global/css/main.css`:
   - make `.ob-subnav` transparent and remove its bottom border;
   - frame `.ob-subnav-nav`'s workspace group as a segmented control;
   - style workspace links as rounded segments;
   - retain the approved selected, hover, focus, dropdown, breadcrumb, and
     action hierarchy.
2. Add the smallest semantic wrapper needed in
   `src/front/templates/base.html` around the four workspace targets. Do not
   move the breadcrumb or action group into that wrapper.
3. Update `src/front/static/global/js/navbar.js` so the active workspace target
   does not open a dropdown while inactive targets retain Bootstrap dropdown
   behavior.
4. Extend existing frontend navigation contract tests under
   `tests/units/front/` to lock the transparent L2 surface, segmented wrapper,
   active behavior, and preserved dropdowns.
5. Update `.cursor/11-frontend-design.mdc` so its L2 visual contract describes
   the segmented transparent treatment instead of the full-width warm strip.

## Testing

- Static contract tests verify:
  - `#obSubnav` and existing IDs remain present;
  - the workspace wrapper contains Domain, Ontology, Mapping, and Knowledge
    Graph in the existing order;
  - the breadcrumb and actions remain outside that wrapper and in their
    existing order;
  - L2 has no full-width background or bottom border;
  - the selected segment uses shared primary tokens;
  - non-selected targets retain dropdown behavior while the active target does
    not open one;
  - mobile selectors retain icon-only access without overflow clipping.
- Run targeted frontend tests.
- Run `uv run --frozen pytest -q -m "not scenario"`.
- Browser-check desktop `1600×1000` and mobile `390×844`, including one open
  inactive dropdown and keyboard focus states.

## Out of Scope

- L1 navbar redesign
- Dropdown content or menu grouping changes
- Sidebar redesign
- Route or menu configuration changes
- Breadcrumb behavior changes
- Save, version switching, close, permission, or edit-lock behavior changes
