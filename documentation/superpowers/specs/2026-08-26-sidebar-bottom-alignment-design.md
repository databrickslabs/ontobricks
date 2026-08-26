# Sidebar Bottom Alignment — Design Specification

## Purpose

On every sidebar page, the left menu and the active main pane must share one
desktop bottom edge: a single 0.5rem viewport gutter. Page-local `100vh`
offsets, extra bottom padding, and `calc(100% - 1rem)` nav heights made that
edge drift. This spec records the approved, implemented contract.

## Scope

Five sidebar page families, all using `.sidebar-layout` from
`src/front/templates/{domain,ontology,mapping,dtwin,settings}.html`:

| Family | Page | Representative sections |
|--------|------|-------------------------|
| Domain | `domain.html` | Information, Data Sources, Documents, OWL, R2RML, Audit |
| Ontology | `ontology.html` | Designer, Entities, Relationships, Data Quality, Wizard, Pitfalls |
| Mapping | `mapping.html` | Designer, Manual, Auto-map, Diagnostics, R2RML, Spark SQL |
| Knowledge Graph | `dtwin.html` | Explorer, Query, Data Quality, Chat, Cohorts |
| Settings | `settings.html` | UI, Logs, Teams, Databricks, Health |

**Out of scope**

- Standalone Layout 2 pages: Home, About, Access Denied. They stay in
  `.content-wrapper` with the default 2rem padding and no sidebar shell.
- True Bootstrap modals. They are siblings of the stretch root, not measured
  as the primary pane. Opening a modal must not change the underlying pane
  bottom.
- Registry browse/bridges live as the Registry modal, not a sixth sidebar
  family.

## Decisions

- `--ob-chrome-height` (navbar + L2 subnav, owned by `breadcrumb.js`) is the
  only viewport-height input.
- `.sidebar-layout` owns the 0.5rem outer gutter (`padding` and `gap`).
- `.sidebar-nav` is a framed card with `margin: 0` and `height: 100%`.
- `.sidebar-content` adds a 0.5rem inset on top/left/right and **no** extra
  bottom inset.
- Long-form sections scroll inside `.sidebar-content` (or a scoped inner
  scroller). Fixed-height canvases and consoles scroll internally and must
  not grow the document on desktop.
- Mobile (`max-width: 768px`) uses normal document flow. No pane-level
  `100vh`.
- Rendered invariant: `|sidebar.bottom - pane.bottom| <= 1px` at 1600×1000.

## Desktop shell

```text
viewport
  L1 navbar
  L2 subnav          ← --ob-chrome-height
  .sidebar-layout    height: calc(100vh - var(--ob-chrome-height, 60px))
                     padding: 0.5rem; gap: 0.5rem; box-sizing: border-box
                     overflow: hidden
    .sidebar-nav     margin: 0; height: 100%; framed card
    .sidebar-content padding: 0.5rem 0.5rem 0; flex column; min-height: 0
      .sidebar-section.active
        .content-section
          header + primary surface
```

Geometry:

| Edge | How it is made |
|------|----------------|
| Outer gutter | Shell `padding: 0.5rem` on all four sides. |
| Nav–content gap | Shell `gap: 0.5rem`. |
| Content top / left / right | Shell 0.5rem + content 0.5rem = **1rem** total. |
| Content bottom | Shell 0.5rem only. Content padding-bottom is **0**. |

The shell padding is what exposes the nav card’s four rounded corners. Do not
restore `margin: 0.5rem` / `height: calc(100% - 1rem)` on `.sidebar-nav`.

## Flex and min-height chain

Every stretching ancestor must be a column flex container with `min-height: 0`
so nested `overflow` can resolve:

1. `.sidebar-content` — `display: flex; flex-direction: column; min-height: 0`.
2. `.sidebar-section.active` — `flex: 1 1 0; min-height: 0`.
3. `.sidebar-section > .content-section` — `flex: 1; display: flex;
   flex-direction: column; min-height: 0`.
4. Direct-child `.card` / `.ob-tabs-wrap` / split shell — `flex: 1; min-height: 0`.

Partials that previously skipped `.content-section` now wrap their primary
surface in that root. Modals stay **siblings** of the root, never inside it.

Domain Documents uses a neutral inner stack (`.domain-documents-stack`) so
stacked cards keep natural flow instead of inheriting the direct-child card
stretch contract.

## Scroll ownership

**Default / long-form.** `.sidebar-content` is `overflow-y: auto`. Forms,
documents, and stacked cards scroll in that pane. Alignment applies to the
main surface, not to forcing every field into one viewport.

**Fixed-height panes.** When the active section is Explorer, Query, Data
Quality, or Logs, `.sidebar-content` is `overflow: hidden`. An inner owner
scrolls (canvas, GraphiQL, DQ tab body, `#logsConsoleWrap`). On desktop the
document must not exceed the viewport by more than 1px.

**Data Quality and Logs — no extra bottom padding.** The shared Data Quality
`:has(#dataquality-section.active)` rule and `#logs-section.active` use a
zero bottom padding (`0.5rem 0.5rem 0` and `0.5rem 1rem 0` respectively).
Logs console scrolling is CSS-owned (`#logsConsoleWrap`: `flex: 1`,
`min-height: 100px`, `overflow-y: auto`), not inline style.

**No pane-level `100vh`.** Only `.sidebar-layout` may set `height` /
`max-height` to `calc(100vh - var(--ob-chrome-height, …))`.
`.sidebar-content` may mirror that as `max-height` if needed. Area CSS must
use `flex: 1` + `min-height: 0`, never `calc(100vh - 80px)` or similar.

## Mobile

Below `768px` the shell stacks: `height: auto`, `max-height: none`,
`overflow: visible`. Matching `:has()` resets restore visible overflow on
content and section so desktop `overflow: hidden` cannot win. The page
scrolls as a normal document. Knowledge Graph Data Quality uses an exact
`#dataquality-section.active` mobile rule (`height: auto; overflow: visible`)
after its desktop override. Graph Chat likewise uses a same-selector
`#chat-section.sidebar-section.active` mobile rule with the same declarations
after its desktop overflow clamp.

## Testing

Static:

- `tests/units/front/test_clarity_design_contract.py` — shell gutter, flex
  chain, sole `100vh` owner, mobile natural-flow media query.
- `tests/units/front/test_sidebar_content_stretch_contract.py` — section
  inventory, stretch roots, modal siblings, DQ/Logs padding, mobile DQ and
  Graph Chat exact-selector resets, CSS-owned Logs scroll, internal-scroll
  chains.

Rendered pytest (`tests/e2e/navigation/test_sidebar_bottom_alignment.py`)
covers a representative route set, not every sidebar item:

- Desktop 1600×1000: `.sidebar-nav` vs primary pane bottoms, `delta <= 1`.
- Fixed-height routes: `documentHeight <= viewportHeight + 1`.
- Mobile 390×844: visible overflow, window scroll, no horizontal overflow.

Manual browser verification at implementation close-out (not in-repo e2e
parameter counts): 46/46 accessible desktop sidebar routes, 7/7 mobile
spot checks, 5/5 true modals. Permission-gated sections that did not
render a pane were not treated as failures.

## Anti-patterns

- `.sidebar-nav { margin: 0.5rem; height: calc(100% - 1rem); }`.
- Extra `padding-bottom` on `.sidebar-content` or a fixed pane that shortens
  the surface relative to the menu.
- Pane-level `100vh` / `calc(100vh - Npx)` in area CSS.
- Inline flex/overflow on `#logsConsoleWrap`.
- Measuring a section wrapper instead of the visible card (Knowledge Graph
  Data Quality uses `#dataquality-section > .dq-card-fill`).
- Putting a modal inside `.content-section` so it inherits overflow clipping.
- Treating Home / About / Access Denied as sidebar geometry.
