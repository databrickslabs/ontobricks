# Card-Integrated Page Tabs Design

## Goal

Define and apply a canonical card-integrated tab pattern for page-level forms
whose tabs are sections of one workspace surface. Apply it now to
Domain → Information and Ontology → Generate, using Ontology → Data Quality as
the visual reference.

## Scope

This change covers:

- Keeping Domain → Information on the corrected card-integrated structure.
- Migrating Ontology → Generate from a standalone tab rail plus
  `ob-tab-content` surface to the Data Quality card structure.
- Updating the canonical Cursor frontend rule.
- Adding a thin Claude frontend-design skill and registering its trigger.
- Adding focused structural regression tests and browser verification.

It does not migrate unrelated existing tabbed pages. Those pages may retain
their current approved patterns until they are intentionally changed.

## Canonical Pattern

A page-level form or editor whose tabs divide one logical workspace uses:

```html
<div class="card h-100">
    <div class="card-body p-0 ob-tabs-wrap">
        <ul class="nav nav-tabs ob-tabs nav-fill"
            id="<unique>Tabs" role="tablist">
            ...
        </ul>
        <div class="tab-content p-3" id="<unique>TabContent">
            ...
        </div>
    </div>
</div>
```

When the outer element must own form submission, `<form>` replaces the outer
`<div>` and carries the same `card h-100` classes. No nested form is introduced.

The single outer card owns the white surface, one-pixel border, 14px radius,
and full-height behavior. The rail sits flush inside the card. The tab content
has no independent border, radius, or right margin and uses Bootstrap `p-3`
padding.

The existing shared `.ob-tabs` component continues to own typography, colors,
active underline, hover, focus, no-wrap behavior, and horizontal scrolling.
No page-specific tab-link styling is added.

## Ontology Generate Migration

Ontology → Generate keeps its existing header, actions, tab IDs, pane IDs,
controls, event handlers, and generation behavior. Only the tab workspace
structure changes:

- Add one outer `card h-100`.
- Add `card-body p-0 ob-tabs-wrap`.
- Change `#wizardTabs` to `nav nav-tabs ob-tabs nav-fill`.
- Change `#wizardTabContent` to `tab-content p-3`.
- Keep the informational note inside the card content flow without creating a
  second bordered surface.

The rail remains pinned while long pane content scrolls internally on desktop.
On mobile, the rail remains single-line and horizontally scrollable.

## Rules and Skills

`.cursor/11-frontend-design.mdc` remains the single source of truth. Its tab
section will:

- Distinguish card-integrated page tabs from independent tab surfaces.
- Include the canonical card skeleton above.
- Name Domain → Information, Ontology → Generate, Ontology → Data Quality,
  Business Rules, and Axioms as examples of the integrated pattern.
- Preserve the existing compact-panel and independent-surface patterns.
- Add anti-patterns for placing a standalone `ob-tab-content` immediately
  below a rail when both represent one logical card workspace.

A new `.claude/skills/frontend-design/SKILL.md` will be a thin workflow wrapper,
not a duplicate rule set. It will require reading `.cursor/11`, comparing with
the closest canonical page, preserving shared components, browser-testing
desktop and mobile, checking console errors, running tests, and invoking the
changelog routine.

`CLAUDE.md` will register the new skill trigger in its existing Claude-only
skills table. It will not duplicate frontend rules.

## Testing and Verification

Static UI contracts will verify:

- Domain → Information retains the integrated card structure.
- Ontology → Generate uses the same outer card, card body, `nav-fill` rail, and
  `p-3` tab content.
- Ontology → Generate no longer uses `ob-tab-content` for this workspace.
- The canonical rule and Claude skill contain the expected references without
  duplicating CSS declarations.

Browser verification will compare Ontology → Generate with Data Quality for
card border, radius, background, rail geometry, and content padding. It will
also verify all tabs, generation controls, internal scrolling, mobile
horizontal overflow, read-only behavior, and console/network health.

The mandatory suite is:

`uv run --frozen pytest -q -m "not scenario"`
