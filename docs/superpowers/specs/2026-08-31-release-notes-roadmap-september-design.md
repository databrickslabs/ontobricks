# Release Notes v0.8.0 and September 2026 Roadmap Design

## Goal

Create two English product documents:

1. `releases/ReleaseNotes_V0.8.0.md`, describing the capabilities actually
   delivered in v0.8.0.
2. `releases/Roadmap_2026-09.md`, replacing the August roadmap with a complete
   forward-looking roadmap organized by release.

## Sources of truth

### Release notes

- All files under `changelogs/v0.8.0/`.
- The v0.8.0 implementation and documentation in the repository.
- The structure and tone of earlier release notes, especially
  `releases/ReleaseNotes_V0.7.0.md`.
- The latest full non-scenario test result recorded in the changelog.

The release notes must describe only shipped behavior. Planned Asana work must
not appear as delivered.

### Roadmap

- The active `OntoBricks-Product` Asana project is the primary source for
  scope and priority.
- Repository state and v0.8.0 changelogs determine whether an item is already
  delivered.
- The August roadmap provides strategic context and detailed capability
  descriptions where they remain current.
- Asana dates are not authoritative and must not be reproduced as commitments.

## Roadmap prioritization

The roadmap is organized by target release: v0.8.x follow-up, v0.9.0, v1.0.0,
then requests to evaluate. Within each release, items are ranked using:

1. Explicit Asana priority.
2. Active Asana status, with in-progress and analyzing items ahead of
   not-started items of equal priority.
3. Product value and dependency order.
4. Evidence from the repository about delivered prerequisites.

Items without an Asana priority remain visible but are marked as needing
triage. No forecast dates or effort-based delivery windows are included.

## Release notes structure

1. Header with release window and verified test status.
2. Highlights.
3. Functional sections grouped by user-visible capability.
4. Security, reliability, deployment, and documentation changes.
5. Selected fixes.
6. Upgrade notes for operators.

The document should synthesize changes rather than copy changelog entries.

## Roadmap structure

1. Executive summary and strategic direction.
2. Current state after v0.8.0.
3. Prioritization policy and status legend.
4. Release sections ordered from nearest to furthest:
   - v0.8.x follow-up and release-readiness work.
   - v0.9.0 product and workflow priorities.
   - v1.0.0 GA and ecosystem priorities.
   - Requests to evaluate.
5. Dependencies, open decisions, and contribution guidance.

Each planned item must expose its Asana-derived priority/status when available.
The roadmap must avoid calendar promises and clearly distinguish committed
release scope from untriaged requests.

## Validation

- Every major release-note claim maps to at least one v0.8.0 changelog entry.
- No incomplete Asana item is represented as shipped.
- Every active Asana item is represented or explicitly deferred/untriaged.
- Version names, statuses, and priority labels are internally consistent.
- Both documents contain no placeholders or stale statements that v0.7.0 is
  the current release.
