# Worktree convention — multi-agent coordination

Under CNS (§3.7), worktrees are the escape hatch when Cursor can't span what you need. This README is the protocol so two agents (human or LLM) working the same day don't collide.

## Why a worktree?

Open one only when:

1. Mid-T3/T4/T5 and a P0 production bug lands (T6). Stash is risky because your IDE chat context is tied to current state.
2. Running a long eval / DAB deploy / Hypothesis sweep and you want to keep coding.
3. `superpowers:dispatching-parallel-agents` is going to operate on a clean state.

If the change is <30 min and you can stash safely, **don't open a worktree.** Tax > benefit.

## Naming

```
.claude/worktrees/<slug>-<8-char-hash>/
```

- `slug` matches the `.planning/<slug>/` directory name (the issue title, lowercased and hyphenated).
- `hash` is `git log -1 --format=%h` from main, truncated to 8.

Examples:
- `.claude/worktrees/digitaltwin-split-p1-1db8647c/`
- `.claude/worktrees/icon-bug-3f9c2a1e/`

The branch inside the worktree has the same name. So `git worktree list` reads naturally.

## Mechanics

```bash
# Create from a clean main
HASH=$(git -C <main-checkout> log -1 --format=%h)
SLUG="my-feature"
git worktree add ".claude/worktrees/${SLUG}-${HASH}" -b "${SLUG}-${HASH}"

# Open in Cursor: open a second window at the worktree path.
# OR open Claude Code in the worktree terminal — preferred for agent-driven parallel work.

# Cleanup after merge
git worktree remove ".claude/worktrees/${SLUG}-${HASH}"
git branch -d "${SLUG}-${HASH}"
```

## `.planning/` is per-worktree

Each worktree has its own `.planning/<slug>/PLAN.md` (and SPEC.md for agent features). PLAN.md is the resumption substrate — re-open the worktree, `cat .planning/<slug>/PLAN.md`, pick up where the checkboxes left off.

Don't share PLAN.md across worktrees. If two slugs need to share decisions, lift them into `.planning/ROADMAP.md` instead.

## `changelogs/` is **shared**

Both worktrees write into the same `changelogs/<YYYY-MM-DD>.log`. To avoid stomping each other on the same day:

**Convention:** every section header includes a worktree suffix when there's parallel work.

```markdown
## SHACL severity filter (worktree shacl-severity-a1b2c3d4)
...

## Icon bug fix (worktree icon-bug-3f9c2a1e)
...
```

Two agents writing the same day → two `##` headings, both unique. The merge is naturally a string-append, no conflict.

CI dedupes if the headers happen to collide (M3.P2 changelog-presence gate compares the diff against `changelogs/`, not full equality).

## Two harnesses in two worktrees

| Setup | When | Notes |
|---|---|---|
| Cursor in main + Cursor in worktree | Two humans, one repo, one day | OK. Two windows, two contexts. |
| Cursor in main + Claude Code in worktree | One human, parallel automated work | **Best for T5 refactors** — Claude Code runs the parallel-agent sweep while you keep coding in main. |
| Claude Code in main + Cursor in worktree | One human, agent doing long-running work | OK but unusual. Usually flip the pairing above. |
| Cursor + Cursor in the same checkout | Always | **Don't.** Each Cursor window has its own context — they'll race on writes. |
| Claude Code + Claude Code in the same checkout | Always | **Don't.** Same reason. Spawn parallel **subagents** instead via `superpowers:dispatching-parallel-agents`. |

## Disallowed

- **Never edit `src/` outside the active worktree** during a phase execution. Use the `freeze` skill (gstack) to make it audible.
- **Never invoke `gsd-*` skills.** CNS dropped them in v2 (§3.0); pre-commit hook blocks references.
- **Never push `.claude/worktrees/<slug>/` to remote.** Worktrees are local. The branch is what gets pushed.

## Cleanup checklist

When a worktree's branch merges to main:

1. `git worktree remove .claude/worktrees/<slug>-<hash>` (releases the working tree).
2. `git branch -d <slug>-<hash>` (cleans up the local branch).
3. Update `.planning/ROADMAP.md`: mark the Issue `[x]` with a landing date.
4. Optionally archive `.planning/<slug>/` to `.planning/done/<slug>/` if it had useful research notes; otherwise delete.

## See also

- §3.7 of the methodology plan (`/Users/dermot.smyth/.claude/plans/ultrathink-perform-a-detailed-whimsical-token.md`).
- `superpowers:using-git-worktrees` — the canonical skill.
