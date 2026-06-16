// Commitlint config — Conventional Commits enforcement.
// Closes gap #10. CI checks the PR title via .github/workflows/lint-pr-title.yml.
// Locally: install via `npm install --no-save @commitlint/cli @commitlint/config-conventional`
// then `npx commitlint --from=HEAD~1`. The pre-commit-hooks `commit-msg` stage hooks it in.

module.exports = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    // Type must be one of these (mirrors src/.coding_rules.md and PR template).
    "type-enum": [
      2,
      "always",
      [
        "feat",
        "fix",
        "docs",
        "style",
        "refactor",
        "perf",
        "test",
        "build",
        "ci",
        "chore",
        "revert",
      ],
    ],
    // Scope optional — but when present must be a known package or milestone tag.
    // Soft check: warn rather than fail.
    "scope-enum": [
      1,
      "always",
      [
        // Backend areas
        "dtwin",
        "ontology",
        "mapping",
        "registry",
        "domain",
        "session",
        "agents",
        "mcp",
        "shacl",
        "sparql",
        "r2rml",
        "owl",
        "reasoning",
        "triplestore",
        "graphdb",
        "databricks",
        "lakebase",
        "api",
        "graphql",
        "front",
        "shared",
        // Tooling
        "ci",
        "build",
        "deps",
        "tests",
        "docs",
        "changelog",
        "release",
        // Milestone tags (per ROADMAP)
        "M1.P1",
        "M1.P2",
        "M1.P3",
        "M1.P4",
        "M1.P5",
        "M1.P6",
        "M1.P7",
        "M2.P1",
        "M2.P2",
        "M2.P3",
        "M2.P4",
        "M2.P5",
        "M2.P6",
        "M2.P7",
        "M3.P1",
        "M3.P2",
        "M3.P3",
        "M4.P1",
        "M4.P2",
        "M4.P3",
        // Testing milestone tags
        "T-M0",
        "T-M1",
        "T-M2",
        "T-M3",
        "T-M4",
        "T-M5",
        "T-M6",
      ],
    ],
    // Subject line — imperative mood, lower-case, no trailing period, <=72 chars.
    "subject-case": [2, "always", "lower-case"],
    "subject-empty": [2, "never"],
    "subject-full-stop": [2, "never", "."],
    "header-max-length": [2, "always", 100],
  },
};
