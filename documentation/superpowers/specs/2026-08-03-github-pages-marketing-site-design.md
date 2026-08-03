# GitHub Pages Marketing Site Design

## Context

OntoBricks already has GitHub Pages enabled at
`https://databrickslabs.github.io/ontobricks/`, currently publishing from the
`master` branch folder `/docs` (legacy branch deploy). The `documentation/` tree is
Markdown + Sphinx source for the product, not a marketing site. There is no
`site/` folder yet.

The goal is a public product / marketing website for OntoBricks, authored as
static files in `/site`, and served by GitHub Pages from that folder.

## Goal

Ship a multi-page marketing site under `site/` and switch Pages to publish
from `/site` on `master`, without adding a build toolchain or a new deploy
workflow.

## Approach

Hand-written static HTML/CSS/JS committed in `site/`. No static-site
generator, no Node build, no GitHub Actions Pages workflow. Shared chrome is
duplicated across pages; `assets/js/site.js` handles mobile nav and light
motion only.

## Pages wiring

- Keep GitHub Pages **legacy** deploy (branch + folder).
- Change source from `master` + `/docs` → `master` + `/site`.
- Public URL remains `https://databrickslabs.github.io/ontobricks/`.
- Repository documentation stays in `documentation/` (Markdown / Sphinx). It is no
  longer the Pages root.
- Pages publishes from `master` only. Content must merge to `master` before
  it goes live; `develop` alone does not update the site.
- After `site/` exists on `master`, update the Pages source (GitHub Settings →
  Pages, or `gh api` PATCH on `/repos/databrickslabs/ontobricks/pages`).

## Site structure

```
site/
  index.html              # Home
  features/index.html     # Capabilities & pipeline
  get-started/index.html  # Install, prerequisites, first run
  docs/index.html         # Docs hub → GitHub Markdown docs
  about/index.html        # Labs disclaimer, support, contributors
  assets/
    css/site.css
    js/site.js
    img/                  # logo + hero art
  404.html
```

Shared chrome on every page: logo, nav (Home · Features · Get started · Docs ·
About), primary CTA → GitHub repo, secondary CTA → Get started.

All asset and internal links use relative paths (or the `/ontobricks/`
project base path) so the site works as a GitHub Pages **project** site, not
only at domain root.

## Content (v1)

- **Home:** brand + tagline (“Knowledge Graph Builder for Databricks”), one
  short pitch (ontology → mapping → materialize → query), primary CTA
  (GitHub), secondary (Get started), then a short “four clicks” pipeline
  strip. No feature dump in the hero.
- **Features:** ontology design, R2RML mapping, Delta + Lakebase graph,
  reasoning (OWL / SWRL / SHACL), GraphQL / query — one section each.
- **Get started:** prerequisites, `uv sync` / `scripts/start.sh`, link to
  repo `documentation/get-started.md`.
- **Docs hub:** cards linking to key Markdown docs on GitHub (architecture,
  features, API, deployment). Not a Sphinx rebuild hosted on Pages.
- **About:** Databricks Labs AS-IS disclaimer, issues-only support,
  contributors link.

Copy should align with the README project description and Labs support
language.

## Visual direction

- Copy `src/front/static/global/img/ontobricks-icon.svg` into
  `site/assets/img/` (Pages only publishes `/site`, so app static paths are
  not available at runtime).
- Define CSS variables for color and type. Avoid purple-on-white /
  purple-to-indigo defaults, warm cream + terracotta serif defaults, and
  Inter / Roboto / Arial / system-only stacks.
- Prefer subtle gradient or pattern atmosphere over flat single-color
  backgrounds.
- Desktop and mobile layouts; at least 2–3 intentional motions (hero / nav),
  respecting `prefers-reduced-motion`.
- Cards only where they support interaction or a docs-hub browse pattern;
  keep the home hero free of card clutter.

## Ops & local preview

- Local preview: open files directly, or run a static server from `site/`
  (e.g. `python -m http.server`). When using absolute `/ontobricks/` paths,
  preview under that base path or prefer relative links.
- Verification: every page loads; nav and CTAs resolve; assets work on the
  project base path; mobile nav works; custom 404 is present.

## Out of scope (v1)

- Custom domain
- Publishing Sphinx HTML via Pages
- i18n / localization
- Blog
- Analytics
- CI / Actions-based Pages deploy
- Static site generators (Eleventy, Astro, etc.)

## Testing

No automated app test suite changes are required for static marketing pages.
Manual smoke checklist above is the acceptance bar. Optionally add a short
note in `documentation/` or README pointing at the live Pages URL once `/site` is
live.
