# GitHub Pages Marketing Site — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a multi-page OntoBricks marketing site under `site/` and switch GitHub Pages from `/docs` to `/site` on `master`.

**Architecture:** Hand-written static HTML/CSS/JS committed in `site/`. No SSG, no Node build, no Actions Pages workflow. Shared chrome is duplicated across pages; `assets/js/site.js` owns mobile nav + light motion. Relative asset/nav paths so the project site works under `/ontobricks/`.

**Tech Stack:** Static HTML5, CSS custom properties (`--ob-*`), vanilla JS, Google Fonts for marketing typography, pytest source-contract tests under `tests/units/site/`.

**Spec:** `documentation/superpowers/specs/2026-08-03-github-pages-marketing-site-design.md`

## Global Constraints

- No static-site generator, no Node toolchain, no new GitHub Actions Pages workflow.
- Pages remains **legacy** branch deploy; publish from `master` + `/site` only after `site/` exists on `master`.
- Live URL: `https://databrickslabs.github.io/ontobricks/`.
- All internal links and asset URLs must be **relative** (never assume site is at domain root).
- Copy logo into `site/assets/img/` — Pages only publishes `/site`, so `src/front/...` paths are unavailable at runtime.
- Visual: Databricks red `#FF3621` as brand primary; avoid purple-on-white / purple-indigo gradients, warm cream + terracotta serif look, and Inter/Roboto/Arial/system-only stacks.
- Respect `prefers-reduced-motion` for all animation.
- Do not move or delete `documentation/` Markdown/Sphinx content; only stop using `/docs` as the Pages root.
- After code changes: append changelog under `changelogs/v0.7.0/benoitcayladbx_YYYY-MM-DD.log` (version from `pyproject.toml`).
- Routine tests: `uv run pytest -q -m "not scenario"`.
- Only create git commits when the user asks (or when executing this plan’s commit steps under an execution skill with user approval for commits).

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `site/assets/css/site.css` | Create | Tokens, layout, chrome, page sections, motion, reduced-motion |
| `site/assets/js/site.js` | Create | Mobile nav toggle, hero entrance, active nav highlight |
| `site/assets/img/ontobricks-icon.svg` | Create (copy) | Brand mark for Pages |
| `site/index.html` | Create | Home |
| `site/features/index.html` | Create | Features |
| `site/get-started/index.html` | Create | Get started |
| `site/docs/index.html` | Create | Docs hub |
| `site/about/index.html` | Create | About / Labs disclaimer |
| `site/404.html` | Create | Custom 404 |
| `tests/units/site/test_marketing_site.py` | Create | Structure + content source contracts |
| `README.md` | Modify | Link to live Pages URL |
| `changelogs/v0.7.0/benoitcayladbx_YYYY-MM-DD.log` | Create/append | Post-change routine |

---

### Task 1: Scaffold assets + failing structure tests

**Files:**
- Create: `site/assets/css/site.css`
- Create: `site/assets/js/site.js` (stub)
- Create: `site/assets/img/ontobricks-icon.svg` (copy from app)
- Create: `site/index.html` (minimal stub so path exists)
- Create: `tests/units/site/test_marketing_site.py`

**Interfaces:**
- Produces: `SITE_ROOT` layout under `site/`; CSS custom properties `--ob-*`; failing tests that later tasks satisfy by adding remaining pages and full content.
- Consumes: `src/front/static/global/img/ontobricks-icon.svg`

- [ ] **Step 1: Write the failing structure tests**

Create `tests/units/site/test_marketing_site.py`:

```python
"""Source contracts for the GitHub Pages marketing site under site/."""

from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SITE = REPO / "site"

REQUIRED_PAGES = [
    "index.html",
    "features/index.html",
    "get-started/index.html",
    "documentation/index.html",
    "about/index.html",
    "404.html",
]

REQUIRED_ASSETS = [
    "assets/css/site.css",
    "assets/js/site.js",
    "assets/img/ontobricks-icon.svg",
]


def test_required_files_exist():
    missing = [p for p in REQUIRED_PAGES + REQUIRED_ASSETS if not (SITE / p).is_file()]
    assert missing == [], f"Missing site files: {missing}"


def test_css_defines_ob_tokens():
    css = (SITE / "assets/css/site.css").read_text(encoding="utf-8")
    for token in ("--ob-primary", "--ob-bg", "--ob-text", "--ob-font-display", "--ob-font-body"):
        assert token in css, f"Missing CSS token {token}"


def test_home_has_hero_and_ctas():
    html = (SITE / "index.html").read_text(encoding="utf-8")
    assert "Knowledge Graph Builder for Databricks" in html
    assert "github.com/databrickslabs/ontobricks" in html
    assert "get-started" in html


def test_pages_use_relative_assets():
    for page in REQUIRED_PAGES:
        if page == "404.html":
            continue
        html = (SITE / page).read_text(encoding="utf-8")
        assert "assets/css/site.css" in html
        assert "assets/js/site.js" in html
        assert 'href="/assets/' not in html
        assert 'src="/assets/' not in html


def test_site_js_has_nav_and_motion_hooks():
    js = (SITE / "assets/js/site.js").read_text(encoding="utf-8")
    assert "nav-toggle" in js or "data-nav-toggle" in js
    assert "prefers-reduced-motion" in js


def test_about_has_labs_disclaimer():
    html = (SITE / "about/index.html").read_text(encoding="utf-8")
    assert "AS-IS" in html or "as-is" in html.lower()
    assert "Service Level Agreements" in html or "SLA" in html


def test_docs_hub_links_github_markdown():
    html = (SITE / "documentation/index.html").read_text(encoding="utf-8")
    assert "github.com/databrickslabs/ontobricks/blob/" in html
    assert "architecture.md" in html or "features.md" in html
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `uv run pytest -q tests/units/site/test_marketing_site.py`

Expected: FAIL (missing `site/` pages / incomplete content).

- [ ] **Step 3: Copy logo and create CSS foundation + stubs**

```bash
mkdir -p site/assets/css site/assets/js site/assets/img \
  site/features site/get-started site/docs site/about
cp src/front/static/global/img/ontobricks-icon.svg site/assets/img/ontobricks-icon.svg
```

Create `site/assets/css/site.css` with at least:

```css
@import url("https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;9..144,700&family=IBM+Plex+Sans:wght@400;500;600&display=swap");

:root {
  --ob-primary: #ff3621;
  --ob-primary-dark: #e62e1c;
  --ob-accent: #0f766e; /* teal, not purple */
  --ob-bg: #0b1220;
  --ob-bg-elevated: #121a2b;
  --ob-surface: #f4f7fb;
  --ob-text: #e8eef7;
  --ob-text-muted: #9aa8bc;
  --ob-text-on-light: #1b1c1d;
  --ob-border: rgba(232, 238, 247, 0.12);
  --ob-font-display: "Fraunces", Georgia, serif;
  --ob-font-body: "IBM Plex Sans", "Segoe UI", sans-serif;
  --ob-radius: 12px;
  --ob-shadow: 0 18px 50px rgba(0, 0, 0, 0.35);
  --ob-transition: 0.25s ease;
  --ob-max: 1080px;
}

*,
*::before,
*::after {
  box-sizing: border-box;
}

html {
  scroll-behavior: smooth;
}

body {
  margin: 0;
  font-family: var(--ob-font-body);
  color: var(--ob-text);
  background:
    radial-gradient(1200px 600px at 10% -10%, rgba(255, 54, 33, 0.18), transparent 55%),
    radial-gradient(900px 500px at 90% 0%, rgba(15, 118, 110, 0.22), transparent 50%),
    var(--ob-bg);
  line-height: 1.6;
  min-height: 100vh;
}

a {
  color: inherit;
}

img {
  max-width: 100%;
  height: auto;
}

.site-header {
  position: sticky;
  top: 0;
  z-index: 20;
  backdrop-filter: blur(12px);
  background: rgba(11, 18, 32, 0.8);
  border-bottom: 1px solid var(--ob-border);
}

.site-header__inner,
.site-main,
.site-footer__inner {
  width: min(var(--ob-max), calc(100% - 2rem));
  margin-inline: auto;
}

.site-header__inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  padding: 0.85rem 0;
}

.brand {
  display: inline-flex;
  align-items: center;
  gap: 0.65rem;
  text-decoration: none;
  font-weight: 600;
}

.brand img {
  width: 36px;
  height: 36px;
}

.site-nav {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.site-nav a {
  text-decoration: none;
  color: var(--ob-text-muted);
  font-size: 0.95rem;
}

.site-nav a[aria-current="page"],
.site-nav a:hover {
  color: var(--ob-text);
}

.nav-toggle {
  display: none;
  background: transparent;
  border: 1px solid var(--ob-border);
  color: var(--ob-text);
  border-radius: 8px;
  padding: 0.4rem 0.65rem;
  cursor: pointer;
}

.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.4rem;
  padding: 0.7rem 1.1rem;
  border-radius: 999px;
  text-decoration: none;
  font-weight: 600;
  transition: transform var(--ob-transition), background var(--ob-transition);
}

.btn:hover {
  transform: translateY(-1px);
}

.btn-primary {
  background: var(--ob-primary);
  color: #fff;
}

.btn-primary:hover {
  background: var(--ob-primary-dark);
}

.btn-secondary {
  border: 1px solid var(--ob-border);
  color: var(--ob-text);
  background: transparent;
}

.hero {
  padding: 4.5rem 0 3rem;
}

.hero__brand {
  font-family: var(--ob-font-display);
  font-size: clamp(2.6rem, 6vw, 4.2rem);
  line-height: 1.05;
  margin: 0 0 0.75rem;
}

.hero__tagline {
  font-size: 1.25rem;
  color: var(--ob-text-muted);
  margin: 0 0 1rem;
}

.hero__pitch {
  max-width: 38rem;
  margin: 0 0 1.75rem;
  color: var(--ob-text-muted);
}

.hero__actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
}

.pipeline {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.75rem;
  margin: 2.5rem 0 1rem;
}

.pipeline__step {
  border: 1px solid var(--ob-border);
  border-radius: var(--ob-radius);
  padding: 1rem;
  background: rgba(18, 26, 43, 0.7);
}

.pipeline__step strong {
  display: block;
  margin-bottom: 0.35rem;
}

.section {
  padding: 2.5rem 0;
}

.section h2 {
  font-family: var(--ob-font-display);
  font-size: clamp(1.8rem, 3vw, 2.4rem);
  margin: 0 0 0.75rem;
}

.feature-grid,
.doc-grid {
  display: grid;
  gap: 1rem;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}

.feature-card,
.doc-card {
  border: 1px solid var(--ob-border);
  border-radius: var(--ob-radius);
  padding: 1.15rem;
  background: var(--ob-bg-elevated);
  text-decoration: none;
}

.doc-card:hover {
  border-color: rgba(255, 54, 33, 0.45);
}

.site-footer {
  border-top: 1px solid var(--ob-border);
  margin-top: 3rem;
  padding: 1.5rem 0 2rem;
  color: var(--ob-text-muted);
  font-size: 0.9rem;
}

.is-hidden-mobile {
  /* overridden below */
}

.hero.is-ready .hero__brand,
.hero.is-ready .hero__tagline,
.hero.is-ready .hero__pitch,
.hero.is-ready .hero__actions {
  animation: rise 0.7s ease both;
}

.hero.is-ready .hero__tagline { animation-delay: 0.08s; }
.hero.is-ready .hero__pitch { animation-delay: 0.16s; }
.hero.is-ready .hero__actions { animation-delay: 0.24s; }

@keyframes rise {
  from { opacity: 0; transform: translateY(12px); }
  to { opacity: 1; transform: translateY(0); }
}

@media (max-width: 800px) {
  .nav-toggle { display: inline-flex; }
  .site-nav {
    display: none;
    position: absolute;
    inset: 100% 1rem auto;
    flex-direction: column;
    align-items: flex-start;
    padding: 1rem;
    border: 1px solid var(--ob-border);
    border-radius: var(--ob-radius);
    background: var(--ob-bg-elevated);
  }
  .site-nav.is-open { display: flex; }
  .pipeline { grid-template-columns: 1fr 1fr; }
}

@media (prefers-reduced-motion: reduce) {
  html { scroll-behavior: auto; }
  .hero.is-ready .hero__brand,
  .hero.is-ready .hero__tagline,
  .hero.is-ready .hero__pitch,
  .hero.is-ready .hero__actions {
    animation: none;
  }
  .btn:hover { transform: none; }
}
```

Create stub `site/assets/js/site.js`:

```javascript
(function () {
  "use strict";
  // Filled in Task 2: data-nav-toggle, prefers-reduced-motion, hero.is-ready
})();
```

Create minimal `site/index.html` stub (enough for directory; full home in Task 3):

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OntoBricks</title>
  <link rel="stylesheet" href="assets/css/site.css">
</head>
<body>
  <p>stub</p>
  <script src="assets/js/site.js"></script>
</body>
</html>
```

- [ ] **Step 4: Re-run structure tests**

Run: `uv run pytest -q tests/units/site/test_marketing_site.py`

Expected: still FAIL on missing pages / incomplete content / incomplete JS — CSS token test should PASS.

- [ ] **Step 5: Commit** (when user approves commits)

```bash
git add site/assets tests/units/site/test_marketing_site.py site/index.html
git commit -m "$(cat <<'EOF'
test: add marketing site structure contracts and asset scaffold

EOF
)"
```

---

### Task 2: Shared JS (nav + motion)

**Files:**
- Modify: `site/assets/js/site.js`

**Interfaces:**
- Consumes: `[data-nav-toggle]`, `[data-site-nav]`, `.hero` in HTML from later tasks
- Produces: mobile nav open/close; `aria-expanded`; `hero.is-ready` when motion allowed; sets `aria-current` from path

- [ ] **Step 1: Implement `site/assets/js/site.js`**

```javascript
(function () {
  "use strict";

  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function initNav() {
    var toggle = document.querySelector("[data-nav-toggle]");
    var nav = document.querySelector("[data-site-nav]");
    if (!toggle || !nav) return;

    toggle.addEventListener("click", function () {
      var open = nav.classList.toggle("is-open");
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
    });

    nav.querySelectorAll("a").forEach(function (link) {
      link.addEventListener("click", function () {
        nav.classList.remove("is-open");
        toggle.setAttribute("aria-expanded", "false");
      });
    });
  }

  function markActiveNav() {
    var path = window.location.pathname.replace(/\/$/, "") || "/";
    document.querySelectorAll("[data-site-nav] a").forEach(function (link) {
      var href = link.getAttribute("href");
      if (!href) return;
      var resolved = new URL(href, window.location.href).pathname.replace(/\/$/, "") || "/";
      if (resolved === path) {
        link.setAttribute("aria-current", "page");
      }
    });
  }

  function initHeroMotion() {
    var hero = document.querySelector(".hero");
    if (!hero) return;
    if (reduceMotion) return;
    requestAnimationFrame(function () {
      hero.classList.add("is-ready");
    });
  }

  initNav();
  markActiveNav();
  initHeroMotion();
})();
```

- [ ] **Step 2: Confirm JS contract substring**

Run: `uv run pytest -q tests/units/site/test_marketing_site.py::test_site_js_has_nav_and_motion_hooks`

Expected: PASS.

- [ ] **Step 3: Commit** (when user approves commits)

```bash
git add site/assets/js/site.js
git commit -m "$(cat <<'EOF'
feat(site): add mobile nav and hero motion hooks

EOF
)"
```

---

### Task 3: Home page

**Files:**
- Modify: `site/index.html`

**Interfaces:**
- Consumes: `assets/css/site.css`, `assets/js/site.js`, `assets/img/ontobricks-icon.svg`
- Produces: full home with hero + pipeline strip + shared chrome

**Shared chrome snippet** (use on every page; adjust relative prefixes):

Root-relative example for `site/index.html` (`ASSET=assets`, `ROOT=.`):

```html
<header class="site-header">
  <div class="site-header__inner">
    <a class="brand" href="index.html">
      <img src="assets/img/ontobricks-icon.svg" alt="" width="36" height="36">
      <span>OntoBricks</span>
    </a>
    <button class="nav-toggle" type="button" data-nav-toggle aria-expanded="false" aria-controls="site-nav">Menu</button>
    <nav id="site-nav" class="site-nav" data-site-nav>
      <a href="index.html">Home</a>
      <a href="features/">Features</a>
      <a href="get-started/">Get started</a>
      <a href="documentation/">Docs</a>
      <a href="about/">About</a>
      <a class="btn btn-primary" href="https://github.com/databrickslabs/ontobricks">GitHub</a>
    </nav>
  </div>
</header>
```

For nested pages (`features/`, etc.), use `../assets/...`, `../index.html`, `../features/`, etc.

- [ ] **Step 1: Write full `site/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="OntoBricks — Knowledge Graph Builder for Databricks. Design ontologies, map Unity Catalog tables, materialize and query graphs.">
  <title>OntoBricks — Knowledge Graph Builder for Databricks</title>
  <link rel="icon" href="assets/img/ontobricks-icon.svg" type="image/svg+xml">
  <link rel="stylesheet" href="assets/css/site.css">
</head>
<body>
  <!-- header chrome from Interfaces above -->

  <main class="site-main">
    <section class="hero">
      <p class="hero__eyebrow" style="color:var(--ob-text-muted);letter-spacing:0.04em;text-transform:uppercase;font-size:0.8rem;margin:0 0 0.75rem;">Databricks Labs</p>
      <h1 class="hero__brand">OntoBricks</h1>
      <p class="hero__tagline">Knowledge Graph Builder for Databricks</p>
      <p class="hero__pitch">
        Transform Unity Catalog tables into a materialized knowledge graph.
        Design ontologies (OWL), map with R2RML, materialize into Delta and
        Lakebase, reason with OWL 2 RL / SWRL / SHACL, and query through GraphQL —
        with LLM-assisted automation along the way.
      </p>
      <div class="hero__actions">
        <a class="btn btn-primary" href="https://github.com/databrickslabs/ontobricks">View on GitHub</a>
        <a class="btn btn-secondary" href="get-started/">Get started</a>
      </div>
    </section>

    <section class="section" aria-label="Pipeline">
      <h2>From tables to graph in four clicks</h2>
      <div class="pipeline">
        <div class="pipeline__step"><strong>1. Import</strong>Pull UC metadata into a domain.</div>
        <div class="pipeline__step"><strong>2. Design</strong>Model entities, relations, and rules.</div>
        <div class="pipeline__step"><strong>3. Map</strong>Bind ontology properties to table columns.</div>
        <div class="pipeline__step"><strong>4. Materialize</strong>Build the graph and start querying.</div>
      </div>
    </section>
  </main>

  <footer class="site-footer">
    <div class="site-footer__inner">
      <p>OntoBricks is a Databricks Labs project provided AS-IS for exploration.</p>
      <p><a href="about/">About &amp; support</a> · <a href="https://github.com/databrickslabs/ontobricks">GitHub</a></p>
    </div>
  </footer>
  <script src="assets/js/site.js"></script>
</body>
</html>
```

Insert the header chrome where the HTML comment indicates. Do **not** leave the eyebrow as inline `style` in the final file — move `.hero__eyebrow` rules into `site.css` instead.

- [ ] **Step 2: Move eyebrow styles into CSS**

Append to `site/assets/css/site.css`:

```css
.hero__eyebrow {
  color: var(--ob-text-muted);
  letter-spacing: 0.04em;
  text-transform: uppercase;
  font-size: 0.8rem;
  margin: 0 0 0.75rem;
}
```

Remove any inline `style=` from the home page.

- [ ] **Step 3: Run home contract**

Run: `uv run pytest -q tests/units/site/test_marketing_site.py::test_home_has_hero_and_ctas`

Expected: PASS.

- [ ] **Step 4: Commit** (when user approves commits)

```bash
git add site/index.html site/assets/css/site.css
git commit -m "$(cat <<'EOF'
feat(site): add marketing home page

EOF
)"
```

---

### Task 4: Features page

**Files:**
- Create: `site/features/index.html`

**Interfaces:**
- Consumes: shared chrome with `../` prefixes; `../assets/...`

- [ ] **Step 1: Create `site/features/index.html`**

Page must include:
- Same header/footer chrome with nested relative paths
- H1 “Features”
- Five sections (or feature-cards) covering exactly:
  1. Ontology design (OWL)
  2. R2RML mapping to Unity Catalog
  3. Delta + Lakebase graph materialization
  4. Reasoning (OWL 2 RL, SWRL, SHACL)
  5. GraphQL / query

Example body structure:

```html
<main class="site-main section">
  <h1>Features</h1>
  <p>Everything needed to go from Lakehouse tables to a queryable knowledge graph.</p>
  <div class="feature-grid">
    <article class="feature-card">
      <h2>Ontology design</h2>
      <p>Visual OWL modeling for entities, attributes, and relationships.</p>
    </article>
    <article class="feature-card">
      <h2>R2RML mapping</h2>
      <p>Bind ontology properties to Unity Catalog tables and columns.</p>
    </article>
    <article class="feature-card">
      <h2>Delta + Lakebase graph</h2>
      <p>Materialize triples into a Delta-backed store and Lakebase Postgres graph engine.</p>
    </article>
    <article class="feature-card">
      <h2>Reasoning</h2>
      <p>Apply OWL 2 RL, SWRL rules, and SHACL data-quality shapes.</p>
    </article>
    <article class="feature-card">
      <h2>Query</h2>
      <p>Explore the graph visually and via an auto-generated GraphQL API.</p>
    </article>
  </div>
</main>
```

Asset links must be `../assets/css/site.css` and `../assets/js/site.js`.

- [ ] **Step 2: Smoke-check file exists**

Run: `test -f site/features/index.html && rg -n "R2RML|SHACL|GraphQL" site/features/index.html`

Expected: file exists; keywords present.

- [ ] **Step 3: Commit** (when user approves commits)

```bash
git add site/features/index.html
git commit -m "$(cat <<'EOF'
feat(site): add features page

EOF
)"
```

---

### Task 5: Get started page

**Files:**
- Create: `site/get-started/index.html`

**Interfaces:**
- Consumes: nested chrome; link out to GitHub `documentation/get-started.md`

- [ ] **Step 1: Create `site/get-started/index.html`**

Must include:
- Prerequisites: Python 3.10+, Databricks workspace / Apps, SQL Warehouse, Lakebase Autoscaling, UC Volume, `uv`, `psql`
- Commands:

```bash
git clone https://github.com/databrickslabs/ontobricks.git
cd ontobricks
uv sync --extra lakebase
cp .env.example .env
scripts/start.sh
```

- Link: `https://github.com/databrickslabs/ontobricks/blob/master/docs/get-started.md` (and note develop may differ)
- CTA back to GitHub

Use `<pre><code>` for commands (no syntax highlighter required).

- [ ] **Step 2: Verify content**

Run: `rg -n "uv sync|scripts/start.sh|get-started.md" site/get-started/index.html`

Expected: all three match.

- [ ] **Step 3: Commit** (when user approves commits)

```bash
git add site/get-started/index.html
git commit -m "$(cat <<'EOF'
feat(site): add get-started page

EOF
)"
```

---

### Task 6: Docs hub

**Files:**
- Create: `site/docs/index.html`

**Interfaces:**
- Produces: cards linking into GitHub Markdown under `documentation/` — not Sphinx HTML

- [ ] **Step 1: Create `site/docs/index.html`**

Include at least these cards (blob URLs on `master`):

| Label | URL |
|---|---|
| Get started | `https://github.com/databrickslabs/ontobricks/blob/master/docs/get-started.md` |
| Architecture | `https://github.com/databrickslabs/ontobricks/blob/master/docs/architecture.md` |
| Features | `https://github.com/databrickslabs/ontobricks/blob/master/docs/features.md` |
| API | `https://github.com/databrickslabs/ontobricks/blob/master/docs/api.md` |
| Deployment | `https://github.com/databrickslabs/ontobricks/blob/master/docs/deployment.md` |
| User guide | `https://github.com/databrickslabs/ontobricks/blob/master/docs/user-guide.md` |

Use `.doc-grid` / `.doc-card` anchors. Intro text: docs live in the repo; this hub points to them.

- [ ] **Step 2: Run docs-hub contract**

Run: `uv run pytest -q tests/units/site/test_marketing_site.py::test_docs_hub_links_github_markdown`

Expected: PASS once file exists with required strings.

- [ ] **Step 3: Commit** (when user approves commits)

```bash
git add site/docs/index.html
git commit -m "$(cat <<'EOF'
feat(site): add docs hub linking to GitHub Markdown

EOF
)"
```

---

### Task 7: About + 404

**Files:**
- Create: `site/about/index.html`
- Create: `site/404.html`

**Interfaces:**
- About consumes Labs disclaimer language from README Project Support
- 404 uses relative assets carefully (GitHub Pages serves 404 from site root)

- [ ] **Step 1: Create `site/about/index.html`**

Must include:
- Databricks Labs exploration-only / AS-IS disclaimer
- No formal SLAs; do not file Databricks support tickets for this project
- File issues on the GitHub repo
- Link to `https://github.com/databrickslabs/ontobricks/blob/master/CONTRIBUTORS.md`

Align wording with README “Project Support” section.

- [ ] **Step 2: Create `site/404.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Page not found — OntoBricks</title>
  <link rel="stylesheet" href="assets/css/site.css">
</head>
<body>
  <main class="site-main section">
    <h1>Page not found</h1>
    <p>That URL is not part of the OntoBricks site.</p>
    <p><a class="btn btn-primary" href="index.html">Back home</a></p>
  </main>
</body>
</html>
```

- [ ] **Step 3: Run about + full suite**

Run: `uv run pytest -q tests/units/site/test_marketing_site.py`

Expected: all PASS.

- [ ] **Step 4: Local smoke preview**

```bash
cd site && python -m http.server 8765
```

Manually open `http://127.0.0.1:8765/` and click through Home → Features → Get started → Docs → About; resize to check mobile nav; hit a missing path to see 404 only after Pages is configured (local server 404 differs).

- [ ] **Step 5: Commit** (when user approves commits)

```bash
git add site/about/index.html site/404.html
git commit -m "$(cat <<'EOF'
feat(site): add about page and custom 404

EOF
)"
```

---

### Task 8: README note, changelog, Pages cutover checklist

**Files:**
- Modify: `README.md` (Documentation / top section — add live site link)
- Create/append: `changelogs/v0.7.0/benoitcayladbx_YYYY-MM-DD.log`

**Interfaces:**
- Does not auto-change GitHub Settings (requires human or `gh` with admin). Plan documents exact cutover commands.

- [ ] **Step 1: Add README link**

Near the top (under the tagline badges) or in `### Documentation`, add:

```markdown
**Website:** [https://databrickslabs.github.io/ontobricks/](https://databrickslabs.github.io/ontobricks/)
```

- [ ] **Step 2: Append changelog**

Section title: `GitHub Pages marketing site`

Context: Static multi-page marketing site under `site/`; Pages source to switch from `/docs` to `/site` on `master`.

List created/modified files with short descriptions. Record test result of `uv run pytest -q -m "not scenario"` and the site unit file.

- [ ] **Step 3: Run full routine test suite**

Run: `uv run pytest -q -m "not scenario"`

Expected: 0 failures.

- [ ] **Step 4: Pages cutover (after `site/` is on `master`)**

Verify current config:

```bash
gh api repos/databrickslabs/ontobricks/pages
```

Switch source to `/site` on `master`:

```bash
gh api -X PUT repos/databrickslabs/ontobricks/pages \
  -f build_type=legacy \
  -f source[branch]=master \
  -f source[path]=/site
```

If the API shape rejects nested fields, use Settings → Pages in the GitHub UI: Source = Deploy from a branch, Branch = `master`, Folder = `/site`.

Confirm: `https://databrickslabs.github.io/ontobricks/` serves the new home (may take a minute).

- [ ] **Step 5: Commit docs/changelog** (when user approves commits)

```bash
git add README.md changelogs/v0.7.0/
git commit -m "$(cat <<'EOF'
docs: link GitHub Pages marketing site and changelog

EOF
)"
```

---

## Spec coverage (self-review)

| Spec requirement | Task |
|---|---|
| Static HTML in `site/`, no SSG/Actions | Tasks 1–7 |
| Switch Pages `/docs` → `/site` on `master` | Task 8 Step 4 |
| Pages: Home, Features, Get started, Docs, About, 404 | Tasks 3–7 |
| Shared chrome + relative paths | Tasks 3–7 + test_pages_use_relative_assets |
| Copy logo into `site/assets/img/` | Task 1 |
| Home hero + four-click pipeline | Task 3 |
| Features five topics | Task 4 |
| Get started prereqs + uv/start + docs link | Task 5 |
| Docs hub → GitHub Markdown | Task 6 |
| About Labs AS-IS | Task 7 |
| Visual tokens, no purple/cream defaults, expressive fonts | Task 1 CSS |
| Motion + prefers-reduced-motion | Tasks 1–2 |
| README live URL note | Task 8 |
| Manual smoke / optional contracts | Tasks 1, 7, 8 |

No TBD/placeholder steps remain. Nested path prefixes are explicit. Pages cutover is gated on `master` merge.
