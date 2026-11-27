# Intelligence

**Intelligence** is the workspace where you *learn from* and *experiment
with* a built knowledge graph. It never changes your source tables: Discovery
results are previews until you explicitly materialise them, and simulations
run on private copies.

This is the operator guide for every Intelligence page. The [User Guide](user-guide.md)
covers ontology, mapping and Graph; [Cohort discovery](advanced_features.md#cohort-discovery)
is the deep reference for writing cohort rules.

<!-- toc -->
**Contents**

- [Overview](#overview)
- [Home](#home)
- [Before you start](#before-you-start)
- [Configuration → Cohorts](#configuration--cohorts)
- [Discovery → Analytics](#discovery--analytics)
- [Discovery → Communities](#discovery--communities)
- [Discovery → Patterns](#discovery--patterns)
- [Patterns, Communities or Cohorts?](#patterns-communities-or-cohorts)
- [Simulation → Scenarios](#simulation--scenarios)
- [Simulation → Compare](#simulation--compare)
- [Run history](#run-history)
- [End-to-end example (energy & utilities)](#end-to-end-example-energy--utilities)
- [Limits and troubleshooting](#limits-and-troubleshooting)
<!-- /toc -->

## Overview

The left sidebar has three groups. Opening Intelligence from the app switcher
always lands on **Home**.

| Group | Page | Business question it answers | Writes to the Original Graph? |
|-------|------|------------------------------|---------------------------|
| *(landing)* | **Home** | *Where do I start in this workspace?* | Never |
| **Configuration** | **Cohorts** | *Which entities belong together according to rules I define?* | Only when you **Materialise** a cohort run |
| **Discovery** | **Analytics** | *Which entities matter most structurally (influence, bridges, density)?* | Never (reads the mapped graph; results cached in the registry) |
| **Discovery** | **Communities** | *Which entities are tightly connected to each other?* | Only when you **Materialize** the partition |
| **Discovery** | **Patterns** | *Which entities look alike (same kind of neighbourhood and profile)?* | Never (saved as a [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) snapshot) |
| **Simulation** | **Scenarios** | *What would change if these facts were true?* | Never (private sandbox per scenario) |
| **Simulation** | **Compare** | *How does a scenario differ from today's Original Graph, and what does it mean for the business?* | Never |

**Common to every Intelligence page.**

- **How it works tab.** Communities, Patterns and Scenarios each have two tabs:
  the working view and **How it works**. How it works explains the feature in
  business language with diagrams and an energy & utilities example (customers,
  contracts, meters, sites), followed by a collapsed **Technical details**
  section with the exact algorithm.
- **Graph ready.** Most Intelligence page titles include the same badge as
  Graph: *Building…*, *Graph ready · \<backend\>*, or *Graph not built* with
  **Go to Build**.
- **Ontology discussion.** The chat-bubble button in the page header opens the
  domain discussion so you can comment on a finding with your team, optionally
  tagging an entity.
- **Background tasks.** Long operations (Detect, Materialize, scenario Run,
  Analytics) run in the background. You can leave the page; follow progress in
  the navbar **Background Tasks** hourglass and come back later.
- **Shareable URLs.** Sections and opened items are in the URL
  (`/intelligence/?section=patterns`, `?section=scenarios&simulation=<id>`),
  so a refresh or a shared link reopens the same view.
- **Timestamps.** Dates and times in the UI use `YYYY/MM/DD HH:MM:SS` in the
  browser's local time zone (for example `2026/11/05 07:36:01`). The JSON API
  still returns ISO-8601.

## Home

The Intelligence **Home** page is a directory of tiles grouped like the
sidebar: **Configuration**, **Discovery**, **Simulation**. Each tile opens
that page. The **?** on a tile (and the header **Help** button) opens this
guide in the Help Center. Resources at the bottom open the
[Starter Guide](getting-started.md#first-steps-in-ontobricks) (in-app
Help → Starter Guide), this operator guide, and the Step-by-Step Workflow.

Suggested first visit after a Build: Analytics → Communities → Patterns,
then a Scenario from Explorer **Simulate from here**.

## Before you start

Intelligence needs:

1. A domain saved to the registry.
2. A built knowledge graph (**Graph → Management → Build**).
3. For **Analytics**, **Communities** and **Patterns**: the *Compute large-graph
   metrics on Databricks* toggle in **Settings → Global**, and the matching
   Databricks jobs deployed (`make deploy`). When a prerequisite is missing the
   page says why and the action button stays disabled.

Simulation works on the **[Lakebase](https://docs.databricks.com/aws/en/oltp/)**, **Lakehouse (Delta)** and **Neo4j** graph
backends. Analytics, Communities and Patterns always run as serverless
Databricks jobs over the mapped `…_data` snapshot (not over inferred triples).

## Configuration → Cohorts

**In plain words.** A cohort is a group of entities that you define with
explicit, explainable rules, for example *customers who share the same
substation and the same tariff family*. Unlike Communities and Patterns, you
decide the grouping logic; OntoBricks applies it consistently.

The page has two tabs:

- **Design** — pick the target class, the linking paths (which relationships
  connect members), the compatibility filters and the grouping type, then
  **Save**. You can also describe the rule in natural language and let the
  assistant draft it.
- **Runs** — pick a saved rule, **Preview** the groups it finds on the live
  graph, and **Materialise** them as `:inCohort` triples and/or a Unity
  Catalog table. **Outputs** configures those targets. **Purge**
  removes materialised cohort and reasoning triples.

**Example.** Target class *Customer*, linking path `Customer → holds → Contract
→ servedBy → Substation`, group by substation. Preview might show *North-7*
(412 customers) and *South-2* (288 customers). Materialise writes
`:inCohort` so Explorer, Chat and a Simulation **cohort** scope can reuse
those groups.

Saved cohorts are reused as a **scenario scope** in Simulation. Full
authoring reference: [Cohort discovery](advanced_features.md#cohort-discovery).

## Discovery → Analytics

**In plain words.** Analytics scores every entity in the mapped graph for
*who matters structurally*: global influence, bridges, raw connectivity,
reachability and local density. It never writes to the graph.

Open **Intelligence → Discovery → Analytics**. The page has **Analytics** and
**Runs** tabs. The Analytics tab always shows the **last** result for this
domain version (a "Last computed …" line). Past runs live on **Runs**.

### Run an analysis

1. Click **Run Analysis**. The **Analysis scope** dialog opens.
2. Optionally pick an **entity type** (for example *Customer*). The default is
   the full graph. Scope resets every time the dialog opens.
3. Confirm. The job runs in the background. When it finishes, the **Dashboard**
   shows six KPI tiles (Nodes, Edges, Components, Avg Degree, Density, Elapsed),
   a distribution strip of five histograms, and a ranking chart.

**Example.** On an energy graph, PageRank ranks substations and large
industrial customers first; betweenness highlights meters that sit between
two grid zones. Restricting scope to *Customer* still scores the connected
subgraph, then charts only Customer instances.

### Reading the Dashboard

| Metric | What it measures |
|--------|-----------------|
| **PageRank** | Global influence — nodes pointed to by other influential nodes |
| **Betweenness** | Bridging role — nodes on many shortest paths (Brandes–Pich estimate) |
| **Degree** | Raw connectivity |
| **Closeness** | Reachability (Brandes–Pich estimate) |
| **Clustering** | Local density — neighbours that are also connected to each other |

Click a histogram tile or the segmented control to switch the ranking chart.
Click a ranking point to open that entity in Explorer. **Top N** only trims
the detail table, not the chart. **Interpret** (needs an LLM on the domain)
writes Key Findings, Notable Entities and Recommendations; **Add to audit
trail** posts that commentary on the domain version.

**Data Model Health** flags entity types that look flat (no relationships, or
a single relationship across many instances) — candidates to exclude from the
sync.

Chart internals, Brandes–Pich pivots, view-only Lakehouse snapshots and the
series API: [User Guide — Analytics](user-guide.md#analytics-intelligence--discovery).

## Discovery → Communities

**In plain words.** Communities finds *clusters*: groups of entities that are
much more connected to each other than to the rest of the graph, like
neighbourhoods in a city. Nobody defines the groups; they emerge from the
links.

*Energy example:* sites, meters and customers fed by the same substation end
up in one community (*North grid*), another region forms another (*South
grid*), without anyone listing which site belongs where.

**How groups form.** Every entity starts in its own group. In each round,
every entity adopts the group that is most common among its neighbours. Groups
spread through densely linked areas and stop at the few links between areas.
When nothing changes any more, the groups are final.

### Using the page

1. Optionally choose a **Scope** (one entity type, for example only *Site*);
   the default is all entity types. The algorithm is *Label propagation
   (Databricks)*.
2. Press **Detect**. The job runs in the background on Databricks.
3. Read the **Preview**: size statistics, number of communities, modularity,
   rounds, whether the run converged, and a graph of the largest communities
   (capped at 1,500 nodes). Click a node to page through the entities of that
   community.
4. Press **Materialize** to write the partition into the knowledge graph
   (`Cluster` individuals and `:inCluster` membership). This replaces the
   previous partition. The **Materialized (Explorer)** card shows what is
   currently in the graph.
5. Open **Explorer** to colour entities by community and collapse a community
   into a single super-node. **Purge** removes the materialised
   partition (the last Detect preview is kept).

**Reading the results.**

- **Modularity** is a quality score: the higher it is, the more clearly the
  groups stand apart. Use it to compare runs, not as a pass/fail threshold.
- **Not converged** means the run hit the round limit while a few entities
  were still switching; the result is usable and flagged, not failed.
- **Typical uses:** natural operating areas (grid zones, field-team
  territories), entities that bridge two areas, small disconnected islands
  that reveal missing links in the data.

**Under the hood.** Label propagation runs on a serverless Databricks job over
the domain's `…_data` snapshot. The update is *semi-synchronous* (half of the
entities update per round, chosen by a hash) so bipartite graphs such as
Customer → Contract → Meter do not oscillate; ties break on the smallest
label so runs are deterministic. Communities are ranked by size into ids
`c-1`, `c-2`, …

## Discovery → Patterns

**In plain words.** Patterns finds entities that *look alike*: the same kind
of connections and similar characteristics. Think of typical profiles
discovered automatically.

*Energy example:* among 10,000 customers, Patterns might find that 3,200 have
a residential contract, a smart meter and high consumption, while 900 have a
business contract with several meters. Each group is a pattern, and you can
list the customers behind it.

### How it works, step by step

1. **Pick what to profile**: one entity type, for example *Customer*.
2. **Look around each one**: OntoBricks follows **outgoing** relationships up
   to *Hops* steps away (contracts, their tariff, sites, meters…). Inbound
   links (something pointing *at* an entity) are not walked.
3. **Describe in simple terms**: numbers become ranges (a consumption of
   4,812 kWh becomes *bin 7/8*, i.e. high), short category lists are kept
   (*residential*, *business*), unique identifiers (ids, emails, dates) are
   ignored so they cannot make every customer unique.
4. **Group look-alikes**: customers with the same description share a
   pattern; the number of customers in it is its **support**.
5. **Simplify automatically**: if almost every customer is still unique,
   finer details are dropped step by step until at least half of the
   customers belong to a pattern.

Very high-degree hubs are not walked *through* during Detect — they still
appear as neighbours, but expansion stops there so a hub cannot pull in every
entity behind it.

### Detect settings

**New** on the Patterns list opens the *Detect patterns* dialog. Each field
has an (i) tooltip.

| Setting | Default | Range | Meaning |
|---------|---------|-------|---------|
| **Entity** | — (required) | any class | The entity type to profile (the *anchor*). |
| **Hops** | 3 | 1–5 | How far to look around each entity. 2–3 is usually enough. |
| **Numeric bins** | 8 | 2–32 | How finely numbers are cut into ranges (quantiles). |
| **Min support** | 3 | ≥ 1 | Smallest group size worth showing. |
| **Refine top-N** | 0 (off) | ≥ 0 | Merges near-identical top-N patterns that differ only by relationship counts. |

### Reading a pattern

Open a saved run (or let **Detect** open it), pick a pattern in the picker:

- **Run KPIs**: **Hops**, **Entities** (how many instances of the chosen type
  were in the run), **Patterns** (how many neighbourhood profiles were found),
  **Coverage** (share of that type population that follows at least one
  pattern). Older snapshots saved before population was persisted show
  Coverage as *—*.
- **Bar chart** (collapsible): one bar per pattern, X = Patterns, Y =
  Matching entities. Each bar is labelled with its share of the type
  population.
- **Detail** badge: *Exact*, *Neighbours (1 hop)*, *Own attributes* or
  *Structure only* — the level that covered at least half of the anchors.
- **Diagram**: the anchor type on the left, then columns for 1 hop, 2 hops, …
  Each box is one entity type at that hop; badges are attribute ranges;
  arrows are relationships with typical counts (hover for min / max / count).
  Hover a box to highlight its path.
- **Matching entities**: dialog listing the entities that share the pattern,
  with links to Graph Explorer.

### Saved runs

Every Detect run is saved automatically to [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) (table
`graph_pattern_saved_<slug>`) under a name built from the entity type and the
local date/time (for example `Customer — 2026/11/14 14:06:32`). The snapshot
stores the pattern catalog, cardinalities and diagrams — not every member
row. Matching entities follow the latest Detect output while those pattern
ids are still current. Run **Detect** again to capture a new result.

The list uses the same card layout as Scenarios. **New** opens the details
pane, auto-saves the run, and opens that snapshot. Opening a card later
restores the catalog and diagrams from save time.

**Typical uses:** customer or asset segmentation, spotting unusual set-ups
(rare patterns), checking data quality (an entity type that should look
uniform but splits into many patterns), and seeding cohort rules or scenario
ideas.

## Patterns, Communities or Cohorts?

| You want to… | Use |
|--------------|-----|
| Find **who looks alike** (similar profiles, even if never connected) | **Patterns** |
| Find **who is connected together** (clusters of linked entities, whatever their profile) | **Communities** |
| Group entities with **rules you define and can explain** | **Cohorts** |
| Test **what would happen if** something changed | **Scenarios**, then **Compare** |

They combine well: a rare pattern or a bridging community member is a good
starting point for a scenario; a cohort is a ready-made scenario scope.

## Simulation → Scenarios

**In plain words.** A scenario is a safe *what if*. You change a few facts on
a copy of part of your knowledge graph, let your business rules react, and
see what would change — real data is never touched.

*Energy example:* "What if customer C-102 switched its contract to the Green
tariff?" In the scenario, your rules may flag C-102 as *eligible for the
green rebate* and drop its *Standard plan* discount; you see both effects
before deciding anything in real life.

### Kinds of scenarios

Every scenario combines a **scope** and one or more **facts**:

| Scope | Example |
|-------|---------|
| **Entity neighbourhood**: one entity and what is up to *N* hops around it (default 2) | Customer C-102 with its contracts, sites and meters |
| **Cohort**: every member of a saved Cohort rule | All customers fed by substation North-7 |

| Fact | What it does | Example |
|------|--------------|---------|
| **Set** | Replaces every value of a property | Contract K-77 tariff = *Green* (was *Standard*) |
| **Add** | Adds a value or a link, keeping existing ones | Customer C-102 *holds* new contract K-90 |
| **Remove** | Deletes one exact value or link | Meter M-55 no longer *installed at* site S-12 |

Common questions: tariff or plan changes, asset decommissioning, consumption
shocks, new contracts or bundles, detaching claims or incidents.

### How a scenario runs

A simulation never edits your knowledge graph. Each run works on a private
copy of a slice of the graph, called a **sandbox**:

```
Live knowledge graph ──(read only)──▶ 1. Scope ──▶ 2. Copy into sandbox
                                                          │
                     5. Compare ◀── 4. Reason ◀── 3. Apply your facts
```

1. **Scope**: an entity and its neighbours up to *N* hops, or the members of a
   cohort rule. The Original Graph is only read in this step.
2. **Copy into sandbox**: every triple whose subject is one of those entities
   is copied into a separate table named `<graph>__sim_<scenario id>` (Delta
   for Lakehouse, or an isolated marker-labelled graph for Neo4j). Overlay
   keys (`<graph>__smp_<scenario id>`) remember every `(subject, predicate)`
   the sandbox has owned, including predicates emptied by Remove.
3. **Apply your facts** in the order you listed them, inside the sandbox only.
4. **Reason**: the phases you ticked run against the sandbox — the same
   engines as Graph → Inference (Simple rules, graph reasoning, Complex
   rules, OWL 2 RL). Conclusions are written into the sandbox too.
5. **Compare**: the final sandbox is the **after** picture versus the copy
   (**before**). Added triples are tagged *your fact* or *inferred* (hover
   for the rule). Removed triples are listed separately.

Once a scenario is **ready**, go to [Simulation → Compare](#simulation--compare)
to overlay it on today's Original Graph.

**Isolation.** The sandbox store refuses to write to any table that is not a
simulation sandbox. Re-running a scenario rebuilds its sandbox from the
current Original Graph. Deleting a scenario drops its sandbox. **Purge** drops the
sandbox and keeps the scenario as a draft.

**Limits.** Scope is capped at 5,000 entities and 250,000 copied triples. A
larger neighbourhood or cohort fails the run with a message instead of being
silently cut. Result lists keep the first 2,000 rows; counts are always
exact.

**What a slice means for rules.** Rules only see what was copied. Each
hypothetical entity that was not already in the scope is copied from live
with one neighbour hop (so an extra contract brings its meters). If IF-THEN
is ticked and the rules cannot run, the run **fails** instead of finishing as
ready with zero inferences. A rule that needs a global aggregate will not
fire on a neighbourhood slice — widen the hops or use a cohort scope.

### Scenarios list

Lists the saved scenarios of the active domain version with their scope,
number of facts, status (`draft`, `running`, `ready`, `failed`) and the
added / removed counts of the last run. Specs and run history are stored in
the domain registry. Creating a **new domain version** copies the scenarios
(as drafts) and their run history; sandboxes are not copied — run again on
the new version.

**New scenario** (or Explorer **Simulate from here**) opens the editor on the
same page (`?section=scenarios&simulation=` the scenario id). The editor is
not a separate sidebar item. The **How it works** tab next to the list
explains scopes, facts, reasoning, Results, Compare and Evaluate.

### Scenario editor

The editor has four tabs. **Save** and **Run** stay in the page header.

1. **General** — name, description, graph slice, then the sandbox objects
   the run creates. After a run, item counts appear under the table.
   - *Entity neighbourhood*: an entity URI and hops (default 2). The search
     button opens the same live-graph picker as the fact wizard.
   - *Cohort*: one of the saved Cohort rules.
2. **Hypothetical facts** — compact cards: **Set** / **Add** / **Remove**,
   then subject, predicate and object. **Add fact** or **Edit** opens the
   fact wizard (entity type → predicate → object). **Describe the facts**
   plus **Propose facts** lets an agent look up classes, properties and
   live-graph entities, fill the cards, and rewrite **General →
   Description**. Review both before you save or run. Ontology predicate
   URIs are resolved to the mapped data predicate when the sandbox applies
   the fact. A relationship object that is only a local id (`ECON00018`) is
   expanded to the instance URI at run time.
3. **Reasoning** — the same phase tree as Graph → Inference, in execution
   order: T-Box (OWL 2 RL), Simple rules, Graph reasoning, then Complex
   rules. Simple rules and graph reasoning are on by default. Expand Simple
   or Complex rules to tick individual rules. Rule names are links to the
   editors (read-only without ontology edit rights).
4. **Results** — KPIs (entities in scope, triples before / after, new
   inferences, removed triples, affected entities) and two paged tables:
   **Added** (tagged *your fact* or *inferred*) and **Removed**. Subject and
   object URIs open Explorer on this sandbox. **Explorer** opens Graph
   Explorer on this sandbox (same as picking the scenario in Explorer's
   Graph dropdown). A `?simulation=` link always selects that sandbox.

**Run** saves the scenario and starts it as a background task. Follow it in
the navbar **Background Tasks** hourglass and on the workbench progress bar
(scope, seed, facts, reasoning, diff). When it finishes, open **Results** or
the clipboard icon on the Scenarios list.

Tip: in Explorer, the entity details panel has **Simulate from here**, which
opens the editor pre-filled with that entity's neighbourhood and an empty
*Set* fact.

Explorer, Query (SPARQL / GraphQL) and Chat share a **Graph** dropdown:
live inferred, live asserted, and one entry per ready sandbox. API, MCP and
Explorer always read that overlay (`simulation_id` on the public endpoints).

## Simulation → Compare

**In plain words.** Results (in the scenario editor) show what changed
*inside* the sandbox, before vs after your facts. **Compare** answers:
*how would our real graph, as it is today, differ if this scenario happened
— and what does that mean for the business?*

**Header.** Pick the **Scenario** (only **ready** scenarios are listed) and
the **Original Graph**:

- *Certified Graph*: today's data **including** reasoning conclusions.
- *Original Graph (asserted only)*: the raw mapped data only.

### Graphs tab

When a ready scenario is selected, OntoBricks loads the overlay automatically
(press **Refresh** to reload). With no ready scenario, the empty state stays
visible. The overlay uses:

- **Nodes:** grey unchanged, amber modified (attributes, types or links),
  green added, red removed.
- **Links:** grey in both, green added in the sandbox (fact or inferred —
  click to see which rule), red present in live only.

**Metrics** shows exact counts. Click a node or a link for details. The
drawing is limited to about 400 entities and 1,500 links, changes first;
counts are always exact. The share button opens the sandbox in Explorer.

### Evaluate tab

Type a business question in **Prompt** (for example *"Which customers are
eligible for the green rebate?"*) and press **Ask both**. Graph Chat answers
twice, side by side: once on the Original Graph and once on the sandbox. When
both answers are ready, **Business insights** comments on the difference.
**Open report** builds a printable summary (facts, Compare metrics and graph
if the overlay already loaded, both answers and the analysis).
**Export PDF** opens the browser print dialog (choose *Save as PDF*).

### Getting the most out of Compare

1. **Run the scenario first**; a draft or failed scenario is not listed.
2. **Start with the Graphs tab** to see the size and shape of the impact.
3. **Click the green links** to check which rule produced each conclusion.
4. **Switch to *asserted only*** to separate facts from inferred knock-ons.
5. **Ask the business question in Evaluate**, then **Open report**.
6. **Re-run when live data moved.** Compare uses today's Original Graph; the
   sandbox reflects the moment of the run.
7. **Compare alternatives** by creating one scenario per option (for example
   *Green tariff* vs *Time-of-use*) and evaluating the same question on each.

## Run history

Producer pages keep the last result; full history lives on **Runs** tabs and
in Settings.

| Surface | What it lists |
|---------|----------------|
| **Intelligence → Discovery → Analytics → Runs** | Every analysis launched from Analytics: date & time, scope, version, status, nodes, edges, components, avg degree, density, duration, **Details** |
| **Intelligence → Configuration → Cohorts → Runs** | Preview and materialise of saved cohort rules (not the Settings Detect ledger) |
| **Intelligence Home** | Directory of tiles; not a run ledger |
| **Settings → Automation → Runs** (admin) | Build, analytics, **community**, **pattern** and **simulation** history across **every** domain, with a Domain filter (default *All domains*) and paging (25 / 50 / 100) |

**Community** and **Pattern** Settings tabs list every Detect run that
**completed** or **failed**, newest first: scope (entity type or anchor),
version, status, counts (nodes/communities, or anchors/patterns), duration.
**Details** shows parameters, counts, error text if the run failed, and a
link to the Databricks job run. Materialize and Save are not listed.

**Simulation** Settings tab lists every scenario run on the domain version:
start time, scenario, status, outcome, duration and who ran it. History is
copied when you branch a new version.

Both the per-page Runs tabs and Settings read the same registry tables, so a
run shows up in both.

## End-to-end example (energy & utilities)

1. **Patterns** on *Customer* (Hops 2) reveals a pattern of 900 business
   customers with several meters and a *Standard* tariff.
2. **Communities** shows most of them sit in the *North grid* community.
3. A **Cohort** rule *business customers on substation North-7* turns that
   insight into an explicit, reusable group.
4. A **Scenario** with the cohort as scope **Sets** their tariff to *Green*
   and ticks the IF-THEN rules; the run flags 640 customers as *eligible for
   the green rebate* and removes 410 *Standard plan* discounts.
5. **Compare → Graphs** confirms the impact is limited to the cohort and
   its contracts; **Evaluate** asks *"What is the yearly rebate cost?"* on
   both graphs, and **Open report** produces the PDF for the pricing team.

## Limits and troubleshooting

| Symptom | Cause | What to do |
|---------|-------|-----------|
| Detect / Run Analysis is disabled | Domain not built, or *Compute large-graph metrics on Databricks* is off, or the job bundle is not deployed | Build the graph; enable the toggle in Settings → Global; `make deploy` |
| Patterns run shows *Structure only* | Profiles were too unique at finer levels | Lower **Numeric bins** or **Hops**, or accept the structural view |
| Very few patterns | **Min support** too high | Lower it to surface rarer profiles |
| Communities *not converged* | Round limit reached | Usable as is; Detect again after data changes |
| Scenario run fails on scope size | More than 5,000 entities or 250,000 triples | Reduce hops or use a smaller cohort |
| Scenario ready but no inferences | Rules need data outside the slice | Widen hops or use a cohort scope; check the ticked rules |
| Scenario missing from Compare | Not **ready** (draft, running, failed or purged) | Run it again |
| Compare shows unexpected red/amber items | The Original Graph changed since the run | Re-run the scenario, then compare again |
| Analytics *Not computed for this run* on betweenness/closeness | BFS hit the depth cap | Raise `ONTOBRICKS_ANALYTICS_JOB_MAX_DEPTH` and re-run, or treat PageRank/Degree as the ranking |
