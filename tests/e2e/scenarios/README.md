# Scenario campaign (live E2E integration)

These suites are a **single, ordered journey** over one durable domain
(`test_scenario_1`) against a **running** OntoBricks app. Unlike the unit/mcp
suites they are stateful, order-dependent, billable (warehouse + LLM), and
mutate the registry the app reads — so they are **opt-in** (`-m scenario`,
gated by `ONTOBRICKS_SCENARIO_LIVE=1`) and excluded from routine/agent runs.

## The journey

| Order | File | Role |
| --- | --- | --- |
| 1 | `test_scenario_01_generate_live.py` | Import → generate ontology → automap → **build KG** → publish **V1**. Creates the domain. |
| 2 | `test_scenario_02_collab_lifecycle.py` | Comments, tasks, review lifecycle on V1; branches a **DRAFT V2**. |
| 3 | `test_scenario_03_rules_analysis.py` | SHACL + business rules + reasoning + analysis on **V2**; builds V2's graph. Leaves V1 untouched. |
| … | *(future `test_scenario_NN_*.py`)* | New steps slot in here, before validation. |
| last | `test_scenario_validation.py` | **Read-only.** Asserts the cumulative state of every prior scenario and emits a report. |

Files are **zero-padded** so lexical collection order == run order even past 9.

## Running

```bash
# whole campaign against the local dev server (start it first: scripts/start.sh)
make scenario-campaign

# against a deployed / isolated instance
make scenario-campaign ONTOBRICKS_LIVE_BASE=https://<app-url>

# a single step, iteratively, against an already set-up registry
ONTOBRICKS_SCENARIO_LIVE=1 uv run pytest \
  tests/e2e/scenarios/test_scenario_03_rules_analysis.py -v -s --no-cov
```

`make scenario-campaign` preflights `/health`, then runs every `-m scenario`
suite in order and writes reports to `artifacts/scenarios/`:

- `campaign.xml` — JUnit (CI)
- `campaign.html` — self-contained HTML (open in a browser)
- `campaign_report.md` — the validation summary (written by the last suite)

## Environment variables

| Var | Default | Effect |
| --- | --- | --- |
| `ONTOBRICKS_SCENARIO_LIVE` | *(unset)* | **Required.** `1` un-skips the suites. |
| `ONTOBRICKS_LIVE_BASE` | `http://localhost:8000` | Target app base URL. |
| `ONTOBRICKS_SCENARIO_BASE` | *(unset)* | Alternate base override (falls back after `LIVE_BASE`). |
| `ONTOBRICKS_SCENARIO_CHAIN` | *(unset)* | `1` enables cross-file dependency chaining (set by the make target — see below). |
| `ONTOBRICKS_SCENARIO_REPORT` | `artifacts/scenarios/campaign_report.md` | Where the validation Markdown report is written. |
| `ONTOBRICKS_SCENARIO_CATALOG` / `_SCHEMA` | *(scenario default)* | Source tables scenario 1 imports from. |

The make recipe inherits your shell environment, so exporting any of the above
before `make scenario-campaign` works.

## Dependency chaining

With `ONTOBRICKS_SCENARIO_CHAIN=1` (set automatically by `make
scenario-campaign`) the suites use `pytest-dependency`: if scenario 1 fails or
is skipped, 2/3/validation are **skipped** instead of producing a confusing
cascade of independent failures. Wiring lives in `_harness.chain_marker`.

Chaining is **off by default** so that running a single suite in isolation is
never blocked by the run-based dependency — each suite still has its own
data-driven prerequisite skips (it checks the registry for what it needs).

## Isolation (avoid clobbering a shared registry)

The scenarios write a **durable** domain to whatever registry the *target app*
is configured with (catalog / schema / volume / Lakebase). The tests cannot
pick the registry — the running app owns that config. To keep a campaign off a
shared or production registry, **point the campaign at an app instance backed
by a disposable registry**:

1. Provision a throwaway catalog/schema (+ Lakebase branch) for the run.
2. Deploy or start an app configured against it (see `deploy.config.sh` /
   `app.yaml`).
3. `make scenario-campaign ONTOBRICKS_LIVE_BASE=<that-app-url>`.
4. Tear the registry down afterwards.

Against the shared int workspace, treat a campaign run as mutating and
coordinate with others.

## Adding a new scenario

1. Create `test_scenario_NN_<slug>.py` (zero-padded `NN`, next number before
   validation).
2. Import shared helpers/fixtures from `_harness` (`base_url`, `csrf_headers`,
   `json_body`, `make_step`, `poll_task`, `chain_marker`) — don't re-implement
   them; the `scenario_base` / `scenario_page` fixtures come from `conftest.py`.
3. Gate + chain it:

   ```python
   pytestmark = [
       pytest.mark.skipif(
           os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
           reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run",
       ),
       *chain_marker("scenario_N", depends=("scenario_<prev>",)),
   ]
   ```

4. Add its `name` to the validation suite's `chain_marker(..., depends=(...))`
   and extend `test_scenario_validation.py` with checks for whatever assets it
   produced.
