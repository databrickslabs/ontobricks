# Neo4j backend — manual smoke test

Manual procedure to validate the Neo4j graph DB engine end-to-end against a live Aura instance. Run this once before marking PR #47 ready-for-review.

## Prerequisites

- OntoBricks v0.5 deployed on a workspace with the `feature/neo4j-graphdb-skeleton` branch (or merged into `develop`).
- A reachable Neo4j Aura instance. Default for the OntoBricks team:
  ```
  URI:      neo4j+s://b4810af7.databases.neo4j.io
  Database: neo4j
  ```
  Credentials in `~/Documents/CODE/ontobricks/briefs/2026-05M-12/5/neo4j_connection_details.txt` (chmod 600, gitignored — do NOT commit).
- Neo4j Browser tab open at https://browser.neo4j.io/ logged into the same instance.

## Step 1 — Switch the engine

1. Open the deployed app: https://ontobricks-050-7474653167307611.aws.databricksapps.com
2. Navigate to **Settings → Triple store → Global**.
3. Confirm the **Graph DB Engine** dropdown lists both `Lakebase (Postgres)` and `Neo4j (Bolt)`.
4. Select **Neo4j (Bolt)** and click **Save** at the top.
5. **Expected:** save succeeds, banner says *"All settings saved"*.
6. **Screenshot:** dropdown + Save confirmation → save as `01-settings-dropdown.png`.

## Step 2 — Configure the connection

1. In the left menu, click **Triple store → Neo4j**.
2. Fill the form:
   - **Bolt URI:** `neo4j+s://b4810af7.databases.neo4j.io`
   - **Database:** `neo4j`
   - **Auth method:** Basic
   - **Username:** `neo4j`
   - **Password:** (from credentials file above)
   - **Encrypted:** checked (default)
3. Click **Save**.
4. **Expected:** banner says *"All settings saved"*. No error in `#neo4jTestResult`.
5. **Screenshot:** filled Neo4j section after save → `02-neo4j-section-saved.png`.

## Step 3 — Run a build against a small test domain

1. Open the existing test domain (`Cust360Auto V5` or any small ontology with a few classes + R2RML mappings).
2. Navigate to **Build** (or Digital Twin → Build).
3. Click **Build**.
4. **Expected:**
   - The build pipeline runs without exceptions.
   - The third stage (Graph DB) shows the Neo4j backend writing triples.
   - Final state: triples count > 0.
5. **Screenshot:** Build status page after successful run → `03-build-success.png`.

## Step 4 — Verify in Neo4j Browser

1. In Neo4j Browser (https://browser.neo4j.io/ connected to the Aura instance), run:
   ```cypher
   MATCH (t:Triple) RETURN labels(t) AS labels, count(t) AS cnt
   ```
2. **Expected:** at least one row with `labels` containing `:Triple:<sanitised_domain_name>` and `cnt > 0`.
3. Run a sample read:
   ```cypher
   MATCH (t:Triple) RETURN t.subject, t.predicate, t.object LIMIT 25
   ```
4. **Expected:** rows of `(subject, predicate, object)` matching what the source data + R2RML mapping should produce.
5. **Screenshot:** Neo4j Browser results pane → `04-neo4j-browser-triples.png`.

## Step 5 — Verify graceful degradation on Inference

1. In the app, navigate to **Digital Twin → Inference**.
2. Open any rule, click **Run**.
3. **Expected:** UI reports zero violations / zero inferences. No crash. The fastapi log should contain the warning `SWRLFlatCypherTranslator.build_violation_cypher: SWRL→Cypher translation is not implemented yet.`
4. **Screenshot:** Inference page showing 0 results → `05-inference-no-op.png`.

## Capture

Save the 5 screenshots in `~/Documents/CODE/ontobricks/briefs/2026-06-09/1/`. Then ping back and the PR description gets updated with embedded screenshots + this transcript link.

## Roll-back

If anything looks wrong:
1. Switch the dropdown back to `Lakebase (Postgres)` and Save.
2. Re-run the build → should write to Lakebase as before.
3. Lakebase is unchanged by this PR — full revert is just the engine switch.
