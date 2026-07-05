# Scenario validation report — `test_scenario_1`

**Result: PASS** — 13 passed, 0 failed, 2 info

## [Scenario 1] Generate + Auto-Map + Build (V1)

| Status | Check | Detail |
| --- | --- | --- |
| PASS | domain present in registry | test_scenario_1 |
| PASS | V1 exists in the registry | PUBLISHED |
| PASS | ontology has classes | 11 classes / 99 properties |
| PASS | mappings have SQL | 11 entity + 9 relationship mappings with SQL |
| PASS | V1 knowledge graph built | 52738 triples in test_scenario_1_V1 |

## [Scenario 2] Collaboration + Review lifecycle (V1)

| Status | Check | Detail |
| --- | --- | --- |
| PASS | comments added (>= 3) | 3 comment(s) |
| PASS | tasks created (>= 2) | 2 task(s) |
| PASS | review lifecycle ran (submitted → published) | approved, commented, published, reopened, submitted |
| PASS | V1 is PUBLISHED | PUBLISHED |
| PASS | new version branched (V2) | status=DRAFT |

## [Scenario 3] Rules + Quality + Reasoning + Analysis (V2)

| Status | Check | Detail |
| --- | --- | --- |
| PASS | V2 knowledge graph built | 33861 triples in test_scenario_1_V2 |
| PASS | SHACL data-quality shapes (>= 1) | 99 shape(s) |
| INFO | business rules present | total=11 (swrl=1, decision_tables=4, sparql=2, aggregate=4) |
| PASS | analysis recorded in audit trail | 1 analysis event(s) of 8 total |
| INFO | analysis write-up comment present | yes |
