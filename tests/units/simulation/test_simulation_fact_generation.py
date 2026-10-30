"""Apply generated hypothetical facts and check simulation result tags.

These tests take patches the facts agent (or the eval dataset) would
propose, apply them to a sandbox, and assert the Results diff classifies
them as patch-origin — the check operators use after a run.
"""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from back.objects.simulation import Diff, Patch, Sandbox
from tests.units.agents.test_agent_simulation_facts import (
    _PATCH,
    _llm_full_generation,
    _run,
)

pytestmark = pytest.mark.unit

DATASET = (
    Path(__file__).resolve().parents[2]
    / "eval/datasets/agent_simulation_facts/baseline.jsonl"
)


class _SandboxStore:
    def __init__(self, triples):
        self.rows = {Diff.key(t) for t in triples}

    def insert_triples(self, table, triples):
        for t in triples:
            self.rows.add(Diff.key(t))
        return len(triples)

    def delete_triples(self, table, triples):
        for t in triples:
            self.rows.discard(Diff.key(t))
        return len(triples)

    def delete_subject_predicate(self, table, pairs):
        for pair in pairs:
            self.rows = {
                r for r in self.rows if (r[0], r[1]) != (pair["subject"], pair["predicate"])
            }
        return 1

    def query_triples(self, table):
        return [
            {"subject": s, "predicate": p, "object": o} for s, p, o in self.rows
        ]


def _sandbox(triples):
    sandbox = object.__new__(Sandbox)
    sandbox.table = "g__sim_s1"
    sandbox.store = _SandboxStore(triples)
    return sandbox


def _dataset_rows():
    return [
        json.loads(line)
        for line in DATASET.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _apply_generated(before, patches):
    sandbox = _sandbox(before)
    parsed = [Patch.from_dict(raw) for raw in patches]
    asserted = sandbox.apply(parsed)
    diff = Diff.compute(before, sandbox.triples(), patch_added=asserted)
    return asserted, diff


class TestGeneratedFactsCheckResults:
    def test_agent_generated_set_is_tagged_patch_in_results(self, monkeypatch):
        result = _run(monkeypatch, _llm_full_generation())
        before = [
            {
                "subject": _PATCH["subject"],
                "predicate": _PATCH["predicate"],
                "object": "710",
            }
        ]
        asserted, diff = _apply_generated(before, result.proposed_patches)
        summary = diff["summary"]
        assert summary["added_patch_count"] == 1
        assert summary["added_inferred_count"] == 0
        assert summary["removed_count"] == 1
        added = diff["added"][0]
        assert added["origin"] == "patch"
        assert added["object"] == "420"
        assert Diff.key(added) in asserted

    def test_eval_set_score_fact_replaces_prior_value(self):
        row = next(r for r in _dataset_rows() if r["id"] == "set-score-001")
        patch = row["expected"]["patches"][0]
        before = [
            {
                "subject": patch["subject"],
                "predicate": patch["predicate"],
                "object": "710",
            }
        ]
        _, diff = _apply_generated(before, [patch])
        assert diff["summary"]["added_patch_count"] == 1
        assert diff["added"][0]["object"] == "420"
        assert diff["removed"][0]["object"] == "710"

    def test_eval_add_link_fact_is_extra_triple(self):
        row = next(r for r in _dataset_rows() if r["id"] == "add-contracts-001")
        patch = row["expected"]["patches"][0]
        before = [
            {
                "subject": patch["subject"],
                "predicate": "https://ex.com/onto/name",
                "object": "Jacob",
            }
        ]
        _, diff = _apply_generated(before, [patch])
        assert diff["summary"]["added_count"] == 1
        assert diff["summary"]["removed_count"] == 0
        assert diff["added"][0]["origin"] == "patch"
        assert diff["added"][0]["object"].endswith("ACME-1")

    def test_eval_remove_fact_clears_triple_from_results(self):
        row = next(r for r in _dataset_rows() if r["id"] == "remove-employment-001")
        patch = row["expected"]["patches"][0]
        before = [
            {
                "subject": patch["subject"],
                "predicate": patch["predicate"],
                "object": patch["object"],
            }
        ]
        _, diff = _apply_generated(before, [patch])
        assert diff["summary"]["added_count"] == 0
        assert diff["summary"]["removed_count"] == 1
        assert diff["removed"][0]["origin"] == "patch"

    def test_eval_multi_op_fact_splits_add_and_remove(self):
        row = next(r for r in _dataset_rows() if r["id"] == "multi-op-001")
        patches = row["expected"]["patches"]
        before = [
            {
                "subject": patches[0]["subject"],
                "predicate": patches[0]["predicate"],
                "object": "700",
            },
            {
                "subject": patches[1]["subject"],
                "predicate": patches[1]["predicate"],
                "object": patches[1]["object"],
            },
        ]
        _, diff = _apply_generated(before, patches)
        assert diff["summary"]["added_patch_count"] == 1
        assert diff["summary"]["removed_count"] == 2
        assert {r["object"] for r in diff["removed"]} == {"700", patches[1]["object"]}
        assert diff["added"][0]["object"] == "410"

    def test_generated_facts_do_not_steal_inferred_origin(self):
        patch = Patch.from_dict(_PATCH)
        before = [
            {
                "subject": patch.subject,
                "predicate": patch.predicate,
                "object": "710",
            }
        ]
        sandbox = _sandbox(before)
        asserted = sandbox.apply([patch])
        inferred = {
            "subject": patch.subject,
            "predicate": "https://ex.com/onto/riskBand",
            "object": "high",
        }
        sandbox.store.insert_triples(sandbox.table, [inferred])
        risk_key = Diff.key(inferred)
        diff = Diff.compute(
            before,
            sandbox.triples(),
            patch_added=asserted,
            provenance={risk_key: "swrl:Risk"},
        )
        by_pred = {r["predicate"]: r for r in diff["added"]}
        assert by_pred[patch.predicate]["origin"] == "patch"
        assert by_pred[inferred["predicate"]]["origin"] == "inferred"
        assert by_pred[inferred["predicate"]]["provenance"] == "swrl:Risk"
        assert diff["summary"]["added_patch_count"] == 1
        assert diff["summary"]["added_inferred_count"] == 1


def test_skip_existing_eval_row_produces_no_result_rows():
    row = next(r for r in _dataset_rows() if r["id"] == "skip-existing-001")
    assert row["expected"]["patches"] == []
    _, diff = _apply_generated(
        [
            {
                "subject": "https://ex.com/Person/JacobMartin",
                "predicate": "https://ex.com/onto/creditScore",
                "object": "420",
            }
        ],
        row["expected"]["patches"],
    )
    assert diff["summary"]["added_count"] == 0
    assert diff["summary"]["removed_count"] == 0
