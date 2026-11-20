"""Offline execution of one Evolve dataset row (no warehouse, no LLM).

Runs the real EvolutionService and EvolutionPipeline over the fixture domain
variant named by the row, using the in-memory source probe and registry.
Lifecycle rows (stale parent, cancel, interrupted retry) are driven through
the same service calls the API uses.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.errors import ConflictError, ValidationError
from back.objects.ontology.evolution import EvolutionPipeline, EvolutionService
from tests.fixtures.evolution.memory_probe import MemorySourceProbe, maybe_denied
from tests.fixtures.evolution.memory_registry import FakeRegistry
from tests.fixtures.evolution.variants import variant_document

FOLDER = "EvoSupplyChain"


def observe(row: Dict[str, Any]) -> Dict[str, Any]:
    variant = row["input"]["domain_variant"]
    registry = FakeRegistry({"1": variant_document(variant)})
    probe = MemorySourceProbe(denied=maybe_denied(variant) or ())
    svc = EvolutionService(registry, FOLDER)
    pipe = EvolutionPipeline(svc, probe)
    simulate = row["expected"].get("simulate")

    run = svc.create_run(
        "1",
        row["input"]["intent"],
        question=row["input"]["question"],
        owner="eval@example.com",
        idempotency_key=f"eval-{row['id']}",
    )
    if simulate == "worker_interrupted_then_retry":
        run = svc.update(run["id"], run["revision"], {"status": "RUNNING"})
        svc.recover_interrupted()
        run = svc.create_run(
            "1",
            row["input"]["intent"],
            question=row["input"]["question"],
            owner="eval@example.com",
            idempotency_key=f"eval-{row['id']}-retry",
        )
    run = pipe.assess(run["id"], run["revision"])
    coverage = run["coverage"]
    observed: Dict[str, Any] = {
        "ready": coverage["ready"],
        "states": coverage["states"],
        "gap": coverage["gap"],
        "proposal_ops": [],
        "validation_failures": [],
    }
    hint = coverage["decision_hint"]
    if hint != "PROPOSE":
        observed["decision"] = hint
        return observed
    try:
        run = pipe.propose(run["id"], run["revision"])
    except ValidationError:
        observed["decision"] = "ABSTAIN"
        return observed
    proposal = run["proposal"]
    observed["proposal_ops"] = [
        op["op"] for op in proposal["alternatives"][0]["operations"]
    ]
    observed["validation_failures"] = proposal["validation"]["failures"]
    observed["decision"] = "PASS" if proposal["validation"]["passed"] else "BLOCKED"

    if simulate == "parent_changed_before_accept":
        registry.documents["1"]["versions"]["1"]["ontology"][
            "description"
        ] = "edited by someone else"
        try:
            svc.ensure_parent_unchanged(run)
        except ConflictError:
            observed["decision"] = "STALE"
    elif simulate == "cancel_during_candidate_build":
        run = svc.update(run["id"], run["revision"], {"status": "RUNNING"})
        run = svc.cancel(run["id"], run["revision"])
        try:
            pipe.assess(run["id"], run["revision"])
        except ConflictError:
            observed["decision"] = "CANCELLED"
    return observed
