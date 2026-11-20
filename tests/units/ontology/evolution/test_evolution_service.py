"""Run lifecycle: explicit parent, idempotency, CAS, invalidation, cancel, stale parent."""

from __future__ import annotations

import pytest

from back.core.errors import ConflictError, NotFoundError, ValidationError
from back.objects.ontology.evolution import EvolutionService, Intent, RunStatus

from .fakes import FakeRegistry

pytestmark = pytest.mark.unit

INTENT = {
    "output_class": "Order",
    "mentions": ["orders", "products", "Acme"],
    "filters": [
        {"class": "Supplier", "attribute": "supplierName", "op": "eq", "value": "Acme"}
    ],
}


def _svc(registry=None):
    return EvolutionService(registry or FakeRegistry(), "EvoSupplyChain")


def _create(svc, key="k1", **over):
    kwargs = dict(
        question="Which orders contain products supplied by Acme?",
        owner="a@x",
        idempotency_key=key,
    )
    kwargs.update(over)
    return svc.create_run("1", INTENT, **kwargs)


class TestCreate:
    def test_records_parent_hash_and_normalised_intent(self):
        svc = _svc()
        run = _create(svc)
        assert run["created"] is True
        assert run["status"] == RunStatus.CREATED.value
        assert run["parent_version"] == "1"
        assert len(run["manifest"]["parent_content_hash"]) == 64
        assert run["intent"]["filters"][0]["op"] == "eq"
        assert run["intent"]["max_edges"] == 3

    def test_same_idempotency_key_returns_the_first_run(self):
        svc = _svc()
        first = _create(svc)
        again = _create(svc)
        assert again["created"] is False
        assert again["id"] == first["id"]
        assert len(svc.list_runs()) == 1

    def test_unknown_parent_version_is_not_found(self):
        with pytest.raises(NotFoundError):
            _svc().create_run("9", INTENT, question="q", owner="a", idempotency_key="k")

    @pytest.mark.parametrize("key", ["", "x" * 201])
    def test_idempotency_key_is_required_and_bounded(self, key):
        with pytest.raises(ValidationError):
            _create(_svc(), key=key)

    def test_unsaved_context_is_rejected(self):
        with pytest.raises(ValidationError):
            EvolutionService(FakeRegistry(), "")


class TestIntentValidation:
    @pytest.mark.parametrize(
        "bad",
        [
            None,
            {"filters": [{"attribute": "x"}]},
            {"max_edges": 4},
            {"max_edges": "lots"},
            {"aggregate": "sum"},
            {"mentions": ["m"] * 21},
        ],
    )
    def test_rejects_malformed_intent(self, bad):
        with pytest.raises(ValidationError):
            Intent.from_dict(bad)

    def test_keeps_unsupported_parts_for_reporting(self):
        intent = Intent.from_dict(
            {
                "output_class": "OrderLine",
                "aggregate": {"fn": "sum"},
                "select": ["Product.unitCost"],
            }
        )
        assert intent.aggregate == {"fn": "sum"}
        assert intent.select == ["Product.unitCost"]


class TestUpdates:
    def test_stale_revision_conflicts(self):
        svc = _svc()
        run = _create(svc)
        svc.update(run["id"], run["revision"], {"stage": "coverage"})
        with pytest.raises(ConflictError):
            svc.update(run["id"], run["revision"], {"stage": "again"})

    def test_new_intent_clears_derived_results(self):
        svc = _svc()
        run = _create(svc)
        run = svc.update(
            run["id"],
            run["revision"],
            {"coverage": {"ready": False}, "status": "ASSESSED"},
        )
        run = svc.update_intent(
            run["id"], run["revision"], dict(INTENT, output_class="Product")
        )
        assert run["intent"]["output_class"] == "Product"
        assert (
            run["coverage"] == {} and run["proposal"] == {} and run["evaluation"] == {}
        )
        assert run["status"] == RunStatus.CREATED.value

    def test_intent_cannot_change_while_running(self):
        svc = _svc()
        run = _create(svc)
        run = svc.update(run["id"], run["revision"], {"status": "RUNNING"})
        with pytest.raises(ConflictError):
            svc.update_intent(run["id"], run["revision"], INTENT)

    def test_terminal_runs_are_frozen(self):
        svc = _svc()
        run = _create(svc)
        run = svc.cancel(run["id"], run["revision"])
        assert run["status"] == RunStatus.CANCELLED.value
        with pytest.raises(ConflictError):
            svc.update(run["id"], run["revision"], {"stage": "x"})


class TestCancelAndRecovery:
    def test_cancel_of_running_run_only_requests_it(self):
        svc = _svc()
        run = _create(svc)
        run = svc.update(run["id"], run["revision"], {"status": "RUNNING"})
        run = svc.cancel(run["id"], run["revision"])
        assert run["status"] == "RUNNING" and run["cancel_requested"] is True

    def test_running_run_cannot_be_deleted(self):
        svc = _svc()
        run = _create(svc)
        svc.update(run["id"], run["revision"], {"status": "RUNNING"})
        with pytest.raises(ConflictError):
            svc.delete_run(run["id"])

    def test_recover_marks_running_runs_interrupted(self):
        svc = _svc()
        run = _create(svc)
        svc.update(run["id"], run["revision"], {"status": "RUNNING"})
        assert svc.recover_interrupted() == 1
        assert svc.get_run(run["id"])["status"] == RunStatus.INTERRUPTED.value


class TestStaleParent:
    def test_unchanged_parent_passes(self):
        svc = _svc()
        svc.ensure_parent_unchanged(_create(svc))

    def test_edited_parent_is_detected(self):
        registry = FakeRegistry()
        svc = _svc(registry)
        run = _create(svc)
        registry.documents["1"]["versions"]["1"]["ontology"]["classes"].pop()
        with pytest.raises(ConflictError):
            svc.ensure_parent_unchanged(run)

    def test_generated_r2rml_does_not_change_the_hash(self):
        registry = FakeRegistry()
        svc = _svc(registry)
        run = _create(svc)
        registry.documents["1"]["versions"]["1"]["assignment"][
            "r2rml_output"
        ] = "@prefix rr: <x> ."
        svc.ensure_parent_unchanged(run)
