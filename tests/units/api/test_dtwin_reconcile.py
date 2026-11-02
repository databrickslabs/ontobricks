"""``POST /dtwin/incremental/reconcile`` and the collaborator it delegates to."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.core.streaming import SlotPipelineSpec
from back.objects.digitaltwin.build import TwinSlotReconciliation

pytestmark = pytest.mark.unit

_SLOTS = "main.kg.triplestore_sales_v1_slots"
_DATA = "main.kg.triplestore_sales_v1_data"
_COMPARED = {
    "slots": {"rows": 3, "hash": 7},
    "data": {"rows": 3, "hash": 7},
    "drift": False,
    "by_map_id": [],
}


class _Domain:
    def __init__(self, r2rml: str = "<r2rml/>"):
        self._r2rml = r2rml
        self.ontology = {"base_uri": "http://example.org/"}
        self.assignment = {}

    def get_r2rml(self):
        return self._r2rml


@pytest.fixture
def names(monkeypatch):
    from back.core.graphdb.delta import _table_naming

    monkeypatch.setattr(_table_naming, "slots_table_fqn", lambda d, s=None: _SLOTS)
    monkeypatch.setattr(_table_naming, "data_table_fqn", lambda d, s=None: _DATA)


@pytest.fixture
def mappings(monkeypatch):
    """R2RML extraction and augmentation produce one entity mapping; the compiler is faked."""
    from back.core.w3c import sparql
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    monkeypatch.setattr(sparql, "extract_r2rml_mappings", lambda r2rml: ({"Customer": {}}, []))
    monkeypatch.setattr(DigitalTwin, "augment_mappings_from_config", lambda ent, *a: ent)
    monkeypatch.setattr(DigitalTwin, "augment_relationships_from_config", lambda rels, *a: rels)


def _compiler(monkeypatch, *, batch: bool, raising: bool = False):
    from back.core.streaming import SlotProjectionCompiler

    def compile(ent, rels, *, base_uri, slots_fqn):
        if raising:
            raise ValidationError("unsupported mapping shape")
        return SlotPipelineSpec(slots_fqn=slots_fqn, batch_mappings=[{"x": 1}] if batch else [])

    monkeypatch.setattr(SlotProjectionCompiler, "compile", staticmethod(compile))


class TestTwinSlotReconciliation:
    def test_the_comparison_is_returned_with_the_relation_names(self, monkeypatch, names, mappings):
        from back.core.graphdb.delta import materialize
        from back.core.streaming import SlotReconciler

        monkeypatch.setattr(materialize, "relation_exists", lambda client, fqn: True)
        monkeypatch.setattr(SlotReconciler, "compare", lambda client, s, d: dict(_COMPARED))
        _compiler(monkeypatch, batch=False)

        result = TwinSlotReconciliation.run(_Domain(), object(), MagicMock())

        assert result["slots_table"] == _SLOTS
        assert result["data_table"] == _DATA
        assert result["drift"] is False
        assert result["batch_branches_present"] is False
        assert result["drift_meaningful"] is True

    def test_batch_mappings_make_the_drift_not_meaningful(self, monkeypatch, names, mappings):
        from back.core.graphdb.delta import materialize
        from back.core.streaming import SlotReconciler

        monkeypatch.setattr(materialize, "relation_exists", lambda client, fqn: True)
        monkeypatch.setattr(SlotReconciler, "compare", lambda client, s, d: {**_COMPARED, "drift": True})
        _compiler(monkeypatch, batch=True)

        result = TwinSlotReconciliation.run(_Domain(), object(), MagicMock())

        assert result["drift"] is True
        assert result["batch_branches_present"] is True
        assert result["drift_meaningful"] is False

    def test_mappings_that_do_not_compile_count_as_batch(self, monkeypatch, names, mappings):
        _compiler(monkeypatch, batch=False, raising=True)

        assert TwinSlotReconciliation.batch_branches_present(_Domain(), slots_fqn=_SLOTS) is True

    def test_a_domain_without_r2rml_counts_as_batch(self):
        assert TwinSlotReconciliation.batch_branches_present(_Domain(r2rml=""), slots_fqn=_SLOTS) is True

    def test_a_missing_slots_table_is_not_found(self, monkeypatch, names):
        from back.core.graphdb.delta import materialize

        monkeypatch.setattr(materialize, "relation_exists", lambda client, fqn: False)

        with pytest.raises(NotFoundError, match="no incremental build"):
            TwinSlotReconciliation.run(_Domain(), object(), MagicMock())

    def test_unresolvable_names_are_a_validation_error(self, monkeypatch):
        from back.core.graphdb.delta import _table_naming

        monkeypatch.setattr(_table_naming, "slots_table_fqn", lambda d, s=None: "")
        monkeypatch.setattr(_table_naming, "data_table_fqn", lambda d, s=None: "")

        with pytest.raises(ValidationError, match="fully qualified"):
            TwinSlotReconciliation.run(_Domain(), object(), MagicMock())


class TestReconcileRoute:
    @pytest.mark.asyncio
    async def test_the_route_returns_the_comparison(self, monkeypatch):
        from api.routers.internal import dtwin
        from back.core.graphdb.delta import DeltaBase

        domain = _Domain()
        monkeypatch.setattr(dtwin, "get_domain", lambda _mgr: domain)
        monkeypatch.setattr(DeltaBase, "create_databricks_client", lambda d, s: "client")
        seen = {}

        def run(d, s, client):
            seen.update(domain=d, client=client)
            return {**_COMPARED, "batch_branches_present": False, "drift_meaningful": True}

        monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(run))

        resp = await dtwin.reconcile_incremental_slots(session_mgr=object(), settings=object())

        assert resp["success"] is True
        assert resp["drift"] is False
        assert resp["drift_meaningful"] is True
        assert seen == {"domain": domain, "client": "client"}

    @pytest.mark.asyncio
    async def test_a_missing_slots_table_propagates_as_not_found(self, monkeypatch):
        from api.routers.internal import dtwin
        from back.core.graphdb.delta import DeltaBase

        monkeypatch.setattr(dtwin, "get_domain", lambda _mgr: _Domain())
        monkeypatch.setattr(DeltaBase, "create_databricks_client", lambda d, s: "client")

        def run(d, s, client):
            raise NotFoundError("Slots table main.kg.x_slots does not exist")

        monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(run))

        with pytest.raises(NotFoundError):
            await dtwin.reconcile_incremental_slots(session_mgr=object(), settings=object())
