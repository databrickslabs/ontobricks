"""``POST /dtwin/incremental/reconcile`` and the collaborator it delegates to."""

from __future__ import annotations

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.streaming import SlotPipelineSpec
from back.objects.digitaltwin.build import TwinSlotReconciliation

pytestmark = pytest.mark.unit

_SLOTS = "main.kg.triplestore_sales_V1_slots"
_DATA = "main.kg.triplestore_sales_V1_data"
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


class _Client:
    """Answers ``SHOW TABLES`` from a canned listing; anything else is unexpected."""

    def __init__(self, tables=(), *, fail: bool = False):
        self.calls: list[str] = []
        self._tables = list(tables)
        self._fail = fail

    def execute_query(self, sql: str):
        self.calls.append(sql)
        if self._fail:
            raise RuntimeError("PERMISSION_DENIED: User does not have USE SCHEMA")
        assert sql.startswith("SHOW TABLES IN main.kg LIKE "), sql
        return [{"database": "kg", "tableName": t, "isTemporary": False} for t in self._tables]


@pytest.fixture
def names(monkeypatch):
    from back.core.graphdb.delta import _table_naming

    monkeypatch.setattr(_table_naming, "slots_table_fqn", lambda d, s=None: _SLOTS)
    monkeypatch.setattr(_table_naming, "data_table_fqn", lambda d, s=None: _DATA)


@pytest.fixture(autouse=True)
def incremental_mode(monkeypatch):
    """The domain requests ``incremental`` unless a test says otherwise."""
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    monkeypatch.setattr(
        GraphDBFactory, "resolve_materialization_mode", staticmethod(lambda d, s: "incremental")
    )


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

    seen = {}

    def compile(ent, rels, *, base_uri, slots_fqn):
        seen.update(base_uri=base_uri, slots_fqn=slots_fqn)
        if raising:
            raise ValidationError("unsupported mapping shape")
        return SlotPipelineSpec(slots_fqn=slots_fqn, batch_mappings=[{"x": 1}] if batch else [])

    monkeypatch.setattr(SlotProjectionCompiler, "compile", staticmethod(compile))
    return seen


def _compare(monkeypatch, result):
    from back.core.streaming import SlotReconciler

    monkeypatch.setattr(SlotReconciler, "compare", lambda client, s, d: dict(result))


class TestTwinSlotReconciliation:
    def test_the_comparison_is_returned_with_the_relation_names(self, monkeypatch, names, mappings):
        _compare(monkeypatch, _COMPARED)
        _compiler(monkeypatch, batch=False)
        client = _Client(tables=[_SLOTS.rsplit(".", 1)[1].lower()])

        result = TwinSlotReconciliation.run(_Domain(), object(), client)

        assert result["slots_table"] == _SLOTS
        assert result["data_table"] == _DATA
        assert result["drift"] is False
        assert result["batch_branches_present"] is False
        assert result["drift_meaningful"] is True
        assert client.calls == ["SHOW TABLES IN main.kg LIKE 'triplestore_sales_v1_slots'"]

    def test_batch_mappings_make_the_drift_not_meaningful(self, monkeypatch, names, mappings):
        _compare(monkeypatch, {**_COMPARED, "drift": True})
        _compiler(monkeypatch, batch=True)

        result = TwinSlotReconciliation.run(_Domain(), object(), _Client(tables=["triplestore_sales_v1_slots"]))

        assert result["drift"] is True
        assert result["batch_branches_present"] is True
        assert result["drift_meaningful"] is False

    def test_microbatch_slots_run_ahead_so_drift_is_not_meaningful(self, monkeypatch, names, mappings):
        """A continuous slot pipeline keeps moving ``_slots`` between builds."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        monkeypatch.setattr(
            GraphDBFactory, "resolve_materialization_mode", staticmethod(lambda d, s: "microbatch")
        )
        _compare(monkeypatch, {**_COMPARED, "drift": True})
        _compiler(monkeypatch, batch=False)

        result = TwinSlotReconciliation.run(_Domain(), object(), _Client(tables=["triplestore_sales_v1_slots"]))

        assert result["batch_branches_present"] is False
        assert result["slots_run_ahead"] is True
        assert result["drift_meaningful"] is False

    def test_mappings_that_do_not_compile_count_as_batch(self, monkeypatch, names, mappings):
        _compiler(monkeypatch, batch=False, raising=True)

        assert TwinSlotReconciliation.batch_branches_present(_Domain(), slots_fqn=_SLOTS) is True

    def test_a_domain_without_r2rml_counts_as_batch(self):
        assert TwinSlotReconciliation.batch_branches_present(_Domain(r2rml=""), slots_fqn=_SLOTS) is True

    def test_the_build_default_base_uri_is_used_when_the_ontology_has_none(self, monkeypatch, mappings):
        from shared.config.constants import DEFAULT_BASE_URI

        seen = _compiler(monkeypatch, batch=False)
        domain = _Domain()
        domain.ontology = {}

        TwinSlotReconciliation.batch_branches_present(domain, slots_fqn=_SLOTS)

        assert seen["base_uri"] == DEFAULT_BASE_URI

    def test_a_missing_slots_table_is_not_found(self, names):
        with pytest.raises(NotFoundError, match="no incremental build"):
            TwinSlotReconciliation.run(_Domain(), object(), _Client(tables=[]))

    def test_a_catalog_that_cannot_be_read_is_not_a_missing_table(self, names):
        """A permission gap or an outage must surface, not read as 'never built'."""
        with pytest.raises(InfrastructureError, match="Could not list the tables"):
            TwinSlotReconciliation.run(_Domain(), object(), _Client(fail=True))

    def test_no_client_is_an_infrastructure_error(self, names):
        with pytest.raises(InfrastructureError, match="No Databricks SQL client"):
            TwinSlotReconciliation.run(_Domain(), object(), None)

    def test_unresolvable_names_are_a_validation_error(self, monkeypatch):
        from back.core.graphdb.delta import _table_naming

        monkeypatch.setattr(_table_naming, "slots_table_fqn", lambda d, s=None: "")
        monkeypatch.setattr(_table_naming, "data_table_fqn", lambda d, s=None: "")

        with pytest.raises(ValidationError, match="fully qualified"):
            TwinSlotReconciliation.run(_Domain(), object(), _Client())


class TestReconcileRoute:
    @pytest.fixture
    def route(self, monkeypatch):
        from api.routers.internal import dtwin
        import back.core.helpers as helpers_pkg
        from back.core.graphdb.delta import DeltaBase

        domain = _Domain()
        seen: dict = {}
        monkeypatch.setattr(dtwin, "get_domain", lambda _mgr: domain)
        monkeypatch.setattr(helpers_pkg, "resolve_build_warehouse_id", lambda d, s: "wh-build")

        def create_client(d, s, **kwargs):
            seen["client_kwargs"] = kwargs
            return "client"

        monkeypatch.setattr(DeltaBase, "create_databricks_client", create_client)
        return dtwin, domain, seen

    @pytest.mark.asyncio
    async def test_the_route_returns_the_comparison(self, monkeypatch, route):
        dtwin, domain, seen = route

        def run(d, s, client):
            seen.update(domain=d, client=client)
            return {**_COMPARED, "batch_branches_present": False, "drift_meaningful": True}

        monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(run))

        resp = await dtwin.reconcile_incremental_slots(session_mgr=object(), settings=object())

        assert resp["success"] is True
        assert resp["drift"] is False
        assert resp["drift_meaningful"] is True
        assert seen["domain"] is domain and seen["client"] == "client"

    @pytest.mark.asyncio
    async def test_the_scans_run_on_the_build_warehouse_as_the_caller(self, monkeypatch, route):
        """``warehouse_id`` swaps the warehouse and keeps the OBO identity;
        ``for_write`` would drop it (the Task 7 lesson)."""
        dtwin, _domain, seen = route
        monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(lambda d, s, c: dict(_COMPARED)))

        await dtwin.reconcile_incremental_slots(session_mgr=object(), settings=object())

        assert seen["client_kwargs"] == {"warehouse_id": "wh-build"}

    @pytest.mark.asyncio
    async def test_a_missing_slots_table_propagates_as_not_found(self, monkeypatch, route):
        dtwin, _domain, _seen = route

        def run(d, s, client):
            raise NotFoundError("Slots table main.kg.x_slots does not exist")

        monkeypatch.setattr(TwinSlotReconciliation, "run", staticmethod(run))

        with pytest.raises(NotFoundError):
            await dtwin.reconcile_incremental_slots(session_mgr=object(), settings=object())
