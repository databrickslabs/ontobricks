"""``InferredTripleLedger``: a Neo4j store's inferred triples listed in ``…_inferred``."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.delta import InferredTripleLedger, materialize

pytestmark = pytest.mark.unit

INFERRED = "main.ob.triplestore_d_V1_inferred"
TRIPLES = [{"subject": "http://ex/s", "predicate": "http://ex/p", "object": "http://ex/o"}]


class _Client:
    """Records statements; *fail_on* makes a matching statement raise."""

    def __init__(self, fail_on: str = ""):
        self.fail_on = fail_on
        self.statements: list[str] = []

    def execute_query(self, sql):
        return []

    def execute_statement(self, sql):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("warehouse refused")
        self.statements.append(sql)
        return True

    def get_sql_connection_params(self):  # pragma: no cover - the inline path is used
        raise AssertionError("no Thrift connection in unit tests")


@pytest.fixture
def inline(monkeypatch):
    """Make ``DeltaFlatStore`` insert through ``execute_statement`` (no Thrift session)."""
    from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore

    monkeypatch.setattr(DeltaFlatStore, "_uses_inline_sql", lambda self: True)


class TestForDomain:
    @staticmethod
    def _for(mode: str):
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        with (
            patch.object(GraphDBFactory, "resolve_materialization_mode", return_value=mode),
            patch("back.core.graphdb.delta._table_naming.inferred_table_fqn", return_value=INFERRED),
        ):
            return InferredTripleLedger.for_domain(SimpleNamespace(), SimpleNamespace())

    def test_a_full_domain_keeps_no_ledger(self):
        assert self._for("full") is None

    @pytest.mark.parametrize("mode", ["incremental", "microbatch"])
    def test_an_incremental_domain_lists_in_its_inferred_companion(self, mode):
        assert self._for(mode).inferred_fqn == INFERRED


class TestRecord:
    def test_the_companion_is_ensured_then_the_triples_appended(self, inline):
        client = _Client()

        assert InferredTripleLedger(lambda: client, INFERRED).record(TRIPLES) == 1

        assert client.statements[0] == materialize.build_ensure_inferred_sql(INFERRED)
        assert client.statements[1].startswith(f"INSERT INTO {INFERRED} (subject, predicate, object)")

    def test_no_warehouse_refuses_the_write(self):
        with pytest.raises(InfrastructureError, match="No SQL warehouse"):
            InferredTripleLedger(lambda: None, INFERRED).record(TRIPLES)

    def test_a_failed_insert_is_an_infrastructure_error(self, inline):
        with pytest.raises(InfrastructureError, match="Could not list the inferred triples"):
            InferredTripleLedger(lambda: _Client(fail_on="INSERT"), INFERRED).record(TRIPLES)


class TestRecordFor:
    def test_a_store_without_a_ledger_is_left_alone(self):
        InferredTripleLedger.record_for(MagicMock(), TRIPLES)  # a mock's auto-attribute is no ledger

    def test_a_store_with_a_ledger_records(self):
        ledger = InferredTripleLedger(lambda: None, INFERRED)
        store = SimpleNamespace(inferred_ledger=ledger)

        with patch.object(InferredTripleLedger, "record") as record:
            InferredTripleLedger.record_for(store, TRIPLES)

        record.assert_called_once_with(TRIPLES)


class TestTheWritersListBeforeTheyWrite:
    @staticmethod
    def _store(order: list) -> MagicMock:
        store = MagicMock(supports_adjacency=False)
        store.inferred_ledger = InferredTripleLedger(lambda: None, INFERRED)
        store.insert_triples.side_effect = lambda *a, **kw: order.append("store") or 1
        return store

    @staticmethod
    def _result():
        from back.core.reasoning import InferredTriple, ReasoningResult

        triple = InferredTriple(
            subject="http://ex/s", predicate="http://ex/p", object="http://ex/o", provenance="swrl:r"
        )
        return ReasoningResult(inferred_triples=[triple])

    def test_reasoning(self):
        from back.core.reasoning import ReasoningService

        order: list = []
        store = self._store(order)
        result = self._result()

        with patch.object(InferredTripleLedger, "record", side_effect=lambda t: order.append("ledger")):
            ReasoningService(MagicMock(), store).materialize_inferred(result)

        assert order == ["ledger", "store"]

    def test_a_failed_listing_writes_nothing_to_the_store(self):
        from back.core.reasoning import ReasoningService

        store = self._store([])
        result = self._result()

        with pytest.raises(InfrastructureError):
            ReasoningService(MagicMock(), store).materialize_inferred(result)
        store.insert_triples.assert_not_called()

    def test_cohorts(self):
        from back.core.graph_analysis.CohortBuilder import CohortBuilder

        order: list = []
        store = self._store(order)
        builder = CohortBuilder(store, "G_V1", base_uri="http://ex/")

        with (
            patch.object(builder, "_build_cohort_triples", return_value=TRIPLES),
            patch.object(InferredTripleLedger, "record", side_effect=lambda t: order.append("ledger")),
        ):
            builder.materialize_to_graph(SimpleNamespace(id="r1"), SimpleNamespace())

        assert order == ["ledger", "store"]


def test_the_neo4j_factory_hands_the_store_its_ledger():
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    ledger = InferredTripleLedger(lambda: None, INFERRED)
    domain = SimpleNamespace(info={"name": "D", "neo4j_connection": "c"}, current_version="1")
    with (
        patch("back.core.graphdb.neo4j.NEO4J_AVAILABLE", True),
        patch("back.core.graphdb.engine_config.resolve_neo4j_connection", return_value={"uri": "bolt://x"}),
        patch("back.core.graphdb.neo4j.Neo4jStore.Neo4jStore") as store_cls,
        patch.object(InferredTripleLedger, "for_domain", return_value=ledger),
    ):
        GraphDBFactory()._create_neo4j(domain, SimpleNamespace(), engine_config={})

    assert store_cls.call_args.kwargs["inferred_ledger"] is ledger
