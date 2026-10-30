"""DataQualitySnapshot: CTAS from the gateway VIEW, drop, fallback."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from back.objects.digitaltwin.quality.DataQualitySnapshot import DataQualitySnapshot

pytestmark = pytest.mark.unit


def _domain():
    return SimpleNamespace(
        info={"name": "Dom"},
        current_version=3,
        delta={"catalog": "cat", "schema": "sch"},
    )


def test_disabled_snapshot_keeps_the_live_view():
    with DataQualitySnapshot(_domain(), None, "cat.sch.v", "t1", enabled=False) as snap:
        assert snap.table == "cat.sch.v"
        assert snap.parallel is False


def test_successful_ctas_yields_the_snapshot_and_drops_it(monkeypatch):
    statements = []
    client = SimpleNamespace(execute_statement=statements.append)
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None, *, for_write=False: client,
    )
    monkeypatch.setattr(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        lambda c, view, table: c.execute_statement(f"CTAS {table} FROM {view}"),
    )
    monkeypatch.setattr(
        "back.core.graphdb.delta.materialize.drop_relation",
        lambda c, fqn, *, kind: c.execute_statement(f"DROP {fqn}"),
    )

    with DataQualitySnapshot(_domain(), None, "cat.sch.v", "t9", enabled=True) as snap:
        assert snap.table == "cat.sch.triplestore_dom_V3_dq_t9"
        assert snap.parallel is True
        assert statements == ["CTAS cat.sch.triplestore_dom_V3_dq_t9 FROM cat.sch.v"]

    assert statements[1] == "DROP cat.sch.triplestore_dom_V3_dq_t9"


def test_snapshot_is_dropped_when_the_run_raises(monkeypatch):
    dropped = []
    client = SimpleNamespace(execute_statement=lambda sql: None)
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None, *, for_write=False: client,
    )
    monkeypatch.setattr(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        lambda c, view, table: None,
    )
    monkeypatch.setattr(
        "back.core.graphdb.delta.materialize.drop_relation",
        lambda c, fqn, *, kind: dropped.append(fqn),
    )

    with pytest.raises(RuntimeError, match="job died"):
        with DataQualitySnapshot(_domain(), None, "cat.sch.v", "t9", enabled=True):
            raise RuntimeError("job died")

    assert dropped == ["cat.sch.triplestore_dom_V3_dq_t9"]


def test_ctas_failure_falls_back_to_the_live_view(monkeypatch):
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None, *, for_write=False: SimpleNamespace(),
    )
    monkeypatch.setattr(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        lambda c, view, table: (_ for _ in ()).throw(RuntimeError("no DDL")),
    )
    dropped = []
    monkeypatch.setattr(
        "back.core.graphdb.delta.materialize.drop_relation",
        lambda c, fqn, *, kind: dropped.append(fqn),
    )

    with DataQualitySnapshot(_domain(), None, "cat.sch.v", "t9", enabled=True) as snap:
        assert snap.table == "cat.sch.v"
        assert snap.parallel is False
    assert dropped == []


def test_missing_warehouse_falls_back_to_the_live_view(monkeypatch):
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None, *, for_write=False: None,
    )
    with DataQualitySnapshot(_domain(), None, "cat.sch.v", "t9", enabled=True) as snap:
        assert snap.table == "cat.sch.v"
        assert snap.parallel is False
