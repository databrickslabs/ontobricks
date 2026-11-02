"""``TwinIncrementalReadiness``: the readiness preflight and the row-tracking fix."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.digitaltwin.build import TwinIncrementalBuild, TwinIncrementalReadiness
from tests.units.objects import test_build_pipeline_modes as _modes

pytestmark = pytest.mark.unit

_CUSTOMERS = "main.sales.customers"
_ORDERS = "main.sales.orders"


def _domain(mode="incremental"):
    return SimpleNamespace(
        info={"name": "dom", "materialization_mode": mode, "graph_backend": "databricks"},
        current_version=3,
        delta={"catalog": "cat", "schema": "sch"},
        uc_domain_folder="dom",
    )


class _Client:
    """Answers the row-tracking probe from *tracked*; records and optionally fails ALTERs."""

    def __init__(self, tracked=None, *, probe_error=None, alter_error_on=()):
        self.tracked = dict(tracked or {})
        self.queries: list[str] = []
        self.statements: list[str] = []
        self._probe_error = probe_error
        self._alter_error_on = tuple(alter_error_on)

    def execute_query(self, sql):
        self.queries.append(sql)
        if self._probe_error is not None:
            raise self._probe_error
        table = sql.split()[2]
        return [{"key": "delta.enableRowTracking", "value": str(self.tracked.get(table, False)).lower()}]

    def execute_statement(self, sql):
        self.statements.append(sql)
        if any(t in sql for t in self._alter_error_on):
            raise RuntimeError("PERMISSION_DENIED: not the table owner")
        return True


@pytest.fixture
def two_sources(monkeypatch):
    """The domain's mappings: two row-local entity mappings on two tables."""
    other = {**_modes._row_local(), "table": _ORDERS, "class_uri": _modes._ORDER}
    entities = {_modes._CUSTOMER: _modes._row_local(), _modes._ORDER: other}
    monkeypatch.setattr(
        TwinIncrementalBuild,
        "mappings_for",
        staticmethod(lambda domain: (entities, [], "http://ex.org/")),
    )


class TestSourceTables:
    def test_the_row_local_sources_of_the_domain(self, two_sources):
        assert TwinIncrementalReadiness.source_tables(_domain(), None) == sorted([_CUSTOMERS, _ORDERS])

    def test_a_domain_without_r2rml_has_none(self, monkeypatch):
        monkeypatch.setattr(TwinIncrementalBuild, "mappings_for", staticmethod(lambda d: None))

        assert TwinIncrementalReadiness.source_tables(_domain(), None) == []


class TestReport:
    def test_an_untracked_source_keeps_the_domain_on_full(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: False})

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), client)

        assert report["mode_requested"] == "incremental"
        assert report["decision"] == {"effective": "full", "reason": "row_tracking_missing"}
        assert report["sources"] == [
            {"table": _CUSTOMERS, "row_tracking": True},
            {"table": _ORDERS, "row_tracking": False},
        ]
        assert report["serverless_available"] is True
        assert report["previews"] == {"slot_pipeline_channel": "PREVIEW"}

    def test_tracked_sources_keep_the_requested_mode(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: True})

        report = TwinIncrementalReadiness.report(_domain("microbatch"), SimpleNamespace(), client)

        assert report["decision"] == {"effective": "microbatch", "reason": ""}

    def test_without_serverless_the_gate_says_why(self, two_sources):
        settings = SimpleNamespace(serverless_pipelines_enabled=False)

        report = TwinIncrementalReadiness.report(_domain(), settings, _Client({_CUSTOMERS: True}))

        assert report["serverless_available"] is False
        assert report["decision"]["reason"] == "serverless_unavailable"

    def test_a_failed_probe_is_an_infrastructure_error(self, two_sources):
        with pytest.raises(InfrastructureError):
            TwinIncrementalReadiness.report(
                _domain(), SimpleNamespace(), _Client(probe_error=RuntimeError("no USE SCHEMA"))
            )


class TestEnableRowTracking:
    def test_the_untracked_sources_are_altered(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: False})

        result = TwinIncrementalReadiness.enable_row_tracking(_domain(), None, client)

        assert result == {"tables": [{"table": _ORDERS, "success": True}]}
        assert client.statements == [
            f"ALTER TABLE {_ORDERS} SET TBLPROPERTIES "
            "(delta.enableRowTracking = true, delta.enableDeletionVectors = true)"
        ]

    def test_a_listed_subset_is_altered(self, two_sources):
        client = _Client()

        result = TwinIncrementalReadiness.enable_row_tracking(
            _domain(), None, client, [_CUSTOMERS.upper()]
        )

        assert result == {"tables": [{"table": _CUSTOMERS, "success": True}]}

    def test_a_table_outside_the_domain_is_refused_before_anything_runs(self, two_sources):
        client = _Client()

        with pytest.raises(ValidationError, match="main.hr.salaries"):
            TwinIncrementalReadiness.enable_row_tracking(
                _domain(), None, client, [_CUSTOMERS, "main.hr.salaries"]
            )
        assert client.statements == []

    def test_a_refused_alter_is_reported_per_table(self, two_sources):
        client = _Client(alter_error_on=(_ORDERS,))

        result = TwinIncrementalReadiness.enable_row_tracking(
            _domain(), None, client, [_CUSTOMERS, _ORDERS]
        )

        assert result["tables"][0] == {"table": _CUSTOMERS, "success": True}
        assert result["tables"][1]["success"] is False
        assert "PERMISSION_DENIED" in result["tables"][1]["error"]
