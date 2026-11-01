"""Lakehouse//RT guardrail routing for ``DeltaFlatStore.execute_query``.

A read that trips a Lakehouse//RT guardrail is retried once on the build
warehouse; any other failure propagates unchanged and never creates the
fallback client.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.delta.DeltaFlatStore import (
    RT_FALLBACK_MARKERS,
    DeltaFlatStore,
)

pytestmark = pytest.mark.unit

# The package re-exports the class under the module's name, so resolve the
# real module explicitly (a dotted-string monkeypatch would hit the class).
_STORE_MODULE = importlib.import_module("back.core.graphdb.delta.DeltaFlatStore")
_SQL = "SELECT subject FROM cat.sch.t_graph LIMIT 5"


class _FakeClient:
    """Minimal read client: no ``sql`` service, just ``execute_query``."""

    def __init__(self, result=None, exc=None):
        self._result = result if result is not None else []
        self._exc = exc
        self.queries = []

    def execute_query(self, query):
        self.queries.append(query)
        if self._exc is not None:
            raise self._exc
        return self._result


class _FallbackFactory:
    """Stands in for ``create_databricks_client`` and records each call."""

    def __init__(self, client):
        self.client = client
        self.calls = []

    def __call__(self, domain, settings=None, *, for_write=False, warehouse_id=None):
        self.calls.append(
            {
                "domain": domain,
                "settings": settings,
                "for_write": for_write,
                "warehouse_id": warehouse_id,
            }
        )
        return self.client


def _install_factory(monkeypatch, client, *, build_warehouse_id="wh-build"):
    factory = _FallbackFactory(client)
    monkeypatch.setattr(_STORE_MODULE, "create_databricks_client", factory)
    monkeypatch.setattr(
        _STORE_MODULE, "resolve_build_warehouse_id", lambda d, s: build_warehouse_id
    )
    return factory


def _store(client, *, domain=True):
    return DeltaFlatStore(
        client,
        domain=MagicMock() if domain else None,
        settings=MagicMock() if domain else None,
    )


class TestGuardrailFallback:
    def test_guardrail_error_retries_once_on_build_client(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("RT guardrail hit: 2 min"))
        build_client = _FakeClient(result=[{"subject": "s"}])
        _install_factory(monkeypatch, build_client)
        store = _store(query_client)

        rows = store.execute_query(_SQL)

        assert rows == [{"subject": "s"}]
        assert build_client.queries == [_SQL]
        assert query_client.queries == [_SQL]

    def test_fallback_keeps_caller_identity_and_swaps_the_warehouse(self, monkeypatch):
        # for_write=False keeps the read branch (OBO token); only the warehouse changes.
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)

        store.execute_query(_SQL)

        assert [(c["for_write"], c["warehouse_id"]) for c in factory.calls] == [
            (False, "wh-build")
        ]

    def test_route_is_build_fallback_after_fallback(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)

        store.execute_query(_SQL)

        assert store.last_read_route == "build-fallback"

    @pytest.mark.parametrize("marker", RT_FALLBACK_MARKERS)
    def test_every_marker_triggers_the_fallback(self, monkeypatch, marker):
        query_client = _FakeClient(exc=RuntimeError(f"Boom: {marker.upper()} reached"))
        build_client = _FakeClient(result=[{"ok": 1}])
        _install_factory(monkeypatch, build_client)
        store = _store(query_client)

        assert store.execute_query(_SQL) == [{"ok": 1}]

    def test_fallback_client_is_cached_across_reads(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("exceeds the limit"))
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)

        store.execute_query(_SQL)
        store.execute_query(_SQL)

        assert len(factory.calls) == 1

    def test_error_on_the_fallback_retry_propagates(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        build_client = _FakeClient(exc=ValueError("build failed too"))
        _install_factory(monkeypatch, build_client)
        store = _store(query_client)

        with pytest.raises(ValueError, match="build failed too"):
            store.execute_query(_SQL)

        assert build_client.queries == [_SQL]

    def test_warning_logs_sql_length_and_prefix(self, monkeypatch):
        long_sql = "SELECT " + "x" * 300
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)
        warning = MagicMock()
        monkeypatch.setattr(_STORE_MODULE.logger, "warning", warning)

        store.execute_query(long_sql)

        args = warning.call_args.args
        assert len(long_sql) in args
        assert long_sql[:120] in args
        assert long_sql not in args


class TestNoFallback:
    def test_non_marked_error_propagates_without_creating_fallback(self, monkeypatch):
        query_client = _FakeClient(exc=RuntimeError("boom"))
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)

        with pytest.raises(RuntimeError, match="boom"):
            store.execute_query(_SQL)

        assert factory.calls == []
        assert store.last_read_route == "query"

    def test_success_never_touches_the_fallback(self, monkeypatch):
        query_client = _FakeClient(result=[{"subject": "s"}])
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)

        assert store.execute_query(_SQL) == [{"subject": "s"}]
        assert factory.calls == []
        assert store.last_read_route == "query"

    def test_unbound_domain_reraises_original_error(self, monkeypatch):
        error = InfrastructureError("guardrail")
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(_FakeClient(exc=error), domain=False)

        with pytest.raises(InfrastructureError) as excinfo:
            store.execute_query(_SQL)

        assert excinfo.value is error
        assert factory.calls == []

    def test_missing_fallback_client_reraises_original_error(self, monkeypatch):
        error = InfrastructureError("guardrail")
        _install_factory(monkeypatch, None)
        store = _store(_FakeClient(exc=error))

        with pytest.raises(InfrastructureError) as excinfo:
            store.execute_query(_SQL)

        assert excinfo.value is error


class TestReadStatementsOnly:
    @pytest.mark.parametrize(
        "statement",
        [
            "INSERT INTO cat.sch.t_inferred SELECT * FROM cat.sch.t_graph",
            "MERGE INTO cat.sch.t USING s ON t.k = s.k WHEN MATCHED THEN DELETE",
            "DELETE FROM cat.sch.t WHERE subject = 'x'",
            "CREATE OR REPLACE VIEW cat.sch.v AS SELECT 1",
            "OPTIMIZE cat.sch.t",
            "-- SELECT in a comment\nINSERT INTO cat.sch.t VALUES (1)",
            "/* a */ INSERT INTO t SELECT 1 /* b */ SELECT 2",
            "",
        ],
    )
    def test_write_statement_error_propagates_without_fallback(
        self, monkeypatch, statement
    ):
        error = InfrastructureError("exceeds the RT guardrail")
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(_FakeClient(exc=error))

        with pytest.raises(InfrastructureError) as excinfo:
            store.execute_query(statement)

        assert excinfo.value is error
        assert factory.calls == []

    @pytest.mark.parametrize(
        "statement",
        [
            "select 1",
            "  \n\tSELECT 1",
            "WITH level_0 AS (SELECT 1) SELECT * FROM level_0",
            "with x as (select 1) select * from x",
            "-- explain\nSELECT 1",
            "/* hint */ SELECT 1",
            "-- a\n  /* b */ -- c\n  WITH x AS (SELECT 1) SELECT * FROM x",
        ],
    )
    def test_read_statement_with_noise_still_falls_back(self, monkeypatch, statement):
        build_client = _FakeClient(result=[{"ok": 1}])
        _install_factory(monkeypatch, build_client)
        store = _store(_FakeClient(exc=InfrastructureError("guardrail")))

        assert store.execute_query(statement) == [{"ok": 1}]


class TestSameWarehouseSkipsRetry:
    def test_same_warehouse_as_primary_reraises_original(self, monkeypatch):
        error = InfrastructureError("guardrail")
        query_client = _FakeClient(exc=error)
        query_client.warehouse_id = "wh-build"
        factory = _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)

        with pytest.raises(InfrastructureError) as excinfo:
            store.execute_query(_SQL)

        assert excinfo.value is error
        assert factory.calls == []
        assert query_client.queries == [_SQL]

    def test_different_primary_warehouse_retries(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        query_client.warehouse_id = "wh-rt"
        build_client = _FakeClient(result=[{"ok": 1}])
        _install_factory(monkeypatch, build_client)
        store = _store(query_client)

        assert store.execute_query(_SQL) == [{"ok": 1}]

    @pytest.mark.parametrize("build_id", ["", None])
    def test_empty_build_warehouse_reraises_original(self, monkeypatch, build_id):
        error = InfrastructureError("guardrail")
        factory = _install_factory(
            monkeypatch, _FakeClient(), build_warehouse_id=build_id
        )
        store = _store(_FakeClient(exc=error))

        with pytest.raises(InfrastructureError) as excinfo:
            store.execute_query(_SQL)

        assert excinfo.value is error
        assert factory.calls == []

    def test_client_without_warehouse_id_skips_the_comparison(self, monkeypatch):
        build_client = _FakeClient(result=[{"ok": 1}])
        _install_factory(monkeypatch, build_client)
        store = _store(_FakeClient(exc=InfrastructureError("guardrail")))

        assert store.execute_query(_SQL) == [{"ok": 1}]


class TestLastReadRoute:
    def test_defaults_to_query(self):
        assert _store(_FakeClient()).last_read_route == "query"

    def test_is_read_only(self):
        store = _store(_FakeClient())

        with pytest.raises(AttributeError):
            store.last_read_route = "build-fallback"  # type: ignore[misc]

    def test_resets_to_query_after_a_later_success(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)
        store.execute_query(_SQL)
        assert store.last_read_route == "build-fallback"

        query_client._exc = None
        store.execute_query(_SQL)

        assert store.last_read_route == "query"

    def test_failed_read_does_not_report_a_stale_route(self, monkeypatch):
        query_client = _FakeClient(exc=InfrastructureError("guardrail"))
        _install_factory(monkeypatch, _FakeClient())
        store = _store(query_client)
        store.execute_query(_SQL)
        assert store.last_read_route == "build-fallback"

        query_client._exc = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            store.execute_query(_SQL)

        assert store.last_read_route == "query"


class TestStatementTimeoutPreserved:
    def test_sql_service_receives_statement_timeout(self):
        client = MagicMock()
        client.sql.execute_query.return_value = [{"n": 1}]
        store = _store(client)

        assert store.execute_query(_SQL) == [{"n": 1}]

        _, kwargs = client.sql.execute_query.call_args
        assert kwargs["statement_timeout_s"] > 0
        client.execute_query.assert_not_called()

    def test_fallback_client_also_gets_the_statement_timeout(self, monkeypatch):
        query_client = MagicMock()
        query_client.sql.execute_query.side_effect = InfrastructureError("guardrail")
        build_client = MagicMock()
        build_client.sql.execute_query.return_value = [{"n": 2}]
        _install_factory(monkeypatch, build_client)
        store = _store(query_client)

        assert store.execute_query(_SQL) == [{"n": 2}]

        _, kwargs = build_client.sql.execute_query.call_args
        assert kwargs["statement_timeout_s"] > 0
