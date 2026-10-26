"""Tests for Databricks Apps INLINE Statement Execution reads."""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import Mock, call, patch

import pytest

from back.core.errors import InfrastructureError
from back.core.databricks.StatementExecutionWarehouse import (
    StatementExecutionWarehouse,
)


def _auth() -> Mock:
    auth = Mock()
    auth.host = "https://workspace.databricks.com"
    auth.warehouse_id = "wh-rt"
    auth.is_app_mode = True
    auth.has_valid_auth.return_value = True
    auth.get_auth_headers.return_value = {"Authorization": "Bearer token"}
    return auth


def _response(payload: dict) -> Mock:
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _succeeded(*, data_array=None, columns=None, result=None, truncated=False):
    return {
        "statement_id": "stmt-1",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "truncated": truncated,
            "schema": {"columns": columns or []},
        },
        "result": result if result is not None else {"data_array": data_array or []},
    }


def test_execute_query_requests_inline_json_and_decodes_manifest_types():
    columns = [
        {"name": "name", "type_name": "STRING"},
        {"name": "count", "type_name": "LONG"},
        {"name": "ratio", "type_name": "DOUBLE"},
        {"name": "enabled", "type_name": "BOOLEAN"},
        {"name": "amount", "type_name": "DECIMAL"},
        {"name": "day", "type_name": "DATE"},
        {"name": "created", "type_name": "TIMESTAMP"},
        {"name": "tags", "type_name": "ARRAY"},
    ]
    response = _response(
        _succeeded(
            columns=columns,
            data_array=[
                [
                    "Alice",
                    "42",
                    "1.5",
                    "true",
                    "12.30",
                    "2026-09-15",
                    "2026-09-15T12:30:00Z",
                    '["a", "b"]',
                ],
                [None, None, None, None, None, None, None, None],
            ],
        )
    )

    with patch(
        "back.core.databricks.StatementExecutionWarehouse.requests.request",
        return_value=response,
    ) as request:
        rows = StatementExecutionWarehouse(_auth()).execute_query(
            "SELECT * FROM graph", statement_timeout_s=12
        )

    assert rows == [
        {
            "name": "Alice",
            "count": 42,
            "ratio": 1.5,
            "enabled": True,
            "amount": Decimal("12.30"),
            "day": date(2026, 9, 15),
            "created": datetime.fromisoformat("2026-09-15T12:30:00+00:00"),
            "tags": ["a", "b"],
        },
        {column["name"]: None for column in columns},
    ]
    payload = request.call_args.kwargs["json"]
    assert payload == {
        "statement": "SELECT * FROM graph",
        "warehouse_id": "wh-rt",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "byte_limit": 24 * 1024 * 1024,
        "wait_timeout": "12s",
        "on_wait_timeout": "CONTINUE",
    }
    assert request.call_args.args[:2] == (
        "POST",
        "https://workspace.databricks.com/api/2.0/sql/statements",
    )


def test_execute_query_polls_pending_statement_and_follows_internal_chunks():
    pending = _response(
        {
            "statement_id": "stmt-1",
            "status": {"state": "PENDING"},
        }
    )
    first = _response(
        _succeeded(
            columns=[{"name": "value", "type_name": "INT"}],
            result={
                "data_array": [["1"]],
                "next_chunk_internal_link": (
                    "/api/2.0/sql/statements/stmt-1/result/chunks/1"
                ),
            },
        )
    )
    second = _response({"data_array": [["2"]]})

    with patch(
        "back.core.databricks.StatementExecutionWarehouse.requests.request",
        side_effect=[pending, first, second],
    ) as request, patch(
        "back.core.databricks.StatementExecutionWarehouse.time.sleep"
    ):
        rows = StatementExecutionWarehouse(_auth()).execute_query("SELECT value")

    assert rows == [{"value": 1}, {"value": 2}]
    assert request.call_args_list[1].args[:2] == (
        "GET",
        "https://workspace.databricks.com/api/2.0/sql/statements/stmt-1",
    )
    assert request.call_args_list[2].args[:2] == (
        "GET",
        "https://workspace.databricks.com/api/2.0/sql/statements/stmt-1/result/chunks/1",
    )


@pytest.mark.parametrize(
    ("payload", "detail"),
    [
        (
            {
                "statement_id": "stmt-1",
                "status": {
                    "state": "FAILED",
                    "error": {"error_code": "BAD_REQUEST", "message": "bad SQL"},
                },
            },
            "BAD_REQUEST",
        ),
        (_succeeded(truncated=True), "truncated"),
        (
            _succeeded(
                result={
                    "data_array": [],
                    "external_links": [{"external_link": "https://storage/file"}],
                }
            ),
            "external",
        ),
    ],
)
def test_execute_query_rejects_failed_truncated_or_external_results(payload, detail):
    with patch(
        "back.core.databricks.StatementExecutionWarehouse.requests.request",
        return_value=_response(payload),
    ):
        with pytest.raises(InfrastructureError) as error:
            StatementExecutionWarehouse(_auth()).execute_query("SELECT 1")

    assert detail.lower() in (error.value.detail or "").lower()


def test_execute_query_cancels_statement_on_client_timeout():
    pending = _response(
        {
            "statement_id": "stmt-1",
            "status": {"state": "RUNNING"},
        }
    )
    cancelled = _response({})

    with patch(
        "back.core.databricks.StatementExecutionWarehouse.requests.request",
        side_effect=[pending, pending, cancelled],
    ) as request, patch(
        "back.core.databricks.StatementExecutionWarehouse.time.monotonic",
        side_effect=[0.0, 2.0],
    ), patch(
        "back.core.databricks.StatementExecutionWarehouse.time.sleep"
    ):
        with pytest.raises(InfrastructureError) as error:
            StatementExecutionWarehouse(_auth()).execute_query(
                "SELECT SLEEP(10)", statement_timeout_s=1
            )

    assert "timed out" in (error.value.detail or "").lower()
    assert request.call_args_list[-1] == call(
        "POST",
        "https://workspace.databricks.com/api/2.0/sql/statements/stmt-1/cancel",
        headers={"Authorization": "Bearer token"},
        timeout=30,
    )


def test_execute_statement_reuses_inline_query_path():
    service = StatementExecutionWarehouse(_auth())
    with patch.object(service, "execute_query", return_value=[]) as execute:
        assert service.execute_statement("TRUNCATE TABLE cat.sch.g_inferred") is True

    execute.assert_called_once_with("TRUNCATE TABLE cat.sch.g_inferred")


def test_test_connection_uses_inline_query():
    service = StatementExecutionWarehouse(_auth())
    with patch.object(service, "execute_query", return_value=[{"1": 1}]) as execute:
        success, message = service.test_connection()

    assert success is True
    assert "successful" in message
    execute.assert_called_once_with("SELECT 1")
