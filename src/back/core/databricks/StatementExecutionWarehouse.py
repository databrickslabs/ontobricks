"""INLINE Statement Execution transport for Lakehouse/RT reads in Apps."""

from __future__ import annotations

import base64
import json
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import requests

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from shared.config.constants import MSG_WAREHOUSE_ID_REQUIRED

from .DatabricksAuth import DatabricksAuth
from .constants import _REQUEST_TIMEOUT, _SQL_SOCKET_TIMEOUT

logger = get_logger(__name__)

_STATEMENTS_PATH = "/api/2.0/sql/statements"
_INLINE_BYTE_LIMIT = 24 * 1024 * 1024
_MAX_INITIAL_WAIT_SECONDS = 50
_POLL_INTERVAL_SECONDS = 0.2
_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
_INTEGER_TYPES = {"BYTE", "SHORT", "INT", "INTEGER", "LONG", "BIGINT"}
_FLOAT_TYPES = {"FLOAT", "DOUBLE"}
_COMPLEX_TYPES = {"ARRAY", "MAP", "STRUCT", "VARIANT"}


class StatementExecutionWarehouse:
    """Execute queries through SEA without external CloudFetch links."""

    uses_inline_statements = True

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth

    @property
    def warehouse_id(self) -> str:
        return self._auth.warehouse_id

    def _require_warehouse(self) -> None:
        if not self.warehouse_id:
            raise ValidationError(MSG_WAREHOUSE_ID_REQUIRED)

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        if not path.startswith(f"{_STATEMENTS_PATH}/") and path != _STATEMENTS_PATH:
            raise InfrastructureError(
                "Databricks SQL returned an unsupported result link",
                detail=f"Rejected non-statement internal link: {path}",
            )
        response = requests.request(
            method,
            f"{self._auth.host.rstrip('/')}{path}",
            headers=self._auth.get_auth_headers(),
            timeout=kwargs.pop("timeout", _REQUEST_TIMEOUT),
            **kwargs,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _state(payload: Dict[str, Any]) -> str:
        return str((payload.get("status") or {}).get("state") or "").upper()

    def _cancel(self, statement_id: str) -> None:
        try:
            self._request("POST", f"{_STATEMENTS_PATH}/{statement_id}/cancel")
        except Exception as exc:  # noqa: BLE001 - best-effort cancellation
            logger.warning("Could not cancel timed-out SQL statement %s: %s", statement_id, exc)

    def _wait_for_terminal(
        self,
        payload: Dict[str, Any],
        *,
        deadline: float,
    ) -> Dict[str, Any]:
        statement_id = str(payload.get("statement_id") or "")
        while self._state(payload) not in _TERMINAL_STATES:
            if not statement_id:
                raise InfrastructureError(
                    "Databricks SQL returned an invalid response",
                    detail="Pending Statement Execution response has no statement_id",
                )
            if time.monotonic() >= deadline:
                self._cancel(statement_id)
                raise InfrastructureError(
                    "Databricks SQL query timed out",
                    detail=f"Statement {statement_id} timed out before completion",
                )
            time.sleep(_POLL_INTERVAL_SECONDS)
            payload = self._request("GET", f"{_STATEMENTS_PATH}/{statement_id}")
        return payload

    @staticmethod
    def _raise_for_terminal_error(payload: Dict[str, Any]) -> None:
        state = StatementExecutionWarehouse._state(payload)
        if state == "SUCCEEDED":
            return
        error = (payload.get("status") or {}).get("error") or {}
        detail = json.dumps(error, sort_keys=True) if error else f"state={state or 'UNKNOWN'}"
        raise InfrastructureError(
            "Databricks SQL statement failed",
            detail=detail,
        )

    @staticmethod
    def _decode_value(value: Any, type_name: str) -> Any:
        if value is None:
            return None
        normalized = type_name.upper()
        if normalized in _INTEGER_TYPES:
            return int(value)
        if normalized in _FLOAT_TYPES:
            return float(value)
        if normalized == "DECIMAL":
            return Decimal(value)
        if normalized == "BOOLEAN":
            return str(value).lower() == "true"
        if normalized == "DATE":
            return date.fromisoformat(value)
        if normalized in {"TIMESTAMP", "TIMESTAMP_NTZ"}:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if normalized == "BINARY":
            return base64.b64decode(value)
        if normalized in _COMPLEX_TYPES:
            return json.loads(value)
        return value

    @classmethod
    def _decode_rows(
        cls,
        columns: List[Dict[str, Any]],
        data_array: List[List[Any]],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for values in data_array:
            row: Dict[str, Any] = {}
            for index, column in enumerate(columns):
                value = values[index] if index < len(values) else None
                row[str(column.get("name") or "")] = cls._decode_value(
                    value,
                    str(column.get("type_name") or column.get("type_text") or "STRING"),
                )
            rows.append(row)
        return rows

    @staticmethod
    def _result(payload: Dict[str, Any]) -> Dict[str, Any]:
        result = payload.get("result")
        return result if isinstance(result, dict) else payload

    @staticmethod
    def _reject_external_or_truncated(
        payload: Dict[str, Any],
        result: Dict[str, Any],
    ) -> None:
        manifest = payload.get("manifest") or {}
        if bool(manifest.get("truncated")) or bool(result.get("truncated")):
            raise InfrastructureError(
                "Databricks SQL result is too large for inline delivery",
                detail="INLINE Statement Execution result was truncated",
            )
        if result.get("external_links") or result.get("external_link"):
            raise InfrastructureError(
                "Databricks SQL returned an unsupported result link",
                detail="External result links are disabled for Databricks Apps",
            )

    def execute_query(
        self,
        query: str,
        statement_timeout_s: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Execute *query* with SEA ``INLINE`` / ``JSON_ARRAY`` results."""
        self._require_warehouse()
        timeout_s = max(1, int(statement_timeout_s or _SQL_SOCKET_TIMEOUT))
        wait_s = min(timeout_s, _MAX_INITIAL_WAIT_SECONDS)
        deadline = time.monotonic() + timeout_s
        payload = {
            "statement": query,
            "warehouse_id": self.warehouse_id,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
            "byte_limit": _INLINE_BYTE_LIMIT,
            "wait_timeout": f"{wait_s}s",
            "on_wait_timeout": "CONTINUE",
        }

        try:
            response = self._request(
                "POST",
                _STATEMENTS_PATH,
                json=payload,
                timeout=wait_s + 5,
            )
            response = self._wait_for_terminal(response, deadline=deadline)
            self._raise_for_terminal_error(response)

            manifest = response.get("manifest") or {}
            columns = (manifest.get("schema") or {}).get("columns") or []
            result = self._result(response)
            self._reject_external_or_truncated(response, result)
            rows = self._decode_rows(columns, result.get("data_array") or [])

            next_link = result.get("next_chunk_internal_link")
            while next_link:
                chunk = self._request("GET", str(next_link))
                chunk_result = self._result(chunk)
                self._reject_external_or_truncated(chunk, chunk_result)
                rows.extend(
                    self._decode_rows(columns, chunk_result.get("data_array") or [])
                )
                next_link = chunk_result.get("next_chunk_internal_link")
            return rows
        except InfrastructureError:
            raise
        except Exception as exc:
            logger.exception("INLINE Statement Execution query failed: %s", exc)
            raise InfrastructureError(
                "Databricks SQL request failed",
                detail=str(exc),
            ) from exc

    def execute_statement(self, statement: str) -> bool:
        """Run DDL/DML through the same INLINE Statement Execution path as reads.

        Databricks Apps cannot open a Thrift SQL session. Graph writes
        (CREATE/TRUNCATE/INSERT/OPTIMIZE) must reuse SEA.
        """
        self.execute_query(statement)
        return True

    def test_connection(self) -> Tuple[bool, str]:
        """Test the Apps M2M and Lakehouse/RT Statement Execution path."""
        if not self.warehouse_id:
            return False, "Missing SQL Warehouse ID"
        if not self._auth.has_valid_auth():
            return False, "Missing OAuth credentials (DATABRICKS_CLIENT_ID/SECRET)"
        try:
            self.execute_query("SELECT 1")
            return True, "Connection successful (OAuth INLINE Statement Execution)"
        except Exception as exc:  # noqa: BLE001 - connectivity probe
            return False, f"Connection failed: {exc}"
