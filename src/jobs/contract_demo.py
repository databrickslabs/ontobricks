"""Shared table helpers for the create/delete fake-contract test Jobs.

Test-only: not declared in the bundle. Create each Job by hand in the
workspace as a serverless Python script task pointing at
``create_fake_contract_job.py`` / ``delete_contract_job.py``, with Job
parameters ``entity_id`` and ``contract_table`` passed as
``--entity-id {{job.parameters.entity_id}}
--contract-table {{job.parameters.contract_table}}``. The ``entity_id``
parameter is what makes the Job bindable as a class Action.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

_IDENTIFIER_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
)

FAKE_CONTRACT_PREFIX = "FAKE-"
DEFAULT_AMOUNT = 100.0
DEFAULT_STATUS = "DRAFT"


def sql_escape(value: str) -> str:
    """Escape single quotes for a SQL string literal."""
    return (value or "").replace("'", "''")


def validate_identifier(name: str, label: str) -> str:
    """Reject anything that is not a plain (optionally qualified) identifier."""
    if not _IDENTIFIER_RE.match(name or ""):
        raise ValueError(
            f"{label} must be a plain identifier of the form "
            f"catalog.schema.table, got {name!r}"
        )
    return name


def require_entity_id(entity_id: str) -> str:
    text = (entity_id or "").strip()
    if not text:
        raise ValueError("entity_id is required")
    return text


class ContractDemoTable:
    """SQL and row builders for ``catalog.schema.demo_contracts``."""

    def __init__(self, table: str) -> None:
        self.table = validate_identifier(table, "--contract-table")

    def ensure_table_sql(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table} ("
            "contract_id STRING, "
            "customer_id STRING, "
            "label STRING, "
            "status STRING, "
            "amount DOUBLE, "
            "created_at TIMESTAMP"
            ") USING DELTA"
        )

    def fake_row(
        self,
        entity_id: str,
        *,
        now: Optional[datetime] = None,
        suffix: Optional[str] = None,
    ) -> Dict[str, object]:
        customer_id = require_entity_id(entity_id)
        token = suffix if suffix is not None else uuid.uuid4().hex[:8]
        created = now or datetime.now(timezone.utc).replace(tzinfo=None)
        contract_id = f"{FAKE_CONTRACT_PREFIX}{customer_id}-{token}"
        return {
            "contract_id": contract_id,
            "customer_id": customer_id,
            "label": f"Fake contract for {customer_id}",
            "status": DEFAULT_STATUS,
            "amount": DEFAULT_AMOUNT,
            "created_at": created,
        }

    def delete_sql(self, entity_id: str) -> str:
        """Delete the contract itself, or fake contracts owned by this customer."""
        escaped = sql_escape(require_entity_id(entity_id))
        prefix = sql_escape(FAKE_CONTRACT_PREFIX)
        return (
            f"DELETE FROM {self.table} WHERE contract_id = '{escaped}' "
            f"OR (customer_id = '{escaped}' AND contract_id LIKE '{prefix}%')"
        )
