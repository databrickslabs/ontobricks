"""Warehouse queries that name Unity Catalog relations: safe names, translated errors."""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.helpers.SQLHelpers import SAFE_SQL_IDENT, SQLHelpers


class WarehouseStatements:
    """Guards and error translation shared by the warehouse probes and readers.

    Relation names are interpolated into SQL (``table_changes`` even puts one
    inside a quoted literal), so only letters, digits, ``_`` and ``.`` get
    through. A driver exception becomes :class:`InfrastructureError`, so a
    caller can fall back on any :class:`OntoBricksError`.
    """

    @staticmethod
    def require_fqn(fqn: str, *, what: str = "table name") -> None:
        """Raise :class:`ValidationError` for an empty or unsafe relation name."""
        SQLHelpers.validate_table_name(fqn)
        if SAFE_SQL_IDENT.fullmatch(fqn) is None:
            raise ValidationError(f"Invalid {what}: {fqn!r}")

    @staticmethod
    def query(client: Any, sql: str, what: str) -> list:
        """``client.execute_query(sql)`` as a list; driver errors say "Could not <what>"."""
        try:
            return list(client.execute_query(sql) or [])
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(f"Could not {what}", detail=str(exc)) from exc
