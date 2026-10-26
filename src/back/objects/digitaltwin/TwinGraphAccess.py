"""Graph table names, backend store, and cached analytics lookup.

Extracted from ``api.routers.internal.dtwin`` (Fowler Extract Class).
The route module keeps ``_graph_query_table`` aliases for tests that patch them.
"""

from __future__ import annotations

from typing import Any, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.core.graphdb import get_graphdb
from back.core.helpers import effective_graph_query_table, effective_view_table
from back.core.logging import get_logger

logger = get_logger(__name__)


class TwinGraphAccess:
    """Resolve query/DQ tables, require a graph store, load cached metrics."""

    @staticmethod
    def graph_query_table(
        domain,
        settings,
        store=None,
        *,
        include_inferred: bool = True,
    ) -> str:
        """Resolve the physical graph table for read queries (Lakebase or Delta)."""
        return effective_graph_query_table(
            domain,
            settings,
            include_inferred=include_inferred,
            store=store,
        )

    @staticmethod
    def dataquality_table(domain, settings, *, view_table_fn=None) -> str:
        """Resolve the VIEW used for SQL data-quality checks.

        Checks always compile to SQL and run against the triple-store VIEW.
        Reasoning triples live in the graph store and are out of scope.
        """
        lookup = view_table_fn or effective_view_table
        table = lookup(domain, settings).strip()
        if not table:
            raise ValidationError(
                "The triple-store VIEW is not available. "
                "Build the Knowledge Graph first."
            )
        return table

    @staticmethod
    def require_graph_store(domain, settings, *, get_store=None) -> Any:
        """Return the graph-backend triple store or raise InfrastructureError."""
        lookup = get_store or get_graphdb
        store = lookup(domain, settings)
        if not store:
            raise InfrastructureError("Graph backend is not configured")
        return store

    @staticmethod
    def load_stored_metrics(domain, settings) -> Optional[dict]:
        """Return the cached ``graph_analytics`` row, or ``None``. Never raises."""
        from back.objects.registry.RegistryService import RegistryService

        folder = getattr(domain, "uc_domain_folder", "") or ""
        version = str(getattr(domain, "current_version", "") or "")
        if not folder or not version:
            return None
        try:
            svc = RegistryService.from_context(domain, settings)
            return svc.load_graph_analytics(folder, version)
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_graph_analytics failed: %s", exc)
            return None
