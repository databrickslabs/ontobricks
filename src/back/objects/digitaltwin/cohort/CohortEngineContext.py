"""Pre-resolved graph store + CohortService for engine routes.

Extracted from ``api.routers.internal.dtwin`` (Fowler Extract Class /
Introduce Parameter Object). FastAPI ``Depends`` stays in the route module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from back.core.errors import ValidationError
from back.objects.digitaltwin.cohort.CohortService import CohortService


@dataclass
class CohortEngineContext:
    """Graph backend, query table, and CohortService for one request."""

    domain: Any
    settings: Any
    store: Any
    graph_name: str
    service: CohortService

    @classmethod
    def from_domain(
        cls,
        domain,
        settings,
        *,
        require_store: Callable,
        query_table: Callable,
    ) -> CohortEngineContext:
        """Open the graph store and resolve the query table name.

        *require_store* / *query_table* are injected so the route module can
        pass its own aliases (tests patch those names on ``dtwin``).
        """
        store = require_store(domain, settings)
        graph_name = query_table(domain, settings, store)
        if not graph_name:
            raise ValidationError("Graph name is not configured")
        return cls(
            domain=domain,
            settings=settings,
            store=store,
            graph_name=graph_name,
            service=CohortService(domain),
        )
