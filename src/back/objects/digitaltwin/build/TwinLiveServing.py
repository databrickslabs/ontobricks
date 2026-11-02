"""A realtime domain's Lakebase ``_live`` table: seeded and served, or emptied and retired."""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)

_SLOT_COLUMNS = "map_id, source_key, predicate, subject, object"
_SEED_READ_BATCH = 5000


class TwinLiveServing:
    """The ``_live`` lifecycle of one domain version on its Lakebase store.

    :meth:`start` creates the table (granting the stream's Postgres role),
    seeds it from the ``…_slots`` snapshot — read on the warehouse in batches,
    at the snapshot ``seq`` — and marks it served; :meth:`retire` empties it
    and stops serving it. Both re-create the union view (the synced view, or
    the app-managed layout), so readers see the change at once.
    """

    @staticmethod
    def live(store: Any) -> Any:
        """The store's ``_live`` table, or ``ValidationError`` for a store without one."""
        from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase

        if not isinstance(store, LakebaseBase):
            raise ValidationError("Realtime serving needs the Lakebase graph backend")
        return store.live_table()

    @staticmethod
    def start(store: Any, domain: Any, settings: Any, client: Any) -> int:
        """Create, seed and serve *domain*'s ``_live``; return the slots seeded."""
        from back.core.graphdb.delta import _table_naming
        from back.core.helpers import WarehouseStatements, effective_graph_name

        live = TwinLiveServing.live(store)
        graph = effective_graph_name(domain)
        live.ensure(graph, writer_role=str(getattr(settings, "realtime_lakebase_user", "") or ""))
        slots = _table_naming.slots_table_fqn(domain, settings)
        WarehouseStatements.require_fqn(slots, what="slots table name")
        if client is None:
            raise InfrastructureError("No warehouse client to read the slots snapshot")
        try:
            rows = client.iter_rows(f"SELECT {_SLOT_COLUMNS} FROM {slots}", batch_size=_SEED_READ_BATCH)
            seeded = live.attach(graph, rows)
        except InfrastructureError:
            raise
        except Exception as exc:  # noqa: BLE001 — driver errors, translated
            raise InfrastructureError(f"Could not seed {graph} live from {slots}", detail=str(exc)) from exc
        TwinLiveServing.refresh_view(store, graph)
        return seeded

    @staticmethod
    def retire(store: Any, domain: Any) -> bool:
        """Empty and stop serving *domain*'s ``_live`` when it is served; True when it was."""
        from back.core.helpers import effective_graph_name

        live = TwinLiveServing.live(store)
        graph = effective_graph_name(domain)
        if not live.serving(graph):
            return False
        live.detach(graph)
        TwinLiveServing.refresh_view(store, graph)
        logger.info("Live serving of %s retired", graph)
        return True

    @staticmethod
    def serving(store: Any, domain: Any) -> bool:
        from back.core.helpers import effective_graph_name

        return TwinLiveServing.live(store).serving(effective_graph_name(domain))

    @staticmethod
    def purge(store: Any, domain: Any, older_than_hours: int) -> int:
        from back.core.helpers import effective_graph_name

        return TwinLiveServing.live(store).purge_tombstones(
            effective_graph_name(domain), older_than_hours
        )

    @staticmethod
    def refresh_view(store: Any, graph: str) -> None:
        """Re-create *graph*'s union view so it picks up (or drops) the ``_live`` rows."""
        try:
            if getattr(store, "is_synced", False) is True:
                store.ensure_synced_union_view(graph)
            else:
                store.create_table(graph)
        except Exception as exc:  # noqa: BLE001 — the next build or tick re-creates it
            logger.warning("Union view of %s not refreshed yet: %s", graph, exc)
