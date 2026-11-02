"""Reconcile a domain's ``_slots`` streaming table with its ``_data`` relation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)


class TwinSlotReconciliation:
    """Domain-aware wrapper around :class:`SlotReconciler`.

    Resolves the two relation names for the domain, refuses when no incremental
    build has created the slots table yet, and adds ``batch_branches_present``:
    with batch (join / ``sql_query``) mappings the ``_data`` MV holds more than
    the slots, so a mismatch is expected and ``drift`` is not meaningful.
    """

    @staticmethod
    def run(
        domain: Any, settings: Any, client: Any, *, get_store: Callable[..., Any] | None = None
    ) -> dict:
        """Compare the domain's slots, gateway VIEW and synced table with its ``_data``.

        The gateway VIEW recomputes every triple from the sources (the batch
        recompute); *get_store* opens the graph store, whose Lakebase synced
        table is counted when it is ``managed_synced``. ``pairs_meaningful``
        says which pairs can tell drift: ``slots_data`` only without batch
        branches, and no pair while the slots run ahead (``microbatch``).
        Both other pairs compare as of the last refresh of ``_data``: a source
        changed since shows as drift until the next build or tick.
        ``reconcile_drift`` is true when a meaningful pair differs.

        Raises :class:`ValidationError` when the relation names cannot be
        resolved, :class:`InfrastructureError` when there is no warehouse
        client or the catalog cannot be read (an unknown answer is not a
        "no"), and :class:`NotFoundError` when the catalog lists no slots
        table (the domain never ran an incremental or microbatch build).
        """
        from back.core.graphdb.delta import _table_naming, materialize
        from back.core.streaming import SlotReconciler

        if client is None:
            raise InfrastructureError(
                "No Databricks SQL client for this domain: check the Lakehouse host, token and warehouse"
            )
        slots = _table_naming.slots_table_fqn(domain, settings)
        data = _table_naming.data_table_fqn(domain, settings)
        if not slots or not data:
            raise ValidationError("The triple-store tables of this domain are not fully qualified")
        if not materialize.relation_exists(client, slots, strict=True):
            raise NotFoundError(
                f"Slots table {slots} does not exist: no incremental build has run for this domain"
            )
        batch = TwinSlotReconciliation.batch_branches_present(domain, slots_fqn=slots)
        continuous = TwinSlotReconciliation.slots_run_ahead(domain, settings)
        result = SlotReconciler.compare(
            client,
            slots,
            data,
            view_fqn=_table_naming.view_fqn(domain, settings) or None,
            sync_rows=TwinSlotReconciliation.sync_rows(domain, settings, get_store),
        )
        meaningful = {
            "slots_data": not batch and not continuous,
            "view_data": not continuous,
            "sync_data": not continuous,
        }
        pairs = result.get("pairs") or {}
        return {
            **result,
            "slots_table": slots,
            "data_table": data,
            "batch_branches_present": batch,
            "slots_run_ahead": continuous,
            "drift_meaningful": meaningful["slots_data"],
            "pairs_meaningful": meaningful,
            "reconcile_drift": any(pairs.get(name) and meaningful[name] for name in meaningful),
        }

    @staticmethod
    def sync_rows(
        domain: Any, settings: Any, get_store: Callable[..., Any] | None
    ) -> int | None:
        """Rows of the domain's Lakebase synced table (``managed_synced`` only), else ``None``.

        Best-effort: a store that cannot be opened or counted is left out of
        the comparison (warning) rather than failing it.
        """
        from back.core.helpers import effective_graph_name

        if get_store is None:
            return None
        try:
            store = get_store(domain, settings)
            if getattr(store, "is_synced", False) is not True:
                return None
            return int(store.count_triples(store.synced_table_name(effective_graph_name(domain))))
        except Exception as exc:  # noqa: BLE001 — store drivers raise their own errors
            logger.warning("Slot reconciliation: synced table not counted: %s", exc)
            return None

    @staticmethod
    def slots_run_ahead(domain: Any, settings: Any) -> bool:
        """True for a domain whose slot pipeline runs continuously (``microbatch``).

        Between two builds the continuous pipeline keeps folding source changes
        into ``_slots`` while ``_data`` only moves when it is refreshed, so a
        mismatch is expected there and says nothing about correctness.
        """
        from back.core.graphdb.GraphDBFactory import (
            GraphDBFactory,
            normalize_materialization_mode,
        )

        requested = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(domain, settings)
        )
        return requested in ("microbatch", "realtime")

    @staticmethod
    def batch_branches_present(domain: Any, *, slots_fqn: str) -> bool:
        """True when any of the domain's mappings is batch (not row-local).

        Mappings are derived the way a build derives them (R2RML plus the
        mapping configuration). Anything that stops the compiler counts as
        batch: an unknown mapping shape cannot be reconciled either.
        """
        from back.core.streaming import SlotProjectionCompiler
        from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

        try:
            mappings = TwinIncrementalBuild.mappings_for(domain)
            if mappings is None:
                return True
            ent, rels, base_uri = mappings
            spec = SlotProjectionCompiler.compile(ent, rels, base_uri=base_uri, slots_fqn=slots_fqn)
        except ValidationError as exc:
            logger.warning("Slot reconciliation: mappings do not compile (%s); counted as batch", exc)
            return True
        return bool(spec.batch_mappings)
