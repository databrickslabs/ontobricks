"""Reconcile a domain's ``_slots`` streaming table with its ``_data`` relation."""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)


class TwinSlotReconciliation:
    """Domain-aware wrapper around :class:`SlotReconciler`.

    Resolves the two relation names for the domain, refuses when no incremental
    build has created the slots table yet, and adds ``batch_branches_present``:
    with batch (join / ``sql_query``) mappings the ``_data`` MV holds more than
    the slots, so a mismatch is expected and ``drift`` is not meaningful.
    """

    @staticmethod
    def run(domain: Any, settings: Any, client: Any) -> dict:
        """Compare the domain's slots with its ``_data`` relation.

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
        result = SlotReconciler.compare(client, slots, data)
        return {
            **result,
            "slots_table": slots,
            "data_table": data,
            "batch_branches_present": batch,
            "slots_run_ahead": continuous,
            "drift_meaningful": not batch and not continuous,
        }

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
        from back.core.w3c import sparql
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        r2rml = domain.get_r2rml() if hasattr(domain, "get_r2rml") else ""
        if not r2rml:
            return True
        base_uri = (getattr(domain, "ontology", None) or {}).get("base_uri", DEFAULT_BASE_URI)
        mapping_config = getattr(domain, "assignment", None)
        ontology_config = getattr(domain, "ontology", None)
        try:
            ent, rels = sparql.extract_r2rml_mappings(r2rml)
            ent = DigitalTwin.augment_mappings_from_config(ent, mapping_config, base_uri, ontology_config)
            rels = DigitalTwin.augment_relationships_from_config(
                rels, mapping_config, base_uri, ontology_config
            )
            spec = SlotProjectionCompiler.compile(ent, rels, base_uri=base_uri, slots_fqn=slots_fqn)
        except ValidationError as exc:
            logger.warning("Slot reconciliation: mappings do not compile (%s); counted as batch", exc)
            return True
        return bool(spec.batch_mappings)
