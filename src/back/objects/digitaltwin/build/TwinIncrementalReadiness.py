"""Readiness preflight of an incremental / microbatch domain, and the row-tracking fix."""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)
from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild
from back.objects.digitaltwin.build.TwinSlotPipeline import TwinSlotPipeline
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)


class TwinIncrementalReadiness:
    """What the domain's requested mode would run as, and which sources hold it back.

    The report compiles the domain's mappings the way a build does
    (:meth:`TwinIncrementalBuild.compile_for`), probes row tracking on the slot
    pipeline's source tables (:class:`SourceReadiness`) and resolves the mode
    with the build's resolver inputs. The fix runs
    ``SourceReadiness.enable_sql`` on the domain's own untracked sources only —
    never on a name the caller made up — as the caller, so Unity Catalog decides
    who may alter which table.
    """

    @staticmethod
    def report(domain: Any, settings: Any, client: Any) -> dict:
        """``mode_requested``, ``decision``, ``serverless_available``, ``sources``, ``previews``.

        A probe that fails raises :class:`InfrastructureError` (an unknown
        answer is not a "no").
        """
        from back.core.graphdb.GraphDBFactory import (
            GraphDBFactory,
            normalize_materialization_mode,
        )
        from back.core.streaming import SourceReadiness

        tables = TwinIncrementalReadiness.source_tables(domain, settings)
        tracked = SourceReadiness.check(client, tables)
        decision = TwinBuildPostMaterialize.resolve_for(
            domain, settings, sources_row_tracked=all(tracked.values())
        )
        return {
            "mode_requested": normalize_materialization_mode(
                GraphDBFactory.resolve_materialization_mode(domain, settings)
            ),
            "decision": {"effective": decision.effective, "reason": decision.reason},
            "serverless_available": TwinBuildPostMaterialize.serverless_available(settings),
            "sources": [{"table": table, "row_tracking": tracked[table]} for table in tables],
            "previews": {"slot_pipeline_channel": TwinSlotPipeline.channel(settings)},
        }

    @staticmethod
    def enable_row_tracking(
        domain: Any, settings: Any, client: Any, tables: list | None = None
    ) -> dict:
        """Turn row tracking on for the untracked sources (or the listed *tables*).

        *tables* must all be source tables of the domain's slot pipeline
        (compared case-insensitively), else :class:`ValidationError`. Each table
        is altered on its own; the result lists ``{table, success[, error]}``.
        """
        from back.core.streaming import SourceReadiness

        sources = TwinIncrementalReadiness.source_tables(domain, settings)
        if tables is None:
            targets = [t for t, ok in SourceReadiness.check(client, sources).items() if not ok]
        else:
            known = {table.lower(): table for table in sources}
            unknown = [str(t) for t in tables if str(t).lower() not in known]
            if unknown:
                raise ValidationError(
                    "Not a source table of this domain's slot pipeline: " + ", ".join(unknown)
                )
            targets = [known[str(t).lower()] for t in tables]
        return {"tables": [TwinIncrementalReadiness._enable(client, t) for t in targets]}

    @staticmethod
    def source_tables(domain: Any, settings: Any) -> list:
        """The slot pipeline's source tables for the domain's current mappings (``[]`` without)."""
        mappings = TwinIncrementalReadiness.domain_mappings(domain)
        if mappings is None:
            return []
        entity_mappings, relationship_mappings, base_uri = mappings
        spec = TwinIncrementalBuild.compile_for(
            domain, settings, entity_mappings, relationship_mappings, base_uri=base_uri
        )
        return list(spec.source_tables)

    @staticmethod
    def domain_mappings(domain: Any) -> tuple | None:
        """``(entity mappings, relationship mappings, base URI)`` as a build derives them.

        R2RML plus the mapping configuration; ``None`` when the domain has no
        R2RML yet.
        """
        from back.core.w3c import sparql
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        r2rml = domain.get_r2rml() if hasattr(domain, "get_r2rml") else ""
        if not r2rml:
            return None
        ontology = getattr(domain, "ontology", None) or {}
        base_uri = ontology.get("base_uri", DEFAULT_BASE_URI)
        mapping_config = getattr(domain, "assignment", None)
        ent, rels = sparql.extract_r2rml_mappings(r2rml)
        ent = DigitalTwin.augment_mappings_from_config(ent, mapping_config, base_uri, ontology)
        rels = DigitalTwin.augment_relationships_from_config(
            rels, mapping_config, base_uri, ontology
        )
        return ent, rels, base_uri

    @staticmethod
    def _enable(client: Any, table: str) -> dict:
        from back.core.graphdb.delta import materialize
        from back.core.streaming import SourceReadiness

        sql = SourceReadiness.enable_sql(table)
        try:
            TwinIncrementalBuild.warehouse_step(
                f"enable row tracking on {table}", lambda: materialize.run_sql(client, sql)
            )
        except InfrastructureError as exc:
            logger.warning("Could not enable row tracking on %s: %s", table, exc)
            return {"table": table, "success": False, "error": str(exc)}
        logger.info("Enabled row tracking on %s", table)
        return {"table": table, "success": True}
