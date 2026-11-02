"""Readiness preflight of an incremental / microbatch domain, and the row-tracking fix."""

from __future__ import annotations

from typing import Any

from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import WarehouseStatements
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildPostMaterialize import (
    TwinBuildPostMaterialize,
)
from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild
from back.objects.digitaltwin.build.TwinSlotFeed import TwinSlotFeed
from back.objects.digitaltwin.build.TwinSlotPipeline import TwinSlotPipeline

logger = get_logger(__name__)

#: The :meth:`TwinIncrementalReadiness._compiled` facts the report passes on as they are.
_MAPPING_FACTS = ("compile_error", "mappings", "slot_mappings", "batch_mappings")


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
        """``mode_requested``, ``decision``, ``serverless_available``, ``sources``,
        ``row_tracking_required``, ``compile_error``, ``mappings``, ``slot_mappings``,
        ``batch_mappings``, ``realtime``, ``previews``, ``read_relation``, ``lakehouse_rt``.

        ``decision`` is what a build would run: ``effective``, ``reason`` and
        ``merge_fallback`` — an ``incremental`` request without serverless runs
        as ``incremental`` by MERGE (:meth:`TwinMergeFallback.promotes`), as
        the build does. Mappings that do not compile are reported in
        ``compile_error`` with the build's ``incremental_failed:ValidationError``
        decision (``mappings`` and its counts: :meth:`_compiled`). A slot
        pipeline fed by the realtime topic reads no change feed:
        ``row_tracking_required`` is false and its sources are listed unprobed
        (``row_tracking: None``). A probe that fails raises
        :class:`InfrastructureError` (an unknown answer is not a "no").
        ``read_relation`` is what Lakehouse graph reads take now
        (``GraphReadRelation``: ``graph`` or ``slots_live``), ``lakehouse_rt``
        :meth:`lakehouse_rt_facts`.
        """
        from back.core.graphdb.delta import GraphReadRelation
        from back.core.graphdb.GraphDBFactory import (
            GraphDBFactory,
            normalize_materialization_mode,
        )
        from back.core.graphdb.ModeDecision import ModeDecision
        from back.core.streaming import SourceReadiness
        from back.objects.digitaltwin.build.TwinMergeFallback import TwinMergeFallback

        requested = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(domain, settings)
        )
        compiled = TwinIncrementalReadiness._compiled(domain, settings, client)
        tracked = SourceReadiness.check(client, compiled["probed"])
        decision = TwinBuildPostMaterialize.resolve_for(
            domain, settings, sources_row_tracked=all(tracked.values())
        )
        if compiled["compile_error"] and requested != "full":
            decision = ModeDecision(requested, "full", "incremental_failed:ValidationError")
        merge = TwinMergeFallback.promotes(decision)
        read_relation = GraphReadRelation.decide(domain, settings, client=client)
        return {
            "mode_requested": requested,
            "decision": {
                "effective": "incremental" if merge else decision.effective,
                "reason": decision.reason,
                "merge_fallback": merge,
            },
            "serverless_available": TwinBuildPostMaterialize.serverless_available(settings),
            "sources": [{"table": t, "row_tracking": tracked.get(t)} for t in compiled["tables"]],
            "row_tracking_required": compiled["required"],
            **{key: compiled[key] for key in _MAPPING_FACTS},
            "realtime": TwinIncrementalReadiness.realtime_facts(domain, settings),
            "previews": {"slot_pipeline_channel": TwinSlotPipeline.channel(settings)},
            "read_relation": read_relation,
            "lakehouse_rt": TwinIncrementalReadiness.lakehouse_rt_facts(
                domain, settings, read_relation
            ),
        }

    @staticmethod
    def mapping_classes(entity_mappings: Any, relationship_mappings: Any) -> list:
        """One ``{class_uri, predicate, map_id, kind, reason}`` per mapping the slot compiler sees.

        In :meth:`SlotProjectionCompiler.compile`'s order: each class of an
        entity mapping (``class_uri``), then each relationship mapping
        (``predicate``). ``kind`` is ``slot`` for a row-local mapping (its
        ``map_id``; ``reason`` ``""``) — the one a ``microbatch`` pipeline
        streams — and ``batch`` for one ``…_data``'s batch branch recomputes
        (``map_id`` ``None``; ``reason`` is
        :meth:`SlotProjectionCompiler.batch_reason`: a join, a ``sql_query``
        source, a relationship, ...).
        """
        from back.core.streaming import SlotProjectionCompiler

        classes = []
        for class_uri, mapping in (entity_mappings or {}).items():
            tagged = {**mapping, "class_uri": class_uri}
            reason = SlotProjectionCompiler.batch_reason(tagged)
            map_id = None if reason else SlotProjectionCompiler.map_id(tagged)
            classes.append(TwinIncrementalReadiness._class(class_uri, None, map_id, reason))
        for mapping in relationship_mappings or []:
            reason = SlotProjectionCompiler.batch_reason(mapping)
            classes.append(TwinIncrementalReadiness._class(None, mapping.get("predicate"), None, reason))
        return classes

    @staticmethod
    def lakehouse_rt_facts(domain: Any, settings: Any, read_relation: str) -> dict | None:
        """``query_warehouse_configured``, ``use_sea`` and ``fast_path`` of a Lakehouse-engine domain.

        Whether ``graph_engine_config.lakehouse.warehouse_id`` names a query
        warehouse, whether "Use Lakehouse//RT for queries" is on, and
        *read_relation* (the ``read_relation`` already decided). ``None`` on
        any other engine: Lakehouse//RT serves the Delta engine only.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.helpers import DatabricksHelpers

        if GraphDBFactory._resolve_triple_store_backend(domain, settings) != "databricks":
            return None
        return {
            "query_warehouse_configured": bool(
                DatabricksHelpers.resolve_lakehouse_warehouse_id(domain, settings)
            ),
            "use_sea": bool(DatabricksHelpers.resolve_lakehouse_use_sea(domain, settings)),
            "fast_path": read_relation,
        }

    @staticmethod
    def realtime_facts(domain: Any, settings: Any) -> dict:
        """``enabled`` (the setting), ``backend_eligible`` (Lakebase only), ``source_configured``,
        ``kind``, ``experimental`` (the kind) and ``topic`` of the domain.

        No secret is read: the config holds the secret's scope and key names only.
        """
        from back.core.graphdb import RealtimeSource
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.graphdb.RealtimeSource import EXPERIMENTAL_SOURCE_KINDS

        config = RealtimeSource.of(domain)
        return {
            "enabled": getattr(settings, "realtime_enabled", False) is True,
            "backend_eligible": GraphDBFactory._resolve_graph_backend(domain) == "lakebase",
            "source_configured": RealtimeSource.is_complete(config),
            "kind": config.get("kind", ""),
            "experimental": config.get("kind", "") in EXPERIMENTAL_SOURCE_KINDS,
            "topic": config.get("topic", ""),
        }

    @staticmethod
    def enable_row_tracking(
        domain: Any, settings: Any, client: Any, tables: list | None = None
    ) -> dict:
        """Turn row tracking on for the untracked sources (or the listed *tables*).

        *tables* must all be sources whose row tracking the domain's slot
        pipeline needs (:meth:`source_tables`; none for a topic-fed one),
        compared case-insensitively, else :class:`ValidationError`. Each table
        is altered on its own; the result lists ``{table, success[, error]}``.
        """
        from back.core.streaming import SourceReadiness

        sources = TwinIncrementalReadiness.source_tables(domain, settings, client)
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
    def source_tables(domain: Any, settings: Any, client: Any = None) -> list:
        """The sources whose row tracking the domain's slot pipeline needs (``[]`` without mappings).

        Every source of a change-feed pipeline, none of a topic-fed one
        (:meth:`TwinSlotFeed.feed_tables`): its sources are never altered.
        """
        mappings = TwinIncrementalBuild.mappings_for(domain)
        spec = TwinIncrementalReadiness._spec(domain, settings, client, mappings)
        return [] if spec is None else TwinSlotFeed.feed_tables(spec)

    @staticmethod
    def _compiled(domain: Any, settings: Any, client: Any) -> dict:
        """The domain's mappings compiled as a build does, for :meth:`report`.

        ``tables`` (every slot-pipeline source), ``probed`` (those whose row
        tracking the feed needs), ``required``, ``mappings``
        (:meth:`mapping_classes`), ``slot_mappings`` / ``batch_mappings`` (how
        many of them are ``slot`` / ``batch``) and ``compile_error`` (``""``, or
        why they do not compile: then no tables, no mappings and ``None``
        counts).
        """
        facts = {"compile_error": "", "tables": [], "probed": [], "required": True, "mappings": []}
        try:
            mappings = TwinIncrementalBuild.mappings_for(domain)
            spec = TwinIncrementalReadiness._spec(domain, settings, client, mappings)
            if spec is not None:
                facts.update(
                    tables=list(spec.source_tables),
                    probed=TwinSlotFeed.feed_tables(spec),
                    required=spec.needs_row_tracking,
                    mappings=TwinIncrementalReadiness.mapping_classes(mappings[0], mappings[1]),
                )
        except ValidationError as exc:
            return {**facts, "compile_error": str(exc), "slot_mappings": None, "batch_mappings": None}
        kinds = [entry["kind"] for entry in facts["mappings"]]
        return {**facts, "slot_mappings": kinds.count("slot"), "batch_mappings": kinds.count("batch")}

    @staticmethod
    def _spec(domain: Any, settings: Any, client: Any, mappings: tuple | None) -> Any:
        """The slot pipeline spec of *mappings* (:meth:`TwinIncrementalBuild.mappings_for`) as a
        build compiles it on *client*; ``None`` without mappings."""
        if mappings is None:
            return None
        entity_mappings, relationship_mappings, base_uri = mappings
        return TwinIncrementalBuild.compile_for(
            domain, settings, entity_mappings, relationship_mappings, base_uri=base_uri, client=client
        )

    @staticmethod
    def _class(class_uri: Any, predicate: Any, map_id: Any, reason: Any) -> dict:
        """One :meth:`mapping_classes` entry: ``slot`` without a batch *reason*, else ``batch``."""
        return {
            "class_uri": class_uri,
            "predicate": predicate,
            "map_id": map_id,
            "kind": "batch" if reason else "slot",
            "reason": reason or "",
        }

    @staticmethod
    def _enable(client: Any, table: str) -> dict:
        from back.core.graphdb.delta import materialize
        from back.core.streaming import SourceReadiness

        sql = SourceReadiness.enable_sql(table)
        try:
            WarehouseStatements.step(
                f"enable row tracking on {table}", lambda: materialize.run_sql(client, sql)
            )
        except InfrastructureError as exc:
            logger.warning("Could not enable row tracking on %s: %s", table, exc)
            return {"table": table, "success": False, "error": str(exc)}
        logger.info("Enabled row tracking on %s", table)
        return {"table": table, "success": True}
