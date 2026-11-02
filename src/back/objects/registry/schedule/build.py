"""Scheduled Knowledge Graph build.

Regenerates the Triple-Store VIEW from the domain's R2RML mappings and
repopulates the graph store. Scheduled builds are always full rebuilds:
the persisted ``drop_existing`` option is kept for backward
compatibility with older schedule configs but no longer changes
behaviour. In Lakebase ``managed_synced`` mode the build brings ``…_data`` up
to date — the incremental main build's materialized view is refreshed in
place (``TwinScheduledRefresh``), anything else is rebuilt by CTAS — and
registers the Lakeflow synced table on it with the schedule the domain's
materialization mode asks for, as the main build does (``TwinLakebaseSync``).
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_GRAPH_NAME

from back.objects.registry.schedule.RunOutcome import RunOutcome
from back.objects.registry.schedule.TaskContext import TaskContext

logger = get_logger(__name__)


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return {"drop_existing": bool((config or {}).get("drop_existing", True))}


def _generate_sql_from_r2rml(domain, domain_name: str):
    """Generate Spark SQL from R2RML mappings.

    Returns ``(sql_text, view_table, graph_name, base_uri, ent, rels)``.
    """
    from back.core.w3c import sparql
    from back.objects.digitaltwin.build import TwinIncrementalBuild

    logger.info("Scheduled build [%s]: generating SQL from R2RML", domain_name)
    mappings = TwinIncrementalBuild.mappings_for(domain)
    if mappings is None:
        raise ValidationError("No R2RML mapping available")
    ent, rels, base_uri = mappings

    delta = domain.delta or {}
    _name = (domain.info or {}).get("name", DEFAULT_GRAPH_NAME)
    _version = getattr(domain, "current_version", "1") or "1"
    _safe = re.sub(r"[^a-z0-9_]", "_", _name.lower())
    _view_name = f"triplestore_{_safe}_V{_version}"
    view_parts = [delta.get("catalog", ""), delta.get("schema", ""), _view_name]
    view_table = ".".join(p for p in view_parts if p)
    if not view_table or len(view_table.split(".")) != 3:
        raise ValidationError(f"View not fully qualified: {view_table}")
    graph_name = f"{_name}_V{_version}"

    if not ent and not rels:
        raise ValidationError("No valid mappings found")

    sparql_q = (
        f"PREFIX : <{base_uri}>\n"
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
        "SELECT DISTINCT ?subject ?predicate ?object\n"
        "WHERE {\n    ?subject ?predicate ?object .\n}"
    )
    res = sparql.translate_sparql_to_spark(sparql_q, ent, None, rels, dialect="spark")

    return res["sql"], view_table, graph_name, base_uri, ent, rels


def _stream_into_store(
    store, src, graph_name: str, select_sql: str, batch: int = 5000
) -> int:
    """Stream warehouse rows into ``store.bulk_insert_iter`` (or list fallback)."""
    rows = src.iter_rows(select_sql, batch_size=batch)
    if hasattr(store, "bulk_insert_iter"):
        return store.bulk_insert_iter(graph_name, rows, batch_size=batch)
    return store.insert_triples(graph_name, list(rows), batch_size=min(batch, 500))


def _is_managed_synced(store) -> bool:
    """Lakebase store in managed_synced mode -- bulk goes via Lakeflow."""
    return bool(getattr(store, "is_synced", False))


def _view_sql_for_graph_store(sql_text: str, store) -> str:
    """Return VIEW DDL, emitting ``object_hash`` when Lakeflow sync is active."""
    if store and _is_managed_synced(store):
        from back.core.graphdb.lakebase._companion_ddl import (
            wrap_triple_view_sql_for_lakeflow,
        )

        return wrap_triple_view_sql_for_lakeflow(sql_text)
    return sql_text


def _apply_synced_pipeline(
    store,
    src,
    delta_cfg: Dict[str, Any],
    graph_name: str,
    view_table: str,
    *,
    full: bool,
    domain_name: str,
    domain: Any = None,
    settings: Any = None,
    source_tables: Optional[List[str]] = None,
    mappings: Optional[Dict[str, Any]] = None,
) -> None:
    """Trigger the Lakeflow synced-table refresh for *graph_name*.

    Mirrors :meth:`_BuildPipeline._apply_via_synced_pipeline` so scheduled
    builds also keep bulk data movement on the data plane: ``…_data`` is
    materialized (with ``object_hash`` and row tracking) and the synced table
    is registered on the source and schedule :meth:`TwinLakebaseSync.sync_plan_for`
    picks for the domain's effective materialization mode
    (:meth:`TwinBuildPostMaterialize.decide_mode_for`, probing row tracking on
    *source_tables* exactly like the main build), so the main build's
    registration is reused, not recreated. *source_tables* ``None`` means the
    sources are unknown: nothing is probed and they count as untracked.
    *mappings* (``entity_mappings``, ``relationship_mappings`` and the
    compiled ``spec``) let an incremental ``…_data`` view be checked against
    the current definition before it is refreshed in place; its ``facts``
    receive the slot refresh plan's facts and ``sync_state`` the synced
    table's state after its refresh. Without a *domain* the
    ``…_data`` name cannot be resolved and the gateway VIEW stays the source.
    """
    from back.core.graphdb.lakebase.LakebaseFlatStore import (
        resolve_sync_uc_fallback_catalog,
    )
    from back.core.graphdb.lakebase._sync_uc_schema import (
        ensure_uc_schema_for_synced_table_fqn,
    )

    mgr = store.synced_manager()
    if domain is not None and settings is not None:
        fallback_cat = resolve_sync_uc_fallback_catalog(domain, settings, delta_cfg)
    else:
        fallback_cat = (delta_cfg or {}).get("catalog", "")
    synced_uc = store.synced_uc_name(graph_name, fallback_catalog=fallback_cat)
    logger.info(
        "Scheduled build [%s]: managed-sync UC target %s "
        "(sync_uc_catalog=%r; fallback_catalog=%r; graph_schema=%s)",
        domain_name,
        synced_uc,
        (store.sync_uc_catalog or "").strip() or None,
        fallback_cat or None,
        store.graph_schema,
    )
    ensure_uc_schema_for_synced_table_fqn(
        src,
        synced_uc,
        task_log_prefix=f"Scheduled build [{domain_name}]",
    )
    from back.core.graphdb.delta import _table_naming
    from back.objects.digitaltwin.build import TwinLakebaseSync

    data_table = _table_naming.data_table_fqn(domain, settings) if domain is not None else ""
    # Decided (and probed) before anything is written: a failed probe drops nothing.
    data_mode, sync_mode = _tick_modes(domain, settings, src, source_tables)
    kept = bool(data_table) and _refresh_data_relation(
        src, domain, settings, view_table, data_table, data_mode, mappings
    )
    plan = _tick_sync_plan(store, view_table, data_table, sync_mode, mappings, kept)
    mgr.ensure(
        synced_uc,
        before_delete=TwinLakebaseSync.union_view_dropper(store, graph_name),
        **plan.ensure_kwargs(),
    )
    store.ensure_synced_companion(graph_name)
    state, plan = TwinLakebaseSync.trigger_and_wait(
        mgr, synced_uc, plan, timeout_s=store.sync_timeout_s, data_is_mv=kept,
        before_delete=TwinLakebaseSync.union_view_dropper(store, graph_name),
    )
    if mappings is not None:
        mappings["sync_state"] = state
    logger.info(
        "Scheduled build [%s]: synced table %s state=%s",
        domain_name,
        synced_uc,
        state,
    )
    store.ensure_synced_union_view(graph_name, source_kind=plan.source_kind)
    if full:
        try:
            store.truncate_companion(graph_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Scheduled build [%s]: companion truncate failed (non-fatal): %s",
                domain_name,
                exc,
            )


def _tick_modes(
    domain: Any, settings: Any, src: Any, source_tables: Optional[List[str]]
) -> tuple[str, str]:
    """``(data_mode, sync_mode)`` of a tick, decided as the main build decides them.

    Same resolver and same row-tracking probe (``decide_mode_for``): a tick on
    another schedule than the main build would recreate the synced table
    (drift). *data_mode* is the resolver's effective mode; *sync_mode* is
    ``incremental`` for a decision the main build promotes to a MERGE
    (``TwinMergeFallback.promotes``), else the same. A failed probe raises.
    """
    from back.objects.digitaltwin.build import TwinBuildPostMaterialize, TwinMergeFallback

    decision = TwinBuildPostMaterialize.decide_mode_for(
        domain, settings, src, source_tables=source_tables
    )
    promoted = TwinMergeFallback.promotes(decision)
    return decision.effective, "incremental" if promoted else decision.effective


def _tick_sync_plan(
    store: Any,
    view_table: str,
    data_table: str,
    sync_mode: str,
    mappings: dict[str, Any] | None,
    kept: bool,
) -> Any:
    """The tick's :class:`SyncPlan`, decided as the main build decides it.

    *kept* (the ``…_data`` view was refreshed in place) means the slot pipeline
    is live, so the compiled ``spec``'s slots can be the source exactly as
    :meth:`TwinLakebaseSync.sync_plan` allows; a rebuilt ``…_data`` syncs itself.
    """
    from back.objects.digitaltwin.build import TwinLakebaseSync

    spec = (mappings or {}).get("spec") if kept else None
    return TwinLakebaseSync.sync_plan_for(
        view_table=view_table,
        data_table=data_table,
        mode_effective=sync_mode,
        configured_sync_table_mode=store.sync_table_mode,
        slots_table=getattr(spec, "slots_fqn", "") or "",
        batch_mappings=len(spec.batch_mappings) if spec is not None else None,
    )


def _refresh_data_relation(
    src: Any,
    domain: Any,
    settings: Any,
    view_table: str,
    data_table: str,
    data_mode: str,
    mappings: Optional[Dict[str, Any]],
    *,
    relation: str = "table",
    with_object_hash: bool = True,
) -> bool:
    """Bring ``…_data`` up to date for the synced table (or the Lakehouse graph).

    The incremental main build's materialized view is refreshed in place when
    it still has the current definition (``TwinScheduledRefresh``); anything
    else — a ``full`` or MERGE-promoted domain, no view yet, mappings edited
    since, a missing slot pipeline — is built from the gateway VIEW as the
    main build builds it: a *relation* ``table`` (with *with_object_hash* and
    row tracking for a synced table) or a Lakehouse ``view``. Returns ``True``
    when the view was kept (``TwinScheduledRefresh`` then refreshed a
    ``…_graph`` materialized view too), ``False`` after a rebuild.
    """
    from back.core.graphdb.delta import materialize
    from back.objects.digitaltwin.build import TwinScheduledRefresh

    tick = mappings or {}
    refreshed = TwinScheduledRefresh.refresh(
        src,
        domain,
        settings,
        data_table,
        data_mode,
        entity_mappings=tick.get("entity_mappings"),
        relationship_mappings=tick.get("relationship_mappings"),
        spec=tick.get("spec"),
        facts=tick.get("facts"),
    )
    if refreshed:
        return True
    materialize.apply_data_relation(
        src, view_table, data_table, mode=relation, with_object_hash=with_object_hash
    )
    return False


def _slot_spec(
    domain: Any,
    settings: Any,
    ent: Any,
    rels: Any,
    base_uri: str,
    domain_name: str,
    client: Any = None,
) -> Any:
    """The slot pipeline spec of the mappings, compiled once per tick.

    Its ``source_tables`` are what the row-tracking probe reads (the same list
    the main build probes, ``TwinIncrementalBuild.compile_for``), and
    ``TwinScheduledRefresh`` checks the ``…_data`` view against it; a
    topic-fed spec reads its sources' column types on *client*, as the main
    build does, so both give the same SQL. ``None`` — sources unknown, counted
    as untracked — without a domain or settings, for a ``full`` domain
    (nothing to probe), and when the mappings do not compile
    (``ValidationError``). Anything else raises and fails the tick before it
    writes: a topic-fed source that cannot be described is an
    ``InfrastructureError`` (compiling without its types would give other SQL).
    """
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    if domain is None or settings is None:
        return None
    if GraphDBFactory.resolve_materialization_mode(domain, settings) == "full":
        return None
    from back.objects.digitaltwin.build import TwinIncrementalBuild

    try:
        spec = TwinIncrementalBuild.compile_for(
            domain, settings, ent, rels, base_uri=base_uri, client=client
        )
    except ValidationError as exc:
        logger.warning(
            "Scheduled build [%s]: the mappings do not compile into a slot pipeline; "
            "the row-tracking probe is skipped (sources count as untracked): %s",
            domain_name,
            exc,
        )
        return None
    return spec


def _probed_tables(slot_spec: Any) -> list[str] | None:
    """The tables whose row tracking the tick probes, as the main build does.

    ``None`` (sources unknown, counted as untracked) without a compiled spec;
    none for a topic-fed realtime pipeline, which reads no change feed
    (``TwinSlotFeed.feed_tables``).
    """
    if slot_spec is None:
        return None
    from back.objects.digitaltwin.build import TwinSlotFeed

    return TwinSlotFeed.feed_tables(slot_spec)


def _reconcile_after_tick(
    store,
    src,
    domain: Any,
    settings: Any,
    slot_spec: Any,
    domain_name: str,
    *,
    sync_state: str = "",
) -> Dict[str, Any]:
    """The facts reconciling the tick's ``…_data`` view records (warnings logged).

    ``{"reconcile_drift": True}`` for an ``incremental`` domain whose view
    drifted (the next slot update owes a full refresh, ``TwinSlotRefreshPlan``),
    ``{"live_drift": True}`` for a ``realtime`` domain whose ``_live`` table
    and slots differ (the stream to restart), else ``{}``. Only for a
    Lakeflow-synced store (*slot_spec* set) whose ``…_data`` is still the
    materialized view the tick refreshed in place: a CTAS table equals the VIEW
    by construction. A ``microbatch`` domain is not reconciled (its slots run
    ahead: no pair can tell). An ``incremental`` tick also runs the batch
    recompute; the synced table is counted only when its refresh came
    ``ONLINE`` (*sync_state*). Best-effort: a reconciliation that fails, for
    any reason, never fails the tick.
    """
    from back.core.graphdb.delta import _table_naming, materialize
    from back.core.graphdb.GraphDBFactory import GraphDBFactory, normalize_materialization_mode
    from back.objects.digitaltwin.build import TwinSlotReconciliation

    if slot_spec is None or not _is_managed_synced(store):
        return {}
    try:
        mode = normalize_materialization_mode(
            GraphDBFactory.resolve_materialization_mode(domain, settings)
        )
        if mode == "microbatch":
            return {}
        data_table = _table_naming.data_table_fqn(domain, settings)
        if materialize.relation_kind(src, data_table) != "mv":
            return {}
        counted = mode == "realtime" or str(sync_state or "").upper().startswith("ONLINE")
        result = TwinSlotReconciliation.run(
            domain,
            settings,
            src,
            get_store=(lambda d, s: store) if counted else None,
            recompute=mode != "realtime",
        )
    except Exception as exc:  # noqa: BLE001 — reconciliation is advisory; the tick goes on
        logger.warning("Scheduled build [%s]: reconciliation skipped: %s", domain_name, exc)
        return {}
    if mode == "realtime":
        if not result.get("live_drift"):
            return {}
        logger.warning(
            "Scheduled build [%s]: the realtime _live table and the slots differ "
            "(live %s); restart the stream",
            domain_name,
            result.get("live"),
        )
        return {"live_drift": True}
    if not result.get("reconcile_drift"):
        return {}
    logger.warning(
        "Scheduled build [%s]: reconciliation found drift (%s); the next slot update "
        "runs as a full refresh",
        domain_name,
        sorted(name for name, differs in (result.get("pairs") or {}).items() if differs),
    )
    return {"reconcile_drift": True}


def _tick_facts(ctx: TaskContext) -> Dict[str, Any]:
    """The tick's ``stats.materialization`` (slot refresh plan, reconciliation).

    Attached to the run's stats right away, so a tick that fails half-way
    still records the full refresh it left pending.
    """
    facts: Dict[str, Any] = {}
    ctx.scratch["stats"]["materialization"] = facts
    return facts


def _close_tick_facts(ctx: TaskContext, tick: Dict[str, Any], store, src) -> None:
    """Reconcile at the end of the tick (drift / live drift recorded), then drop empty facts.

    *tick* is what the tick handed down (``spec``, ``facts``) and got back
    (``sync_state``, the synced table's state after its refresh).
    """
    facts = tick["facts"]
    facts.update(
        _reconcile_after_tick(
            store,
            src,
            ctx.domain,
            ctx.settings,
            tick.get("spec"),
            ctx.domain_name,
            sync_state=tick.get("sync_state", ""),
        )
    )
    facts.update(_realtime_tick(ctx, store))
    if not facts:
        del ctx.scratch["stats"]["materialization"]


def _realtime_tick(ctx: TaskContext, store) -> Dict[str, Any]:
    """A realtime domain's tick housekeeping (``TwinRealtimeStream.after_tick``), best-effort.

    Retires a served ``_live`` whose stream no longer runs and purges old
    tombstones; any other domain or store records nothing.
    """
    from back.core.graphdb.GraphDBFactory import GraphDBFactory, normalize_materialization_mode
    from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase
    from back.objects.digitaltwin.build import TwinRealtimeStream

    mode = normalize_materialization_mode(
        GraphDBFactory.resolve_materialization_mode(ctx.domain, ctx.settings)
    )
    if mode != "realtime" or not isinstance(store, LakebaseBase):
        return {}
    try:
        return TwinRealtimeStream(ctx.settings).after_tick(ctx.domain, store)
    except Exception as exc:  # noqa: BLE001 — housekeeping never fails the tick
        logger.warning("Scheduled build [%s]: realtime housekeeping skipped: %s", ctx.domain_name, exc)
        return {}


def _trace_stats(stats: Any) -> Dict[str, Any]:
    """The tick's stats for its build-run trace, without an empty ``materialization``.

    A tick that failed before any fact was recorded leaves the dict it attached
    up front empty; an empty dict would still read as "facts" to the readers.
    """
    trace = dict(stats) if isinstance(stats, dict) else {}
    if not trace.get("materialization"):
        trace.pop("materialization", None)
    return trace


def _count_view_triples(src, view_table: str) -> int:
    """Return the server-side triple count for *view_table*.

    Raises :class:`InfrastructureError` when the count cannot run, so a
    scheduled rebuild does not look like an empty mapping.
    """
    from back.core.graphdb.delta import materialize

    return materialize.count_triples(src, view_table)


def _is_lakehouse(domain: Any, settings: Any) -> bool:
    """True for a domain on the Lakehouse (Delta) engine, whose graph is ``…_data``."""
    if domain is None or settings is None:
        return False
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    return GraphDBFactory._resolve_triple_store_backend(domain, settings) == "databricks"


def _refresh_lakehouse_graph(
    src: Any,
    domain: Any,
    settings: Any,
    view_table: str,
    *,
    source_tables: list[str] | None,
    mappings: dict[str, Any] | None,
) -> int:
    """A Lakehouse tick: ``…_data`` and ``…_graph`` brought up to date; the ``…_data`` count.

    As the main build (``TwinIncrementalBuild.skips_graph_stream``), nothing is
    streamed into the store: that would append every asserted triple to
    ``…_inferred``. The mode is decided (and probed) first, so a failed probe
    writes nothing. ``…_inferred`` is emptied, as the main build's reset does
    and as every other tick's store rebuild discards its inferences: they were
    derived from the previous data. ``…_data`` is then the incremental view
    refreshed in place (``TwinScheduledRefresh`` refreshes a ``…_graph``
    materialized view after it) or built from the gateway VIEW (a table, or
    the domain's pass-through view), after which a ``…_graph`` materialized
    view is refreshed here — once either way (a VIEW needs nothing).
    """
    from back.core.graphdb.delta import GraphRelation, _table_naming
    from back.core.graphdb.GraphDBFactory import GraphDBFactory
    from back.core.helpers import WarehouseStatements
    from back.objects.digitaltwin.build import TwinBuildPostMaterialize

    data_table = _table_naming.data_table_fqn(domain, settings)
    inferred = _table_naming.inferred_table_fqn(domain, settings)
    graph = _table_naming.graph_view_fqn(domain, settings)
    if not (data_table and inferred and graph):
        raise ValidationError("Could not resolve the mapped-triples table names")
    data_mode, _sync_mode = _tick_modes(domain, settings, src, source_tables)
    WarehouseStatements.step(
        f"empty {inferred}", lambda: TwinBuildPostMaterialize.empty_inferred(src, inferred)
    )
    kept = _refresh_data_relation(
        src,
        domain,
        settings,
        view_table,
        data_table,
        data_mode,
        mappings,
        relation=GraphDBFactory.resolve_lakehouse_materialization(domain, settings),
        with_object_hash=False,
    )
    if not kept:
        WarehouseStatements.step(f"refresh {graph}", lambda: GraphRelation.refresh(src, graph))
    return _count_view_triples(src, data_table)


def _write_graph_triples(
    store,
    src,
    graph_name: str,
    view_table: str,
    domain_name: str,
    delta_cfg: Optional[Dict[str, Any]] = None,
    domain: Any = None,
    settings: Any = None,
    source_tables: Optional[List[str]] = None,
    mappings: Optional[Dict[str, Any]] = None,
) -> int:
    """Write triples to the graph store. Returns the triple count.

    When the store is in Lakebase ``managed_synced`` mode the entire branch is
    replaced by a Lakeflow synced-table refresh (*source_tables* feeds its
    row-tracking probe) — triples never enter this process. On the Lakehouse
    engine ``…_data`` is the graph: it is brought up to date and nothing is
    streamed (:func:`_refresh_lakehouse_graph`). Otherwise a full
    drop-and-rebuild is performed.
    """
    if _is_managed_synced(store):
        _apply_synced_pipeline(
            store,
            src,
            delta_cfg or {},
            graph_name,
            view_table,
            full=True,
            domain_name=domain_name,
            domain=domain,
            settings=settings,
            source_tables=source_tables,
            mappings=mappings,
        )
        try:
            return _count_view_triples(src, view_table)
        except InfrastructureError as exc:
            logger.warning(
                "Scheduled build [%s]: could not count the synced triples "
                "(the sync itself succeeded): %s",
                domain_name,
                exc,
            )
            return 0
    if _is_lakehouse(domain, settings):
        return _refresh_lakehouse_graph(
            src, domain, settings, view_table, source_tables=source_tables, mappings=mappings
        )

    triple_count = _count_view_triples(src, view_table)
    logger.info(
        "Scheduled build [%s]: %d triples reported by VIEW",
        domain_name,
        triple_count,
    )

    if triple_count > 0:
        store.drop_table(graph_name)
        store.create_table(graph_name)
        _stream_into_store(
            store,
            src,
            graph_name,
            f"SELECT subject, predicate, object FROM {view_table}",
        )
        store.optimize_table(graph_name)
        logger.info(
            "Scheduled build [%s]: graph '%s' populated with %d triples",
            domain_name,
            graph_name,
            triple_count,
        )
    return triple_count


def _persist_domain_metadata(svc, domain, version: str, build_ts: str, domain_name: str):
    """Stamp last_build and write the domain doc via the active store."""
    domain.last_build = build_ts
    try:
        domain_data = domain.export_for_save()
        w_ok, w_msg = svc._store.write_version(domain_name, version, domain_data)
        if w_ok:
            logger.info(
                "Scheduled build [%s]: stamped last_build=%s in registry",
                domain_name,
                build_ts,
            )
        else:
            logger.error(
                "Scheduled build [%s]: write_version returned failure: %s",
                domain_name,
                w_msg,
            )
    except Exception as save_exc:
        logger.warning(
            "Scheduled build [%s]: could not stamp last_build: %s",
            domain_name,
            save_exc,
        )


def run(ctx: TaskContext) -> RunOutcome:
    """Rebuild the domain's VIEW and repopulate its graph store."""
    start = time.time()
    domain_name = ctx.domain_name

    ctx.progress(5, "Loading domain from registry...")
    domain = ctx.domain
    version = ctx.loaded_version

    ctx.progress(10, "Generating SQL from R2RML mappings...")
    sql_text, view_table, graph_name, base_uri, ent_mappings, rel_mappings = (
        _generate_sql_from_r2rml(domain, domain_name)
    )

    from back.objects.digitaltwin.build._build_pipeline import collect_domain_stats

    ctx.scratch.update(
        {
            "view_table": view_table,
            "graph_name": graph_name,
            "entity_count": len(ent_mappings or []),
            "relationship_count": len(rel_mappings or []),
            "sql_chars": len(sql_text or ""),
            "version": str(version),
            # Ontology + mapping stats for the build-run trace (Cockpit picture).
            "stats": collect_domain_stats(
                getattr(domain, "ontology", {}),
                getattr(domain, "assignment", {}),
                constraints=getattr(domain, "constraints", None),
                swrl_rules=getattr(domain, "swrl_rules", None),
                axioms=getattr(domain, "axioms", None),
                shacl_shapes=getattr(domain, "shacl_shapes", None),
            ),
        }
    )

    # Resolve the graph backend before VIEW creation so managed_synced builds
    # can emit object_hash (Lakeflow keys the synced PK on that column).
    store = ctx.graph_store
    src = ctx.warehouse_client

    ctx.advance(f"Creating VIEW {view_table}...")
    view_sql = _view_sql_for_graph_store(sql_text, store)
    cat, sch, vname = view_table.split(".")
    logger.info("Scheduled build [%s]: creating VIEW %s", domain_name, view_table)
    view_ok, view_msg = src.create_or_replace_view(cat, sch, vname, view_sql)
    if not view_ok:
        from back.objects.digitaltwin import DigitalTwin

        detail = DigitalTwin.diagnose_view_error(view_msg, ent_mappings, rel_mappings)
        logger.error(
            "Scheduled build [%s]: VIEW creation failed:\n%s", domain_name, detail
        )
        raise InfrastructureError(f"Failed to create VIEW: {detail}")
    ctx.progress(40, "VIEW created")

    ctx.advance(f"Applying to graph {graph_name}...")
    if not store:
        raise InfrastructureError("Could not initialize graph backend")

    slot_spec = _slot_spec(
        domain, ctx.settings, ent_mappings, rel_mappings, base_uri, domain_name, client=src
    )
    tick = {
        "entity_mappings": ent_mappings,
        "relationship_mappings": rel_mappings,
        "spec": slot_spec,
        "facts": _tick_facts(ctx),
    }
    triple_count = _write_graph_triples(
        store,
        src,
        graph_name,
        view_table,
        domain_name,
        delta_cfg=getattr(domain, "delta", None) or {},
        domain=domain,
        settings=ctx.settings,
        source_tables=_probed_tables(slot_spec),
        mappings=tick,
    )
    _close_tick_facts(ctx, tick, store, src)

    ctx.progress(95, "Saving domain metadata...")
    _persist_domain_metadata(
        ctx.registry_service, domain, version, ctx.run_ts, domain_name
    )

    return RunOutcome(
        status="success",
        message=f"Built {triple_count} triples in {time.time() - start:.1f}s",
        count=triple_count,
        task_result={
            "triple_count": triple_count,
            "duration_seconds": time.time() - start,
        },
    )


def on_finish(ctx: TaskContext, outcome: RunOutcome, duration_s: float) -> None:
    """Append the build-run trace row that powers the Runs pages.

    Independent of the ``schedule_runs`` history the harness writes: this
    is the cross-path build trace (session / api / scheduled) the Cockpit
    reads. Best-effort — a build must never fail because tracing did not
    land.
    """
    if ctx._svc is None:
        return
    try:
        from back.objects.digitaltwin.build._build_pipeline import step_times_from_task

        task = ctx.tm.get_task(ctx.task_id) if ctx.tm else None
        scratch = ctx.scratch
        ctx.registry_service.record_build_run(
            ctx.domain_name,
            {
                "version": scratch.get("version", str(ctx.version)),
                "build_kind": "scheduled",
                "status": outcome.status,
                "message": outcome.message if outcome.status == "success" else "",
                "error": outcome.message if outcome.status != "success" else "",
                "started_at": ctx.run_ts,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "duration_s": duration_s,
                "triple_count": outcome.count,
                "entity_count": scratch.get("entity_count", 0),
                "relationship_count": scratch.get("relationship_count", 0),
                "sql_chars": scratch.get("sql_chars", 0),
                "graph_engine": "",
                "sync_mode": "",
                "view_table": scratch.get("view_table", ""),
                "graph_name": scratch.get("graph_name", ""),
                "task_id": ctx.task_id or "",
                "phase_times": step_times_from_task(task) if task else {},
                "stats": _trace_stats(scratch.get("stats", {})),
            },
        )
    except Exception as trace_exc:  # noqa: BLE001
        logger.warning(
            "Scheduled build [%s]: could not record build-run trace: %s",
            ctx.domain_name,
            trace_exc,
        )
