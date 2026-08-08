"""Scheduled Knowledge Graph build.

Regenerates the Triple-Store VIEW from the domain's R2RML mappings and
repopulates the graph store. Scheduled builds are always full rebuilds:
the persisted ``drop_existing`` option is kept for backward
compatibility with older schedule configs but no longer changes
behaviour.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_GRAPH_NAME

from .context import RunOutcome, TaskContext

logger = get_logger(__name__)


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return {"drop_existing": bool((config or {}).get("drop_existing", True))}


def _generate_sql_from_r2rml(domain, domain_name: str):
    """Generate Spark SQL from R2RML mappings.

    Returns ``(sql_text, view_table, graph_name, base_uri, ent, rels)``.
    """
    from back.core.w3c import sparql
    from back.objects.digitaltwin import (
        augment_mappings_from_config,
        augment_relationships_from_config,
    )

    r2rml = domain.get_r2rml()
    if not r2rml:
        raise ValidationError("No R2RML mapping available")

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

    base_uri = domain.ontology.get("base_uri", "http://example.org/")
    mapping_config = domain.assignment
    ontology_config = domain.ontology

    logger.info("Scheduled build [%s]: generating SQL from R2RML", domain_name)
    ent, rels = sparql.extract_r2rml_mappings(r2rml)
    ent = augment_mappings_from_config(ent, mapping_config, base_uri, ontology_config)
    rels = augment_relationships_from_config(
        rels, mapping_config, base_uri, ontology_config
    )
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
) -> None:
    """Trigger the Lakeflow synced-table refresh for *graph_name*.

    Mirrors :meth:`_BuildPipeline._apply_via_synced_pipeline` so scheduled
    builds also keep bulk data movement on the data plane.
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
    from back.core.graphdb.lakebase._companion_ddl import LAKEFLOW_SYNC_PRIMARY_KEY

    mgr.ensure(
        synced_uc,
        source_table_full_name=view_table,
        primary_key_columns=list(LAKEFLOW_SYNC_PRIMARY_KEY),
        sync_mode=store.sync_table_mode,
    )
    store.ensure_synced_companion(graph_name)
    state = mgr.trigger_and_wait(synced_uc, timeout_s=store.sync_timeout_s)
    logger.info(
        "Scheduled build [%s]: synced table %s state=%s",
        domain_name,
        synced_uc,
        state,
    )
    store.ensure_synced_union_view(graph_name)
    if full:
        try:
            store.truncate_companion(graph_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Scheduled build [%s]: companion truncate failed (non-fatal): %s",
                domain_name,
                exc,
            )


def _count_view_triples(src, view_table: str) -> int:
    """Return the server-side triple count for *view_table*."""
    try:
        rows = src.execute_query(f"SELECT COUNT(*) AS cnt FROM {view_table}")
        return int(rows[0].get("cnt", 0)) if rows else 0
    except Exception:
        return 0


def _write_graph_triples(
    store,
    src,
    graph_name: str,
    view_table: str,
    domain_name: str,
    delta_cfg: Optional[Dict[str, Any]] = None,
    domain: Any = None,
    settings: Any = None,
) -> int:
    """Write triples to the graph store. Returns the triple count.

    When the store is in Lakebase ``managed_synced`` mode the entire branch is
    replaced by a Lakeflow snapshot refresh — triples never enter this process.
    Otherwise a full drop-and-rebuild is performed.
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
        )
        return _count_view_triples(src, view_table)

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
    sql_text, view_table, graph_name, _base_uri, ent_mappings, rel_mappings = (
        _generate_sql_from_r2rml(domain, domain_name)
    )

    from back.objects.digitaltwin._build_pipeline import collect_domain_stats

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

    triple_count = _write_graph_triples(
        store,
        src,
        graph_name,
        view_table,
        domain_name,
        delta_cfg=getattr(domain, "delta", None) or {},
        domain=domain,
        settings=ctx.settings,
    )

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
        from back.objects.digitaltwin._build_pipeline import step_times_from_task

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
                "stats": scratch.get("stats", {}),
            },
        )
    except Exception as trace_exc:  # noqa: BLE001
        logger.warning(
            "Scheduled build [%s]: could not record build-run trace: %s",
            ctx.domain_name,
            trace_exc,
        )
