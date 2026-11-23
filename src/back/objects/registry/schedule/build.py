"""Scheduled Original Graph build.

Regenerates the R2RML gateway VIEW, exposes the Original Graph ``…_data``
VIEW and refreshes its Delta companions, for every backend. Nothing is loaded
into Lakebase / Neo4j: the certified build does that. The persisted
``drop_existing`` option is kept for backward compatibility with older
schedule configs but no longer changes behaviour.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict

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
    from back.objects.digitaltwin import DigitalTwin

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
    ent = DigitalTwin.augment_mappings_from_config(
        ent, mapping_config, base_uri, ontology_config
    )
    rels = DigitalTwin.augment_relationships_from_config(
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


def _build_original(src, view_table: str, domain: Any, settings: Any, domain_name: str) -> int:
    """Expose ``…_data`` over *view_table* and refresh its companions; return the count.

    The count runs first, so an empty mapping leaves ``…_data`` untouched;
    a count that cannot run raises :class:`InfrastructureError`.
    """
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    builder = GraphDBFactory.get_builder(domain, settings, engine="delta", client=src)
    triple_count = builder.count_triples(view_table)
    logger.info(
        "Scheduled build [%s]: %d triples reported by VIEW", domain_name, triple_count
    )
    if triple_count > 0:
        builder.apply_original_graph(view_table)
        builder.rebuild_original_companions()
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
    """Rebuild the domain's gateway VIEW and its Original Graph."""
    start = time.time()
    domain_name = ctx.domain_name

    ctx.progress(5, "Loading context from registry...")
    domain = ctx.domain
    version = ctx.loaded_version

    ctx.progress(10, "Generating SQL from R2RML mappings...")
    sql_text, view_table, graph_name, _base_uri, ent_mappings, rel_mappings = (
        _generate_sql_from_r2rml(domain, domain_name)
    )

    from back.objects.digitaltwin.build import BuildRunStats

    ctx.scratch.update(
        {
            "view_table": view_table,
            "graph_name": graph_name,
            "entity_count": len(ent_mappings or []),
            "relationship_count": len(rel_mappings or []),
            "sql_chars": len(sql_text or ""),
            "version": str(version),
            # Ontology + mapping stats for the build-run trace (Cockpit picture).
            "stats": BuildRunStats.domain_stats(
                getattr(domain, "ontology", {}),
                getattr(domain, "assignment", {}),
                constraints=getattr(domain, "constraints", None),
                if_then_rules=getattr(domain, "if_then_rules", None),
                axioms=getattr(domain, "axioms", None),
                shacl_shapes=getattr(domain, "shacl_shapes", None),
            ),
        }
    )

    src = ctx.warehouse_client

    ctx.advance(f"Creating VIEW {view_table}...")
    cat, sch, vname = view_table.split(".")
    logger.info("Scheduled build [%s]: creating VIEW %s", domain_name, view_table)
    view_ok, view_msg = src.create_or_replace_view(cat, sch, vname, sql_text)
    if not view_ok:
        from back.objects.digitaltwin import DigitalTwin

        detail = DigitalTwin.diagnose_view_error(view_msg, ent_mappings, rel_mappings)
        logger.error(
            "Scheduled build [%s]: VIEW creation failed:\n%s", domain_name, detail
        )
        raise InfrastructureError(f"Failed to create VIEW: {detail}")
    ctx.progress(40, "VIEW created")

    ctx.advance("Exposing the Original Graph...")
    triple_count = _build_original(src, view_table, domain, ctx.settings, domain_name)

    ctx.progress(95, "Saving context metadata...")
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
        from back.objects.digitaltwin.build import BuildRunStats

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
                "phase_times": BuildRunStats.step_times(task) if task else {},
                "stats": scratch.get("stats", {}),
            },
        )
    except Exception as trace_exc:  # noqa: BLE001
        logger.warning(
            "Scheduled build [%s]: could not record build-run trace: %s",
            ctx.domain_name,
            trace_exc,
        )
