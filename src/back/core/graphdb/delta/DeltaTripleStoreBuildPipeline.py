"""Narrow build pipeline: R2RML VIEW → materialized Delta TABLE (no Lakebase)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from back.core.errors import OntoBricksError, OperationCancelledError
from back.core.graphdb.delta import _table_naming, materialize
from back.core.logging import get_logger
from back.objects.digitaltwin._build_pipeline import collect_domain_stats
from back.objects.digitaltwin.models import DomainSnapshot

logger = get_logger(__name__)


class DeltaTripleStoreBuildPipeline:
    """Materialize the UC Delta triple store from R2RML mappings only."""

    def __init__(
        self,
        *,
        tm: Any,
        task_id: str,
        domain: Any,
        domain_snap: DomainSnapshot,
        settings: Any,
        host: str,
        token: str,
        warehouse_id: str,
        view_table: str,
        data_table: str,
        r2rml_content: str,
        mapping_config: Any,
        ontology_config: Any,
        base_uri: str,
        build_kind: str = "ui",
    ) -> None:
        self.tm = tm
        self.task_id = task_id
        self.domain = domain
        self.domain_snap = domain_snap
        self.settings = settings
        self.host = host
        self.token = token
        self.warehouse_id = warehouse_id
        self.view_table = view_table
        self.data_table = data_table
        self.r2rml_content = r2rml_content
        self.mapping_config = mapping_config
        self.ontology_config = ontology_config
        self.base_uri = base_uri
        self.build_kind = build_kind
        self.is_api = build_kind == "api"
        self.start_time = time.time()
        self.phase_times: Dict[str, float] = {}
        self.parts = view_table.split(".")
        self._build_recorded = False
        self.domain_name = (domain.info or {}).get("name", "<unknown>")
        self.source_client = None
        self.entity_mappings: list = []
        self.relationship_mappings: list = []
        self.spark_sql: str = ""
        self.triple_count: int = 0
        self.inferred_table = _table_naming.inferred_table_fqn(domain, settings)
        self.graph_view = _table_naming.graph_view_fqn(domain, settings)

    def _log_phase(self, name: str, t0_phase: float) -> None:
        elapsed = time.time() - t0_phase
        self.phase_times[name] = elapsed
        logger.info(
            "[DT-DELTA-BUILD %s] phase [%s]: %.2fs", self.task_id, name, elapsed
        )

    def _is_cancelled(self) -> bool:
        try:
            return self.tm.is_cancelled(self.task_id)
        except Exception:  # noqa: BLE001
            return False

    def run(self) -> None:
        logger.info(
            "[DT-DELTA-BUILD %s] START domain=%s view=%s data=%s",
            self.task_id,
            self.domain_name,
            self.view_table,
            self.data_table,
        )
        try:
            t_phase = time.time()
            if not self._prepare_translation():
                return
            self._log_phase("prepare", t_phase)
            self.tm.update_progress(self.task_id, 15, "SQL generated")

            t_phase = time.time()
            if not self._create_view():
                return
            self._log_phase("create_view", t_phase)
            self.tm.update_progress(self.task_id, 40, "VIEW created")

            t_phase = time.time()
            if not self._materialize_data_table():
                return
            self._log_phase("materialize", t_phase)

            t_phase = time.time()
            self._ensure_inferred_companion()
            self._log_phase("ensure_inferred", t_phase)

            t_phase = time.time()
            self._truncate_inferred()
            self._log_phase("truncate_inferred", t_phase)

            t_phase = time.time()
            self._ensure_graph_view()
            self._log_phase("ensure_graph_view", t_phase)

            t_phase = time.time()
            materialize.optimize_table(self.source_client, self.data_table)
            self._log_phase("optimize", t_phase)

            self._complete_task()
        except OperationCancelledError as exc:
            logger.info("[DT-DELTA-BUILD %s] cancelled: %s", self.task_id, exc)
            self._record_build_run("cancelled", message=str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("[DT-DELTA-BUILD %s] failed: %s", self.task_id, exc)
            self.tm.fail_task(self.task_id, str(exc))
            self._record_build_run("error", error=str(exc))
        finally:
            if not self._build_recorded:
                status = "cancelled" if self._is_cancelled() else "error"
                self._record_build_run(status)

    def _prepare_translation(self) -> bool:
        from back.core.databricks import DatabricksClient
        from back.core.w3c import sparql
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        self.tm.start_task(self.task_id, "Preparing mappings...")
        self.source_client = DatabricksClient(
            host=self.host, token=self.token, warehouse_id=self.warehouse_id
        )
        entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(
            self.r2rml_content
        )
        entity_mappings = DigitalTwin.augment_mappings_from_config(
            entity_mappings, self.mapping_config, self.base_uri, self.ontology_config
        )
        relationship_mappings = DigitalTwin.augment_relationships_from_config(
            relationship_mappings,
            self.mapping_config,
            self.base_uri,
            self.ontology_config,
        )
        self.entity_mappings = entity_mappings
        self.relationship_mappings = relationship_mappings
        if not entity_mappings and not relationship_mappings:
            self.tm.fail_task(self.task_id, "No valid mappings found")
            return False

        all_data_sparql = (
            f"PREFIX : <{self.base_uri}>\n"
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
            "SELECT DISTINCT ?subject ?predicate ?object\n"
            "WHERE {\n"
            "    ?subject ?predicate ?object .\n"
            "}"
        )
        try:
            result = sparql.translate_sparql_to_spark(
                all_data_sparql,
                entity_mappings,
                None,
                relationship_mappings,
                dialect="spark",
            )
        except OntoBricksError as exc:
            self.tm.fail_task(self.task_id, exc.message)
            return False

        if self.is_api and not result.get("success"):
            self.tm.fail_task(
                self.task_id, result.get("message", "Translation failed")
            )
            return False

        self.spark_sql = result["sql"]
        return True

    def _create_view(self) -> bool:
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        self.tm.advance_step(self.task_id, f"Creating VIEW {self.view_table}...")
        try:
            catalog, schema, vname = self.parts
            view_ok, view_msg = self.source_client.create_or_replace_view(
                catalog, schema, vname, self.spark_sql
            )
            if not view_ok:
                detail = (
                    view_msg
                    if self.is_api
                    else DigitalTwin.diagnose_view_error(
                        view_msg, self.entity_mappings, self.relationship_mappings
                    )
                )
                self.tm.fail_task(self.task_id, f"Failed to create VIEW: {detail}")
                return False
            return True
        except Exception as exc:  # noqa: BLE001
            if self.is_api:
                self.tm.fail_task(self.task_id, str(exc))
                return False
            detail = DigitalTwin.diagnose_view_error(
                str(exc), self.entity_mappings, self.relationship_mappings
            )
            self.tm.fail_task(self.task_id, f"Failed to create VIEW: {detail}")
            return False

    def _count_view_triples(self) -> int:
        try:
            rows = self.source_client.execute_query(
                f"SELECT COUNT(*) AS cnt FROM {self.view_table}"
            )
            return int(rows[0].get("cnt", 0)) if rows else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not count VIEW %s: %s", self.view_table, exc)
            return 0

    def _materialize_data_table(self) -> bool:
        self.tm.advance_step(
            self.task_id, f"Materializing Delta table {self.data_table}..."
        )
        self.triple_count = self._count_view_triples()
        if self.triple_count == 0:
            msg = "VIEW created but no triples generated (check your mappings)"
            self.tm.complete_task(
                self.task_id,
                result={
                    "triple_count": 0,
                    "view_table": self.view_table,
                    "data_table": self.data_table,
                    "build_mode": "delta_full",
                    "duration_seconds": time.time() - self.start_time,
                },
                message=msg,
            )
            self._record_build_run("success", message=msg)
            return False

        self.tm.update_progress(
            self.task_id,
            85,
            f"Counted {self.triple_count} triples",
        )
        return True

    def _ensure_inferred_companion(self) -> None:
        if not self.inferred_table:
            return
        materialize.ensure_inferred_table(self.source_client, self.inferred_table)

    def _truncate_inferred(self) -> None:
        if not self.inferred_table:
            return
        materialize.truncate_table(self.source_client, self.inferred_table)

    def _ensure_graph_view(self) -> None:
        if not self.graph_view or not self.data_table or not self.inferred_table:
            return
        materialize.ensure_graph_view(
            self.source_client,
            self.graph_view,
            self.data_table,
            self.inferred_table,
        )

    def _complete_task(self) -> None:
        duration = time.time() - self.start_time
        msg = f"Triple store built ({self.triple_count} triples)"
        self.tm.complete_task(
            self.task_id,
            result={
                "triple_count": self.triple_count,
                "view_table": self.view_table,
                "data_table": self.data_table,
                "build_mode": "delta_full",
                "duration_seconds": duration,
            },
            message=msg,
        )
        self._record_build_run("success", message=msg)

    def _record_build_run(self, status: str, *, message: str = "", error: str = "") -> None:
        if self._build_recorded:
            return
        self._build_recorded = True
        try:
            from back.objects.registry.RegistryService import RegistryService
            from back.objects.session import sanitize_domain_folder

            folder = getattr(self.domain, "uc_domain_folder", "") or (
                sanitize_domain_folder(self.domain_name)
            )
            version = (
                getattr(self.domain_snap, "current_version", None)
                or getattr(self.domain, "current_version", None)
                or ""
            )
            now = datetime.now(timezone.utc)
            started = datetime.fromtimestamp(self.start_time, tz=timezone.utc)
            entry = {
                "version": str(version),
                "build_kind": self.build_kind,
                "status": status,
                "message": message,
                "error": error,
                "started_at": started.isoformat(),
                "finished_at": now.isoformat(),
                "duration_s": time.time() - self.start_time,
                "triple_count": int(self.triple_count or 0),
                "entity_count": len(self.entity_mappings or []),
                "relationship_count": len(self.relationship_mappings or []),
                "sql_chars": len(self.spark_sql or ""),
                "triple_store_backend": "databricks",
                "view_table": self.view_table,
                "data_table": self.data_table,
                "task_id": self.task_id,
                "phase_times": dict(self.phase_times),
                "stats": collect_domain_stats(
                    getattr(self.domain_snap, "ontology", {}),
                    getattr(self.domain_snap, "assignment", {}),
                    constraints=getattr(self.domain_snap, "constraints", None),
                    swrl_rules=getattr(self.domain_snap, "swrl_rules", None),
                    axioms=getattr(self.domain_snap, "axioms", None),
                    shacl_shapes=getattr(self.domain_snap, "shacl_shapes", None),
                ),
            }
            svc = RegistryService.from_context(self.domain, self.settings)
            svc.record_build_run(folder, entry)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-DELTA-BUILD %s] could not record build run: %s",
                self.task_id,
                exc,
            )
