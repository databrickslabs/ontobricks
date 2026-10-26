"""Prepare a session Knowledge Graph build (view location, credentials, steps).

Extracted from ``api.routers.internal.dtwin.start_triplestore_sync``
(Fowler Extract Class). The route still owns TaskManager + the worker thread.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as tz
from typing import Any, Callable, List

from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.objects.digitaltwin.models import DomainSnapshot
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)


@dataclass
class TwinGraphBuild:
    """Validate domain state and describe the session build pipeline."""

    view_table: str
    graph_name: str
    r2rml_content: str
    host: str
    token: str
    warehouse_id: str
    base_uri: str
    mapping_config: Any
    ontology_config: Any
    delta_cfg: dict
    domain_snap: DomainSnapshot
    graph_steps: List[dict]

    @staticmethod
    def pipeline_steps(domain, settings) -> List[dict]:
        """Return graph-engine steps matching ``_BuildPipeline`` lakebase mode."""
        try:
            from back.core.graphdb.GraphDBFactory import GraphDBFactory
            from back.core.graphdb.engine_config import lakebase_section

            engine = (
                GraphDBFactory._resolve_graph_engine(domain, settings, force=True) or ""
            )
            ecfg = lakebase_section(
                GraphDBFactory._resolve_graph_engine_config(domain, settings, force=True)
                or {}
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Engine config resolution failed, defaulting to non-synced: %s", exc
            )
            engine = ""
            ecfg = {}
        if engine == "lakebase" and ecfg.get("sync_mode") == "managed_synced":
            return [
                {"name": "uc_schema", "description": "Ensuring Unity Catalog schema"},
                {
                    "name": "sync_register",
                    "description": "Registering synced table in Unity Catalog",
                },
                {"name": "sync_companion", "description": "Creating companion table"},
                {
                    "name": "sync_data",
                    "description": "Syncing data from Delta (Lakeflow)",
                },
                {
                    "name": "union_view",
                    "description": "Creating knowledge graph union view",
                },
                {"name": "finalize", "description": "Finalizing knowledge graph"},
            ]
        return [{"name": "graph", "description": "Updating the knowledge graph"}]

    @classmethod
    def prepare_session_sync(
        cls,
        domain,
        settings,
        *,
        view_table_fn: Callable,
        graph_name_fn: Callable,
        get_credentials: Callable,
        is_databricks_app_fn: Callable,
    ) -> TwinGraphBuild:
        """Validate the domain, stamp last_build, and return a session build plan."""
        view_table = view_table_fn(domain)
        graph_name = graph_name_fn(domain)

        parts = view_table.split(".")
        if len(parts) != 3:
            raise ValidationError(
                "View location must be fully qualified: catalog.schema.view_name "
                "(configure in Domain / Triple Store tab)"
            )

        domain.ensure_generated_content()
        r2rml_content = domain.get_r2rml()
        if not r2rml_content:
            raise ValidationError(
                "No R2RML mapping available. Please ensure ontology and "
                "assignments are configured."
            )

        host, token, warehouse_id = get_credentials(domain, settings)
        app_mode = is_databricks_app_fn()
        if not host and not app_mode:
            raise ValidationError("Databricks not configured")
        if not token and not app_mode:
            raise ValidationError("Databricks not configured")
        if not warehouse_id:
            raise ValidationError("No SQL warehouse configured")

        domain.triplestore.pop("stats", None)
        domain.triplestore.pop("_ts_cache_timestamp", None)
        if domain.last_update:
            domain.triplestore["build_last_update"] = domain.last_update
        domain.last_build = datetime.now(tz.utc).isoformat()
        domain.save()

        return cls(
            view_table=view_table,
            graph_name=graph_name,
            r2rml_content=r2rml_content,
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            base_uri=domain.ontology.get("base_uri", DEFAULT_BASE_URI),
            mapping_config=domain.assignment,
            ontology_config=domain.ontology,
            delta_cfg=domain.delta or {},
            domain_snap=DomainSnapshot(domain),
            graph_steps=cls.pipeline_steps(domain, settings),
        )
