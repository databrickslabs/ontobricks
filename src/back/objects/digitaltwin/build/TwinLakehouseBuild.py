"""Prepare a session Original Graph build (gateway VIEW → ``…_data`` VIEW).

Extracted from ``api.routers.internal.dtwin.start_databricks_triplestore_build``.
The Original Graph is virtual for every domain, whatever its Certified Graph
backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as tz
from typing import Any, Callable

from back.core.errors import ValidationError
from back.objects.digitaltwin.models import DomainSnapshot
from shared.config.constants import DEFAULT_BASE_URI


@dataclass
class TwinLakehouseBuild:
    """Validated inputs of an Original Graph build, and its background start."""

    view_table: str
    data_table: str
    r2rml_content: str
    host: str
    token: str
    warehouse_id: str
    base_uri: str
    mapping_config: Any
    ontology_config: Any
    domain_snap: DomainSnapshot

    @classmethod
    def start(cls, domain, settings, *, build_kind: str = "session") -> str:
        """Validate the domain and run the Original build as a tracked task."""
        from back.core.databricks import is_databricks_app
        from back.core.graphdb.GraphDBFactory import GraphDBFactory
        from back.core.helpers import (
            effective_databricks_table,
            effective_view_table,
            get_triplestore_sql_credentials,
        )
        from back.core.task_manager import get_task_manager

        plan = cls.prepare_session(
            domain,
            settings,
            view_table_fn=effective_view_table,
            data_table_fn=effective_databricks_table,
            get_credentials=get_triplestore_sql_credentials,
            is_databricks_app_fn=is_databricks_app,
        )
        builder = GraphDBFactory.get_builder(domain, settings, engine="delta")
        tm = get_task_manager()
        task = tm.run_background_task(
            "Original Graph Build",
            "original_build",
            plan._run,
            tm,
            builder,
            build_kind,
            steps=builder.build_steps(),
        )
        return task.id

    def _run(self, task: Any, tm: Any, builder: Any, build_kind: str) -> None:
        builder.run_triplestore_build(
            tm=tm,
            task_id=task.id,
            domain_snap=self.domain_snap,
            host=self.host,
            token=self.token,
            warehouse_id=self.warehouse_id,
            view_table=self.view_table,
            data_table=self.data_table,
            r2rml_content=self.r2rml_content,
            mapping_config=self.mapping_config,
            ontology_config=self.ontology_config,
            base_uri=self.base_uri,
            build_kind=build_kind,
        )

    @classmethod
    def prepare_session(
        cls,
        domain,
        settings,
        *,
        view_table_fn: Callable,
        data_table_fn: Callable,
        get_credentials: Callable,
        is_databricks_app_fn: Callable,
    ) -> TwinLakehouseBuild:
        view_table = view_table_fn(domain)
        data_table = data_table_fn(domain, settings)
        if len(view_table.split(".")) != 3:
            raise ValidationError(
                "View location must be fully qualified: catalog.schema.view_name"
            )
        if len(data_table.split(".")) != 3:
            raise ValidationError("Delta data table FQN could not be resolved")

        domain.ensure_generated_content()
        r2rml_content = domain.get_r2rml()
        if not r2rml_content:
            raise ValidationError(
                "No R2RML mapping available. Configure ontology and assignments first."
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
            data_table=data_table,
            r2rml_content=r2rml_content,
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            base_uri=domain.ontology.get("base_uri", DEFAULT_BASE_URI),
            mapping_config=domain.assignment,
            ontology_config=domain.ontology,
            domain_snap=DomainSnapshot(domain),
        )
