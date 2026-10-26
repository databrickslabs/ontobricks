"""Prepare a session Databricks (Delta) triple-store materialization.

Extracted from ``api.routers.internal.dtwin.start_databricks_triplestore_build``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as tz
from typing import Any, Callable

from back.core.errors import ValidationError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.models import DomainSnapshot
from shared.config.constants import DEFAULT_BASE_URI


@dataclass
class TwinLakehouseBuild:
    """Validated inputs for ``run_databricks_triplestore_build``."""

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
    materialization: str

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
        if GraphDBFactory._resolve_triple_store_backend(domain, settings) != "databricks":
            raise ValidationError(
                "Databricks triple-store build is only available when "
                "triple_store_backend is 'databricks' (Settings → Back end)."
            )

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
            materialization=GraphDBFactory.resolve_lakehouse_materialization(
                domain, settings
            ),
        )
