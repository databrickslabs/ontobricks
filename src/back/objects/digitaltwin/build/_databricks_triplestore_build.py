"""Thin delegator for the Databricks-only triple-store build."""

from __future__ import annotations

from typing import Any

from back.objects.digitaltwin.models import DomainSnapshot


def run_databricks_triplestore_build(
    tm: Any,
    task_id: str,
    domain: Any,
    settings: Any,
    domain_snap: DomainSnapshot,
    host: str,
    token: str,
    warehouse_id: str,
    view_table: str,
    data_table: str,
    r2rml_content: str,
    mapping_config: Any,
    ontology_config: Any,
    base_uri: str,
    *,
    build_kind: str = "ui",
) -> None:
    """Entry point for ``POST /dtwin/databricks-build/start`` worker threads."""
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    GraphDBFactory.get_builder(
        domain, settings, engine="delta"
    ).run_triplestore_build(
        tm=tm,
        task_id=task_id,
        domain_snap=domain_snap,
        host=host,
        token=token,
        warehouse_id=warehouse_id,
        view_table=view_table,
        data_table=data_table,
        r2rml_content=r2rml_content,
        mapping_config=mapping_config,
        ontology_config=ontology_config,
        base_uri=base_uri,
        build_kind=build_kind,
    )
