"""Delta (Unity Catalog) implementation of the :class:`GraphBuilder` capability.

Thin facade over the Delta build internals (``_table_naming``, ``materialize``,
``Base.create_databricks_client``, ``MaterializedStore`` and
``BuildPipeline``). All backend logic stays in those modules;
this class only groups them behind the engine-agnostic builder contract so no
caller outside ``back/core/graphdb/`` imports ``delta.*`` directly.

Writes run on the **Build** SQL warehouse (``for_write=True``). A client can be
injected (build pipelines that already hold one) or is lazily created on first
use.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.graphdb.interfaces.GraphBuilder import GraphBuilder
from back.core.logging import get_logger

logger = get_logger(__name__)


class Builder(GraphBuilder):
    """Build / materialize the Delta graph for a domain on the Build warehouse."""

    def __init__(
        self, domain: Any, settings: Any = None, *, client: Any = None
    ) -> None:
        self._domain = domain
        self._settings = settings
        self._client = client
        self._client_resolved = client is not None

    # -- Build warehouse client (lazy, for_write) -----------------------
    def _get_client(self) -> Any:
        if not self._client_resolved:
            from back.core.graphdb.delta.Base import create_databricks_client

            self._client = create_databricks_client(
                self._domain, self._settings, for_write=True
            )
            self._client_resolved = True
        return self._client

    def has_client(self) -> bool:
        return self._get_client() is not None

    # -- Relation naming ------------------------------------------------
    def data_table_fqn(self) -> str:
        from back.core.graphdb.delta import _table_naming

        return _table_naming.data_table_fqn(self._domain, self._settings)

    def graph_view_fqn(self) -> str:
        from back.core.graphdb.delta import _table_naming

        return _table_naming.graph_view_fqn(self._domain, self._settings)

    def inferred_table_fqn(self) -> str:
        from back.core.graphdb.delta import _table_naming

        return _table_naming.inferred_table_fqn(self._domain, self._settings)

    def dq_snapshot_fqn(self, task_id: str = "") -> str:
        from back.core.graphdb.delta import _table_naming

        return _table_naming.dq_snapshot_fqn(self._domain, self._settings, task_id)

    def analytics_snapshot_fqn(self) -> str:
        from back.core.graphdb.delta import _table_naming

        return _table_naming.analytics_snapshot_fqn(self._domain, self._settings)

    # -- Materialize / write -------------------------------------------
    def materialize_triples(
        self, table_name: str, triples: List[Dict[str, str]]
    ) -> int:
        from back.core.graphdb.delta.MaterializedStore import (
            MaterializedStore,
        )

        client = self._get_client()
        store = MaterializedStore(client)
        store.create_table(table_name)
        client.execute_statement(f"DELETE FROM {table_name}")
        return store.insert_triples(table_name, triples)

    def materialize_from_view(self, view_fqn: str, table_fqn: str) -> None:
        from back.core.graphdb.delta import materialize

        materialize.materialize_from_view(self._get_client(), view_fqn, table_fqn)

    def apply_data_relation(self, view_fqn: str, table_fqn: str, *, mode: str) -> None:
        from back.core.graphdb.delta import materialize

        materialize.apply_data_relation(
            self._get_client(), view_fqn, table_fqn, mode=mode
        )

    def drop_relation(self, fqn: str, *, kind: str = "table") -> None:
        from back.core.graphdb.delta import materialize

        materialize.drop_relation(self._get_client(), fqn, kind=kind)

    def count_triples(self, relation_fqn: str) -> int:
        from back.core.graphdb.delta import materialize

        return materialize.count_triples(self._get_client(), relation_fqn)

    # -- Raw SQL on the Build warehouse --------------------------------
    def execute_query(self, sql: str) -> Any:
        return self._get_client().execute_query(sql)

    def execute_statement(self, sql: str) -> Any:
        return self._get_client().execute_statement(sql)

    # -- Triple-store build pipeline -----------------------------------
    def build_steps(self, materialization: str) -> List[str]:
        from back.core.graphdb.delta.BuildPipeline import (
            lakehouse_build_steps,
        )

        return lakehouse_build_steps(materialization)

    def run_triplestore_build(
        self,
        *,
        tm: Any,
        task_id: str,
        domain_snap: Any,
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
        from back.core.graphdb.delta.BuildPipeline import (
            BuildPipeline,
        )

        BuildPipeline(
            tm=tm,
            task_id=task_id,
            domain=self._domain,
            domain_snap=domain_snap,
            settings=self._settings,
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
        ).run()
