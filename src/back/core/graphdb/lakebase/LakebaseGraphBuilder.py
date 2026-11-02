"""Lakebase implementation of the :class:`GraphBuilder` capability.

Thin facade over the Lakebase build / sync internals (``_companion_ddl``,
``_sync_uc_schema``, ``LakebaseFlatStore`` resolution helpers, ``LakebaseBase``
naming). All backend logic stays in those modules; this class only groups the
operations the triple-store build / Lakeflow synced-table pipeline needs behind
the engine-agnostic builder capability so no caller outside
``back/core/graphdb/`` imports ``lakebase.*`` directly.

The bulk Postgres writes themselves run through the Lakebase store
(``MaterializedGraphBackend``) the factory already vends — this builder only
exposes the module-level naming / DDL helpers that previously leaked.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.graphdb.GraphBuilder import GraphBuilder


class LakebaseGraphBuilder(GraphBuilder):
    """Lakeflow synced-table build / naming helpers for a Lakebase domain."""

    def __init__(self, domain: Any, settings: Any = None) -> None:
        self._domain = domain
        self._settings = settings

    # -- Lakeflow sync view DDL ----------------------------------------
    def wrap_view_sql_for_lakeflow(self, view_sql: str) -> str:
        """Emit ``object_hash`` in the triple VIEW so Lakeflow sync has a PK."""
        from back.core.graphdb.lakebase._companion_ddl import (
            wrap_triple_view_sql_for_lakeflow,
        )

        return wrap_triple_view_sql_for_lakeflow(view_sql)

    def lakeflow_primary_key(self) -> List[str]:
        """Primary-key columns for the Lakeflow synced table."""
        from back.core.graphdb.lakebase._companion_ddl import (
            LAKEFLOW_SYNC_PRIMARY_KEY,
        )

        return list(LAKEFLOW_SYNC_PRIMARY_KEY)

    def ensure_uc_schema_for_synced(
        self, client: Any, synced_uc: str, *, task_log_prefix: str = ""
    ) -> None:
        """Ensure the Unity Catalog schema backing a synced table exists."""
        from back.core.graphdb.lakebase._sync_uc_schema import (
            ensure_uc_schema_for_synced_table_fqn,
        )

        ensure_uc_schema_for_synced_table_fqn(
            client, synced_uc, task_log_prefix=task_log_prefix
        )

    # -- Naming / resolution (no connection) ---------------------------
    def resolve_sync_uc_fallback_catalog(
        self, delta_cfg: Optional[Dict[str, Any]] = None
    ) -> str:
        """Fallback UC catalog for the synced table when none is configured."""
        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_sync_uc_fallback_catalog,
        )

        return resolve_sync_uc_fallback_catalog(self._domain, self._settings, delta_cfg)

    def resolve_graph_schema(self, config_schema: str) -> str:
        """Resolve the effective Postgres graph schema for this domain."""
        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            resolve_lakebase_graph_schema,
        )

        return resolve_lakebase_graph_schema(
            self._domain, self._settings, config_schema
        )

    def physical_table_id(self, graph_name: str) -> str:
        """Postgres physical table name for a logical graph name."""
        from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase

        return LakebaseBase.physical_table_id(graph_name)

    def synced_phy(self, graph_name: str) -> str:
        """Physical name of the Lakeflow-synced bulk table for a graph."""
        from back.core.graphdb.lakebase._companion_ddl import synced_phy

        return synced_phy(graph_name)

    def is_index_row_size_error(self, exc: BaseException) -> bool:
        """True when *exc* is a Postgres btree index-row-size overflow."""
        from back.core.graphdb.lakebase.LakebaseFlatStore import (
            _is_index_row_size_error,
        )

        return _is_index_row_size_error(exc)
