"""Lakebase implementation of the :class:`GraphProvisioner` capability.

Thin facade over the Lakebase provisioning / schema / synced-table internals
(``LakebaseBase`` validation + schema helpers, ``LakebaseGraphProvisioner``
instance provisioning, ``SyncedTableManager`` control-plane ops). Settings/API
callers obtain it from :meth:`GraphDBFactory.get_provisioner` so no module
outside ``back/core/graphdb/`` imports ``lakebase.*`` directly.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from back.core.graphdb.GraphProvisioner import GraphProvisioner


class LakebaseProvisioner(GraphProvisioner):
    """Schema / instance / synced-table provisioning for a Lakebase domain."""

    # -- Config / schema validation (no connection) --------------------
    def validate_engine_config(self, config: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate the nested ``lakebase`` engine-config keys."""
        from back.core.graphdb.lakebase.LakebaseBase import (
            validate_engine_config_keys,
        )

        return validate_engine_config_keys(config)

    def default_schema(self) -> str:
        """Default Postgres graph schema name."""
        from back.core.graphdb.lakebase.LakebaseBase import default_schema

        return default_schema()

    def validate_graph_schema(self, name: str) -> str:
        """Validate / normalize a Postgres graph schema name."""
        from back.core.graphdb.lakebase.LakebaseBase import validate_graph_schema

        return validate_graph_schema(name)

    def resolve_postgres_database_override(self, config: Dict[str, Any]) -> str:
        """Resolve the configured Postgres database override (or empty)."""
        from back.core.graphdb.lakebase.LakebaseBase import (
            resolve_postgres_database_override,
        )

        return resolve_postgres_database_override(config)

    # -- Instance provisioning -----------------------------------------
    def default_branch(self) -> str:
        from back.core.graphdb.lakebase.LakebaseGraphProvisioner import DEFAULT_BRANCH

        return DEFAULT_BRANCH

    def default_capacity(self) -> str:
        from back.core.graphdb.lakebase.LakebaseGraphProvisioner import (
            DEFAULT_CAPACITY,
        )

        return DEFAULT_CAPACITY

    def provision_steps(
        self, *, grant_uc: bool, grant_app_permissions: bool
    ) -> List[Any]:
        """Task steps for a Lakebase provisioning run."""
        from back.core.graphdb.lakebase.LakebaseGraphProvisioner import (
            provision_steps,
        )

        return provision_steps(
            grant_uc=grant_uc, grant_app_permissions=grant_app_permissions
        )

    def run_provision(self, **kwargs: Any) -> None:
        """Run a Lakebase graph-DB provisioning job (blocking in its thread)."""
        from back.core.graphdb.lakebase.LakebaseGraphProvisioner import (
            LakebaseGraphProvisioner,
        )

        LakebaseGraphProvisioner(**kwargs).run()

    # -- Synced-table control plane ------------------------------------
    def delete_synced_table(self, full_name: str, *, purge_data: bool = True) -> None:
        """Delete a Lakeflow synced table (and optionally purge its data)."""
        from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager

        SyncedTableManager().delete(full_name, purge_data=purge_data)

    def sync_manager(self) -> Any:
        """A :class:`SyncedTableManager` for control-plane state probes."""
        from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager

        return SyncedTableManager()

    def sync_to_dict(self, obj: Any) -> Dict[str, Any]:
        from back.core.graphdb.lakebase.SyncedTableManager import _to_dict

        return _to_dict(obj)

    def sync_state(self, synced: Any) -> str:
        from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager

        return SyncedTableManager._extract_state(synced)

    def sync_pipeline_id(self, synced: Any) -> str:
        from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager

        return SyncedTableManager._extract_pipeline_id(synced)
