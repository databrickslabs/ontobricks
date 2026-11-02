"""Delta implementation of the :class:`GraphHealthProbe` capability.

Thin facade over the Delta health / object-listing internals (``health``,
``objects``, ``Base.create_databricks_client``). All logic stays in those
modules; this class only groups them behind the engine-agnostic probe contract
so no caller outside ``back/core/graphdb/`` imports ``delta.*`` directly.

Status probes read on the engine's **read/query** warehouse, matching the UI
health page (never the Build warehouse).
"""

from __future__ import annotations

from typing import Any, Dict, List

from back.core.graphdb.interfaces.GraphHealthProbe import GraphHealthProbe


class HealthProbe(GraphHealthProbe):
    """Health, quality and UC object-listing probes for the Delta backend."""

    def __init__(self, domain: Any, settings: Any = None) -> None:
        self._domain = domain
        self._settings = settings

    def probe_status(self, table_fqn: str) -> Dict[str, Any]:
        from back.core.graphdb.delta.Base import create_databricks_client
        from back.core.graphdb.delta.health import probe_from_client

        client = create_databricks_client(self._domain, self._settings)
        return probe_from_client(client, table_fqn) if table_fqn else {}

    def schema_permission_summary(
        self, catalog: str, schema: str, principal: str, assignments: Any
    ) -> Dict[str, Any]:
        from back.core.graphdb.delta.health import schema_permission_summary

        return schema_permission_summary(catalog, schema, principal, assignments)

    def fetch_schema_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        from back.core.graphdb.delta.objects import fetch_uc_schema_tables

        return fetch_uc_schema_tables(catalog, schema)

    def group_triplestore_objects(
        self, raw_tables: List[Dict[str, Any]], catalog: str, schema: str
    ) -> List[Dict[str, Any]]:
        from back.core.graphdb.delta.objects import group_triplestore_objects

        return group_triplestore_objects(raw_tables, catalog, schema)

    def group_analytics_objects(
        self, raw_tables: List[Dict[str, Any]], catalog: str, schema: str
    ) -> List[Dict[str, Any]]:
        from back.core.graphdb.delta.objects import group_analytics_objects

        return group_analytics_objects(raw_tables, catalog, schema)

    def domain_match_key(self, name: str) -> str:
        from back.core.graphdb.delta.objects import domain_match_key

        return domain_match_key(name)
