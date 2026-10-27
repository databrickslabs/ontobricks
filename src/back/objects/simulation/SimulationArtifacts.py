"""Physical objects a simulation creates for the active graph backend."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from back.core.graphdb.GraphDBFactory import normalize_graph_backend
from back.core.helpers import effective_graph_name

_ROLE_SANDBOX = "sandbox"
_ROLE_INFERRED = "explorer_inferred"
_ROLE_ASSERTED = "explorer_asserted"


class SimulationArtifacts:
    """Name and count the sandbox table/views (or Neo4j graph) of one scenario."""

    @classmethod
    def describe(cls, domain: Any, row: Dict[str, Any]) -> Dict[str, Any]:
        """Catalog for the workbench: planned or created objects plus run counts."""
        backend = normalize_graph_backend(
            (getattr(domain, "info", None) or {}).get("graph_backend")
        )
        graph_name = effective_graph_name(domain)
        sim_id = str(row.get("id") or "")
        summary = row.get("diff_summary") or {}
        objects = summary.get("artifacts") or cls.expected(
            backend, graph_name, sim_id, sandbox_table=summary.get("sandbox_table")
        )
        return {
            "status": row.get("status") or "draft",
            "backend": backend,
            "objects": objects,
            "counts": cls.counts(summary) if row.get("status") == "ready" else {},
        }

    @classmethod
    def from_sandbox(cls, sandbox: Any, backend: str) -> List[Dict[str, str]]:
        """Exact names after a run, including Explorer overlay views."""
        kind = "graph" if backend == "neo4j" else "table"
        objects = [cls._object(kind, _ROLE_SANDBOX, getattr(sandbox, "table", ""))]
        if backend == "neo4j":
            return objects
        graph_name = getattr(sandbox, "_graph_name", "") or ""
        sim_id = getattr(sandbox, "_sim_id", "") or ""
        view_name = getattr(sandbox, "_view_name", None)
        if callable(view_name):
            objects.append(cls._object("view", _ROLE_INFERRED, view_name(True)))
            objects.append(cls._object("view", _ROLE_ASSERTED, view_name(False)))
            return objects
        if graph_name and sim_id:
            objects.extend(cls._lakebase_views(graph_name, sim_id))
        return objects

    @classmethod
    def expected(
        cls,
        backend: str,
        graph_name: str,
        sim_id: str,
        *,
        sandbox_table: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """Names this scenario will allocate (draft) or has allocated (ready)."""
        if not sim_id:
            return []
        if backend == "neo4j":
            name = sandbox_table or cls._tokenised(graph_name, sim_id)
            return [cls._object("graph", _ROLE_SANDBOX, name)]
        if backend == "databricks":
            name = sandbox_table or cls._tokenised(graph_name, sim_id)
            return [
                cls._object("table", _ROLE_SANDBOX, name),
                cls._object("view", _ROLE_INFERRED, name.replace("__sim_", "__smu_")),
                cls._object("view", _ROLE_ASSERTED, name.replace("__sim_", "__sms_")),
            ]
        from back.core.graphdb.lakebase import _companion_ddl

        table = sandbox_table or _companion_ddl.sim_phy(graph_name, sim_id)
        objects = [cls._object("table", _ROLE_SANDBOX, table)]
        objects.extend(cls._lakebase_views(graph_name, sim_id))
        return objects

    @staticmethod
    def counts(summary: Dict[str, Any]) -> Dict[str, int]:
        inferred = summary.get("added_inferred_count")
        if inferred is None:
            inferred = summary.get("inferred_total") or 0
        return {
            "entities": int(summary.get("entity_count") or 0),
            "triples": int(summary.get("after_count") or 0),
            "triples_before": int(summary.get("before_count") or 0),
            "added": int(summary.get("added_count") or 0),
            "removed": int(summary.get("removed_count") or 0),
            "inferred": int(inferred or 0),
            "touched": int(summary.get("touched_entity_count") or 0),
        }

    @staticmethod
    def _lakebase_views(graph_name: str, sim_id: str) -> List[Dict[str, str]]:
        from back.core.graphdb.lakebase import _companion_ddl

        return [
            SimulationArtifacts._object(
                "view",
                _ROLE_INFERRED,
                _companion_ddl.sim_union_phy(graph_name, sim_id, include_inferred=True),
            ),
            SimulationArtifacts._object(
                "view",
                _ROLE_ASSERTED,
                _companion_ddl.sim_union_phy(graph_name, sim_id, include_inferred=False),
            ),
        ]

    @staticmethod
    def _tokenised(graph_name: str, sim_id: str) -> str:
        token = re.sub(r"[^a-zA-Z0-9_]", "_", graph_name or "")
        sid = re.sub(r"[^a-zA-Z0-9_]", "_", sim_id or "")
        stem = token.strip("_") or "simulation"
        return f"{stem}__sim_{sid.strip('_') or 'simulation'}"

    @staticmethod
    def _object(kind: str, role: str, name: str) -> Dict[str, str]:
        return {"kind": kind, "role": role, "name": name}
