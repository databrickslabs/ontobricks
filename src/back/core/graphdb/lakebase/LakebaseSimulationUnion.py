"""Explorer read view: live graph UNION a simulation sandbox after-picture."""

from __future__ import annotations

from typing import Any

from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase
from back.core.helpers import validate_table_name


class LakebaseSimulationUnion:
    """Create and drop the read-only live∪sandbox view Explorer queries."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def ensure(self, view: str, live_table: str, sandbox_table: str) -> str:
        validate_table_name(view)
        validate_table_name(live_table)
        validate_table_name(sandbox_table)
        view_id = LakebaseBase.physical_table_id(view)
        live_id = LakebaseBase.physical_table_id(live_table)
        sandbox_id = LakebaseBase.physical_table_id(sandbox_table)
        with self._store._cursor() as cur:
            _companion_ddl.ensure_sim_union_view(cur, view_id, live_id, sandbox_id)
        return view_id

    def drop_all(self, graph_name: str, sim_id: str) -> None:
        with self._store._cursor() as cur:
            view = _companion_ddl.sim_union_phy(graph_name, sim_id)
            _companion_ddl.drop_view(cur, view)
