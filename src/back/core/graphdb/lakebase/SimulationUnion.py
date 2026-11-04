"""Explorer read view: live graph UNION a simulation sandbox after-picture."""

from __future__ import annotations

from typing import Any

from back.core.errors import ValidationError
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.Base import Base
from back.core.helpers import validate_table_name

_SANDBOX_MISSING_MESSAGE = (
    "This simulation's sandbox tables are missing — the graph was likely "
    "rebuilt or re-synced after the run. Re-run the simulation to rebuild it."
)


class SimulationUnion:
    """Create and drop the read-only live∪sandbox view Explorer queries."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def ensure(self, view: str, live_table: str, sandbox_table: str) -> str:
        validate_table_name(view)
        validate_table_name(live_table)
        validate_table_name(sandbox_table)
        view_id = Base.physical_table_id(view)
        live_id = Base.physical_table_id(live_table)
        sandbox_id = Base.physical_table_id(sandbox_table)
        with self._store._cursor() as cur:
            # A sim marked "ready" in the registry can still have lost its
            # physical sandbox table (e.g. the graph was rebuilt/re-synced).
            # Building the union view would then fail deep in CREATE VIEW with
            # an opaque UndefinedTable -> 502; probe first and raise a clear
            # 400 so the UI can tell the user to re-run the simulation.
            if not _companion_ddl.relation_exists(cur, sandbox_id):
                raise ValidationError(_SANDBOX_MISSING_MESSAGE)
            overlay_id = _companion_ddl.overlay_phy_from(sandbox_id)
            if not overlay_id:
                raise ValidationError(_SANDBOX_MISSING_MESSAGE)
            if not _companion_ddl.relation_exists(cur, overlay_id):
                _companion_ddl.ensure_sim_table(
                    cur, self._store._schema, overlay_id
                )
            _companion_ddl.backfill_sim_overlay_keys(cur, sandbox_id, overlay_id)
            _companion_ddl.ensure_sim_union_view(
                cur, view_id, live_id, sandbox_id, overlay_id
            )
        return view_id

    def drop_all(self, graph_name: str, sim_id: str) -> None:
        with self._store._cursor() as cur:
            view = _companion_ddl.sim_union_phy(graph_name, sim_id)
            _companion_ddl.drop_view(cur, view)
