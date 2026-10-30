"""Lakebase triple store pinned to standalone simulation sandbox tables.

A simulation copies a slice of the live graph into ``<graph>__sim_<id>``
and reasons over it. This store reuses every :class:`LakebaseFlatStore`
read/write path but resolves *every* table name to the sandbox itself (no
``_sync`` / ``__app`` / union-view trio) and refuses any name that is not a
sandbox table, so a simulation can never write to the live graph.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Optional

from back.core.errors import ValidationError
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase
from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore
from back.core.helpers import validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)

_SAFE_RELATION = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class LakebaseSimulationStore(LakebaseFlatStore):
    """:class:`LakebaseFlatStore` restricted to ``__sim_`` sandbox tables."""

    supports_materialized_inference_purge = False
    supports_adjacency = False
    supports_entity_search = False
    supports_props = False

    @classmethod
    def from_store(cls, store: LakebaseFlatStore) -> "LakebaseSimulationStore":
        """Build a sandbox store sharing *store*'s connection identity."""
        return cls(store._auth, store._schema, store._database_override)

    @staticmethod
    def sandbox_table_name(graph_name: str, sim_id: str) -> str:
        """Physical sandbox table ``<graph>__sim_<id>`` (Postgres-length safe)."""
        return _companion_ddl.sim_phy(graph_name, sim_id)

    @staticmethod
    def sandbox_table_id(name: str) -> str:
        """Return the physical sandbox table for *name* or raise ValidationError."""
        validate_table_name(name)
        phy = LakebaseBase.physical_table_id(name)
        if not _companion_ddl.is_sim_phy(phy):
            raise ValidationError(
                "Simulation store can only target simulation sandbox tables",
                detail=phy,
            )
        return phy

    # -- Table-name resolution: every name is the sandbox itself ----------

    def _writable_table_id(self, name: str) -> str:
        return self.sandbox_table_id(name)

    def _readable_table_id(self, name: str) -> str:
        return self.sandbox_table_id(name)

    def _sql_relation(self, table_name: str) -> str:
        return self.sandbox_table_id(table_name)

    def synced_table_name(self, table_name: str) -> str:
        return self.sandbox_table_id(table_name)

    def get_inferred_triple_count(self, table_name: str) -> int:
        return 0

    def adjacency_ready(self, table_name: str) -> bool:
        return False

    def rebuild_adjacency(self, table_name: str) -> None:
        return None

    # -- DDL ----------------------------------------------------------------

    def create_table(self, table_name: str) -> None:
        phy = self.sandbox_table_id(table_name)
        with self._cursor() as cur:
            _companion_ddl.ensure_sim_table(cur, self._schema, phy)
        logger.info("Simulation sandbox ready: %s.%s", self._schema, phy)

    def drop_table(self, table_name: str) -> None:
        phy = self.sandbox_table_id(table_name)
        with self._cursor() as cur:
            _companion_ddl.drop_sim_table(cur, phy)
        logger.info("Dropped simulation sandbox %s.%s", self._schema, phy)

    def truncate_companion(self, name: str) -> None:
        raise ValidationError("Simulation sandboxes have no companion table")

    def purge_materialized_triples(self, table_name: str) -> int:
        raise NotImplementedError("Simulation sandboxes are dropped, not purged")

    def bulk_load_into_sync(
        self,
        table_name: str,
        triple_iterator: Iterable[Dict[str, str]],
        batch_size: int = 5000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        raise ValidationError("Simulation sandboxes have no synced table")

    def optimize_table(self, table_name: str) -> None:
        phy = self.sandbox_table_id(table_name)
        with self._cursor() as cur:
            cur.execute(f"VACUUM ANALYZE {phy}")

    def copy_subjects_from(
        self,
        table_name: str,
        source_relation: str,
        subjects: List[str],
        batch_size: int = 1000,
    ) -> int:
        """Copy every *source_relation* triple whose subject is in *subjects*.

        *source_relation* is a live graph relation in the same Postgres
        schema (e.g. the union view); it is only ever read.
        """
        phy = self.sandbox_table_id(table_name)
        if not _SAFE_RELATION.fullmatch(source_relation or ""):
            raise ValidationError("Invalid source relation", detail=source_relation)
        if _companion_ddl.is_sim_phy(source_relation):
            raise ValidationError("Cannot seed a sandbox from another sandbox")
        sql = (
            f"INSERT INTO {phy} (subject, predicate, object, datatype, lang) "
            f"SELECT subject, predicate, object, datatype, lang "
            f"FROM {source_relation} WHERE subject = ANY(%s) "
            f"ON CONFLICT DO NOTHING"
        )
        copied = 0
        with self._cursor() as cur:
            for i in range(0, len(subjects), batch_size):
                cur.execute(sql, (list(subjects[i : i + batch_size]),))
                copied += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        logger.info("Seeded sandbox %s with %d triples", phy, copied)
        return copied

    def delete_subject_predicate(self, table_name: str, pairs: List[Dict[str, str]]) -> int:
        """Delete every ``(s, p, *)`` triple for the given subject/predicate pairs."""
        phy = self.sandbox_table_id(table_name)
        if not pairs:
            return 0
        deleted = 0
        sql = f"DELETE FROM {phy} WHERE subject = %s AND predicate = %s"
        with self._cursor() as cur:
            for pair in pairs:
                cur.execute(sql, (pair.get("subject", ""), pair.get("predicate", "")))
                deleted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        return deleted

    def get_status(self, table_name: str) -> Dict[str, Any]:
        status = super().get_status(table_name)
        status["format"] = "lakebase-simulation"
        return status
