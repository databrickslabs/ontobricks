"""Default (unsupported) Ontology Evolve methods of every registry store."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class EvolutionStoreDefaults:
    """Mixed into :class:`RegistryStore`; only the Lakebase store overrides it.

    Unlike the run-history tables these rows are feature state, so stores
    that cannot hold them raise instead of silently dropping writes.
    """

    def _evolution_unsupported(self):
        from back.core.errors import InfrastructureError

        return InfrastructureError(
            "Ontology Evolve needs the Lakebase registry",
            detail=type(self).__name__,
        )

    def create_evolution_run(self, folder: str, run: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a run or return the one with the same idempotency key."""
        raise self._evolution_unsupported()

    def load_evolution_run(self, folder: str, run_id: str) -> Optional[Dict[str, Any]]:
        raise self._evolution_unsupported()

    def list_evolution_runs(
        self, folder: str, parent_version: Optional[str] = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        raise self._evolution_unsupported()

    def update_evolution_run(
        self, folder: str, run_id: str, expected_revision: int, changes: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compare-and-swap update; ConflictError on a stale revision."""
        raise self._evolution_unsupported()

    def delete_evolution_run(self, folder: str, run_id: str) -> bool:
        raise self._evolution_unsupported()

    def append_evolution_result(
        self, folder: str, run_id: str, result: Dict[str, Any]
    ) -> None:
        raise self._evolution_unsupported()

    def list_evolution_results(self, folder: str, run_id: str) -> List[Dict[str, Any]]:
        raise self._evolution_unsupported()

    def mark_interrupted_evolution_runs(self, stale_after_s: int) -> int:
        raise self._evolution_unsupported()

    def touch_evolution_heartbeat(self, folder: str, run_id: str) -> None:
        raise self._evolution_unsupported()

    def accept_evolution_run(
        self,
        folder: str,
        run_id: str,
        expected_revision: int,
        version_row: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Insert the next DRAFT version from an evaluated run, exactly once."""
        raise self._evolution_unsupported()

    def with_evolution_review_meta(
        self, folder: str, version: str, meta: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Review-event *meta*, plus Evolve evidence on stores that hold it."""
        return dict(meta or {})
