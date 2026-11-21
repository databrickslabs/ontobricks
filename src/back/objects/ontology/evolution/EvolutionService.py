"""Lifecycle of Ontology Evolve runs: create, read, edit intent, cancel, recover."""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from back.core.errors import ConflictError, NotFoundError, ValidationError
from back.core.logging import get_logger
from back.objects.ontology.evolution.Intent import SCHEMA_VERSION, Intent
from back.objects.ontology.evolution.RunStatus import RunStatus
from back.objects.ontology.evolution.content_hash import version_document_hash

logger = get_logger(__name__)

# Results derived from the intent; editing the intent invalidates all of them.
_DERIVED_FIELDS = ("coverage", "proposal", "candidate", "evaluation")


class EvolutionService:
    """Run state for one registry domain folder.

    *registry* is a :class:`RegistryService`. Runs always name their parent
    version explicitly and record the parent's content hash, so later stages
    can detect that the parent changed underneath them.
    """

    INTERRUPTED_AFTER_S = 300

    def __init__(self, registry: Any, folder: str):
        if not folder:
            raise ValidationError(
                "Save the context to the registry before using Evolve"
            )
        self._registry = registry
        self._store = registry.store
        self._folder = folder

    @property
    def folder(self) -> str:
        return self._folder

    @property
    def registry(self) -> Any:
        return self._registry

    def read_parent(self, parent_version: str) -> Dict[str, Any]:
        """Return the parent version document; NotFoundError when it is missing."""
        ok, data, msg = self._registry.read_version(
            self._folder, str(parent_version), include_simulations=False
        )
        if not ok:
            raise NotFoundError(
                f"Version {parent_version} was not found", detail=msg or self._folder
            )
        return data

    def parent_hash(self, parent_version: str) -> str:
        return version_document_hash(
            self.read_parent(parent_version), str(parent_version)
        )

    def create_run(
        self,
        parent_version: str,
        intent: Dict[str, Any],
        *,
        question: str,
        owner: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        """Create a run, or return the one an earlier identical request created."""
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValidationError("An idempotency key (1-200 characters) is required")
        parsed = Intent.from_dict(intent)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "question": str(question or "")[:2000],
            "parent_content_hash": self.parent_hash(parent_version),
        }
        run = {
            "id": uuid.uuid4().hex[:12],
            "parent_version": str(parent_version),
            "status": RunStatus.CREATED.value,
            "idempotency_key": idempotency_key,
            "owner": owner,
            "manifest": manifest,
            "intent": parsed.to_dict(),
        }
        return self._store.create_evolution_run(self._folder, run)

    def get_run(self, run_id: str) -> Dict[str, Any]:
        run = self._store.load_evolution_run(self._folder, run_id)
        if run is None:
            raise NotFoundError("Evolution run not found", detail=str(run_id))
        return run

    def list_runs(self, parent_version: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._store.list_evolution_runs(
            self._folder, parent_version=parent_version
        )

    def update(
        self, run_id: str, revision: int, changes: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compare-and-swap update of a non-terminal run."""
        run = self.get_run(run_id)
        if RunStatus(run["status"]).terminal:
            raise ConflictError(f"The run is {run['status']} and can no longer change")
        return self._store.update_evolution_run(
            self._folder, run_id, int(revision), changes
        )

    def update_intent(
        self, run_id: str, revision: int, intent: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Replace the intent and clear everything derived from the old one."""
        run = self.get_run(run_id)
        if run["status"] == RunStatus.RUNNING.value:
            raise ConflictError(
                "Wait for the running stage to finish or cancel it first"
            )
        parsed = Intent.from_dict(intent)
        changes: Dict[str, Any] = {name: {} for name in _DERIVED_FIELDS}
        changes.update(
            intent=parsed.to_dict(), status=RunStatus.CREATED.value, stage=""
        )
        return self.update(run_id, revision, changes)

    def cancel(self, run_id: str, revision: int) -> Dict[str, Any]:
        """Stop a run. A running stage sees ``cancel_requested`` and stops itself."""
        run = self.get_run(run_id)
        if run["status"] == RunStatus.RUNNING.value:
            return self.update(run_id, revision, {"cancel_requested": True})
        return self.update(
            run_id,
            revision,
            {"cancel_requested": True, "status": RunStatus.CANCELLED.value},
        )

    def delete_run(self, run_id: str) -> bool:
        run = self.get_run(run_id)
        if run["status"] == RunStatus.RUNNING.value:
            raise ConflictError("Cancel the run before deleting it")
        return self._store.delete_evolution_run(self._folder, run_id)

    def heartbeat(self, run_id: str) -> None:
        """Keep a RUNNING run from being marked INTERRUPTED (no revision bump)."""
        self._store.touch_evolution_heartbeat(self._folder, run_id)

    def append_result(self, run_id: str, result: Dict[str, Any]) -> None:
        self._store.append_evolution_result(self._folder, run_id, result)

    def list_results(self, run_id: str) -> List[Dict[str, Any]]:
        self.get_run(run_id)
        return self._store.list_evolution_results(self._folder, run_id)

    def accept_version(
        self, run_id: str, revision: int, version_row: Dict[str, Any]
    ) -> Dict[str, Any]:
        return self._store.accept_evolution_run(
            self._folder, run_id, int(revision), version_row
        )

    def recover_interrupted(self) -> int:
        """Mark runs whose worker stopped heart-beating as INTERRUPTED."""
        count = self._store.mark_interrupted_evolution_runs(self.INTERRUPTED_AFTER_S)
        if count:
            logger.warning("Marked %d evolution run(s) INTERRUPTED", count)
        return count

    def ensure_parent_unchanged(self, run: Dict[str, Any]) -> None:
        """ConflictError when the parent version changed since the run started."""
        expected = (run.get("manifest") or {}).get("parent_content_hash")
        if expected != self.parent_hash(run["parent_version"]):
            raise ConflictError(
                "The parent version changed since this run started — start a new run",
                detail=run["id"],
            )
