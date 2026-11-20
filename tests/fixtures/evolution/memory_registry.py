"""In-memory registry stand-ins for Evolve unit tests and the offline eval runner."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Optional

from back.core.errors import ConflictError, NotFoundError

FIXTURE = Path(__file__).resolve().parent / "supply_chain_domain.json"


def fixture_document(catalog: str = "cat", schema: str = "fix") -> Dict[str, Any]:
    raw = (
        FIXTURE.read_text()
        .replace("__CATALOG__", catalog)
        .replace("__SCHEMA__", schema)
    )
    return json.loads(raw)


class FakeEvolutionStore:
    """Dict-backed evolution store with the same CAS and idempotency rules."""

    def __init__(self):
        self.runs: Dict[str, Dict[str, Any]] = {}
        self.results: list = []

    def create_evolution_run(self, folder, run):
        for existing in self.runs.values():
            if (
                existing["_folder"] == folder
                and existing["idempotency_key"] == run["idempotency_key"]
            ):
                return {**self._public(existing), "created": False}
        row = {
            "_folder": folder,
            "revision": 1,
            "stage": "",
            "task_id": "",
            "cancel_requested": False,
            "coverage": {},
            "proposal": {},
            "candidate": {},
            "evaluation": {},
            "accepted_version": "",
            "error_code": "",
            "error": "",
            **copy.deepcopy(run),
        }
        self.runs[run["id"]] = row
        return {**self._public(row), "created": True}

    def load_evolution_run(self, folder, run_id) -> Optional[Dict[str, Any]]:
        row = self.runs.get(run_id)
        return self._public(row) if row and row["_folder"] == folder else None

    def list_evolution_runs(self, folder, parent_version=None, limit=50):
        return [
            self._public(r)
            for r in self.runs.values()
            if r["_folder"] == folder
            and (not parent_version or r["parent_version"] == parent_version)
        ][:limit]

    def update_evolution_run(self, folder, run_id, expected_revision, changes):
        row = self.runs.get(run_id)
        if not row or row["_folder"] != folder:
            raise NotFoundError("Evolution run not found")
        if row["revision"] != expected_revision:
            raise ConflictError("stale", detail=f"current {row['revision']}")
        changes = dict(changes)
        changes.pop("touch_heartbeat", None)
        row.update(copy.deepcopy(changes))
        row["revision"] += 1
        return self._public(row)

    def delete_evolution_run(self, folder, run_id):
        return self.runs.pop(run_id, None) is not None

    def append_evolution_result(self, folder, run_id, result):
        self.results.append((folder, run_id, copy.deepcopy(result)))

    def list_evolution_results(self, folder, run_id):
        return [r for f, i, r in self.results if f == folder and i == run_id]

    def mark_interrupted_evolution_runs(self, stale_after_s):
        hits = [r for r in self.runs.values() if r["status"] == "RUNNING"]
        for r in hits:
            r["status"] = "INTERRUPTED"
            r["revision"] += 1
        return len(hits)

    @staticmethod
    def _public(row):
        return {k: copy.deepcopy(v) for k, v in row.items() if not k.startswith("_")}


class FakeRegistry:
    """RegistryService stand-in: version documents in memory, evolution store attached."""

    def __init__(self, documents: Optional[Dict[str, Dict[str, Any]]] = None):
        self.store = FakeEvolutionStore()
        self.documents = (
            documents if documents is not None else {"1": fixture_document()}
        )
        self.writes: list = []

    def read_version(self, folder, version, include_simulations=True):
        doc = self.documents.get(str(version))
        if doc is None:
            return False, {}, f"Version {version} not found"
        return True, copy.deepcopy(doc), ""
