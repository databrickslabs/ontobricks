"""Shared execution context for scheduled tasks.

Every task type the scheduler runs needs the same preamble: resolve
Databricks credentials, load the domain out of the registry *without a
user session*, and (usually) resolve a graph backend on top of it. This
module owns that preamble so the task modules stay small and only
express what makes them different.

Resolution is lazy: a task that never touches the graph store never pays
for one. :class:`TaskContext` is created per run and is not reused.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from back.core.errors import NotFoundError
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)


@dataclass
class RunOutcome:
    """What a task executor reports back to the scheduler harness.

    ``count`` is the generic "how much did this run write" number shown
    in the schedule table and history; ``detail`` carries the
    type-specific counters that do not fit it.
    """

    status: str = "success"
    message: str = ""
    count: int = 0
    detail: Dict[str, Any] = field(default_factory=dict)
    task_result: Optional[Dict[str, Any]] = None


class _FakeSessionMgr:
    """Minimal stand-in so DomainSession can load without a real session."""

    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}

    def get(self, key, default=None):
        return self._store.get(key, default)

    def set(self, key, value):
        self._store[key] = value


def load_domain_headless(svc, domain_name: str, version: str, host: str, reg: dict):
    """Load a domain from the registry into a session-free DomainSession.

    Returns ``(domain, loaded_version)``. *version* ``"latest"`` resolves
    to the newest stored version.
    """
    from back.objects.session.DomainSession import DomainSession

    if version and version != "latest":
        ok, data, err = svc.read_version(domain_name, version)
        loaded_version = version
        if not ok:
            raise NotFoundError(
                err or f"Version '{version}' not found for domain '{domain_name}'"
            )
    else:
        ok, data, loaded_version, err = svc.load_latest_domain_data(domain_name)
        if not ok:
            raise NotFoundError(err or f"Domain '{domain_name}' not found in registry")

    domain = DomainSession(_FakeSessionMgr())
    domain.import_from_file(data, version=loaded_version)
    domain.domain_folder = domain_name
    domain.settings["registry"] = reg
    domain.databricks["host"] = host
    domain.ensure_generated_content()
    return domain, loaded_version


@dataclass
class TaskContext:
    """Everything a scheduled task executor needs, resolved on demand."""

    task_type: str
    domain_name: str
    target_key: str
    version: str
    config: Dict[str, Any]
    settings: Any
    registry_cfg: Dict[str, str]
    host: str
    token: str
    tm: Any
    task_id: str
    run_ts: str

    # Free-form space for an executor to hand data to its ``on_finish``
    # hook, which runs after the outcome (success or failure) is known.
    scratch: Dict[str, Any] = field(default_factory=dict)

    _svc: Any = None
    _domain: Any = None
    _loaded_version: str = ""
    _snapshot: Any = None
    _store: Any = None
    _client: Any = None

    # ------------------------------------------------------------------
    # Lazily resolved collaborators
    # ------------------------------------------------------------------

    @property
    def registry_service(self):
        """The :class:`RegistryService` bound to this run's registry."""
        if self._svc is None:
            from back.core.databricks.uc import VolumeFileService
            from back.objects.registry.RegistryService import (
                RegistryCfg,
                RegistryService,
            )

            cfg = RegistryCfg.from_dict(self.registry_cfg)
            uc = VolumeFileService(host=self.host, token=self.token)
            self._svc = RegistryService(cfg, uc)
        return self._svc

    @property
    def domain(self):
        """The headless :class:`DomainSession` for this run."""
        if self._domain is None:
            self._domain, self._loaded_version = load_domain_headless(
                self.registry_service,
                self.domain_name,
                self.version,
                self.host,
                self.registry_cfg,
            )
        return self._domain

    @property
    def loaded_version(self) -> str:
        """The concrete version actually loaded (never ``"latest"``)."""
        if not self._loaded_version:
            _ = self.domain
        return self._loaded_version

    @property
    def snapshot(self):
        """Thread-safe :class:`DomainSnapshot` of :attr:`domain`."""
        if self._snapshot is None:
            from back.objects.digitaltwin.models import DomainSnapshot

            self._snapshot = DomainSnapshot(
                self.domain, host=self.host, token=self.token
            )
        return self._snapshot

    @property
    def graph_store(self):
        """The resolved graph backend, or ``None`` when unavailable."""
        if self._store is None:
            from back.core.graphdb import get_graphdb

            self._store = get_graphdb(self.snapshot, self.settings)
        return self._store

    @property
    def graph_name(self) -> str:
        """``<DomainName>_V<version>`` — the graph this run targets."""
        info = getattr(self.domain, "info", None) or {}
        version = getattr(self.domain, "current_version", None) or self.loaded_version
        return f"{info.get('name', DEFAULT_GRAPH_NAME)}_V{version or '1'}"

    @property
    def warehouse_client(self):
        """A :class:`DatabricksClient` on the domain's SQL warehouse."""
        if self._client is None:
            from back.core.databricks.DatabricksClient import DatabricksClient
            from back.core.errors import InfrastructureError
            from back.core.helpers import resolve_warehouse_id

            warehouse_id = resolve_warehouse_id(self.domain, self.settings)
            if not warehouse_id:
                raise InfrastructureError("No SQL warehouse configured")
            self._client = DatabricksClient(
                host=self.host, token=self.token, warehouse_id=warehouse_id
            )
        return self._client

    # ------------------------------------------------------------------
    # Progress helpers (no-ops when the run has no task tracker)
    # ------------------------------------------------------------------

    def progress(self, percent: int, message: str) -> None:
        if self.tm and self.task_id:
            self.tm.update_progress(self.task_id, percent, message)

    def advance(self, message: str) -> None:
        if self.tm and self.task_id:
            self.tm.advance_step(self.task_id, message)

    @property
    def log_prefix(self) -> str:
        target = f"/{self.target_key}" if self.target_key else ""
        return f"Scheduled {self.task_type} [{self.domain_name}{target}]"
