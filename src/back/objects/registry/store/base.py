"""Abstract registry data-store.

Every operation that today reads or writes a JSON blob in the registry
volume must go through a :class:`RegistryStore`. Backends are free to
persist that data however they like (JSON-on-Volume, Postgres tables,
…) as long as they honour the contracts defined here.

Contracts:

- All methods are synchronous and return ``(ok, payload, message)`` or
  ``(ok, message)`` tuples — matching the existing service signatures
  to keep the refactor behaviour-preserving.
- Unknown domains / versions return ``(False, …)`` with a non-empty
  ``message``; they must NOT raise.
- ``initialize`` is idempotent.
- ``cache_key`` is used by the registry-level TTL cache to bind cached
  results to *this* store's identity. Two stores pointing at the same
  registry triplet (catalog/schema/volume) for Volume backends, or the
  same Postgres schema for Lakebase, must return the same key.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, TypedDict


class StoreError(RuntimeError):
    """Raised when a backend hits a non-recoverable error.

    Most store methods return ``(ok, …, msg)`` tuples and never raise.
    :class:`StoreError` is reserved for true infrastructure failures
    (connection loss, schema corruption) where the caller cannot
    meaningfully continue.
    """


class DomainSummary(TypedDict, total=False):
    """Per-domain metadata returned by :meth:`RegistryStore.list_domains_with_metadata`.

    Mirrors the dict shape produced today by
    ``RegistryService.list_domain_details``.
    """

    name: str
    base_uri: str
    description: str
    versions: List[Dict[str, Any]]


class ScheduleHistoryEntry(TypedDict, total=False):
    """One row in a domain's scheduled-build history."""

    timestamp: str
    status: str
    message: str
    duration_s: float
    triple_count: int


class RegistryStore(ABC):
    """Single seam in front of all registry-shaped JSON storage."""

    # ------------------------------------------------------------------
    # Identity / lifecycle
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def backend(self) -> str:
        """Backend tag — ``"volume"`` or ``"lakebase"``."""

    @property
    @abstractmethod
    def cache_key(self) -> str:
        """Stable identity used by the registry-level TTL cache."""

    @abstractmethod
    def is_initialized(self) -> bool:
        """Return ``True`` when the backing store is ready for use."""

    @abstractmethod
    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        """Bring the backend up to a usable state (idempotent).

        For :class:`VolumeRegistryStore` this means creating the UC
        Volume (if missing) and writing the ``.registry`` marker.

        For :class:`LakebaseRegistryStore` this applies the DDL in
        ``store/lakebase_schema.sql`` and verifies connectivity with a
        ``SELECT 1`` wake probe.
        """

    # ------------------------------------------------------------------
    # Domain folder listing
    # ------------------------------------------------------------------

    @abstractmethod
    def list_domain_folders(self) -> Tuple[bool, List[str], str]:
        """Sorted domain folder names; hidden entries excluded."""

    @abstractmethod
    def list_domains_with_metadata(self) -> Tuple[bool, List[DomainSummary], str]:
        """Like :meth:`list_domain_folders` but enriched with per-version
        ``active``/``last_update``/``last_build`` and the latest version's
        ``description`` + ``base_uri``.
        """

    @abstractmethod
    def domain_exists(self, folder: str) -> bool: ...

    @abstractmethod
    def delete_domain(self, folder: str) -> List[str]:
        """Delete a domain (versions + permissions + history). Returns
        a list of error messages — empty on success.
        """

    # ------------------------------------------------------------------
    # Version CRUD
    # ------------------------------------------------------------------

    @abstractmethod
    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]: ...

    @abstractmethod
    def read_version(
        self, folder: str, version: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        """Return the parsed domain document (``info``/``versions``/…)."""

    @abstractmethod
    def write_version(
        self, folder: str, version: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Persist the parsed document. Implementations may serialise
        to JSON (Volume) or normalise into typed columns (Lakebase).
        """

    @abstractmethod
    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]: ...

    # ------------------------------------------------------------------
    # Domain-level permissions
    # ------------------------------------------------------------------

    @abstractmethod
    def load_domain_permissions(self, folder: str) -> Dict[str, Any]:
        """Return ``{"version": 1, "permissions": [...]}`` (empty when
        unset). Must NOT raise on missing-file / missing-row.
        """

    @abstractmethod
    def save_domain_permissions(
        self, folder: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]: ...

    # ------------------------------------------------------------------
    # Schedules + history
    # ------------------------------------------------------------------

    @abstractmethod
    def load_schedules(self) -> Dict[str, Dict[str, Any]]:
        """Return ``{ domain_name: schedule_dict }`` (may be empty)."""

    @abstractmethod
    def save_schedules(
        self, schedules: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]: ...

    @abstractmethod
    def load_schedule_history(self, folder: str) -> List[ScheduleHistoryEntry]:
        """Oldest-first list of run history entries (capped server-side)."""

    @abstractmethod
    def append_schedule_history(
        self, folder: str, entry: ScheduleHistoryEntry, *, max_entries: int = 50
    ) -> None:
        """Append *entry* and trim to the last *max_entries* rows."""

    # ------------------------------------------------------------------
    # Global config
    # ------------------------------------------------------------------

    @abstractmethod
    def load_global_config(self) -> Dict[str, Any]:
        """Return the merged global-config blob. Empty dict when unset."""

    @abstractmethod
    def save_global_config(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        """Merge *updates* into the persisted blob (last-write-wins)."""

    # ------------------------------------------------------------------
    # Optional helpers
    # ------------------------------------------------------------------

    def health_check(self) -> Tuple[bool, str]:
        """Cheap probe used by the settings UI / startup wake-up.

        Default implementation defers to :meth:`is_initialized`.
        """
        try:
            return (True, "ok") if self.is_initialized() else (False, "not initialized")
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def describe(self) -> Dict[str, Any]:
        """Return a JSON-serialisable description of the backend.

        Used by ``GET /settings/registry`` to render the read-only
        connection block in the admin UI.
        """
        return {"backend": self.backend, "cache_key": self.cache_key}

    def table_row_counts(self, tables: Tuple[str, ...]) -> Dict[str, int]:
        """Return ``{table_name: row_count}`` for *tables*.

        Default implementation returns ``0`` for every table — this is
        a Lakebase-flavoured introspection helper used by the admin
        Registry Location panel. Volume-backed stores have no such
        notion and simply report zeros.
        """
        return {t: 0 for t in tables}

    # ------------------------------------------------------------------
    # Default no-op cleanup hooks
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release any held resources. Default: nothing to do."""

    # Helpers used by VolumeRegistryStore for the binary-side path
    # builders. Lakebase store overrides nothing here.

    @abstractmethod
    def domain_folder_id(self, folder: str) -> Optional[str]:
        """Return a stable internal identifier for *folder* (or ``None``).

        Used by the UI's "rename folder" admin action and by the
        migration command. Volume backend returns the folder name
        unchanged; Lakebase backend returns the row's ``id`` (UUID).
        """
