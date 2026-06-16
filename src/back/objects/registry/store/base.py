"""Abstract registry data-store.

The :class:`RegistryStore` ABC sits in front of the Lakebase registry
implementation. A single concrete subclass
(:class:`LakebaseRegistryStore`) exists today — the ABC is retained to
keep the seam in place for future stores (Neo4j, Cosmos, …) and to
make tests easy to fake.

Contracts:

- All methods are synchronous and return ``(ok, payload, message)`` or
  ``(ok, message)`` tuples — matching the existing service signatures.
- Unknown domains / versions return ``(False, …)`` with a non-empty
  ``message``; they must NOT raise.
- ``initialize`` is idempotent.
- ``cache_key`` is used by the registry-level TTL cache to bind cached
  results to *this* store's identity. Two stores pointing at the same
  Lakebase database + schema must return the same key.
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
    review_quorum: int       # per-domain sign-off quorum (>= 1)
    versions: List[Dict[str, Any]]


class ScheduleHistoryEntry(TypedDict, total=False):
    """One row in a domain's scheduled-build history."""

    timestamp: str
    status: str
    message: str
    duration_s: float
    triple_count: int


class BuildRunEntry(TypedDict, total=False):
    """One row in a domain's build-run trace (``build_runs`` table).

    Captures the full statistics of a single Digital Twin build,
    regardless of which path triggered it (``session`` / ``api`` /
    ``scheduled``). The grain is the tuple ``(folder, version)``; many
    entries per tuple are expected and the most recent successful one
    is considered the "active" build (derived at read time).
    """

    id: int                  # row id (0 for stores without a serial PK)
    version: str
    build_kind: str          # 'session' | 'api' | 'scheduled'
    status: str              # 'success' | 'error' | 'cancelled'
    message: str
    error: str
    started_at: str          # ISO timestamp
    finished_at: str         # ISO timestamp
    duration_s: float
    triple_count: int
    entity_count: int
    relationship_count: int
    sql_chars: int
    graph_engine: str
    sync_mode: str
    view_table: str
    graph_name: str
    task_id: str
    phase_times: Dict[str, Any]
    stats: Dict[str, Any]


class ReviewEvent(TypedDict, total=False):
    """One row in the domain-version review / validation audit log.

    Captures a single workflow decision or lifecycle change for the
    tuple ``(folder, version)``. ``from_status`` / ``to_status``
    snapshot the lifecycle transition the event drove (empty on pure
    sign-off / comment rows). Rows are append-only and ordered by
    ``created_at``.
    """

    id: str                  # row id (UUID string; "" for stores without one)
    folder: str              # domain folder (populated by registry-wide reads)
    version: str
    actor: str               # acting user email
    action: str              # submitted|approved|changes_requested|published|reopened|commented
    from_status: str
    to_status: str
    comment: str
    meta: Dict[str, Any]
    created_at: str          # ISO timestamp


class RegistryStore(ABC):
    """Single seam in front of all registry-shaped JSON storage."""

    # ------------------------------------------------------------------
    # Identity / lifecycle
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def backend(self) -> str:
        """Backend tag — always ``"lakebase"`` for now."""

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
    def get_domain_quorum(self, folder: str) -> int:
        """Return the per-domain review sign-off quorum (always >= 1).

        Defaults to ``1`` for domains that predate the setting or when the
        backend cannot resolve it. Must NOT raise.
        """

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

    @abstractmethod
    def update_version_status(
        self, folder: str, version: str, status: str
    ) -> Tuple[bool, str]:
        """Set the lifecycle ``status`` (DRAFT / IN-REVIEW / PUBLISHED) of
        a single (domain, version) without rewriting the full document.
        """

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
    # Build-run trace (analytics)
    #
    # One immutable row per Digital Twin build — across every path
    # (UI session / external API / scheduler). Linked to the domain
    # row; grain is the tuple ``(folder, version)``. Unlike
    # ``schedule_runs`` this is *not* capped — the whole point is a
    # full history for analytics. All methods are best-effort: a build
    # must never fail because tracing could not be written.
    # ------------------------------------------------------------------

    @abstractmethod
    def record_build_run(self, folder: str, entry: BuildRunEntry) -> None:
        """Append a build-run trace row for *folder*. Best-effort; must
        NOT raise (log + swallow on failure).
        """

    @abstractmethod
    def load_build_runs(
        self,
        folder: str,
        *,
        version: Optional[str] = None,
        limit: int = 100,
    ) -> List[BuildRunEntry]:
        """Newest-first build runs for *folder* (optionally a single
        *version*), capped at *limit* rows. Empty list on any error.
        """

    @abstractmethod
    def build_analytics(
        self, folder: str, *, version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Aggregate build statistics for *folder* (optionally scoped to
        a single *version*).

        Returns a dict with at least::

            {
              "total_runs": int,
              "success_runs": int,
              "failed_runs": int,
              "success_rate": float,        # 0..1
              "avg_duration_s": float,
              "min_duration_s": float,
              "max_duration_s": float,
              "last_triple_count": int,
              "active_build": BuildRunEntry | None,  # latest success
              "per_version": [               # newest version first
                {"version": str, "total_runs": int,
                 "success_runs": int, "last_status": str,
                 "last_triple_count": int, "last_run": str}
              ],
            }

        Empty/zeroed dict on any error.
        """

    # ------------------------------------------------------------------
    # Review / validation audit log
    #
    # Append-only history of workflow decisions and lifecycle changes
    # per (folder, version): submit-for-review, sign-off (approve),
    # request changes, publish, reopen, comment. Best-effort writes —
    # a transition must never fail because the audit row could not be
    # written (the lifecycle ``status`` on ``domain_versions`` stays
    # the source of truth). Reads return oldest-first.
    # ------------------------------------------------------------------

    @abstractmethod
    def record_review_event(
        self,
        folder: str,
        version: str,
        actor: str,
        action: str,
        *,
        from_status: str = "",
        to_status: str = "",
        comment: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        """Append a review-audit row for ``(folder, version)``.

        Best-effort: returns ``(False, msg)`` instead of raising so a
        lifecycle transition is never rolled back by a failed audit
        write.
        """

    @abstractmethod
    def list_review_events(
        self, folder: str, version: Optional[str] = None
    ) -> List[ReviewEvent]:
        """Oldest-first review events for *folder* (optionally a single
        *version*). Empty list on any error.
        """

    @abstractmethod
    def list_all_review_events(self) -> List[ReviewEvent]:
        """All review events across the registry, each enriched with its
        ``folder``. Oldest-first. Backs the cross-domain "My Tasks"
        worklist. Empty list on any error.
        """

    # ------------------------------------------------------------------
    # Cohort schedules + history
    #
    # Cohort schedules are keyed by ``"<domain_name>::<rule_id>"`` so a
    # single domain can host many independent schedules (one per saved
    # cohort rule). Default implementations stash the data inside the
    # global-config blob under ``cohort_schedules`` /
    # ``cohort_schedule_history`` — that keeps backends free of new DDL
    # while still persisting to whichever store (Volume or Lakebase)
    # holds the registry. Backends are free to override with dedicated
    # tables / files later.
    # ------------------------------------------------------------------

    def load_cohort_schedules(self) -> Dict[str, Dict[str, Any]]:
        """Return ``{ "<domain>::<rule_id>": cohort_schedule_dict }``."""
        cfg = self.load_global_config()
        return dict(cfg.get("cohort_schedules") or {})

    def save_cohort_schedules(
        self, schedules: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]:
        return self.save_global_config({"cohort_schedules": schedules})

    def load_cohort_schedule_history(
        self, key: str
    ) -> List[ScheduleHistoryEntry]:
        """Oldest-first run history for the cohort schedule *key*."""
        cfg = self.load_global_config()
        histories = cfg.get("cohort_schedule_history") or {}
        return list(histories.get(key) or [])

    def append_cohort_schedule_history(
        self,
        key: str,
        entry: ScheduleHistoryEntry,
        *,
        max_entries: int = 50,
    ) -> None:
        cfg = self.load_global_config()
        histories = dict(cfg.get("cohort_schedule_history") or {})
        entries = list(histories.get(key) or [])
        entries.append(dict(entry))
        if len(entries) > max_entries:
            entries = entries[-max_entries:]
        histories[key] = entries
        self.save_global_config({"cohort_schedule_history": histories})

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

        Default implementation returns ``0`` for every table — used by
        the admin Registry Location panel for an at-a-glance inventory.
        """
        return {t: 0 for t in tables}

    # ------------------------------------------------------------------
    # Default no-op cleanup hooks
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release any held resources. Default: nothing to do."""

    @abstractmethod
    def domain_folder_id(self, folder: str) -> Optional[str]:
        """Return a stable internal identifier for *folder* (or ``None``).

        Used by the UI's "rename folder" admin action. The Lakebase
        backend returns the row's ``id`` (UUID).
        """
