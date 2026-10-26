"""Asynchronous, coalescing GitHub mirror fed by registry store writes.

:class:`GitHubMirrorHooks` calls the ``notify_*`` hooks, which return immediately. A
single daemon worker waits :attr:`GitHubMirror.DEBOUNCE_S` and then pushes
the *latest* registry state for each touched ``(folder, version)``, so a
burst of saves becomes one commit. Failures are recorded in
:meth:`GitHubMirror.status` and never surface to the registry caller.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from back.core.databricks.request_identity import get_request_identity
from back.core.logging import get_logger
from back.objects.registry.gitsync.GitHubConfig import GitHubConfig
from back.objects.registry.version_lifecycle import STATUS_PUBLISHED

logger = get_logger(__name__)

_OP_PUSH = "push"
_OP_DELETE_VERSION = "delete_version"
_OP_DELETE_DOMAIN = "delete_domain"

_AUDIT_SOURCE = "github"
_AUDIT_ACTIONS = {_OP_PUSH: "github_pushed", _OP_DELETE_VERSION: "github_version_deleted"}


def _actor() -> str:
    """Caller email for the commit message (empty outside a request)."""
    return get_request_identity().email


@dataclass
class _Job:
    op: str
    store: Any
    folder: str
    version: Optional[str]
    actor: str = ""
    due: float = field(default_factory=time.monotonic)


class GitHubMirror:
    """Process-wide queue of pending GitHub pushes."""

    DEBOUNCE_S = 2.0
    CONFIG_TTL_S = 60.0

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._pending: Dict[Tuple[str, Optional[str]], _Job] = {}
        self._thread: Optional[threading.Thread] = None
        self._cfg_cache: Dict[str, Tuple[float, Optional[GitHubConfig]]] = {}
        self._claimed: set = set()
        self._pause_depth = 0
        self._status: Dict[str, Any] = {
            "state": "idle",
            "at": "",
            "target": "",
            "commit_sha": "",
            "message": "",
            "errors": [],
        }

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def config_for(self, store) -> Optional[GitHubConfig]:
        """Return the active mirror config for *store*'s registry, or ``None``."""
        key = getattr(store, "cache_key", "") or ""
        now = time.monotonic()
        cached = self._cfg_cache.get(key)
        if cached and now - cached[0] < self.CONFIG_TTL_S:
            return cached[1]
        cfg: Optional[GitHubConfig] = None
        try:
            data = store.load_global_config()
            if isinstance(data, dict):
                candidate = GitHubConfig.from_dict(data.get("github_config"))
                if candidate.is_ready and candidate.push_on_save:
                    cfg = candidate
        except Exception as exc:  # noqa: BLE001 - mirror stays off
            logger.debug("GitHub mirror config unavailable: %s", exc)
        self._cfg_cache[key] = (now, cfg)
        return cfg

    def invalidate_config(self) -> None:
        self._cfg_cache.clear()
        self._claimed.clear()

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def notify_write(self, store, folder: str, version: str) -> None:
        self._enqueue(_Job(_OP_PUSH, store, folder, str(version), _actor()))

    def notify_delete_version(self, store, folder: str, version: str) -> None:
        self._enqueue(_Job(_OP_DELETE_VERSION, store, folder, str(version), _actor()))

    def notify_delete_domain(self, store, folder: str) -> None:
        self._enqueue(_Job(_OP_DELETE_DOMAIN, store, folder, None, _actor()))

    @contextmanager
    def paused(self):
        """Skip queueing mirror jobs for the duration of the block."""
        with self._cond:
            self._pause_depth += 1
        try:
            yield
        finally:
            with self._cond:
                self._pause_depth = max(0, self._pause_depth - 1)

    def _enqueue(self, job: _Job) -> None:
        try:
            if self._pause_depth or not job.folder or self.config_for(job.store) is None:
                return
            job.due = time.monotonic() + self.DEBOUNCE_S
            with self._cond:
                if job.op == _OP_DELETE_DOMAIN:
                    for key in [k for k in self._pending if k[0] == job.folder]:
                        del self._pending[key]
                self._pending[(job.folder, job.version)] = job
                self._ensure_worker()
                self._cond.notify()
        except Exception as exc:  # noqa: BLE001 - never break a registry write
            logger.warning("GitHub mirror enqueue failed: %s", exc)

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    def _ensure_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="github-mirror", daemon=True
        )
        self._thread.start()

    def _next_due(self) -> Optional[_Job]:
        with self._cond:
            while True:
                if not self._pending:
                    self._cond.wait(timeout=30)
                    if not self._pending:
                        continue
                key, job = min(self._pending.items(), key=lambda kv: kv[1].due)
                wait = job.due - time.monotonic()
                if wait <= 0:
                    del self._pending[key]
                    return job
                self._cond.wait(timeout=wait)

    def _run(self) -> None:
        while True:
            job = self._next_due()
            if job is not None:
                self.process(job)

    def process(self, job: _Job) -> None:
        """Run one job synchronously (also used by tests)."""
        from back.objects.registry.gitsync.GitHubSyncService import GitHubSyncService

        cfg = self.config_for(job.store)
        if cfg is None:
            return
        target = job.folder + (f"/V{job.version}" if job.version else "")
        sync = GitHubSyncService(cfg)
        try:
            claim_key = f"{cfg.label}|{cfg.instance_uuid}"
            if claim_key not in self._claimed:
                sync.ensure_claim()
                self._claimed.add(claim_key)
            if job.op == _OP_PUSH:
                result = sync.push_version(
                    job.store, job.folder, job.version, actor=job.actor
                )
            elif job.op == _OP_DELETE_VERSION:
                result = sync.delete_version(
                    job.store, job.folder, job.version, actor=job.actor
                )
            else:
                result = sync.delete_domain(job.folder, actor=job.actor)
            sha = result.get("commit_sha") or ""
            errors = result.get("errors") or []
            self.record("ok", target, sha, "", errors)
            if job.op != _OP_DELETE_DOMAIN and (sha or errors):
                self.audit(
                    job.store, cfg, job.folder, job.version, job.actor,
                    _AUDIT_ACTIONS[job.op], commit_sha=sha, errors=errors,
                    tag=f"{job.folder}/V{job.version}"
                    if result.get("status") == STATUS_PUBLISHED else "",
                )
        except Exception as exc:  # noqa: BLE001 - recorded for the Settings UI
            logger.warning("GitHub mirror %s %s failed: %s", job.op, target, exc)
            message = getattr(exc, "message", "") or str(exc)
            self.record("error", target, "", message, [])
            if job.op != _OP_DELETE_DOMAIN:
                self.audit(
                    job.store, cfg, job.folder, job.version, job.actor,
                    "github_push_failed", message=message,
                )
        finally:
            sync.close()

    # ------------------------------------------------------------------
    # Audit trail
    # ------------------------------------------------------------------

    @staticmethod
    def audit(
        store,
        cfg: GitHubConfig,
        folder: str,
        version: Optional[str],
        actor: str,
        action: str,
        *,
        commit_sha: str = "",
        message: str = "",
        errors: Optional[List[str]] = None,
        tag: str = "",
    ) -> None:
        """Append one ``source='github'`` row to the domain change audit log.

        Shown in Domain → Audit trail → GitHub. Best effort: an audit
        failure is logged and never affects the push.
        """
        if not folder or not version:
            return
        short = commit_sha[:7]
        summary = {
            "github_pushed": f"Pushed to {cfg.label}" + (f" ({short})" if short else ""),
            "github_synced": f"Synced to {cfg.label}" + (f" ({short})" if short else ""),
            "github_version_deleted": f"Removed V{version} from {cfg.label}",
            "github_push_failed": f"Push to {cfg.label} failed",
        }.get(action, action)
        meta = {
            "repository": cfg.label,
            "commit_sha": commit_sha,
            "commit_url": cfg.commit_url(commit_sha),
            "tag": tag,
            "message": message,
            "errors": list(errors or []),
        }
        event = {
            "source": _AUDIT_SOURCE,
            "action": action,
            "entity_type": "github",
            "entity_ref": short,
            "summary": summary,
            "meta": meta,
        }
        try:
            store.record_change_events(folder, str(version), actor, [event])
        except Exception as exc:  # noqa: BLE001 - audit is best effort
            logger.warning("GitHub audit %s %s/V%s failed: %s", action, folder, version, exc)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def record(
        self, state: str, target: str, commit_sha: str, message: str, errors: list
    ) -> None:
        self._status = {
            "state": state,
            "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "target": target,
            "commit_sha": commit_sha,
            "message": message,
            "errors": list(errors),
        }

    def status(self) -> Dict[str, Any]:
        with self._cond:
            pending = len(self._pending)
        return {**self._status, "pending": pending}


github_mirror = GitHubMirror()
