"""Keep a SQLite registry file durable on a Unity Catalog Volume.

A Databricks App's local disk is wiped on every restart or redeploy, and a
UC Volume is reachable only through the Files API (no filesystem mount), so
SQLite can't open a file on the Volume directly. Instead the database lives
on local disk and this class mirrors it:

- ``restore()`` downloads the last snapshot when the local file is missing
  (first request after a restart);
- ``mark_dirty()`` schedules an upload a few seconds after the last committed
  write, so a burst of writes costs one upload;
- ``flush()`` uploads immediately; it also runs at interpreter exit, which
  covers a normal stop or redeploy.

A crash between a write and the next upload loses that window of writes.
That is the accepted trade-off of this backend; Lakebase or an external
Postgres has no such window.

Stores are created per request, so snapshots are shared per local file via
:meth:`SQLiteSnapshot.shared`.
"""

from __future__ import annotations

import atexit
import os
import sqlite3
import tempfile
import threading
from typing import Any, Callable, Dict, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_DEBOUNCE_SECONDS = 3.0


class SQLiteSnapshot:
    """Mirrors one local SQLite registry file to a UC Volume path."""

    _shared: Dict[str, "SQLiteSnapshot"] = {}
    _shared_guard = threading.Lock()

    def __init__(
        self,
        local_path: str,
        volume_path: str,
        volume_service_factory: Callable[[], Any],
        debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
    ) -> None:
        self._local_path = local_path
        self._volume_path = volume_path
        self._volume_service_factory = volume_service_factory
        self._debounce_seconds = debounce_seconds
        self._timer: Optional[threading.Timer] = None
        self._timer_guard = threading.Lock()
        self._restored = False
        self._dirty = False

    @classmethod
    def shared(
        cls,
        local_path: str,
        volume_path: str,
        volume_service_factory: Callable[[], Any],
    ) -> "SQLiteSnapshot":
        """Return the process-wide snapshot for *local_path*, creating it once."""
        with cls._shared_guard:
            snap = cls._shared.get(local_path)
            if snap is None:
                snap = cls(local_path, volume_path, volume_service_factory)
                cls._shared[local_path] = snap
                atexit.register(snap.flush_if_dirty)
            return snap

    @property
    def volume_path(self) -> str:
        return self._volume_path

    def restore(self) -> bool:
        """Download the snapshot if the local file is missing. Returns True when restored."""
        if self._restored or os.path.exists(self._local_path):
            self._restored = True
            return False
        self._restored = True
        ok, data, msg = self._volume_service_factory().read_binary_file(self._volume_path)
        if not ok or not data:
            logger.info("No SQLite registry snapshot at %s (%s); starting empty", self._volume_path, msg)
            return False
        os.makedirs(os.path.dirname(self._local_path) or ".", exist_ok=True)
        with open(self._local_path, "wb") as fh:
            fh.write(data)
        logger.info("Restored SQLite registry from %s (%d bytes)", self._volume_path, len(data))
        return True

    def mark_dirty(self) -> None:
        """Schedule an upload after the debounce window (one pending upload at a time)."""
        with self._timer_guard:
            self._dirty = True
            if self._timer is not None and self._timer.is_alive():
                return
            self._timer = threading.Timer(self._debounce_seconds, self.flush)
            self._timer.daemon = True
            self._timer.start()

    def flush_if_dirty(self) -> bool:
        """Upload only when writes happened since the last upload (used at exit)."""
        return self.flush() if self._dirty else False

    def flush(self) -> bool:
        """Upload a consistent copy of the database now. Returns True on success."""
        if not os.path.exists(self._local_path):
            return False
        self._dirty = False
        try:
            data = self._consistent_copy()
            ok, msg = self._volume_service_factory().write_binary_file(self._volume_path, data)
        except Exception as exc:  # noqa: BLE001 -- a failed upload must never break a request
            self._dirty = True
            logger.warning("SQLite registry snapshot to %s failed: %s", self._volume_path, exc)
            return False
        if not ok:
            self._dirty = True
            logger.warning("SQLite registry snapshot to %s failed: %s", self._volume_path, msg)
            return False
        logger.info("SQLite registry snapshot saved to %s (%d bytes)", self._volume_path, len(data))
        return True

    def _consistent_copy(self) -> bytes:
        """Copy through SQLite's backup API, so WAL content is included and pages are consistent."""
        fd, tmp_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            src = sqlite3.connect(self._local_path)
            dst = sqlite3.connect(tmp_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
                src.close()
            with open(tmp_path, "rb") as fh:
                return fh.read()
        finally:
            os.unlink(tmp_path)
