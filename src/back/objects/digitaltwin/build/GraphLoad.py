"""Load a relation of triples into an opened graph backend store.

Extracted from the build pipeline so any build can reuse the backend load:
the build from the mapping reads the gateway VIEW, and a certified build will
read its composed relation. The relation is the only input that changes.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)


class GraphLoad:
    """Full rebuild of a graph store from a source relation, streamed in batches."""

    LOADED = "loaded"
    EMPTY = "empty"
    FAILED = "failed"

    BATCH_SIZE = 5000

    def __init__(
        self,
        *,
        tm: Any,
        task_id: str,
        is_api: bool,
        domain: Any,
        settings: Any,
        store: Any,
        source_client: Any,
        graph_name: str,
        source_relation: str,
        delta_cfg: Optional[dict] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.tm = tm
        self.task_id = task_id
        self.is_api = is_api
        self.domain = domain
        self.settings = settings
        self.store = store
        self.source_client = source_client
        self.graph_name = graph_name
        self.source_relation = source_relation
        self.delta_cfg = delta_cfg or {}
        self.cancel_check = cancel_check or (lambda: False)
        self.triple_count = 0
        self.phase_times: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self) -> str:
        """Load the source relation; return ``LOADED``, ``EMPTY`` or ``FAILED``."""
        t_fetch = time.time()
        logger.info(
            "[DT-BUILD %s] full rebuild: reading all triples from %s",
            self.task_id,
            self.source_relation,
        )
        self._progress(40, "Reading all triples from VIEW...")
        self.triple_count = self.count_source_triples()
        self._log_phase("fetch_triples", t_fetch)
        logger.info(
            "[DT-BUILD %s] source reports %d triples to ingest",
            self.task_id,
            self.triple_count,
        )
        if self.triple_count == 0:
            self._warn_empty_source()
            return self.EMPTY

        t_insert = time.time()
        self._progress(50, f"Full rebuild: writing {self.triple_count} triples...")
        self._recreate_graph()
        self._load_rows()
        logger.info(
            "[DT-BUILD %s] optimizing graph table %s", self.task_id, self.graph_name
        )
        self.store.optimize_table(self.graph_name)
        self.rebuild_adjacency_if_supported()
        self._log_phase("graph_insert", t_insert)
        return self.LOADED

    # ------------------------------------------------------------------
    # Shared with SyncedGraphLoad
    # ------------------------------------------------------------------

    def count_source_triples(self) -> int:
        """Return the number of triples in the source relation (server-side COUNT).

        Raises :class:`InfrastructureError` when the count cannot run, so a
        lost connection fails the build instead of reading as 0 triples.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        logger.debug("[DT-BUILD %s] counting %s", self.task_id, self.source_relation)
        builder = GraphDBFactory.get_builder(
            self.domain, self.settings, engine="delta", client=self.source_client
        )
        count = builder.count_triples(self.source_relation)
        logger.debug(
            "[DT-BUILD %s] %s: %d triple(s)", self.task_id, self.source_relation, count
        )
        return count

    def rebuild_adjacency_if_supported(self) -> None:
        """Rebuild the adjacency / search companions on a Postgres store."""
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        if self.store is None:
            return
        sql_flavor_fn = getattr(self.store, "sql_flavor", None)
        flavor = sql_flavor_fn() if callable(sql_flavor_fn) else None
        if flavor != "postgres":
            return
        rebuild_graph_cache_if_enabled(self.store, self.graph_name, self.domain)

    def _log_phase(self, name: str, t0_phase: float) -> None:
        elapsed = time.time() - t0_phase
        self.phase_times[name] = elapsed
        logger.info("[DT-BUILD %s] phase [%s]: %.2fs", self.task_id, name, elapsed)

    def _progress(self, pct: int, message: str) -> None:
        if not self.is_api:
            self.tm.update_progress(self.task_id, pct, message)

    # ------------------------------------------------------------------
    # Streaming load
    # ------------------------------------------------------------------

    def _warn_empty_source(self) -> None:
        logger.warning(
            "[DT-BUILD %s] source %s returned 0 triples — "
            "possible causes: (1) R2RML mappings do not match source table "
            "columns, (2) source tables are empty, (3) SQL query filters "
            "out all rows. The VIEW was created successfully but the graph "
            "will be empty until mappings are corrected.",
            self.task_id,
            self.source_relation,
        )

    def _recreate_graph(self) -> None:
        logger.info(
            "[DT-BUILD %s] dropping & recreating graph table %s",
            self.task_id,
            self.graph_name,
        )
        self.store.drop_table(self.graph_name)
        self.store.create_table(self.graph_name)

    def _load_rows(self) -> None:
        """Stream the source rows into the store (Lakebase ``_sync`` or generic)."""
        logger.info(
            "[DT-BUILD %s] streaming %d triples into %s (batch_size=%d)",
            self.task_id,
            self.triple_count,
            self.graph_name,
            self.BATCH_SIZE,
        )
        select_sql = f"SELECT subject, predicate, object FROM {self.source_relation}"
        on_progress = self._write_progress_callback()
        if hasattr(self.store, "bulk_load_into_sync"):
            # Lakebase app_managed: warehouse data goes into *_sync; app writes
            # (reasoning / materialise) target the companion (*__app).
            triple_iter = self.source_client.iter_rows(
                select_sql, batch_size=self.BATCH_SIZE
            )
            written = self.store.bulk_load_into_sync(
                self.graph_name,
                triple_iter,
                batch_size=self.BATCH_SIZE,
                on_progress=on_progress,
            )
            logger.info(
                "[DT-BUILD %s] bulk_load_into_sync wrote %d rows into %s_sync",
                self.task_id,
                written,
                self.graph_name,
            )
            return
        self.stream_triples_into_store(
            select_sql, insert_batch_size=self.BATCH_SIZE, on_progress=on_progress
        )

    def _write_progress_callback(self) -> Callable[[int, int], None]:
        """Progress callback anchored on the warehouse-side COUNT total.

        ``bulk_insert_iter`` passes the running written count as ``total``, so
        the denominator must be the upfront count or the UI reads ``x/x``.
        """
        total_count = self.triple_count

        def _on_progress(written: int, total: int) -> None:
            denom = total_count or total or 1
            progress = 50 + int(written / denom * 40)
            if not self.is_api:
                progress = min(progress, 90)
            self.tm.update_progress(
                self.task_id, progress, f"Written {written}/{denom} triples..."
            )

        return _on_progress

    def stream_triples_into_store(
        self,
        select_sql: str,
        *,
        insert_batch_size: int = BATCH_SIZE,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Stream warehouse rows into the graph store via the bulk insert iterator."""
        use_iter = hasattr(self.store, "bulk_insert_iter")
        logger.debug(
            "[DT-BUILD %s] streaming triples into %s (batch_size=%d method=%s)",
            self.task_id,
            self.graph_name,
            insert_batch_size,
            "bulk_insert_iter" if use_iter else "insert_triples",
        )
        triple_iter = self.source_client.iter_rows(
            select_sql, batch_size=insert_batch_size
        )
        if use_iter:
            written = self.store.bulk_insert_iter(
                self.graph_name,
                triple_iter,
                batch_size=insert_batch_size,
                on_progress=on_progress,
            )
            method = "bulk_insert_iter"
        else:
            written = self.store.insert_triples(
                self.graph_name,
                list(triple_iter),
                batch_size=min(insert_batch_size, 500),
                on_progress=on_progress,
            )
            method = "insert_triples"
        logger.info(
            "[DT-BUILD %s] %s wrote %d rows into %s",
            self.task_id,
            method,
            written,
            self.graph_name,
        )
        return written
