"""Lakebase managed-synced load: Lakeflow moves the triples, never the app."""

from __future__ import annotations

import time
from typing import Any, Optional, Tuple

from back.core.errors import InfrastructureError, OperationCancelledError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.GraphLoad import GraphLoad

logger = get_logger(__name__)

_SYNC_STATE_LABELS = {
    "PROVISIONING": "provisioning pipeline…",
    "PROVISIONING_PIPELINE_RESOURCES": "provisioning pipeline resources…",
    "PROVISIONING_INITIAL_SNAPSHOT": "initial snapshot in progress…",
    "ONLINE_TRIGGERED_UPDATE": "snapshot update running…",
    "ONLINE_CONTINUOUS_UPDATE": "continuous update running…",
    "ONLINE_PIPELINE_FAILED": "pipeline error — retrying…",
    "OFFLINE": "pipeline offline — waiting…",
}


class SyncedGraphLoad(GraphLoad):
    """Load a source relation into Lakebase through a Lakeflow synced table.

    Steps (the first one is the apply step announced by the build):

    1. Build the synced UC FQN from ``engine_config.sync_uc_catalog``.
    2. ``CREATE SCHEMA IF NOT EXISTS`` in Unity Catalog.
    3. ``SyncedTableManager.ensure`` — registers the synced table on the
       source relation.
    4. ``ensure_synced_companion`` — Postgres companion table.
    5. ``trigger_and_wait`` — wait until sync reaches an online state.
    6. ``ensure_synced_union_view`` — union view over ``_sync`` ∪ companion.
    7. ``TRUNCATE`` the companion, then rebuild the adjacency companions.

    The source relation must expose the Lakeflow primary-key columns
    (``object_hash``); the build prepares it before the load.
    """

    def run(self) -> str:
        t0 = time.time()
        resolved = self._resolve_synced_uc()
        if resolved is None:
            return self.FAILED
        mgr, builder, synced_uc = resolved
        actual_synced_uc = self._register_synced_table(mgr, builder, synced_uc)
        if actual_synced_uc is None:
            return self.FAILED
        self._raise_if_cancelled()
        self._advance()  # → "Creating companion table"
        if not self._ensure_companion():
            return self.FAILED
        self._advance()  # → "Syncing data from Delta (Lakeflow)"
        if not self._trigger_sync(mgr, synced_uc, actual_synced_uc, t0):
            return self.FAILED
        self._advance()  # → "Creating graph viewer union view"
        if not self._ensure_union_view(synced_uc, actual_synced_uc):
            return self.FAILED
        self._advance()  # → "Finalizing graph viewer"
        if not self._finalize():
            return self.FAILED
        self._report_post_sync_triple_count()
        logger.info(
            "[DT-BUILD %s] synced load complete: triples=%d total_elapsed=%.1fs",
            self.task_id,
            self.triple_count,
            time.time() - t0,
        )
        return self.LOADED

    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------

    def _resolve_synced_uc(self) -> Optional[Tuple[Any, Any, str]]:
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        builder = GraphDBFactory.get_builder(
            self.domain, self.settings, engine="lakebase"
        )
        try:
            mgr = self.store.synced_manager()
            fallback_cat = builder.resolve_sync_uc_fallback_catalog(self.delta_cfg)
            synced_uc = self.store.synced_uc_name(
                self.graph_name, fallback_catalog=fallback_cat
            )
        except InfrastructureError as exc:
            logger.error(
                "[DT-BUILD %s] managed_synced setup failed: %s", self.task_id, exc
            )
            self.tm.fail_task(self.task_id, str(exc))
            return None
        logger.info(
            "[DT-BUILD %s] Managed-sync registers UC synced table at %s "
            "(graph_engine_config.sync_uc_catalog=%r; fallback_catalog=%r; "
            "UC schema segment=%s)",
            self.task_id,
            synced_uc,
            (self.store.sync_uc_catalog or "").strip() or None,
            fallback_cat or None,
            self.store.graph_schema,
        )
        return mgr, builder, synced_uc

    def _register_synced_table(
        self, mgr: Any, builder: Any, synced_uc: str
    ) -> Optional[str]:
        """Steps 2-3: UC schema, then the synced table. Return its actual FQN."""
        self._progress(45, f"Ensuring UC schema for {synced_uc}…")
        try:
            t_step = time.time()
            builder.ensure_uc_schema_for_synced(
                self.source_client,
                synced_uc,
                task_log_prefix=f"[DT-BUILD {self.task_id}]",
            )
            logger.info(
                "[DT-BUILD %s] step 2/7 done: UC schema verified in %.2fs",
                self.task_id,
                time.time() - t_step,
            )
            self._raise_if_cancelled()
            self._advance()  # → "Registering synced table in Unity Catalog"
            t_step = time.time()
            synced_obj = mgr.ensure(
                synced_uc,
                source_table_full_name=self.source_relation,
                primary_key_columns=builder.lakeflow_primary_key(),
                sync_mode=self.store.sync_table_mode,
            )
        except OperationCancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] failed to register synced table %s: %s "
                "(source=%s pg_schema=%s sync_table_mode=%s)",
                self.task_id,
                synced_uc,
                exc,
                self.source_relation,
                getattr(self.store, "graph_schema", "?"),
                getattr(self.store, "sync_table_mode", "?"),
            )
            self.tm.fail_task(
                self.task_id, f"Could not register Lakebase synced table: {exc}"
            )
            return None
        # ensure() may have used a fallback name (ghost control-plane state);
        # downstream steps must use the name it actually registered.
        actual_synced_uc = getattr(synced_obj, "name", None) or synced_uc
        if actual_synced_uc != synced_uc:
            logger.warning(
                "[DT-BUILD %s] synced table registered under fallback UC name %s "
                "(requested %s); downstream steps will use the fallback.",
                self.task_id,
                actual_synced_uc,
                synced_uc,
            )
        logger.info(
            "[DT-BUILD %s] step 3/7 done: synced table registered in %.2fs",
            self.task_id,
            time.time() - t_step,
        )
        return actual_synced_uc

    def _ensure_companion(self) -> bool:
        """Step 4: create the companion table in Postgres."""
        t_step = time.time()
        try:
            self.store.ensure_synced_companion(self.graph_name)
        except OperationCancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] failed to upgrade companion table for %s: %s "
                "(pg_schema=%s)",
                self.task_id,
                self.graph_name,
                exc,
                getattr(self.store, "graph_schema", "?"),
            )
            self.tm.fail_task(
                self.task_id, f"Could not prepare Lakebase companion table: {exc}"
            )
            return False
        logger.info(
            "[DT-BUILD %s] step 4/7 done: companion table ready in %.2fs",
            self.task_id,
            time.time() - t_step,
        )
        return True

    def _trigger_sync(
        self, mgr: Any, synced_uc: str, actual_synced_uc: str, t0: float
    ) -> bool:
        """Step 5: trigger the Lakeflow snapshot and wait for ONLINE."""
        if self.store.drop_app_owned_sync_artifacts_if_present(self.graph_name):
            logger.info(
                "[DT-BUILD %s] removed app-owned _sync artifacts for %s "
                "before Lakeflow sync",
                self.task_id,
                self.graph_name,
            )
        t_sync = time.time()
        try:
            state = mgr.trigger_and_wait(
                actual_synced_uc,
                timeout_s=self.store.sync_timeout_s,
                cancel_check=self.cancel_check,
                on_state_change=self._on_sync_state,
            )
        except OperationCancelledError as exc:
            # The task is already CANCELLED — do not flip it to FAILED.
            logger.info(
                "[DT-BUILD %s] step 5/7 aborted by user cancel for %s (%s)",
                self.task_id,
                synced_uc,
                exc,
            )
            return False
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] step 5/7 FAILED: Lakeflow sync for %s "
                "did not complete after %.1fs — state may be stuck in "
                "provisioning; check the Lakebase synced-table pipeline "
                "in the Databricks UI. Error: %s",
                self.task_id,
                synced_uc,
                time.time() - t_sync,
                exc,
            )
            self.tm.fail_task(self.task_id, f"Lakebase sync did not complete: {exc}")
            return False
        logger.info(
            "[DT-BUILD %s] step 5/7 done: Lakeflow pipeline reached %s "
            "in %.1fs (total elapsed from build start=%.1fs)",
            self.task_id,
            state,
            time.time() - t_sync,
            time.time() - t0,
        )
        return True

    def _on_sync_state(self, pipeline_state: str) -> None:
        label = _SYNC_STATE_LABELS.get(
            pipeline_state, pipeline_state.lower().replace("_", " ")
        )
        logger.info(
            "[DT-BUILD %s] Lakeflow pipeline state → %s", self.task_id, pipeline_state
        )
        self._progress(58, f"Lakebase sync — {label}")

    def _ensure_union_view(self, synced_uc: str, actual_synced_uc: str) -> bool:
        """Step 6: create / refresh the union view over ``_sync`` ∪ companion."""
        t_step = time.time()
        # A fallback registration name maps to its own Postgres table name.
        synced_phy: Optional[str] = None
        if actual_synced_uc != synced_uc:
            synced_phy = actual_synced_uc.split(".")[-1]
        try:
            self.store.ensure_synced_union_view(
                self.graph_name, synced_phy_override=synced_phy
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] step 6/7 FAILED: could not create union view "
                "for graph=%s synced_uc=%s actual_synced_uc=%s pg_schema=%s — "
                "the _sync table may not yet be visible in Postgres: %s",
                self.task_id,
                self.graph_name,
                synced_uc,
                actual_synced_uc,
                getattr(self.store, "graph_schema", "?"),
                exc,
            )
            self.tm.fail_task(
                self.task_id, f"Could not create Lakebase union view after sync: {exc}"
            )
            return False
        logger.info(
            "[DT-BUILD %s] step 6/7 done: union view created/refreshed in %.2fs",
            self.task_id,
            time.time() - t_step,
        )
        return True

    def _finalize(self) -> bool:
        """Step 7: truncate the companion, then rebuild the adjacency companions."""
        t_step = time.time()
        try:
            self.store.truncate_companion(self.graph_name)
            logger.info(
                "[DT-BUILD %s] step 7/7 done: companion truncated in %.2fs "
                "(full rebuild — reasoning starts clean)",
                self.task_id,
                time.time() - t_step,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[DT-BUILD %s] step 7/7 WARNING: could not truncate companion "
                "for %s (non-fatal — reasoning data may be stale): %s",
                self.task_id,
                self.graph_name,
                exc,
            )
        try:
            self.rebuild_adjacency_if_supported()
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "[DT-BUILD %s] step 7/7 FAILED: adjacency rebuild failed for %s: %s",
                self.task_id,
                self.graph_name,
                exc,
            )
            self.tm.fail_task(
                self.task_id, f"Could not rebuild Lakebase adjacency tables: {exc}"
            )
            return False
        return True

    def _report_post_sync_triple_count(self) -> None:
        """Count the synced triples for the progress message only.

        A lost warehouse connection must not turn a successful sync into a
        failed build, and must not publish ``0`` as if the graph were empty.
        """
        counted = False
        try:
            self.triple_count = self.count_source_triples()
            counted = True
        except InfrastructureError as exc:
            logger.warning(
                "[DT-BUILD %s] could not count the synced triples "
                "(the sync itself succeeded): %s",
                self.task_id,
                exc,
            )
        else:
            if self.triple_count == 0:
                logger.warning(
                    "[DT-BUILD %s] post-sync source %s contains 0 triples — "
                    "the Lakeflow pipeline may have synced an empty source or "
                    "the VIEW SQL produces no rows; check mappings and source data",
                    self.task_id,
                    self.source_relation,
                )
        self._progress(
            90,
            (
                f"Synced {self.triple_count} triples"
                if counted
                else "Sync succeeded (triple count unavailable)"
            ),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _advance(self) -> None:
        """Advance to the next named task step (no-op in API mode)."""
        if not self.is_api:
            self.tm.advance_step(self.task_id)

    def _raise_if_cancelled(self) -> None:
        """Raise :class:`OperationCancelledError` once the task is cancelled."""
        try:
            if self.cancel_check():
                raise OperationCancelledError("Build cancelled by user")
        except OperationCancelledError:
            raise
        except Exception:  # noqa: BLE001
            pass
