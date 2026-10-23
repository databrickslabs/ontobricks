"""Run-scoped Delta snapshot of the R2RML gateway VIEW for bulk Data Quality."""

from __future__ import annotations

from typing import Any, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)


class DataQualitySnapshot:
    """CTAS the gateway VIEW once per DQ run, then drop the table.

    Yields ``table`` (FQN checks should scan) and ``parallel`` (True only
    when the snapshot was created). On CTAS failure the live VIEW is used
    and parallelism stays off. A failed drop is logged, never raised.
    """

    def __init__(
        self,
        domain: Any,
        settings: Any,
        view_fqn: str,
        task_id: str,
        *,
        enabled: bool = True,
    ) -> None:
        self._domain = domain
        self._settings = settings
        self._view_fqn = (view_fqn or "").strip()
        self._task_id = task_id
        self._enabled = enabled and bool(self._view_fqn)
        self.table = self._view_fqn
        self.parallel = False
        self._created = False
        self._client: Optional[Any] = None
        self._snapshot = ""

    def __enter__(self) -> "DataQualitySnapshot":
        if not self._enabled:
            return self
        try:
            self._try_materialize()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "DQ snapshot CTAS failed; checks will scan the live VIEW: %s",
                exc,
            )
            self.table = self._view_fqn
            self.parallel = False
            self._created = False
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._created and self._client is not None and self._snapshot:
            from back.core.graphdb.delta import materialize

            logger.info("Dropping DQ snapshot %s", self._snapshot)
            materialize.drop_relation(self._client, self._snapshot, kind="table")
        return False

    def _try_materialize(self) -> None:
        from back.core.graphdb.delta import _table_naming, materialize
        from back.core.graphdb.delta.DeltaBase import create_databricks_client

        snapshot = _table_naming.dq_snapshot_fqn(
            self._domain, self._settings, self._task_id
        )
        client = create_databricks_client(
            self._domain, self._settings, for_write=True
        )
        if not snapshot or client is None:
            logger.info(
                "DQ snapshot skipped (no table name or warehouse); using live VIEW"
            )
            return
        logger.info("Materializing DQ snapshot %s from %s", snapshot, self._view_fqn)
        materialize.materialize_from_view(client, self._view_fqn, snapshot)
        self._client = client
        self._snapshot = snapshot
        self._created = True
        self.table = snapshot
        self.parallel = True
