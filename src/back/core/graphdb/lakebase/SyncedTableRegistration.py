"""Compare an existing Lakebase synced-table registration with the one a build requests.

A synced table's definition cannot change in place ("Updating a synced table's
definition in place isn't supported through any interface"), so
:meth:`SyncedTableManager.ensure` deletes a registration whose source table or
scheduling policy differs from the request and creates it again.

The registration comes back from ``SyncedTableManager.get`` as an SDK
``SyncedDatabaseTable`` (``spec.scheduling_policy`` is an enum) or, on the REST
fallback, as a ``_DictNamespace`` (a plain string). A field the response does
not carry counts as unchanged: a spec that cannot be read never deletes
anything (and is logged at warning). The drift check covers every name
``ensure`` reuses, ghost-state fallbacks (``…_b`` / ``_c`` / ``_d``) included; a
fallback registration left from an earlier ghost period is not deleted once the
primary name works again.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)


class SyncedTableRegistration:
    """Read the registered source and schedule of a synced table; detect drift."""

    @staticmethod
    def source_table(synced: Any) -> str:
        """``spec.source_table_full_name`` of *synced*, ``""`` when unreadable."""
        value = getattr(getattr(synced, "spec", None), "source_table_full_name", None)
        return value if isinstance(value, str) else ""

    @staticmethod
    def scheduling_policy(synced: Any) -> str:
        """``spec.scheduling_policy`` upper-cased (``SNAPSHOT`` …), ``""`` when unreadable."""
        value = getattr(getattr(synced, "spec", None), "scheduling_policy", None)
        if isinstance(value, Enum):
            value = value.value
        return value.split(".")[-1].upper() if isinstance(value, str) else ""

    @staticmethod
    def drift(synced: Any, source_table_full_name: str, sync_mode: str) -> str:
        """Describe how *synced* differs from the request; ``""`` when it does not."""
        changes = []
        source = SyncedTableRegistration.source_table(synced)
        key = SyncedTableRegistration._relation_key
        if source and key(source) != key(source_table_full_name or ""):
            changes.append(f"source {source} -> {source_table_full_name}")
        policy = SyncedTableRegistration.scheduling_policy(synced)
        wanted = (sync_mode or "snapshot").upper()
        if policy and policy != wanted:
            changes.append(f"scheduling policy {policy} -> {wanted}")
        return ", ".join(changes)

    @staticmethod
    def discard_if_drifted(
        mgr: Any,
        name: str,
        synced: Any,
        source_table_full_name: str,
        sync_mode: str,
        before_delete: Optional[Callable[[str], None]] = None,
    ) -> Optional[Any]:
        """Return *synced*, or ``None`` once a drifted registration has been deleted.

        The delete purges the Postgres table (``purge_data=True``); a failure
        other than "not found" raises :class:`InfrastructureError` from
        ``mgr.delete`` rather than reuse a registration with the wrong source.
        *before_delete* (called with *name*) runs only on that path, right
        before the delete.
        """
        if synced is not None:
            SyncedTableRegistration._warn_if_unreadable(name, synced)
        drift = SyncedTableRegistration.drift(synced, source_table_full_name, sync_mode)
        if not drift:
            return synced
        logger.info("Synced table %s changed (%s) — deleting it to recreate it", name, drift)
        if before_delete is not None:
            before_delete(name)
        mgr.delete(name, purge_data=True)
        return None

    @staticmethod
    def _warn_if_unreadable(name: str, synced: Any) -> None:
        """Say which part of an existing registration's spec could not be read."""
        unreadable = [
            field
            for field, value in (
                ("source table", SyncedTableRegistration.source_table(synced)),
                ("scheduling policy", SyncedTableRegistration.scheduling_policy(synced)),
            )
            if not value
        ]
        if unreadable:
            logger.warning(
                "Synced table %s: registered %s unreadable; keeping the registration as is",
                name,
                " and ".join(unreadable),
            )

    @staticmethod
    def _relation_key(name: str) -> str:
        """Compare UC names without backticks and case (UC lists names lower-cased)."""
        return name.replace("`", "").strip().lower()
