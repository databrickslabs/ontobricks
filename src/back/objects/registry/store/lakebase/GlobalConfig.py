"""Instance-wide JSONB global_config on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import json

from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _legacy_schedule_keys():
    from back.objects.registry.store.lakebase.store import _LEGACY_SCHEDULE_KEYS

    return _LEGACY_SCHEDULE_KEYS


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class GlobalConfig:
    """Warehouse and engine JSON blob; strips legacy schedule keys."""

    def load_global_config(self) -> Dict[str, Any]:
        try:
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT config FROM {self._q(self._schema)}.global_config
                    WHERE registry_id = %s
                    """,
                    (self._registry(),),
                )
                row = cur.fetchone()
            if not row:
                return {}
            data = dict(row["config"] or {})
            # Schedules live in their own table on Lakebase (``schedules``),
            # their history in ``schedule_runs``. Strip every legacy schedule
            # key so the JSONB blob is the single source of truth only for
            # instance-wide settings.
            for legacy in _legacy_schedule_keys():
                data.pop(legacy, None)
            return data
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_global_config failed: %s", exc)
            return {}

    def save_global_config(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            data = self.load_global_config()
            data["version"] = data.get("version", 1)
            sanitized_updates = {
                k: v
                for k, v in (updates or {}).items()
                if k not in _legacy_schedule_keys()
            }
            for legacy in _legacy_schedule_keys():
                data.pop(legacy, None)
            data.update(sanitized_updates)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.global_config
                        (registry_id, config)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (registry_id)
                    DO UPDATE SET config = EXCLUDED.config,
                                  updated_at = now()
                    """,
                    (self._registry(), json.dumps(data)),
                )
            return True, "Global configuration saved"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def _scrub_global_config_legacy_keys(self) -> None:
        """Remove the schedule keys from the global-config JSONB blob.

        All four (``schedules``, ``schedule_history``, ``cohort_schedules``,
        ``cohort_schedule_history``) belong to dedicated tables on Lakebase
        (``schedules`` and ``schedule_runs``). The build keys used to leak
        into ``global_config.config`` through the Volume → Lakebase
        migration path, which fed the entire Volume ``.global_config.json``
        blob — schedules included — into ``save_global_config``; the cohort
        keys lived there by design until cohort schedules moved into the
        generic ``schedules`` table. The duplicated state was harmless at
        read time (callers go through ``load_schedules``) but caused the
        JSONB blob to grow unbounded and confused operators inspecting the
        row directly. This one-shot ``UPDATE`` runs at every
        ``initialize()`` so existing deployments self-heal on next app
        start. The ``WHERE`` clause keeps the scrub a no-op once the blob
        is clean.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.global_config
                    SET config = (((config - 'schedules')
                                   - 'schedule_history')
                                   - 'cohort_schedules')
                                   - 'cohort_schedule_history',
                        updated_at = now()
                    WHERE config ? 'schedules'
                       OR config ? 'schedule_history'
                       OR config ? 'cohort_schedules'
                       OR config ? 'cohort_schedule_history'
                    """
                )
                scrubbed = cur.rowcount or 0
            if scrubbed:
                logger.info(
                    "Scrubbed legacy schedule keys from global_config "
                    "(%d row(s)) — Lakebase keeps schedules in the "
                    "dedicated 'schedules' table.",
                    scrubbed,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not scrub legacy keys from global_config: %s", exc
            )
