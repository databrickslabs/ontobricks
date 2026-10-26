"""Scheduled tasks and run history on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from ..base import (
    ScheduleHistoryEntry,
    parse_schedule_key,
    schedule_key,
)

from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class Schedules:
    """Scheduled tasks, history, and legacy cohort import."""

    def load_schedules(self) -> Dict[str, Dict[str, Any]]:
        try:
            self._ensure_schedule_task_columns()
            self._import_legacy_cohort_schedules()
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT task_type, domain_name, target_key, interval_minutes,
                           enabled, version, config, last_run, last_status,
                           last_message, last_count
                    FROM {self._q(self._schema)}.schedules
                    WHERE registry_id = %s
                    """,
                    (self._registry(),),
                )
                rows = cur.fetchall()
            out: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                task_type = r["task_type"] or "build"
                target_key = r["target_key"] or ""
                key = schedule_key(task_type, r["domain_name"], target_key)
                out[key] = {
                    "task_type": task_type,
                    "domain_name": r["domain_name"],
                    "target_key": target_key,
                    "interval_minutes": r["interval_minutes"],
                    "enabled": r["enabled"],
                    "version": r["version"] or "latest",
                    "config": dict(r["config"] or {}),
                    "last_run": r["last_run"].isoformat() if r["last_run"] else None,
                    "last_status": r["last_status"],
                    "last_message": r["last_message"],
                    "last_count": int(r["last_count"] or 0),
                }
            return out
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_schedules failed: %s", exc)
            return {}

    def save_schedules(
        self, schedules: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]:
        try:
            self._ensure_schedule_task_columns()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {self._q(self._schema)}.schedules
                    WHERE registry_id = %s
                    """,
                    (self._registry(),),
                )
                for key, cfg in schedules.items():
                    fallback_type, fallback_domain, fallback_target = (
                        parse_schedule_key(key)
                    )
                    config = dict(cfg.get("config") or {})
                    if "drop_existing" not in config and "drop_existing" in cfg:
                        # Pre-generic entries carried the build flag at the
                        # top level (the Volume → Lakebase migration script
                        # still writes that shape).
                        config["drop_existing"] = bool(cfg["drop_existing"])
                    cur.execute(
                        f"""
                        INSERT INTO {self._q(self._schema)}.schedules
                            (registry_id, task_type, domain_name, target_key,
                             interval_minutes, drop_existing, enabled, version,
                             config, last_run, last_status, last_message,
                             last_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                                %s, %s, %s, %s)
                        """,
                        (
                            self._registry(),
                            cfg.get("task_type") or fallback_type,
                            cfg.get("domain_name") or fallback_domain,
                            cfg.get("target_key") or fallback_target,
                            int(cfg.get("interval_minutes", 60)),
                            bool(config.get("drop_existing", True)),
                            bool(cfg.get("enabled", True)),
                            cfg.get("version", "latest") or "latest",
                            json.dumps(config),
                            cfg.get("last_run"),
                            cfg.get("last_status"),
                            cfg.get("last_message"),
                            int(cfg.get("last_count") or 0),
                        ),
                    )
            return True, "Schedules saved"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def load_schedule_history(self, key: str) -> List[ScheduleHistoryEntry]:
        task_type, domain_name, target_key = parse_schedule_key(key)
        try:
            self._ensure_schedule_task_columns()
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT run_ts, status, message, duration_s, triple_count,
                           detail
                    FROM {self._q(self._schema)}.schedule_runs
                    WHERE registry_id = %s AND task_type = %s
                      AND domain_name = %s AND target_key = %s
                    ORDER BY run_ts ASC
                    """,
                    (self._registry(), task_type, domain_name, target_key),
                )
                rows = cur.fetchall()
            return [
                {
                    "timestamp": r["run_ts"].isoformat(),
                    "status": r["status"],
                    "message": r["message"] or "",
                    "duration_s": float(r["duration_s"] or 0),
                    "triple_count": int(r["triple_count"] or 0),
                    "detail": dict(r["detail"] or {}),
                }
                for r in rows
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_schedule_history(%s) failed: %s", key, exc)
            return []

    def append_schedule_history(
        self, key: str, entry: ScheduleHistoryEntry, *, max_entries: int = 50
    ) -> None:
        task_type, domain_name, target_key = parse_schedule_key(key)
        try:
            self._ensure_schedule_task_columns()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.schedule_runs
                        (registry_id, task_type, domain_name, target_key,
                         run_ts, status, message, duration_s, triple_count,
                         detail)
                    VALUES (%s, %s, %s, %s, COALESCE(%s::timestamptz, now()),
                            %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        self._registry(),
                        task_type,
                        domain_name,
                        target_key,
                        entry.get("timestamp"),
                        entry.get("status", ""),
                        entry.get("message", ""),
                        float(entry.get("duration_s", 0) or 0),
                        int(entry.get("triple_count", 0) or 0),
                        json.dumps(dict(entry.get("detail") or {})),
                    ),
                )
                cur.execute(
                    f"""
                    DELETE FROM {self._q(self._schema)}.schedule_runs
                    WHERE registry_id = %s AND task_type = %s
                      AND domain_name = %s AND target_key = %s
                      AND id NOT IN (
                          SELECT id FROM {self._q(self._schema)}.schedule_runs
                          WHERE registry_id = %s AND task_type = %s
                            AND domain_name = %s AND target_key = %s
                          ORDER BY run_ts DESC
                          LIMIT %s
                      )
                    """,
                    (
                        self._registry(),
                        task_type,
                        domain_name,
                        target_key,
                        self._registry(),
                        task_type,
                        domain_name,
                        target_key,
                        max_entries,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("append_schedule_history(%s) failed: %s", key, exc)

    def _import_legacy_cohort_schedules(self) -> None:
        """Move cohort schedules out of the ``global_config`` JSONB blob.

        Cohort schedules predate the generic ``schedules`` table and were
        stashed in the blob under ``cohort_schedules`` /
        ``cohort_schedule_history``, keyed by ``"<domain>::<rule_id>"``.
        This one-shot import rewrites them as ``task_type='cohort'`` rows
        (with ``target_key`` holding the rule id) and then drops both
        blob keys, so an upgraded deployment keeps its schedules without
        the admin doing anything. Best-effort: a failure here leaves the
        blob intact and is retried on the next app start.
        """
        if self._cohort_schedules_imported:
            return
        try:
            # Read the raw blob: ``load_global_config`` strips every legacy
            # schedule key, which is exactly what we are here to harvest.
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
            cfg = dict((row or {}).get("config") or {})
            legacy = dict(cfg.get("cohort_schedules") or {})
            histories = dict(cfg.get("cohort_schedule_history") or {})
            if not legacy and not histories:
                self._cohort_schedules_imported = True
                return
            if not self._ensure_schedule_task_columns():
                return

            with self._connect() as conn, conn.cursor() as cur:
                for legacy_key, entry in legacy.items():
                    domain_name = entry.get("domain_name") or ""
                    rule_id = entry.get("rule_id") or ""
                    if not domain_name or not rule_id:
                        # Fall back to the "<domain>::<rule>" key shape.
                        parts = str(legacy_key).split("::", 1)
                        domain_name = domain_name or parts[0]
                        rule_id = rule_id or (parts[1] if len(parts) > 1 else "")
                    if not domain_name or not rule_id:
                        continue
                    cur.execute(
                        f"""
                        INSERT INTO {self._q(self._schema)}.schedules
                            (registry_id, task_type, domain_name, target_key,
                             interval_minutes, enabled, version, config,
                             last_run, last_status, last_message, last_count)
                        VALUES (%s, 'cohort', %s, %s, %s, %s, %s, %s::jsonb,
                                %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT schedules_type_domain_target_key
                        DO NOTHING
                        """,
                        (
                            self._registry(),
                            domain_name,
                            rule_id,
                            int(entry.get("interval_minutes", 60)),
                            bool(entry.get("enabled", True)),
                            entry.get("version", "latest") or "latest",
                            json.dumps(
                                {
                                    "output_graph": bool(
                                        entry.get("output_graph", True)
                                    ),
                                    "output_uc": bool(entry.get("output_uc", True)),
                                }
                            ),
                            entry.get("last_run"),
                            entry.get("last_status"),
                            entry.get("last_message"),
                            int(entry.get("last_count") or 0),
                        ),
                    )

                for legacy_key, entries in histories.items():
                    parts = str(legacy_key).split("::", 1)
                    domain_name = parts[0]
                    rule_id = parts[1] if len(parts) > 1 else ""
                    if not domain_name or not rule_id:
                        continue
                    for run in list(entries or []):
                        cur.execute(
                            f"""
                            INSERT INTO {self._q(self._schema)}.schedule_runs
                                (registry_id, task_type, domain_name,
                                 target_key, run_ts, status, message,
                                 duration_s, triple_count, detail)
                            VALUES (%s, 'cohort', %s, %s,
                                    COALESCE(%s::timestamptz, now()),
                                    %s, %s, %s, %s, %s::jsonb)
                            """,
                            (
                                self._registry(),
                                domain_name,
                                rule_id,
                                run.get("timestamp"),
                                run.get("status", ""),
                                run.get("message", ""),
                                float(run.get("duration_s", 0) or 0),
                                int(run.get("triple_count", 0) or 0),
                                json.dumps(
                                    {
                                        "materialized_triples": int(
                                            run.get("materialized_triples", 0) or 0
                                        ),
                                        "uc_rows_written": int(
                                            run.get("uc_rows_written", 0) or 0
                                        ),
                                    }
                                ),
                            ),
                        )

            self._scrub_global_config_legacy_keys()
            self._cohort_schedules_imported = True
            logger.info(
                "Imported %d legacy cohort schedule(s) from global_config "
                "into the schedules table",
                len(legacy),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not import legacy cohort schedules: %s", exc)

    def _ensure_schedule_task_columns(self) -> bool:
        """Lazily widen ``schedules`` / ``schedule_runs`` to generic tasks.

        The tables were originally build-only: one row per domain, keyed
        by ``UNIQUE (registry_id, domain_name)``. The scheduler now runs
        several task types per domain, so this adds ``task_type`` /
        ``target_key`` / ``config`` / ``last_count`` / ``detail`` and
        swaps the unique constraint for one that includes the type and
        the target.

        Existing rows default to ``task_type = 'build'``, so builds keep
        working untouched; their legacy ``drop_existing`` column is
        folded into ``config`` in the same pass. Same idempotent,
        ownership-aware pattern as
        :meth:`_ensure_domain_versions_status_column` — the constraint
        swap needs table ownership, so on failure this logs the
        bootstrap hint and returns ``False``.
        """
        if self._schedule_columns_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'schedules' "
                    "AND column_name = 'task_type'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._schedule_columns_ready = True
                    return True

                cur.execute(
                    f"""
                    ALTER TABLE {sch}.schedules
                        ADD COLUMN IF NOT EXISTS task_type text NOT NULL
                            DEFAULT 'build',
                        ADD COLUMN IF NOT EXISTS target_key text NOT NULL
                            DEFAULT '',
                        ADD COLUMN IF NOT EXISTS config jsonb NOT NULL
                            DEFAULT '{{}}'::jsonb,
                        ADD COLUMN IF NOT EXISTS last_count bigint NOT NULL
                            DEFAULT 0
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.schedule_runs
                        ADD COLUMN IF NOT EXISTS task_type text NOT NULL
                            DEFAULT 'build',
                        ADD COLUMN IF NOT EXISTS target_key text NOT NULL
                            DEFAULT '',
                        ADD COLUMN IF NOT EXISTS detail jsonb NOT NULL
                            DEFAULT '{{}}'::jsonb
                    """
                )
                # Fold the legacy build-only column into ``config`` so the
                # executor reads every option from one place.
                cur.execute(
                    f"""
                    UPDATE {sch}.schedules
                    SET config = jsonb_build_object(
                            'drop_existing', COALESCE(drop_existing, true))
                    WHERE config = '{{}}'::jsonb AND task_type = 'build'
                    """
                )
                # The old constraint allows a single row per domain, which
                # blocks a second task type. Drop it by name (Postgres
                # auto-names it) and by lookup, then add the wider one.
                cur.execute(
                    """
                    SELECT con.conname
                    FROM pg_constraint con
                    JOIN pg_class rel ON rel.oid = con.conrelid
                    JOIN pg_namespace ns ON ns.oid = rel.relnamespace
                    WHERE ns.nspname = %s AND rel.relname = 'schedules'
                      AND con.contype = 'u'
                      AND pg_get_constraintdef(con.oid)
                          = 'UNIQUE (registry_id, domain_name)'
                    """,
                    (self._schema,),
                )
                for row in cur.fetchall() or []:
                    cur.execute(
                        f"ALTER TABLE {sch}.schedules "
                        f"DROP CONSTRAINT IF EXISTS {self._q(row[0])}"
                    )
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.schedules
                        ADD CONSTRAINT schedules_type_domain_target_key
                        UNIQUE (registry_id, task_type, domain_name, target_key)
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_schedule_runs_domain
                        ON {sch}.schedule_runs(registry_id, task_type,
                                               domain_name, target_key,
                                               run_ts DESC)
                    """
                )
            self._schedule_columns_ready = True
            logger.info(
                "Migrated schedules/schedule_runs to the generic "
                "scheduled-task shape (task_type/target_key/config)"
            )
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not migrate the schedules tables to generic tasks — "
                "run `make bootstrap-lakebase` (or scripts/bootstrap-lakebase-perms.sh) "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False
