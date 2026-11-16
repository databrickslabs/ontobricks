"""Community and Pattern detection run history on the Lakebase registry.

Two append-only tables, ``community_runs`` and ``pattern_runs``, one row per
Detect task that finished (completed or failed). They mirror
``graph_analytics_runs``: lazily created, keyed by ``(domain_id, version)``,
capped per tuple. Both kinds share one code path driven by ``_KINDS`` so the
two tables cannot drift apart.

Extracted from :class:`LakebaseRegistryStore` (mixin, same as ``Analytics``).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from ..base import CommunityRun, PatternRun

logger = get_logger("back.objects.registry.store.lakebase.store")

DETECT_RUN_KINDS = ("community", "pattern")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


def _text(value: Any) -> str:
    return str(value or "")


def _int(value: Any) -> int:
    return int(value or 0)


def _bool(value: Any) -> bool:
    return bool(value)


def _json_list(value: Any) -> str:
    return json.dumps(list(value or []))


# Per kind: table, lazy-create flag, index, and columns as
# (name, SQL DDL, python -> SQL coercion, SQL row -> python coercion).
_COMMON_COLUMNS: Tuple[Tuple[str, str, Callable, Callable], ...] = (
    ("status", "text NOT NULL DEFAULT 'completed'", _text, _text),
    ("duration_ms", "bigint NOT NULL DEFAULT 0", _int, _int),
    ("task_id", "text NOT NULL DEFAULT ''", _text, _text),
    ("error", "text NOT NULL DEFAULT ''", _text, _text),
    ("run_page_url", "text NOT NULL DEFAULT ''", _text, _text),
)

_KINDS: Dict[str, Dict[str, Any]] = {
    "community": {
        "table": "community_runs",
        "flag": "_community_runs_ready",
        "index": "idx_community_runs_domain_version",
        "columns": (
            (
                "class_filter",
                "jsonb NOT NULL DEFAULT '[]'::jsonb",
                _json_list,
                lambda v: list(v or []),
            ),
            ("node_count", "bigint NOT NULL DEFAULT 0", _int, _int),
            ("cluster_count", "integer NOT NULL DEFAULT 0", _int, _int),
            ("converged", "boolean NOT NULL DEFAULT true", _bool, _bool),
        )
        + _COMMON_COLUMNS,
    },
    "pattern": {
        "table": "pattern_runs",
        "flag": "_pattern_runs_ready",
        "index": "idx_pattern_runs_domain_version",
        "columns": (
            ("anchor_type", "text NOT NULL DEFAULT ''", _text, _text),
            ("hop_count", "integer NOT NULL DEFAULT 0", _int, _int),
            ("min_support", "integer NOT NULL DEFAULT 0", _int, _int),
            ("detail_level", "text NOT NULL DEFAULT ''", _text, _text),
            ("anchor_count", "bigint NOT NULL DEFAULT 0", _int, _int),
            ("pattern_count", "integer NOT NULL DEFAULT 0", _int, _int),
        )
        + _COMMON_COLUMNS,
    },
}


class DetectRuns:
    """Append-only run history for Community and Pattern detection.

    Expects the host store to define one ``_<kind>_runs_ready`` flag per kind
    (see ``_KINDS``), ``_DETECT_RUNS_CAP``, ``_schema``, ``_connect``, ``_q``
    and ``_registry``.
    """

    _DETECT_RUNS_CAP = 100

    def _ensure_detect_runs_table(self, kind: str) -> bool:
        """Lazily create the ``kind`` run table and index. Idempotent."""
        spec = _KINDS[kind]
        if getattr(self, spec["flag"]):
            return True
        table = spec["table"]
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (self._schema, table),
                )
                if not cur.fetchone():
                    column_ddl = "".join(
                        f"\n                        {name} {ddl},"
                        for name, ddl, _, _ in spec["columns"]
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {sch}.{table} (
                            id           bigserial PRIMARY KEY,
                            domain_id    uuid NOT NULL
                                         REFERENCES {sch}.domains(id)
                                         ON DELETE CASCADE,
                            version      text NOT NULL,{column_ddl}
                            computed_at  timestamptz NOT NULL DEFAULT now()
                        )
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS {spec['index']}
                            ON {sch}.{table}(domain_id, version, computed_at DESC)
                        """
                    )
            setattr(self, spec["flag"], True)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create %s table — run `make bootstrap-lakebase` as "
                "the schema owner to apply the migration: %s",
                table,
                exc,
            )
            return False

    def _record_detect_run(
        self, kind: str, folder: str, version: str, entry: Dict[str, Any]
    ) -> None:
        if not self._ensure_detect_runs_table(kind):
            return
        spec = _KINDS[kind]
        table = spec["table"]
        columns = spec["columns"]
        names = [c[0] for c in columns]
        values = [c[2](entry.get(c[0])) for c in columns]
        placeholders = ", ".join(
            "%s::jsonb" if name == "class_filter" else "%s" for name in names
        )
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.{table}
                        (domain_id, version, {", ".join(names)}, computed_at)
                    SELECT d.id, %s, {placeholders},
                           COALESCE(%s::timestamptz, now())
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        str(version),
                        *values,
                        entry.get("computed_at"),
                        self._registry(),
                        folder,
                    ),
                )
                # Keep the newest ``_DETECT_RUNS_CAP`` rows per (domain, version).
                cur.execute(
                    f"""
                    DELETE FROM {sch}.{table}
                    WHERE id IN (
                        SELECT r.id
                        FROM {sch}.{table} r
                        JOIN {sch}.domains d ON d.id = r.domain_id
                        WHERE d.registry_id = %s AND d.folder = %s
                          AND r.version = %s
                        ORDER BY r.computed_at DESC, r.id DESC
                        OFFSET %s
                    )
                    """,
                    (
                        self._registry(),
                        folder,
                        str(version),
                        int(self._DETECT_RUNS_CAP),
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record %s run (%s) failed: %s", kind, folder, exc)

    def record_community_run(
        self, folder: str, version: str, entry: CommunityRun
    ) -> None:
        self._record_detect_run("community", folder, version, dict(entry))

    def record_pattern_run(
        self, folder: str, version: str, entry: PatternRun
    ) -> None:
        self._record_detect_run("pattern", folder, version, dict(entry))

    def load_all_community_runs(
        self,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Tuple[List[CommunityRun], int]:
        return self._load_all_detect_runs(
            "community", folder=folder, limit=limit, offset=offset
        )

    def load_all_pattern_runs(
        self,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Tuple[List[PatternRun], int]:
        return self._load_all_detect_runs(
            "pattern", folder=folder, limit=limit, offset=offset
        )

    def _load_all_detect_runs(
        self,
        kind: str,
        *,
        folder: Optional[str],
        limit: int,
        offset: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        if not self._ensure_detect_runs_table(kind):
            return [], 0
        try:
            _psycopg, dict_row = _require_psycopg()
            spec = _KINDS[kind]
            sch = self._q(self._schema)
            names = [c[0] for c in spec["columns"]]
            where = "WHERE d.registry_id = %s"
            params: List[Any] = [self._registry()]
            if folder:
                where += " AND d.folder = %s"
                params.append(folder)
            source = f"""
                FROM {sch}.{spec["table"]} r
                JOIN {sch}.domains d ON d.id = r.domain_id
                {where}
            """
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(f"SELECT COUNT(*) AS total {source}", tuple(params))
                total_row = cur.fetchone()
                total = int(total_row["total"]) if total_row else 0
                cur.execute(
                    f"""
                    SELECT r.id, d.folder AS domain, r.version,
                           {", ".join("r." + n for n in names)},
                           r.computed_at
                    {source}
                    ORDER BY r.computed_at DESC, r.id DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params) + (int(limit), int(offset)),
                )
                rows = cur.fetchall()
            return [self._detect_run_row_to_entry(kind, r) for r in rows], total
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "load_all %s runs (folder=%s) failed: %s", kind, folder, exc
            )
            return [], 0

    @staticmethod
    def _detect_run_row_to_entry(kind: str, r: Dict[str, Any]) -> Dict[str, Any]:
        computed_at = r.get("computed_at")
        entry: Dict[str, Any] = {
            "id": _int(r.get("id")),
            "domain": r.get("domain") or "",
            "version": r.get("version") or "",
            "computed_at": computed_at.isoformat() if computed_at else "",
        }
        for name, _ddl, _to_sql, from_sql in _KINDS[kind]["columns"]:
            entry[name] = from_sql(r.get(name))
        return entry
