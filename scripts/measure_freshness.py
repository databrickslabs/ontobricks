#!/usr/bin/env python3
"""Measure how fast a source change becomes visible in a domain's graph, and its drift.

The freshness KPIs of the incremental materialization design: ``microbatch``
serves a change through Lakehouse//RT within 30 s p95 (``…_graph``) and
through Lakebase within 60 s p95 (the synced table); ``realtime`` serves it
through ``_live`` within 1 s p99; reconciliation drift is 0.

The probe updates one row of a source table *N* times (``--changes``, default
5, ``--spacing`` seconds apart), each time to a new marker value of a column
the domain maps as a plain literal, and polls every configured path until the
marker is the object of a triple there:

* ``data`` / ``graph``: ``…_data`` / ``…_graph`` on the warehouse
  (``--read-warehouse-id``: the Lakehouse//RT query warehouse);
* ``sync`` / ``live``: the Lakebase synced table / ``_live`` (``--pg-dsn``).

Per path it reports p50 / p95 / p99 (a change not seen within ``--timeout``
counts as never seen), reconciles ``…_data`` with the batch recompute (the
gateway VIEW), the synced table and ``_live`` (``SlotReconciler``), restores
the row's original value and checks the SLOs of ``--mode``. The source row is
updated on the warehouse, or with ``--source-dsn`` on the Postgres database a
``realtime`` domain's Debezium topic captures.

Credentials, as ``tests/external/test_mv_incremental_plan.py``: a CLI profile
(``DATABRICKS_CONFIG_PROFILE`` + ``ONTOBRICKS_EXTERNAL_WAREHOUSE_ID``) or
``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` + ``DATABRICKS_SQL_WAREHOUSE_ID``.
Every option also reads an ``ONTOBRICKS_FRESHNESS_<OPTION>`` variable::

    PYTHONPATH=src uv run --frozen python scripts/measure_freshness.py \\
      --stem main.kg.triplestore_sales_V3 --mode microbatch \\
      --source-table main.sales.customers --key-column id --key-value 42 \\
      --value-column name [--pg-dsn … --sync-table kg.sales_v3_sync] [--json]

Exit status 0 when the SLOs hold (or with ``--no-check``), 1 when one fails.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PG_RELATION = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*")
_PERCENTILES = (50, 95, 99)


class FreshnessProbe:
    """Times source changes until each read path shows them; percentiles and SLO verdicts.

    *write* sets the source row's value column to a marker; *readers* maps a
    path name to ``marker -> visible``. *clock* and *sleep* are injectable so
    the timing logic runs without a workspace.
    """

    PATHS = ("data", "graph", "sync", "live")
    #: ``{mode: {path: (percentile, seconds)}}`` — the design KPIs.
    SLOS: dict[str, dict[str, tuple[int, float]]] = {
        "incremental": {},
        "microbatch": {"graph": (95, 30.0), "sync": (95, 60.0)},
        "realtime": {"live": (99, 1.0), "sync": (95, 60.0)},
    }
    #: Reconciliation pairs that tell drift whatever the mappings
    #: (``slots_data`` cannot when batch mappings feed ``…_data`` too).
    DRIFT_PAIRS = ("view_data", "sync_data", "live_slots")

    def __init__(
        self,
        write: Callable[[str], None],
        readers: Mapping[str, Callable[[str], bool]],
        *,
        timeout_s: float = 120.0,
        poll_s: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._write, self._readers = write, dict(readers)
        self._timeout_s, self._poll_s = timeout_s, poll_s
        self._clock, self._sleep = clock, sleep

    def measure(self, changes: int = 5, spacing_s: float = 10.0) -> dict[str, list[float | None]]:
        """Latency of each change on each path, in seconds; ``None``: not seen within the timeout."""
        run = uuid.uuid4().hex[:8]
        latencies: dict[str, list[float | None]] = {path: [] for path in self._readers}
        for index in range(changes):
            if index:
                self._sleep(spacing_s)
            for path, seconds in self._time_one(f"ob-freshness-{run}-{index}").items():
                latencies[path].append(seconds)
        return latencies

    def _time_one(self, marker: str) -> dict[str, float | None]:
        """Write *marker*, then poll every path until it shows it or the timeout passes."""
        self._write(marker)
        start = self._clock()
        seen: dict[str, float | None] = dict.fromkeys(self._readers)
        while True:
            for path, visible in self._readers.items():
                if seen[path] is None and visible(marker):
                    seen[path] = round(self._clock() - start, 3)
            if all(v is not None for v in seen.values()) or self._clock() - start >= self._timeout_s:
                return seen
            self._sleep(self._poll_s)

    @staticmethod
    def percentile(latencies: list[float | None], q: int) -> float | None:
        """Nearest-rank *q*-th percentile; a change never seen ranks last and gives ``None``."""
        if not latencies:
            return None
        ranked = sorted(latencies, key=lambda v: math.inf if v is None else v)
        return ranked[max(0, math.ceil(q / 100 * len(ranked)) - 1)]

    @staticmethod
    def summarize(latencies: Mapping[str, list[float | None]]) -> dict:
        """``{path: {changes, seen, p50, p95, p99}}``."""
        return {
            path: {
                "changes": len(values),
                "seen": sum(v is not None for v in values),
                **{f"p{q}": FreshnessProbe.percentile(values, q) for q in _PERCENTILES},
            }
            for path, values in latencies.items()
        }

    @staticmethod
    def violations(summary: Mapping[str, dict], mode: str, pairs: Mapping[str, bool]) -> list[str]:
        """The SLOs of *mode* a measured path misses, and every drifting pair."""
        failed = []
        for path, (q, limit) in FreshnessProbe.SLOS.get(mode, {}).items():
            if path not in summary:
                continue
            value = summary[path][f"p{q}"]
            if value is None or value > limit:
                shown = "not seen" if value is None else f"{value:.3f}s"
                failed.append(f"{path} p{q} {shown} > {limit:g}s")
        failed += [f"drift: {pair}" for pair in FreshnessProbe.DRIFT_PAIRS if pairs.get(pair)]
        return failed

    @staticmethod
    def markdown(mode: str, summary: Mapping[str, dict], pairs: Mapping[str, bool], failed: list) -> str:
        """The report as Markdown (a GitHub step summary)."""
        def cell(value: float | None) -> str:
            return "—" if value is None else f"{value:.3f}"

        lines = [
            f"**Freshness ({mode})**",
            "",
            "| path | seen | p50 (s) | p95 (s) | p99 (s) |",
            "|---|---|---|---|---|",
        ]
        for path, row in summary.items():
            p50, p95, p99 = (cell(row[f"p{q}"]) for q in _PERCENTILES)
            lines.append(f"| {path} | {row['seen']}/{row['changes']} | {p50} | {p95} | {p99} |")
        drift = ", ".join(f"{name}={'drift' if differs else 'ok'}" for name, differs in pairs.items())
        lines += ["", f"Reconciliation: {drift or 'not run'}", ""]
        lines.append("SLOs met" if not failed else "SLOs missed: " + "; ".join(failed))
        return "\n".join(lines)


class _Warehouse:
    """The warehouse side: the source row, the ``…_data`` / ``…_graph`` checks, the reconciliation."""

    def __init__(self, client: Any, read_client: Any) -> None:
        self._client, self._read = client, read_client

    @staticmethod
    def literal(value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

    @staticmethod
    def update_sql(table: str, key_column: str, key_value: str, column: str, value: str | None) -> str:
        from back.core.helpers import validate_table_name

        validate_table_name(table)
        _Warehouse.ident(key_column)
        _Warehouse.ident(column)
        new = "NULL" if value is None else _Warehouse.literal(value)
        return f"UPDATE {table} SET {column} = {new} WHERE {key_column} = {_Warehouse.literal(key_value)}"

    @staticmethod
    def visible_sql(relation: str, marker: str) -> str:
        from back.core.helpers import validate_table_name

        validate_table_name(relation)
        return f"SELECT 1 AS hit FROM {relation} WHERE object = {_Warehouse.literal(marker)} LIMIT 1"

    @staticmethod
    def ident(name: str) -> str:
        if not _IDENT.fullmatch(name or ""):
            raise ValueError(f"Not a column name: {name!r}")
        return name

    def current_value(self, table: str, key_column: str, key_value: str, column: str) -> str | None:
        from back.core.helpers import validate_table_name

        validate_table_name(table)
        select = (
            f"SELECT CAST({self.ident(column)} AS STRING) AS v FROM {table} "
            f"WHERE {self.ident(key_column)} = {self.literal(key_value)} LIMIT 1"
        )
        rows = self._client.execute_query(select) or []
        return rows[0].get("v") if rows else None

    def update(self, table: str, key_column: str, key_value: str, column: str, value: str | None) -> None:
        self._client.execute_statement(self.update_sql(table, key_column, key_value, column, value))

    def reader(self, relation: str) -> Callable[[str], bool]:
        return lambda marker: bool(self._read.execute_query(self.visible_sql(relation, marker)))

    def reconcile(self, stem: str, *, recompute: bool, sync_rows: int | None, live_rows: int | None) -> dict:
        from back.core.graphdb.delta import materialize
        from back.core.streaming import SlotReconciler

        slots = f"{stem}_slots"
        return SlotReconciler.compare(
            self._client,
            slots if materialize.relation_exists(self._client, slots) else None,
            f"{stem}_data",
            view_fqn=stem if recompute else None,
            sync_rows=sync_rows,
            live_rows=live_rows,
        )


class _Postgres:
    """A Postgres connection: Lakebase ``_sync`` / ``_live`` checks, or a Debezium-captured source."""

    def __init__(self, dsn: str) -> None:
        import psycopg

        self._conn = psycopg.connect(dsn, autocommit=True)

    @staticmethod
    def relation(name: str) -> Any:
        from psycopg import sql

        if not _PG_RELATION.fullmatch(name or ""):
            raise ValueError(f"Not a schema.table name: {name!r}")
        return sql.Identifier(*name.split("."))

    def _query(self, statement: Any, params: tuple = ()) -> list:
        with self._conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []

    def reader(self, name: str, *, live: bool) -> Callable[[str], bool]:
        from psycopg import sql

        served = sql.SQL(" AND NOT deleted") if live else sql.SQL("")
        statement = sql.SQL("SELECT 1 FROM {} WHERE object = %s{} LIMIT 1").format(
            self.relation(name), served
        )
        return lambda marker: bool(self._query(statement, (marker,)))

    def count(self, name: str, *, live: bool) -> int:
        from psycopg import sql

        served = sql.SQL(" AND NOT deleted") if live else sql.SQL("")
        statement = sql.SQL(
            "SELECT count(*) FROM {} WHERE coalesce(trim(object), '') <> ''{}"
        ).format(self.relation(name), served)
        return int(self._query(statement)[0][0])

    def current_value(self, table: str, key_column: str, key_value: str, column: str) -> str | None:
        from psycopg import sql

        statement = sql.SQL("SELECT {}::text FROM {} WHERE {}::text = %s LIMIT 1").format(
            sql.Identifier(column), self.relation(table), sql.Identifier(key_column)
        )
        rows = self._query(statement, (key_value,))
        return rows[0][0] if rows else None

    def update(self, table: str, key_column: str, key_value: str, column: str, value: str | None) -> None:
        from psycopg import sql

        statement = sql.SQL("UPDATE {} SET {} = %s WHERE {}::text = %s").format(
            self.relation(table), sql.Identifier(column), sql.Identifier(key_column)
        )
        self._query(statement, (value, key_value))


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """The CLI; every option defaults to ``ONTOBRICKS_FRESHNESS_<OPTION>``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])

    def option(name: str, default: Any = "", **kw: Any) -> None:
        env = "ONTOBRICKS_FRESHNESS_" + name.upper().replace("-", "_")
        parser.add_argument(f"--{name}", default=os.environ.get(env, default), **kw)

    option("stem", help="the domain version's gateway VIEW, catalog.schema.triplestore_<dom>_V<n>")
    option("mode", "microbatch", choices=sorted(FreshnessProbe.SLOS))
    option("source-table")
    option("key-column")
    option("key-value")
    option("value-column", help="a column the domain maps as a plain literal")
    option("changes", 5, type=int)
    option("spacing", 10.0, type=float)
    option("timeout", 120.0, type=float)
    option("poll", 0.5, type=float)
    option("read-warehouse-id", help="the query (Lakehouse//RT) warehouse; default: the build one")
    option("pg-dsn", help="Lakebase libpq DSN for the sync / live paths")
    option("sync-table", help="the synced table, schema.table")
    option("live-table", help="the realtime _live table, schema.table")
    option("source-dsn", help="update the source row on this Postgres instead of the warehouse")
    parser.add_argument(
        "--paths",
        default=_env("ONTOBRICKS_FRESHNESS_PATHS") or "data,graph",
        help="the warehouse paths polled (sync / live follow --sync-table / --live-table)",
    )
    parser.add_argument("--no-recompute", action="store_true", help="skip the gateway VIEW recompute")
    parser.add_argument("--no-check", action="store_true", help="report only; exit 0")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--summary-file", default=_env("ONTOBRICKS_FRESHNESS_REPORT"))
    return parser.parse_args(argv)


def _warehouse_clients(read_warehouse_id: str) -> tuple[Any, Any]:
    """``(build client, read client)`` from a CLI profile or host + token + warehouse."""
    from back.core.databricks import DatabricksClient

    profile = _env("DATABRICKS_CONFIG_PROFILE")
    if profile:
        from databricks.sdk.core import Config

        cfg = Config(profile=profile)
        host = cfg.host
        token = cfg.authenticate()["Authorization"].removeprefix("Bearer ")
        warehouse_id = _env("ONTOBRICKS_EXTERNAL_WAREHOUSE_ID")
    else:
        host, token = _env("DATABRICKS_HOST"), _env("DATABRICKS_TOKEN")
        warehouse_id = _env("DATABRICKS_SQL_WAREHOUSE_ID")
    if not (host and token and warehouse_id):
        raise SystemExit("measure_freshness: no Databricks credentials (see the module docstring)")
    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
    if not read_warehouse_id or read_warehouse_id == warehouse_id:
        return client, client
    return client, DatabricksClient(host=host, token=token, warehouse_id=read_warehouse_id)


def run(args: argparse.Namespace) -> dict:
    """Measure, reconcile and judge one domain; the report as a dict (``violations`` empty: met)."""
    client, read_client = _warehouse_clients(args.read_warehouse_id)
    warehouse = _Warehouse(client, read_client)
    lakebase = _Postgres(args.pg_dsn) if args.pg_dsn else None
    source = _Postgres(args.source_dsn) if args.source_dsn else warehouse
    paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    readers: dict[str, Callable[[str], bool]] = {}
    for path in paths:
        if path in ("data", "graph"):
            readers[path] = warehouse.reader(f"{args.stem}_{path}")
    if lakebase is not None and args.sync_table:
        readers["sync"] = lakebase.reader(args.sync_table, live=False)
    if lakebase is not None and args.live_table:
        readers["live"] = lakebase.reader(args.live_table, live=True)
    row = (args.source_table, args.key_column, args.key_value, args.value_column)
    original = source.current_value(*row)
    probe = FreshnessProbe(
        lambda marker: source.update(*row, marker), readers, timeout_s=args.timeout, poll_s=args.poll
    )
    try:
        latencies = probe.measure(args.changes, args.spacing)
    finally:
        source.update(*row, original)
    reconciliation = warehouse.reconcile(
        args.stem,
        recompute=not args.no_recompute,
        sync_rows=lakebase.count(args.sync_table, live=False) if lakebase and args.sync_table else None,
        live_rows=lakebase.count(args.live_table, live=True) if lakebase and args.live_table else None,
    )
    summary = FreshnessProbe.summarize(latencies)
    pairs = reconciliation.get("pairs", {})
    failed = FreshnessProbe.violations(summary, args.mode, pairs)
    return {
        "mode": args.mode,
        "latencies": latencies,
        "summary": summary,
        "pairs": pairs,
        "violations": failed,
        "markdown": FreshnessProbe.markdown(args.mode, summary, pairs, failed),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not (args.stem and args.source_table and args.key_column and args.value_column):
        print("measure_freshness: --stem, --source-table, --key-column and --value-column are required")
        return 2
    report = run(args)
    print(json.dumps(report, indent=2, default=str) if args.json else report["markdown"])
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            handle.write(report["markdown"] + "\n")
    return 0 if args.no_check or not report["violations"] else 1


if __name__ == "__main__":
    sys.exit(main())
