#!/usr/bin/env python3
"""Measure how fast a source change becomes visible in a domain's graph, and its drift.

The freshness KPIs of the incremental materialization design: ``microbatch``
serves a change through Lakehouse//RT within 30 s p95 (``…_graph``) and
through Lakebase within 60 s p95 (the synced table); ``realtime`` serves it
through ``_live`` within 1 s p99; reconciliation drift is 0.

The probe updates one row of a source table *N* times (``--changes``, default
5, ``--spacing`` seconds apart), each time to a new marker value of a column
the domain maps as a plain literal (text, or a number for a numeric column),
and polls every path — each on its own thread, timed from the write — until
the marker is the object of a triple there:

* ``data`` / ``graph``: ``…_data`` / ``…_graph`` on the warehouse
  (``--read-warehouse-id``: the Lakehouse//RT query warehouse);
* ``sync`` / ``live``: the Lakebase synced table / ``_live`` (``--pg-dsn``).

``--paths`` defaults to ``data,graph`` (plus ``live`` in ``realtime``, plus
``sync`` when ``--sync-table`` is set); a Lakebase path without its DSN and
table is a usage error. Per path it reports p50 / p95 / p99 (a change not seen
within ``--timeout`` counts as never seen); once the last change is seen it
reconciles ``…_data`` with the batch recompute (the gateway VIEW), the synced
table and ``_live`` (``SlotReconciler``), then restores the row's original
value and checks the SLOs of ``--mode``. The source row is updated on the
warehouse, or with ``--source-dsn`` on the Postgres database a ``realtime``
domain's Debezium topic captures. Only a fixture table is updated: its name
must match ``--allow-pattern`` (default ``*ob_it_*,*fixture*``), the key must
name an existing row, and the value column must be text or numeric (checked
before anything is written; a numeric column gets a negative marker near the
bottom of its range, :meth:`FreshnessProbe.marker_value`). ``--subject`` /
``--predicate`` restrict the match to the row's triple.

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
import fnmatch
import json
import math
import os
import re
import sys
import time
import uuid
import zlib
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PG_RELATION = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*")
_SQL_TYPE = re.compile(r"[a-z][a-z0-9_ ]*(\(\s*\d+\s*(,\s*\d+\s*)?\))?")
_PERCENTILES = (50, 95, 99)
_TEXT_TYPES = ("string", "varchar", "char", "text", "character")
_INT_TYPES = ("int", "integer", "bigint", "long", "int4", "int8")
_SMALL_INT_TYPES = ("smallint", "int2", "short")
_FLOAT_TYPES = ("double", "float", "real", "double precision", "float4", "float8")
_DECIMAL_TYPES = ("decimal", "numeric", "dec")


class _UsageError(ValueError):
    """A target that must not or cannot be measured (exit status 2)."""


class FreshnessProbe:
    """Times source changes until each read path shows them; percentiles and SLO verdicts.

    *write* sets the source row's value column to a marker and returns the
    object text the paths will show (``None``: the marker itself); *readers*
    maps a path name to ``object -> visible``. Each path is polled on its own
    thread, timed from the write. *clock* and *sleep* are injectable so the
    timing logic runs without a workspace.
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
        write: Callable[[str], str | None],
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
        """Write *marker*, then poll each path on its own thread until it shows it or times out."""
        shown = self._write(marker) or marker
        start = self._clock()
        if not self._readers:
            return {}
        with ThreadPoolExecutor(max_workers=len(self._readers)) as pool:
            waits = {
                path: pool.submit(self._wait, visible, shown, start)
                for path, visible in self._readers.items()
            }
        return {path: wait.result() for path, wait in waits.items()}

    def _wait(self, visible: Callable[[str], bool], shown: str, start: float) -> float | None:
        """Seconds from *start* until *visible*; ``None`` after the timeout."""
        while True:
            if visible(shown):
                return round(self._clock() - start, 3)
            if self._clock() - start >= self._timeout_s:
                return None
            self._sleep(self._poll_s)

    @staticmethod
    def marker_value(marker: str, sql_type: str) -> tuple[str, str]:
        """``(value written, object text shown)`` of *marker* for a column of *sql_type*.

        A text column takes the marker (``ob-freshness-<run>-<i>``, which no
        source holds). A numeric one takes a negative number near the bottom of
        its type's range, where real data does not sit, unique to the run and
        the change (a run offset plus the change's index), printed as
        ``CAST(<column> AS STRING)`` prints it: ``-2 000 000 000 - k`` for an
        integer (inside 32 bits), ``-32 000 - k`` for a smallint, ``-9 900 000
        - k`` for a double (below 10^7, so it prints without an exponent) and
        ``-(10^(p-s) - 1) + k`` for a ``decimal(p, s)`` (at least six integer
        digits). Any other type is a usage error.
        """
        run, index = marker.rsplit("-", 1)
        kind = sql_type.strip().lower()
        family = kind.split("(")[0].strip()
        if family.startswith(_TEXT_TYPES):
            return marker, marker
        run_offset, change = zlib.crc32(run.encode()), int(index)
        if family in _INT_TYPES:
            number = str(-2_000_000_000 - (run_offset % 900) * 100 - change)
            return number, number
        if family in _SMALL_INT_TYPES:
            number = str(-32_000 - (run_offset % 7) * 100 - change)
            return number, number
        if family in _FLOAT_TYPES:
            number = -9_900_000 - (run_offset % 900) * 100 - change
            return str(number), f"{number}.0"
        if family in _DECIMAL_TYPES:
            size = re.search(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", kind)
            precision, scale = (int(size.group(1)), int(size.group(2) or 0)) if size else (10, 0)
            if precision - scale < 6:
                raise _UsageError(f"{sql_type!r} holds too few integer digits for a marker (at least 6)")
            number = -(10 ** (precision - scale) - 1) + (run_offset % 9) * 100 + change
            return str(number), f"{number}.{'0' * scale}" if scale else str(number)
        raise _UsageError(f"Unsupported value column type {sql_type!r} (use a text or numeric column)")

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
    def markdown(
        mode: str,
        summary: Mapping[str, dict],
        pairs: Mapping[str, bool],
        failed: list,
        warnings: tuple | list = (),
    ) -> str:
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
        lines += [f"Warning: {warning}" for warning in warnings]
        return "\n".join(lines)


class _Warehouse:
    """The warehouse side: the source row, the ``…_data`` / ``…_graph`` checks, the reconciliation."""

    def __init__(self, client: Any, read_client: Any) -> None:
        self._client, self._read = client, read_client

    @staticmethod
    def literal(value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

    @staticmethod
    def sql_type(name: str) -> str:
        """*name* when it reads as a SQL type (it is inlined into ``CAST``), else ValueError."""
        kind = (name or "").strip().lower()
        if not _SQL_TYPE.fullmatch(kind):
            raise ValueError(f"Not a SQL type: {name!r}")
        return kind

    @staticmethod
    def update_sql(
        table: str, key_column: str, key_value: str, column: str, value: str | None, sql_type: str
    ) -> str:
        from back.core.helpers import validate_table_name

        validate_table_name(table)
        new = "NULL" if value is None else f"CAST({_Warehouse.literal(value)} AS {_Warehouse.sql_type(sql_type)})"
        return (
            f"UPDATE {table} SET {_Warehouse.ident(column)} = {new} "
            f"WHERE {_Warehouse.ident(key_column)} = {_Warehouse.literal(key_value)}"
        )

    @staticmethod
    def visible_sql(relation: str, marker: str, *, subject: str = "", predicate: str = "") -> str:
        """The marker as an object of *relation*; on the given *subject* / *predicate* only."""
        from back.core.helpers import validate_table_name

        validate_table_name(relation)
        where = [f"object = {_Warehouse.literal(marker)}"]
        where += [f"subject = {_Warehouse.literal(subject)}"] if subject else []
        where += [f"predicate = {_Warehouse.literal(predicate)}"] if predicate else []
        return f"SELECT 1 AS hit FROM {relation} WHERE {' AND '.join(where)} LIMIT 1"

    @staticmethod
    def ident(name: str) -> str:
        if not _IDENT.fullmatch(name or ""):
            raise ValueError(f"Not a column name: {name!r}")
        return name

    def current_value(
        self, table: str, key_column: str, key_value: str, column: str
    ) -> tuple[bool, str | None]:
        """``(row found, its value as text)``."""
        from back.core.helpers import validate_table_name

        validate_table_name(table)
        select = (
            f"SELECT CAST({self.ident(column)} AS STRING) AS v FROM {table} "
            f"WHERE {self.ident(key_column)} = {self.literal(key_value)} LIMIT 1"
        )
        rows = self._client.execute_query(select) or []
        return (True, rows[0].get("v")) if rows else (False, None)

    def column_type(self, table: str, column: str) -> str:
        """The UC type of *column* (``DESCRIBE TABLE``); ValueError when it has none."""
        from back.core.helpers import validate_table_name

        validate_table_name(table)
        for row in self._client.execute_query(f"DESCRIBE TABLE {table}") or []:
            name = str(row.get("col_name") or "").strip()
            if not name or name.startswith("#"):
                break
            if name.lower() == column.lower():
                return str(row.get("data_type") or "").strip().lower()
        raise ValueError(f"{table} has no column {column!r}")

    def update(
        self, table: str, key_column: str, key_value: str, column: str, value: str | None, sql_type: str
    ) -> None:
        sql = self.update_sql(table, key_column, key_value, column, value, sql_type)
        self._client.execute_statement(sql)

    def reader(self, relation: str, *, subject: str = "", predicate: str = "") -> Callable[[str], bool]:
        def visible(marker: str) -> bool:
            sql = self.visible_sql(relation, marker, subject=subject, predicate=predicate)
            return bool(self._read.execute_query(sql))

        return visible

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
    """A Postgres connection: a Lakebase ``_sync`` / ``_live`` path, or a Debezium-captured source."""

    def __init__(self, dsn: str) -> None:
        import psycopg

        self._conn = psycopg.connect(dsn, autocommit=True)

    @staticmethod
    def relation(name: str) -> Any:
        from psycopg import sql

        if not _PG_RELATION.fullmatch(name or ""):
            raise ValueError(f"Not a schema.table name: {name!r}")
        return sql.Identifier(*name.split("."))

    def _query(self, statement: Any, params: tuple = ()) -> tuple[list, int]:
        with self._conn.cursor() as cur:
            cur.execute(statement, params)
            return (cur.fetchall() if cur.description else []), cur.rowcount

    def reader(
        self, name: str, *, live: bool, subject: str = "", predicate: str = ""
    ) -> Callable[[str], bool]:
        from psycopg import sql

        conditions = [sql.SQL("object = %s")]
        conditions += [sql.SQL("subject = %s")] if subject else []
        conditions += [sql.SQL("predicate = %s")] if predicate else []
        conditions += [sql.SQL("NOT deleted")] if live else []
        statement = sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(
            self.relation(name), sql.SQL(" AND ").join(conditions)
        )
        extra = tuple(v for v in (subject, predicate) if v)
        return lambda marker: bool(self._query(statement, (marker, *extra))[0])

    def count(self, name: str, *, live: bool) -> int:
        from psycopg import sql

        served = sql.SQL(" AND NOT deleted") if live else sql.SQL("")
        statement = sql.SQL(
            "SELECT count(*) FROM {} WHERE coalesce(trim(object), '') <> ''{}"
        ).format(self.relation(name), served)
        return int(self._query(statement)[0][0][0])

    def current_value(
        self, table: str, key_column: str, key_value: str, column: str
    ) -> tuple[bool, str | None]:
        from psycopg import sql

        statement = sql.SQL("SELECT {}::text FROM {} WHERE {}::text = %s LIMIT 1").format(
            sql.Identifier(column), self.relation(table), sql.Identifier(key_column)
        )
        rows, _ = self._query(statement, (key_value,))
        return (True, rows[0][0]) if rows else (False, None)

    def column_type(self, table: str, column: str) -> str:
        schema, name = table.split(".", 1)
        rows, _ = self._query(
            "SELECT data_type, numeric_precision, numeric_scale FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (schema, name, column),
        )
        if not rows:
            raise ValueError(f"{table} has no column {column!r}")
        kind, precision, scale = rows[0]
        if kind == "numeric" and precision is not None:
            return f"numeric({precision},{scale or 0})"
        return str(kind)

    def update(
        self, table: str, key_column: str, key_value: str, column: str, value: str | None, sql_type: str
    ) -> None:
        from psycopg import sql

        statement = sql.SQL("UPDATE {} SET {} = CAST(%s AS {}) WHERE {}::text = %s").format(
            self.relation(table),
            sql.Identifier(column),
            sql.SQL(_Warehouse.sql_type(sql_type)),
            sql.Identifier(key_column),
        )
        _, updated = self._query(statement, (value, key_value))
        if updated != 1:
            raise _UsageError(f"{table}: {updated} rows have {key_column} = {key_value!r} (expected 1)")


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """The CLI; every option defaults to ``ONTOBRICKS_FRESHNESS_<OPTION>``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])

    def option(name: str, default: Any = "", **kw: Any) -> None:
        env = "ONTOBRICKS_FRESHNESS_" + name.upper().replace("-", "_")
        # An empty variable (an unset CI variable) reads as unset.
        parser.add_argument(f"--{name}", default=_env(env) or default, **kw)

    option("stem", help="the domain version's gateway VIEW, catalog.schema.triplestore_<dom>_V<n>")
    option("mode", "microbatch", choices=sorted(FreshnessProbe.SLOS))
    option("source-table")
    option("key-column")
    option("key-value")
    option("value-column", help="a text or numeric column the domain maps as a plain literal")
    option("subject", help="the row's subject URI: match the marker on it only (recommended)")
    option("predicate", help="the predicate URI the value column maps to: match on it only")
    option("allow-pattern", "*ob_it_*,*fixture*", help="fnmatch patterns a source table must match")
    option("changes", 5, type=int)
    option("spacing", 10.0, type=float)
    option("timeout", 120.0, type=float)
    option("poll", 0.5, type=float)
    option("paths", help="paths polled (default: data,graph [+live in realtime] [+sync with --sync-table])")
    option("read-warehouse-id", help="the query (Lakehouse//RT) warehouse; default: the build one")
    option("pg-dsn", help="Lakebase libpq DSN for the sync / live paths")
    option("sync-table", help="the synced table, schema.table")
    option("live-table", help="the realtime _live table, schema.table")
    option("source-dsn", help="update the source row on this Postgres instead of the warehouse")
    parser.add_argument(
        "--sea",
        action="store_true",
        help="Statement Execution API instead of the Thrift connector (networks where Thrift fails)",
    )
    parser.add_argument("--no-recompute", action="store_true", help="skip the gateway VIEW recompute")
    parser.add_argument("--no-check", action="store_true", help="report only; exit 0")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--summary-file", default=_env("ONTOBRICKS_FRESHNESS_REPORT"))
    return parser.parse_args(argv)


def paths_for(args: argparse.Namespace) -> list[str]:
    """The paths to poll; :class:`_UsageError` for an unknown one or a Lakebase one not configured."""
    if args.paths:
        paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    else:
        paths = ["data", "graph"] + (["live"] if args.mode == "realtime" else [])
        paths += ["sync"] if args.sync_table else []
    unknown = [p for p in paths if p not in FreshnessProbe.PATHS]
    if unknown:
        raise _UsageError(f"Unknown path(s) {', '.join(unknown)} (expected {', '.join(FreshnessProbe.PATHS)})")
    for path, table in (("sync", args.sync_table), ("live", args.live_table)):
        if path in paths and not (args.pg_dsn and table):
            raise _UsageError(f"The {path} path needs --pg-dsn and --{path}-table")
    return paths


def guard_fixture(args: argparse.Namespace) -> None:
    """Only a table matching ``--allow-pattern`` is updated (the probe rewrites one of its rows)."""
    patterns = [p.strip() for p in (args.allow_pattern or "").split(",") if p.strip()]
    table = (args.source_table or "").lower()
    if not any(fnmatch.fnmatch(table, pattern.lower()) for pattern in patterns):
        raise _UsageError(
            f"Refusing to update {args.source_table!r}: not a fixture table (--allow-pattern {args.allow_pattern!r})"
        )


def _warehouse_clients(read_warehouse_id: str, *, use_sea: bool = False) -> tuple[Any, Any]:
    """``(build client, read client)`` from a CLI profile or host + token + warehouse.

    *use_sea* (``--sea``) talks to the warehouses through the Statement Execution
    API instead of the Thrift connector — for networks where Thrift sessions
    fail (seen live: ``OpenSession`` → ``RequestError`` mid-run).
    """
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
    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id, use_sea=use_sea)
    if not read_warehouse_id or read_warehouse_id == warehouse_id:
        return client, client
    return client, DatabricksClient(
        host=host, token=token, warehouse_id=read_warehouse_id, use_sea=use_sea
    )


def _readers(args: argparse.Namespace, paths: list[str], warehouse: _Warehouse) -> tuple[dict, Any]:
    """``(readers, a Lakebase connection or None)``: one Postgres connection per Lakebase path,
    so the paths' polling threads never wait on each other."""
    readers: dict[str, Callable[[str], bool]] = {}
    lakebase = None
    for path in paths:
        triple = {"subject": args.subject, "predicate": args.predicate}
        if path in ("data", "graph"):
            readers[path] = warehouse.reader(f"{args.stem}_{path}", **triple)
            continue
        connection = _Postgres(args.pg_dsn)
        lakebase = lakebase or connection
        table = args.sync_table if path == "sync" else args.live_table
        readers[path] = connection.reader(table, live=path == "live", **triple)
    return readers, lakebase


def run(args: argparse.Namespace) -> dict:
    """Measure, reconcile and judge one domain; the report as a dict (``violations`` empty: met).

    The reconciliation runs once the last change was seen (or timed out) and
    before the row is restored, so ``…_data`` and the batch recompute both
    hold the last marker. :class:`_UsageError` for a target that cannot be
    measured (nothing written then).
    """
    guard_fixture(args)
    paths = paths_for(args)
    warnings = []
    if not {"data", "graph"} & set(paths):
        warnings.append(
            "no data / graph path: the reconciliation may run before …_data holds the last marker"
        )
    client, read_client = _warehouse_clients(args.read_warehouse_id, use_sea=args.sea)
    warehouse = _Warehouse(client, read_client)
    source = _Postgres(args.source_dsn) if args.source_dsn else warehouse
    readers, lakebase = _readers(args, paths, warehouse)
    row = (args.source_table, args.key_column, args.key_value, args.value_column)
    found, original = source.current_value(*row)
    if not found:
        raise _UsageError(f"{args.source_table} has no row with {args.key_column} = {args.key_value!r}")
    sql_type = source.column_type(args.source_table, args.value_column)
    FreshnessProbe.marker_value("ob-freshness-check-0", sql_type)  # an unsupported type writes nothing

    def write(marker: str) -> str:
        value, shown = FreshnessProbe.marker_value(marker, sql_type)
        source.update(*row, value, sql_type)
        return shown

    probe = FreshnessProbe(write, readers, timeout_s=args.timeout, poll_s=args.poll)
    try:
        latencies = probe.measure(args.changes, args.spacing)
        reconciliation = warehouse.reconcile(
            args.stem,
            recompute=not args.no_recompute,
            sync_rows=lakebase.count(args.sync_table, live=False) if "sync" in readers else None,
            live_rows=lakebase.count(args.live_table, live=True) if "live" in readers else None,
        )
    finally:
        source.update(*row, original, sql_type)
    summary = FreshnessProbe.summarize(latencies)
    pairs = reconciliation.get("pairs", {})
    failed = FreshnessProbe.violations(summary, args.mode, pairs)
    return {
        "mode": args.mode,
        "latencies": latencies,
        "summary": summary,
        "pairs": pairs,
        "violations": failed,
        "warnings": warnings,
        "markdown": FreshnessProbe.markdown(args.mode, summary, pairs, failed, warnings),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not (args.stem and args.source_table and args.key_column and args.value_column):
        print("measure_freshness: --stem, --source-table, --key-column and --value-column are required")
        return 2
    try:
        report = run(args)
    except _UsageError as exc:
        print(f"measure_freshness: {exc}")
        return 2
    for warning in report["warnings"]:
        print(f"measure_freshness: warning: {warning}", file=sys.stderr)
    print(json.dumps(report, indent=2, default=str) if args.json else report["markdown"])
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            handle.write(report["markdown"] + "\n")
    return 0 if args.no_check or not report["violations"] else 1


if __name__ == "__main__":
    sys.exit(main())
