"""Realtime triple stream: a Kafka-compatible topic, slot diffs, the Lakebase ``_live`` table.

Spark Python task of ``resources/triple_rtm.job.yml``, started per ``realtime``
domain version by ``TwinRealtimeStream`` (``jobs.run_now`` with
``job_parameters``). One run streams one domain version:

1. read the topic (``kafka``, ``eventhubs`` through its Kafka endpoint, or
   ``kinesis``); each record is a Debezium-style JSON change envelope of one
   source row (``{"op": "c"|"u"|"d"|"r", "after", "before",
   "source": {"table": "catalog.schema.table"}, "ts_ms"}``, optionally under
   ``"payload"``), and its offset is the event ``seq`` — the topic must be
   keyed by the source row key so every change of one row stays in one
   partition, in order;
2. explode each envelope into one row per row-local mapping of its source table
   (``SlotRowProjector``): ``(map_id, source_key, op, seq, row_json)``;
3. ``groupBy("map_id", "source_key").transformWithState`` with
   :class:`jobs.triple_stream.processor.SlotDiffStatefulProcessor`, the state
   seeded from the domain's ``…_slots`` snapshot;
4. upsert every slot change into ``_live`` — a batch is COPYed into a temporary
   table and merged with ``INSERT … ON CONFLICT … DO UPDATE … WHERE
   EXCLUDED.seq > live.seq``, the ordering guard at the sink.

The trigger is Real-Time Mode (``--real-time-mode``, the default): the
``spark.databricks.streaming.realTimeMode.enabled`` conf and
``trigger(realTime=…)``. A runtime that refuses either falls back to a
``processingTime`` micro-batch trigger (logged). Real-Time Mode supports the
``foreach`` sink, not ``foreachBatch``, so the sink is a ``foreach`` writer that
buffers a partition's rows and upserts them on ``close``.

Everything but :meth:`TripleStreamJob.run` is pure Python and unit-tested; the
Spark, Kafka connector, ``transformWithState`` and Real-Time Mode wiring of
``run`` is live-unverified.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

logger = logging.getLogger("ontobricks.jobs.triple_stream.main")

#: ``src`` on the cluster (the bundle syncs the repo as workspace files).
_SRC = Path(__file__).resolve().parents[2]

SOURCE_KINDS = ("kafka", "eventhubs", "kinesis")
#: A table name: ``catalog.schema.table`` / ``schema.table``.
_QUALIFIED = re.compile(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){1,2}")
_SECRET_NAME = re.compile(r"[A-Za-z0-9_.\-]{1,128}")
_TOPIC = re.compile(r"[A-Za-z0-9_.\-]{1,249}")
#: Columns of a ``_live`` row, in COPY order.
LIVE_COLUMNS = (
    "map_id",
    "source_key",
    "predicate",
    "subject",
    "object",
    "object_hash",
    "deleted",
    "seq",
)
_STAGING = "ob_live_stage"
#: Debezium operation codes: create, update, delete, read (snapshot).
_DELETE_OPS = ("d",)
_UPSERT_OPS = ("c", "u", "r")


class TripleStreamJob:
    """The realtime stream job: arguments, envelope parsing, sink SQL, and the Spark wiring."""

    # ------------------------------------------------------------------
    # Arguments
    # ------------------------------------------------------------------

    @staticmethod
    def parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="OntoBricks realtime triple stream")
        add = parser.add_argument
        add("--domain", required=True)
        add("--version", required=True)
        add("--source-kind", required=True, choices=SOURCE_KINDS)
        add("--bootstrap-servers", required=True, help="host:port,… (Kinesis: the region)")
        add("--topic", required=True, help="topic / Event Hubs entity / Kinesis stream")
        add("--secret-scope", required=True)
        add("--secret-key", required=True)
        add("--slots-table", required=True, help="catalog.schema.table of the …_slots snapshot")
        add("--mappings-json-path", required=True, help="workspace file with the mappings")
        add("--lakebase-host", required=True)
        add("--lakebase-db", required=True)
        add("--lakebase-user", required=True)
        add("--lakebase-secret", required=True, help="scope/key of the Postgres password")
        add("--live-table", required=True, help="schema.table of the _live table")
        add("--checkpoint", required=True)
        add("--real-time-mode", action=argparse.BooleanOptionalAction, default=True)
        add("--real-time-interval", default="5 minutes")
        add("--processing-time", default="10 seconds")
        add("--state-ttl-hours", type=int, default=0, help="0: keep the state")
        return parser

    @staticmethod
    def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
        """Parse and validate *argv*; an invalid value exits like argparse (status 2)."""
        parser = TripleStreamJob.parser()
        args = parser.parse_args(argv)
        problem = TripleStreamJob.problem(args)
        if problem:
            parser.error(problem)
        return args

    @staticmethod
    def problem(args: argparse.Namespace) -> str:
        """The first invalid argument, ``""`` when they are all usable."""
        if not _QUALIFIED.fullmatch(args.slots_table or "") or args.slots_table.count(".") != 2:
            return f"--slots-table must be catalog.schema.table, got {args.slots_table!r}"
        if not _QUALIFIED.fullmatch(args.live_table or ""):
            return f"--live-table must be [schema.]table, got {args.live_table!r}"
        if not _TOPIC.fullmatch(args.topic or ""):
            return f"--topic is not a topic name: {args.topic!r}"
        for flag, value in (("--secret-scope", args.secret_scope), ("--secret-key", args.secret_key)):
            if not _SECRET_NAME.fullmatch(value or ""):
                return f"{flag} is not a secret name: {value!r}"
        scope, _, key = (args.lakebase_secret or "").partition("/")
        if not (_SECRET_NAME.fullmatch(scope) and _SECRET_NAME.fullmatch(key)):
            return f"--lakebase-secret must be scope/key, got {args.lakebase_secret!r}"
        if not (args.checkpoint or "").strip():
            return "--checkpoint must not be empty"
        if args.state_ttl_hours < 0:
            return "--state-ttl-hours must not be negative"
        return ""

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    @staticmethod
    def source_options(kind: str, servers: str, topic: str, secret: str) -> tuple[str, dict]:
        """``(format, options)`` of ``spark.readStream`` for the topic.

        *secret* is the value of the configured secret: the SASL JAAS config
        for ``kafka``, the namespace connection string for ``eventhubs`` (its
        Kafka endpoint, ``$ConnectionString`` login) and ``ACCESS_KEY:SECRET``
        for ``kinesis``.
        """
        if kind == "kinesis":
            access, _, secret_key = secret.partition(":")
            return "kinesis", {
                "streamName": topic,
                "region": servers,
                "initialPosition": "latest",
                "awsAccessKey": access,
                "awsSecretKey": secret_key,
            }
        jaas = secret
        if kind == "eventhubs":
            quoted = secret.replace('"', '\\"')
            jaas = (
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="$ConnectionString" password="{quoted}";'
            )
        return "kafka", {
            "kafka.bootstrap.servers": servers,
            "subscribe": topic,
            "startingOffsets": "latest",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": jaas,
            "failOnDataLoss": "false",
        }

    @staticmethod
    def record_columns(kind: str) -> tuple[str, str]:
        """``(value, seq)`` SQL expressions over the connector's columns."""
        if kind == "kinesis":
            # Kinesis sequence numbers do not fit a BIGINT: order by arrival time.
            return "CAST(data AS STRING)", "CAST(unix_millis(approximateArrivalTimestamp) AS BIGINT)"
        return "CAST(value AS STRING)", "offset"

    # ------------------------------------------------------------------
    # Envelopes
    # ------------------------------------------------------------------

    @staticmethod
    def parse_envelope(value: Any) -> dict | None:
        """``{"op", "row", "table"}`` of a change envelope; ``None`` when it carries no change.

        A delete reads its key from ``before``, any other operation from
        ``after``; a ``payload`` wrapper (Debezium with schemas) is unwrapped.
        Unparsable JSON, a Kafka tombstone (``null`` value), an unknown
        operation or a missing image is skipped.
        """
        if value is None:
            return None
        try:
            record = json.loads(value if isinstance(value, str) else bytes(value).decode("utf-8"))
        except (ValueError, TypeError, UnicodeDecodeError):
            return None
        if isinstance(record, Mapping) and isinstance(record.get("payload"), Mapping):
            record = record["payload"]
        if not isinstance(record, Mapping):
            return None
        op = str(record.get("op") or "").lower()
        source = record.get("source") if isinstance(record.get("source"), Mapping) else {}
        table = str(source.get("table") or "").strip().lower()
        if op in _DELETE_OPS:
            row, kind = record.get("before"), "delete"
        elif op in _UPSERT_OPS:
            row, kind = record.get("after"), "upsert"
        else:
            return None
        if not table or not isinstance(row, Mapping):
            return None
        return {"op": kind, "row": dict(row), "table": table}

    @staticmethod
    def explode(envelope: Mapping, seq: int, projectors_by_table: Mapping) -> list[dict]:
        """One adapter input row per mapping of the envelope's table (``ENVELOPE_FIELDS``).

        A row whose key is NULL for a mapping yields nothing for it, like the
        slot-event view's ``WHERE <key> IS NOT NULL``.
        """
        row_json = json.dumps(envelope["row"], sort_keys=True, default=str)
        rows = []
        for projector in projectors_by_table.get(envelope["table"], ()):
            key = projector.source_key(envelope["row"])
            if key is None:
                continue
            rows.append(
                {
                    "map_id": projector.map_id,
                    "source_key": key,
                    "op": envelope["op"],
                    "seq": int(seq),
                    "row_json": row_json,
                }
            )
        return rows

    @staticmethod
    def explode_records(records: Iterable[tuple[Any, Any]], projectors_by_table: Mapping) -> list[dict]:
        """:meth:`explode` every ``(value, seq)`` record that parses."""
        rows: list[dict] = []
        for value, seq in records:
            envelope = TripleStreamJob.parse_envelope(value)
            if envelope is not None and seq is not None:
                rows.extend(TripleStreamJob.explode(envelope, int(seq), projectors_by_table))
        return rows

    # ------------------------------------------------------------------
    # Mappings
    # ------------------------------------------------------------------

    @staticmethod
    def load_mappings(path: str) -> tuple[dict, list, str]:
        """``(entity mappings, relationship mappings, base URI)`` the orchestrator uploaded."""
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        return (
            dict(payload.get("entity_mappings") or {}),
            list(payload.get("relationship_mappings") or []),
            str(payload.get("base_uri") or ""),
        )

    @staticmethod
    def projectors_by_table(projectors: Iterable[Any]) -> dict[str, list]:
        """The projectors of each source table (lower-cased ``catalog.schema.table``)."""
        by_table: dict[str, list] = {}
        for projector in projectors:
            by_table.setdefault(str(projector.source_table).lower(), []).append(projector)
        return by_table

    # ------------------------------------------------------------------
    # Sink
    # ------------------------------------------------------------------

    @staticmethod
    def staging_sql() -> str:
        """The per-transaction temporary table a batch is COPYed into."""
        return (
            f"CREATE TEMP TABLE IF NOT EXISTS {_STAGING} (map_id TEXT, source_key TEXT, "
            "predicate TEXT, subject TEXT, object TEXT, object_hash TEXT, deleted BOOLEAN, "
            "seq BIGINT) ON COMMIT DELETE ROWS"
        )

    @staticmethod
    def copy_sql() -> str:
        return f"COPY {_STAGING} ({', '.join(LIVE_COLUMNS)}) FROM STDIN"

    @staticmethod
    def upsert_sql(live_table: str) -> str:
        """Merge the staged changes into *live_table*: the newest ``seq`` per slot wins.

        ``DISTINCT ON`` keeps one change per slot (a batch may carry several);
        ``WHERE EXCLUDED.seq > live.seq`` refuses an older change than the
        stored one, a tombstone included.
        """
        if not _QUALIFIED.fullmatch(live_table) and not re.fullmatch(r"[A-Za-z0-9_]+", live_table):
            raise ValueError(f"Not a table name: {live_table!r}")
        columns = ", ".join(LIVE_COLUMNS)
        updates = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in ("subject", "object", "object_hash", "deleted", "seq")
        )
        return (
            f"INSERT INTO {live_table} AS live ({columns}, updated_at) "
            f"SELECT DISTINCT ON (map_id, source_key, predicate) {columns}, now() "
            f"FROM {_STAGING} ORDER BY map_id, source_key, predicate, seq DESC "
            "ON CONFLICT (map_id, source_key, predicate) DO UPDATE SET "
            f"{updates}, updated_at = now() WHERE EXCLUDED.seq > live.seq"
        )

    @staticmethod
    def write_changes(connection: Any, live_table: str, changes: Iterable[Mapping]) -> int:
        """COPY *changes* into the staging table and upsert them; return how many were staged."""
        rows = [tuple(change.get(column) for column in LIVE_COLUMNS) for change in changes]
        if not rows:
            return 0
        with connection.cursor() as cur:
            cur.execute(TripleStreamJob.staging_sql())
            with cur.copy(TripleStreamJob.copy_sql()) as copy:
                for row in rows:
                    copy.write_row(row)
            cur.execute(TripleStreamJob.upsert_sql(live_table))
        connection.commit()
        return len(rows)

    # ------------------------------------------------------------------
    # Trigger and Spark wiring (live-unverified)
    # ------------------------------------------------------------------

    @staticmethod
    def with_trigger(spark: Any, writer: Any, args: argparse.Namespace) -> tuple[Any, str]:
        """*writer* with the Real-Time Mode trigger, or the micro-batch fallback; and which."""
        if args.real_time_mode:
            try:
                spark.conf.set("spark.databricks.streaming.realTimeMode.enabled", "true")
                return writer.trigger(realTime=args.real_time_interval), "real_time"
            except Exception as exc:  # noqa: BLE001 — any refusal falls back, logged
                logger.warning(
                    "Real-time mode unavailable (%s); falling back to a %s micro-batch trigger",
                    exc,
                    args.processing_time,
                )
        return writer.trigger(processingTime=args.processing_time), "micro_batch"

    @staticmethod
    def run(args: argparse.Namespace) -> None:  # pragma: no cover - cluster only
        """Build and start the streaming query; block until it stops."""
        if str(_SRC) not in sys.path:
            sys.path.insert(0, str(_SRC))
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        from back.core.streaming import SlotRowProjector
        from jobs.triple_stream.processor import OUTPUT_SCHEMA, SlotDiffStatefulProcessor

        spark = SparkSession.builder.getOrCreate()
        secrets = DBUtils(spark).secrets
        entities, relationships, base_uri = TripleStreamJob.load_mappings(args.mappings_json_path)
        projectors = SlotRowProjector.for_mappings(entities, relationships, base_uri=base_uri)
        by_table = TripleStreamJob.projectors_by_table(projectors)
        fmt, options = TripleStreamJob.source_options(
            args.source_kind,
            args.bootstrap_servers,
            args.topic,
            secrets.get(args.secret_scope, args.secret_key),
        )
        value_expr, seq_expr = TripleStreamJob.record_columns(args.source_kind)
        records = (
            spark.readStream.format(fmt)
            .options(**options)
            .load()
            .selectExpr(f"{value_expr} AS value", f"{seq_expr} AS seq")
        )
        envelope_schema = "map_id STRING, source_key STRING, op STRING, seq BIGINT, row_json STRING"

        def _explode(batches):
            import pandas as pd

            for batch in batches:
                rows = TripleStreamJob.explode_records(
                    zip(batch["value"], batch["seq"], strict=True), by_table
                )
                yield pd.DataFrame(rows, columns=["map_id", "source_key", "op", "seq", "row_json"])

        initial = spark.read.table(args.slots_table).groupBy("map_id", "source_key")
        ttl_ms = args.state_ttl_hours * 3_600_000 or None
        changes = (
            records.mapInPandas(_explode, schema=envelope_schema)
            .groupBy("map_id", "source_key")
            .transformWithState(
                SlotDiffStatefulProcessor(projectors, state_ttl_ms=ttl_ms),
                outputStructType=OUTPUT_SCHEMA,
                outputMode="append",
                timeMode="None",
                initialState=initial,
            )
        )
        password = secrets.get(*args.lakebase_secret.split("/", 1))
        sink = _LiveSinkWriter(
            lambda: TripleStreamJob.connect(args, password), args.live_table
        )
        writer = (
            changes.writeStream.foreach(sink)
            .option("checkpointLocation", args.checkpoint)
            .queryName(f"ontobricks-triple-rtm-{args.domain}-v{args.version}")
        )
        writer, trigger = TripleStreamJob.with_trigger(spark, writer, args)
        logger.info("Streaming %s into %s (%s trigger)", args.topic, args.live_table, trigger)
        writer.start().awaitTermination()

    @staticmethod
    def connect(args: argparse.Namespace, password: str) -> Any:  # pragma: no cover - cluster only
        import psycopg

        return psycopg.connect(
            host=args.lakebase_host,
            dbname=args.lakebase_db,
            user=args.lakebase_user,
            password=password,
            sslmode="require",
        )


class _LiveSinkWriter:
    """``foreach`` sink: buffers a partition's slot changes, upserts them on ``close``."""

    def __init__(self, connect: Callable[[], Any], live_table: str) -> None:
        self._connect = connect
        self._live_table = live_table
        self._rows: list[dict] = []

    def open(self, partition_id: int, epoch_id: int) -> bool:
        self._rows = []
        return True

    def process(self, row: Any) -> None:
        self._rows.append(row.asDict() if hasattr(row, "asDict") else dict(row))

    def close(self, error: BaseException | None) -> None:
        if error is not None or not self._rows:
            return
        connection = self._connect()
        try:
            TripleStreamJob.write_changes(connection, self._live_table, self._rows)
        finally:
            connection.close()


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - cluster entry point
    logging.basicConfig(level=logging.INFO)
    TripleStreamJob.run(TripleStreamJob.parse_args(argv))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
