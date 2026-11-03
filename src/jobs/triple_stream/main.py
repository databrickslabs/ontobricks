"""Realtime triple stream: a Kafka-compatible topic, slot diffs, the Lakebase ``_live`` table.

Spark Python task of ``resources/triple_rtm.job.yml``, started per ``realtime``
domain version by ``TwinRealtimeStream`` (``jobs.run_now`` with
``job_parameters``). One run streams one domain version:

1. read the topic (``kafka`` or ``eventhubs`` through its Kafka endpoint —
   the supported sources — or the **experimental** ``kinesis``); each record
   is a Debezium-style JSON change envelope of one source row and its offset
   is the event ``seq`` (the topic must be keyed by the source row key, so
   every change of one row stays in one partition, in order);
2. explode each envelope into one row per row-local mapping of its source table
   (:class:`TripleStreamEnvelope`), every value as Spark text;
3. ``groupBy("map_id", "source_key").transformWithState`` with
   :class:`jobs.triple_stream.processor.SlotDiffStatefulProcessor` in
   ``Update`` mode, the state seeded from the domain's ``…_slots`` snapshot at
   the Delta version ``_live`` was seeded at (``--slots-version``), the topic
   replayed from just before that snapshot (``--start-timestamp-ms``) so no
   change between the seed and the stream's start is lost;
4. upsert every slot change into ``_live`` as it arrives (:class:`LiveSink`, a
   ``foreach`` writer guarded by ``WHERE EXCLUDED.seq > live.seq``).

The trigger is Real-Time Mode (``--real-time-mode``, the default): the
``spark.databricks.streaming.realTimeMode.enabled`` conf and
``trigger(realTime=…)``. A runtime that refuses either — when setting the
trigger or starting the query — runs the same query on a ``processingTime``
micro-batch trigger instead (logged).

Everything but :meth:`TripleStreamJob.run` is pure Python and unit-tested; the
Spark, connector, ``transformWithState`` and Real-Time Mode wiring of ``run`` is
live-unverified.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):  # run as a script (spark_python_task): make ``src`` importable
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from back.core.logging import get_logger  # noqa: E402
from jobs.triple_stream.LiveSink import LiveSink  # noqa: E402
from jobs.triple_stream.TripleStreamEnvelope import TripleStreamEnvelope  # noqa: E402

logger = get_logger(__name__)

SOURCE_KINDS = ("kafka", "eventhubs", "kinesis")
#: A table name: ``catalog.schema.table`` / ``schema.table``.
_QUALIFIED = re.compile(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){1,2}")
_SECRET_NAME = re.compile(r"[A-Za-z0-9_.\-]{1,128}")
_TOPIC = re.compile(r"[A-Za-z0-9_.\-]{1,249}")
#: The state TTL when the domain sets none: longer than any expected lateness.
DEFAULT_STATE_TTL_HOURS = 72
_REAL_TIME_CONF = "spark.databricks.streaming.realTimeMode.enabled"
#: The streaming records (``value STRING, seq BIGINT``) the envelope explode SQL reads.
RECORDS_VIEW = "ontobricks_triple_stream_records"


class TripleStreamJob:
    """The realtime stream job: arguments, the source, the trigger and the Spark wiring."""

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
        add(
            "--slots-version",
            type=int,
            default=-1,
            help="Delta version of the …_slots snapshot _live was seeded at (-1: the newest)",
        )
        add(
            "--start-timestamp-ms",
            type=int,
            default=0,
            help="replay the topic from this epoch millisecond (0: from the newest offsets)",
        )
        add("--real-time-mode", action=argparse.BooleanOptionalAction, default=True)
        add("--real-time-interval", default="5 minutes")
        add("--processing-time", default="10 seconds")
        add(
            "--shuffle-partitions",
            type=int,
            default=None,
            help="override spark.sql.shuffle.partitions for this run (the job's cluster sets it from "
            "var.realtime_shuffle_partitions): Real-Time Mode schedules every stage at once and needs "
            "source partitions + shuffle partitions free task slots; Spark's default 200 needs a 200-slot cluster",
        )
        add(
            "--state-ttl-hours",
            type=int,
            default=DEFAULT_STATE_TTL_HOURS,
            help="state eviction (0: keep the state); must exceed the event lateness",
        )
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
        if args.slots_version < -1:
            return "--slots-version must be a Delta version or -1"
        if args.start_timestamp_ms < 0:
            return "--start-timestamp-ms must not be negative"
        return ""

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    @staticmethod
    def source_options(
        kind: str, servers: str, topic: str, secret: str, start_timestamp_ms: int = 0
    ) -> tuple[str, dict]:
        """``(format, options)`` of ``spark.readStream`` for the topic.

        *secret* is the value of the configured secret: the SASL JAAS config
        for ``kafka``, the namespace connection string for ``eventhubs`` (its
        Kafka endpoint, ``$ConnectionString`` login) and ``ACCESS_KEY:SECRET``
        for ``kinesis``. A *start_timestamp_ms* replays the topic from that
        time on a query's first start (a checkpoint wins afterwards): Kafka and
        the Event Hubs Kafka endpoint ``startingTimestamp`` (every partition,
        the newest offset for a partition with nothing newer), Kinesis
        ``initialPosition`` ``at_timestamp``; without one the stream starts at
        the newest records.
        """
        if kind == "kinesis":
            access, _, secret_key = secret.partition(":")
            return "kinesis", {
                "streamName": topic,
                "region": servers,
                "initialPosition": TripleStreamJob.kinesis_position(start_timestamp_ms),
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
        start = (
            {"startingTimestamp": str(start_timestamp_ms), "startingOffsetsByTimestampStrategy": "latest"}
            if start_timestamp_ms > 0
            else {"startingOffsets": "latest"}
        )
        return "kafka", {
            "kafka.bootstrap.servers": servers,
            "subscribe": topic,
            **start,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": jaas,
            "failOnDataLoss": "false",
        }

    @staticmethod
    def kinesis_position(start_timestamp_ms: int) -> str:
        """Kinesis ``initialPosition``: ``at_timestamp`` (UTC) for a replay, else ``latest``."""
        if start_timestamp_ms <= 0:
            return "latest"
        moment = datetime.fromtimestamp(start_timestamp_ms / 1000, tz=timezone.utc)
        return json.dumps(
            {"at_timestamp": moment.strftime("%m/%d/%Y %H:%M:%S +0000"), "format": "MM/dd/yyyy HH:mm:ss ZZZ"}
        )

    @staticmethod
    def initial_state_sql(slots_table: str, slots_version: int) -> str:
        """The ``transformWithState`` initial state: the slot rows, at *slots_version* when pinned."""
        pinned = f" VERSION AS OF {int(slots_version)}" if slots_version >= 0 else ""
        return f"SELECT * FROM {slots_table}{pinned}"

    @staticmethod
    def record_columns(kind: str) -> tuple[str, str]:
        """``(value, seq)`` SQL expressions over the connector's columns.

        Kafka (and Event Hubs' Kafka endpoint): the partition offset, exact per
        key on a topic keyed by the source row key. Kinesis sequence numbers
        are decimal strings longer than a BIGINT: the seq is the arrival
        millisecond times 10^6 plus the sequence number's last six digits, so
        two records of one shard in the same millisecond keep their order.
        """
        if kind == "kinesis":
            return (
                "CAST(data AS STRING)",
                "unix_millis(approximateArrivalTimestamp) * 1000000 "
                "+ CAST(right(sequenceNumber, 6) AS BIGINT)",
            )
        return "CAST(value AS STRING)", "offset"

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

    # ------------------------------------------------------------------
    # Trigger and Spark wiring (live-unverified)
    # ------------------------------------------------------------------

    @staticmethod
    def time_mode(state_ttl_hours: int) -> str:
        """``transformWithState`` time mode: TTL needs ``ProcessingTime``, none keeps ``None``."""
        return "ProcessingTime" if state_ttl_hours > 0 else "None"

    @staticmethod
    def column_types(spark: Any, projectors_by_table: Mapping) -> dict:
        """``{table: {column: spark type}}`` of every source table, from Unity Catalog."""
        return {
            table: {
                field.name.lower(): field.dataType.simpleString()
                for field in spark.table(table).schema.fields
            }
            for table in projectors_by_table
        }

    @staticmethod
    def start(spark: Any, writer: Any, args: argparse.Namespace) -> tuple[Any, str]:
        """Start *writer* on the Real-Time Mode trigger, else on micro-batches; and which.

        A refusal at any step — the conf, the trigger, or ``start()`` itself
        (where an unsupported output mode or sink surfaces) — falls back to a
        ``processingTime`` trigger with the conf turned off again (logged).
        """
        if args.real_time_mode:
            try:
                spark.conf.set(_REAL_TIME_CONF, "true")
                return writer.trigger(realTime=args.real_time_interval).start(), "real_time"
            except Exception as exc:  # noqa: BLE001 — any refusal falls back, logged
                logger.warning(
                    "Real-time mode unavailable (%s); falling back to a %s micro-batch trigger",
                    exc,
                    args.processing_time,
                )
                try:
                    spark.conf.set(_REAL_TIME_CONF, "false")
                except Exception as conf_exc:  # noqa: BLE001
                    logger.debug("Could not turn %s off: %s", _REAL_TIME_CONF, conf_exc)
        return writer.trigger(processingTime=args.processing_time).start(), "micro_batch"

    @staticmethod
    def run(args: argparse.Namespace) -> None:  # pragma: no cover - cluster only
        """Build and start the streaming query; block until it stops."""
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        from back.core.streaming.SlotRowProjector import SlotRowProjector
        from jobs.triple_stream.processor import OUTPUT_SCHEMA, SlotDiffStatefulProcessor

        if args.source_kind == "kinesis":
            logger.warning("The kinesis source is experimental: ordering within ~1 ms is arbitrary")
        spark = SparkSession.builder.getOrCreate()
        secrets = DBUtils(spark).secrets
        entities, relationships, base_uri = TripleStreamJob.load_mappings(args.mappings_json_path)
        projectors = SlotRowProjector.for_mappings(entities, relationships, base_uri=base_uri)
        by_table = TripleStreamEnvelope.projectors_by_table(projectors)
        column_types = TripleStreamJob.column_types(spark, by_table)
        fmt, options = TripleStreamJob.source_options(
            args.source_kind,
            args.bootstrap_servers,
            args.topic,
            secrets.get(args.secret_scope, args.secret_key),
            args.start_timestamp_ms,
        )
        value_expr, seq_expr = TripleStreamJob.record_columns(args.source_kind)
        records = (
            spark.readStream.format(fmt)
            .options(**options)
            .load()
            .selectExpr(f"{value_expr} AS value", f"{seq_expr} AS seq")
        )
        # Real-Time Mode runs every stage concurrently: source + shuffle partitions must fit the slots.
        if args.shuffle_partitions:
            spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        # The envelope explode is Spark SQL: Real-Time Mode refuses mapInPandas (measured live).
        records.createOrReplaceTempView(RECORDS_VIEW)
        changes = (
            spark.sql(TripleStreamEnvelope.explode_sql(by_table, column_types, RECORDS_VIEW))
            .groupBy("map_id", "source_key")
            .transformWithState(
                SlotDiffStatefulProcessor(
                    projectors, state_ttl_ms=args.state_ttl_hours * 3_600_000 or None
                ),
                outputStructType=OUTPUT_SCHEMA,
                outputMode="Update",
                timeMode=TripleStreamJob.time_mode(args.state_ttl_hours),
                initialState=spark.sql(
                    TripleStreamJob.initial_state_sql(args.slots_table, args.slots_version)
                ).groupBy("map_id", "source_key"),
            )
        )
        password = secrets.get(*args.lakebase_secret.split("/", 1))
        writer = (
            changes.writeStream.outputMode("update")
            .foreach(LiveSink(lambda: TripleStreamJob.connect(args, password), args.live_table))
            .option("checkpointLocation", args.checkpoint)
            .queryName(f"ontobricks-triple-rtm-{args.domain}-v{args.version}")
        )
        query, trigger = TripleStreamJob.start(spark, writer, args)
        logger.info("Streaming %s into %s (%s trigger)", args.topic, args.live_table, trigger)
        query.awaitTermination()

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


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - cluster entry point
    logging.basicConfig(level=logging.INFO)
    TripleStreamJob.run(TripleStreamJob.parse_args(argv))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
