"""``jobs.triple_stream.main``: arguments, envelopes, explode, the sink SQL and the trigger."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from back.core.streaming import SlotRowProjector
from jobs.triple_stream.main import LIVE_COLUMNS, TripleStreamJob, _LiveSinkWriter

pytestmark = pytest.mark.unit

_TABLE = "main.sales.customers"
_MAPPING = {
    "table": _TABLE,
    "sql_query": None,
    "uri_template": "http://ex.org/Customer/{id}",
    "id_column": "id",
    "label_column": "name",
    "class_uri": "http://ex.org/Customer",
    "predicates": {"http://www.w3.org/2000/01/rdf-schema#label": {"type": "column", "column": "name"}},
}

_ARGS = [
    "--domain", "sales", "--version", "3", "--source-kind", "kafka",
    "--bootstrap-servers", "b1:9092,b2:9092", "--topic", "sales.customers",
    "--secret-scope", "ontobricks", "--secret-key", "kafka-jaas",
    "--slots-table", "main.kg.triplestore_sales_V3_slots",
    "--mappings-json-path", "/Workspace/Users/app/.ontobricks/realtime/sales-v3.json",
    "--lakebase-host", "pg.lakebase.io", "--lakebase-db", "graphs",
    "--lakebase-user", "ontobricks_rtm", "--lakebase-secret", "ontobricks/pg-password",
    "--live-table", "ontobricks_graph.sales_v3_live",
    "--checkpoint", "/Volumes/main/ontobricks/registry/realtime/sales-v3",
]


def _projectors() -> dict:
    return TripleStreamJob.projectors_by_table(
        SlotRowProjector.for_mappings({"C": _MAPPING}, [], base_uri="http://ex.org/")
    )


class TestArguments:
    def test_the_orchestrator_arguments_parse(self):
        args = TripleStreamJob.parse_args(_ARGS)

        assert args.source_kind == "kafka" and args.real_time_mode is True
        assert args.live_table == "ontobricks_graph.sales_v3_live"

    def test_real_time_mode_can_be_turned_off(self):
        assert TripleStreamJob.parse_args([*_ARGS, "--no-real-time-mode"]).real_time_mode is False

    @pytest.mark.parametrize(
        ("flag", "value"),
        [
            ("--slots-table", "kg.slots"),
            ("--live-table", "x; drop table y"),
            ("--topic", "bad topic"),
            ("--secret-key", "a/b"),
            ("--lakebase-secret", "no-slash"),
            ("--checkpoint", " "),
            ("--state-ttl-hours", "-1"),
        ],
    )
    def test_an_unusable_argument_is_refused(self, flag, value):
        argv = list(_ARGS)
        if flag in argv:
            argv[argv.index(flag) + 1] = value
        else:
            argv += [flag, value]

        with pytest.raises(SystemExit) as exc:
            TripleStreamJob.parse_args(argv)
        assert exc.value.code == 2

    def test_an_unknown_source_kind_is_refused(self):
        argv = list(_ARGS)
        argv[argv.index("--source-kind") + 1] = "rabbitmq"

        with pytest.raises(SystemExit):
            TripleStreamJob.parse_args(argv)


class TestSourceOptions:
    def test_kafka_takes_the_jaas_config_from_the_secret(self):
        fmt, options = TripleStreamJob.source_options("kafka", "b:9092", "t", "JAAS")

        assert fmt == "kafka"
        assert options["kafka.bootstrap.servers"] == "b:9092" and options["subscribe"] == "t"
        assert options["kafka.sasl.jaas.config"] == "JAAS"

    def test_event_hubs_logs_in_with_the_connection_string(self):
        _fmt, options = TripleStreamJob.source_options(
            "eventhubs", "ns.servicebus.windows.net:9093", "t", 'Endpoint=sb://ns/;Key="k"'
        )

        jaas = options["kafka.sasl.jaas.config"]
        assert 'username="$ConnectionString"' in jaas
        assert 'password="Endpoint=sb://ns/;Key=\\"k\\""' in jaas

    def test_kinesis_reads_the_stream_in_its_region(self):
        fmt, options = TripleStreamJob.source_options("kinesis", "us-west-2", "orders", "AK:SK")

        assert fmt == "kinesis"
        assert options["streamName"] == "orders" and options["region"] == "us-west-2"
        assert (options["awsAccessKey"], options["awsSecretKey"]) == ("AK", "SK")

    @pytest.mark.parametrize(("kind", "seq"), [("kafka", "offset"), ("eventhubs", "offset")])
    def test_the_kafka_offset_is_the_seq(self, kind, seq):
        assert TripleStreamJob.record_columns(kind) == ("CAST(value AS STRING)", seq)

    def test_the_kinesis_seq_orders_records_within_a_millisecond(self):
        _value, seq = TripleStreamJob.record_columns("kinesis")

        assert seq == (
            "unix_millis(approximateArrivalTimestamp) * 1000000 "
            "+ CAST(right(sequenceNumber, 6) AS BIGINT)"
        )

    @pytest.mark.parametrize(("hours", "mode"), [(0, "None"), (24, "ProcessingTime")])
    def test_a_state_ttl_needs_the_processing_time_mode(self, hours, mode):
        assert TripleStreamJob.time_mode(hours) == mode


class TestEnvelopes:
    def _envelope(self, **record) -> str:
        return json.dumps({"source": {"table": "Main.Sales.Customers"}, **record})

    def test_an_insert_reads_the_after_image(self):
        envelope = TripleStreamJob.parse_envelope(self._envelope(op="c", after={"id": 1, "name": "a"}))

        assert envelope == {"op": "upsert", "row": {"id": 1, "name": "a"}, "before": None, "table": _TABLE}

    def test_an_update_keeps_its_pre_image(self):
        envelope = TripleStreamJob.parse_envelope(
            self._envelope(op="u", before={"id": 1}, after={"id": 2})
        )

        assert envelope["row"] == {"id": 2} and envelope["before"] == {"id": 1}

    def test_a_delete_reads_the_before_image(self):
        envelope = TripleStreamJob.parse_envelope(
            self._envelope(op="d", before={"id": 1}, after=None)
        )

        assert envelope == {"op": "delete", "row": {"id": 1}, "before": None, "table": _TABLE}

    def test_a_debezium_payload_wrapper_is_unwrapped(self):
        wrapped = json.dumps(
            {"schema": {}, "payload": {"op": "u", "after": {"id": 2}, "source": {"table": _TABLE}}}
        )

        assert TripleStreamJob.parse_envelope(wrapped.encode())["row"] == {"id": 2}

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "not json",
            json.dumps({"op": "t", "source": {"table": _TABLE}}),
            json.dumps({"op": "c", "after": None, "source": {"table": _TABLE}}),
            json.dumps({"op": "c", "after": {"id": 1}}),
            json.dumps([1, 2]),
        ],
        ids=["tombstone", "garbage", "truncate", "no-image", "no-table", "not-an-object"],
    )
    def test_records_without_a_change_are_skipped(self, value):
        assert TripleStreamJob.parse_envelope(value) is None


class TestExplode:
    def test_one_input_row_per_mapping_of_the_table(self):
        envelope = {"op": "upsert", "row": {"id": 7, "name": "Ada"}, "table": _TABLE}

        [row] = TripleStreamJob.explode(envelope, 42, _projectors())

        assert row["source_key"] == "7" and row["op"] == "upsert" and row["seq"] == 42
        assert json.loads(row["row_json"]) == {"id": "7", "name": "Ada"}
        assert row["map_id"] == _projectors()[_TABLE][0].map_id

    def test_every_value_reaches_the_adapter_as_spark_text(self):
        envelope = {"op": "upsert", "row": {"id": 7, "name": 3.5}, "before": None, "table": _TABLE}
        types = {_TABLE: {"id": "int", "name": "double"}}

        [row] = TripleStreamJob.explode(envelope, 1, _projectors(), types)

        assert json.loads(row["row_json"]) == {"id": "7", "name": "3.5"}

    def test_an_update_that_changes_the_key_tombstones_the_old_key(self):
        envelope = {
            "op": "upsert",
            "row": {"id": 8, "name": "Ada"},
            "before": {"id": 7, "name": "Ada"},
            "table": _TABLE,
        }

        rows = TripleStreamJob.explode(envelope, 42, _projectors())

        assert [(r["source_key"], r["op"], r["seq"]) for r in rows] == [
            ("7", "delete", 42),
            ("8", "upsert", 42),
        ]
        assert json.loads(rows[0]["row_json"]) == {"id": "7", "name": "Ada"}

    def test_an_update_that_keeps_the_key_is_one_upsert(self):
        envelope = {"op": "upsert", "row": {"id": 7, "name": "B"}, "before": {"id": 7, "name": "A"}, "table": _TABLE}

        assert [r["op"] for r in TripleStreamJob.explode(envelope, 1, _projectors())] == ["upsert"]

    def test_a_null_key_yields_nothing(self):
        envelope = {"op": "upsert", "row": {"id": None, "name": "x"}, "table": _TABLE}

        assert TripleStreamJob.explode(envelope, 1, _projectors()) == []

    def test_another_table_yields_nothing(self):
        envelope = {"op": "upsert", "row": {"id": 1}, "table": "main.hr.staff"}

        assert TripleStreamJob.explode(envelope, 1, _projectors()) == []

    def test_records_are_parsed_then_exploded(self):
        value = json.dumps({"op": "c", "after": {"id": 1, "name": "a"}, "source": {"table": _TABLE}})

        rows = TripleStreamJob.explode_records([(value, 5), ("garbage", 6), (value, None)], _projectors())

        assert [row["seq"] for row in rows] == [5]

    def test_the_mappings_file_round_trips(self, tmp_path):
        path = tmp_path / "m.json"
        path.write_text(
            json.dumps({"entity_mappings": {"C": _MAPPING}, "relationship_mappings": [], "base_uri": "u"})
        )

        assert TripleStreamJob.load_mappings(str(path)) == ({"C": _MAPPING}, [], "u")


class TestSink:
    def test_the_upsert_keeps_the_newest_seq_per_slot(self):
        assert TripleStreamJob.upsert_sql("ontobricks_graph.sales_v3_live") == (
            "INSERT INTO ontobricks_graph.sales_v3_live AS live (map_id, source_key, predicate, "
            "subject, object, object_hash, deleted, seq, updated_at) "
            "SELECT DISTINCT ON (map_id, source_key, predicate) map_id, source_key, predicate, "
            "subject, object, object_hash, deleted, seq, now() FROM ob_live_stage "
            "ORDER BY map_id, source_key, predicate, seq DESC "
            "ON CONFLICT (map_id, source_key, predicate) DO UPDATE SET "
            "subject = EXCLUDED.subject, object = EXCLUDED.object, "
            "object_hash = EXCLUDED.object_hash, deleted = EXCLUDED.deleted, seq = EXCLUDED.seq, "
            "updated_at = now() WHERE EXCLUDED.seq > live.seq"
        )

    def test_an_unsafe_table_name_is_refused(self):
        with pytest.raises(ValueError):
            TripleStreamJob.upsert_sql("live; drop table x")

    def test_a_batch_is_copied_then_upserted_in_one_transaction(self):
        connection = _Connection()
        change = {column: f"v-{column}" for column in LIVE_COLUMNS} | {"deleted": False, "seq": 3}

        assert TripleStreamJob.write_changes(connection, "g_v3_live", [change]) == 1

        assert connection.cursor_.statements == [
            TripleStreamJob.staging_sql(),
            TripleStreamJob.copy_sql(),
            TripleStreamJob.upsert_sql("g_v3_live"),
        ]
        assert connection.cursor_.copied == [tuple(change[c] for c in LIVE_COLUMNS)]
        assert connection.commits == 1

    def test_an_empty_batch_touches_nothing(self):
        connection = _Connection()

        assert TripleStreamJob.write_changes(connection, "g_v3_live", []) == 0
        assert connection.commits == 0

    def test_the_foreach_writer_upserts_a_partition_on_close(self):
        connection = _Connection()
        sink = _LiveSinkWriter(lambda: connection, "g_v3_live")
        change = {column: None for column in LIVE_COLUMNS} | {"seq": 1, "deleted": True}

        assert sink.open(0, 1) is True
        sink.process(SimpleNamespace(asDict=lambda: change))
        sink.close(None)

        assert connection.commits == 1 and connection.closed

    def test_a_failed_partition_is_not_written(self):
        connection = _Connection()
        sink = _LiveSinkWriter(lambda: connection, "g_v3_live")
        sink.open(0, 1)
        sink.process({"seq": 1})

        sink.close(RuntimeError("task failed"))

        assert connection.commits == 0


class TestTrigger:
    class _Writer:
        def __init__(self, *, refuse_real_time=False):
            self.refuse = refuse_real_time
            self.trigger_kwargs = None

        def trigger(self, **kwargs):
            if "realTime" in kwargs and self.refuse:
                raise TypeError("trigger() got an unexpected keyword argument 'realTime'")
            self.trigger_kwargs = kwargs
            return self

    def _spark(self, *, refuse_conf=False):
        def set_conf(key, value):
            if refuse_conf:
                raise RuntimeError("[CONFIG_NOT_AVAILABLE] realTimeMode")
            confs[key] = value

        confs: dict = {}
        return SimpleNamespace(conf=SimpleNamespace(set=set_conf)), confs

    def test_real_time_mode_turns_the_conf_on_and_sets_the_trigger(self):
        spark, confs = self._spark()
        writer = self._Writer()

        _writer, mode = TripleStreamJob.with_trigger(spark, writer, TripleStreamJob.parse_args(_ARGS))

        assert mode == "real_time"
        assert confs == {"spark.databricks.streaming.realTimeMode.enabled": "true"}
        assert writer.trigger_kwargs == {"realTime": "5 minutes"}

    @pytest.mark.parametrize(("refuse_conf", "refuse_trigger"), [(True, False), (False, True)])
    def test_a_runtime_without_it_falls_back_to_micro_batches(self, refuse_conf, refuse_trigger):
        spark, _ = self._spark(refuse_conf=refuse_conf)
        writer = self._Writer(refuse_real_time=refuse_trigger)

        _writer, mode = TripleStreamJob.with_trigger(spark, writer, TripleStreamJob.parse_args(_ARGS))

        assert mode == "micro_batch" and writer.trigger_kwargs == {"processingTime": "10 seconds"}

    def test_micro_batches_on_request(self):
        spark, confs = self._spark()
        writer = self._Writer()

        TripleStreamJob.with_trigger(
            spark, writer, TripleStreamJob.parse_args([*_ARGS, "--no-real-time-mode"])
        )

        assert confs == {} and writer.trigger_kwargs == {"processingTime": "10 seconds"}


class _Cursor:
    def __init__(self):
        self.statements: list[str] = []
        self.copied: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.statements.append(sql)

    def copy(self, sql):
        self.statements.append(sql)
        cursor = self

        class _Copy:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def write_row(self, row):
                cursor.copied.append(row)

        return _Copy()


class _Connection:
    def __init__(self):
        self.cursor_ = _Cursor()
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True
