"""``TwinRealtimeStream``: start, stop and status of a domain version's stream job (fake SDK)."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.graphdb.lakebase.Base import Base as LakebaseBase
from back.objects.digitaltwin.build import (
    TwinIncrementalBuild,
    TwinLiveServing,
    TwinRealtimeJob,
    TwinRealtimeStream,
)

pytestmark = pytest.mark.unit

_SOURCE = {
    "kind": "kafka",
    "bootstrap_servers": "b:9092",
    "topic": "sales.customers",
    "secret_scope": "ontobricks",
    "secret_key": "kafka-jaas",
    "key_format": "json",
    "state_ttl_hours": 48,
}


def _settings(**over):
    base = dict(
        realtime_enabled=True,
        realtime_job_name="",
        ontobricks_app_name="ontobricks-030",
        realtime_lakebase_user="ontobricks_rtm",
        realtime_lakebase_secret="ontobricks/pg-password",
        realtime_checkpoint_root="/Volumes/main/ob/registry/realtime/",
        databricks_triplestore_table="",
    )
    base.update(over)
    return SimpleNamespace(**base)


def _domain(status="PUBLISHED", source=None):
    return SimpleNamespace(
        info={"name": "sales", "status": status, "realtime_source": dict(source or _SOURCE)},
        current_version="3",
        delta={"catalog": "main", "schema": "kg"},
    )


class _Live:
    def __init__(self):
        self.ensured: list = []
        self.attached: list = []
        self.detached: list = []
        self.purged: list = []
        self.served: set = set()  # the graphs whose _live is served

    @staticmethod
    def table_name(graph):
        return f"{graph.lower()}_live"

    def ensure(self, graph, writer_role=""):
        self.ensured.append((graph, writer_role))
        return self.table_name(graph)

    def attach(self, graph, rows):
        self.attached.append((graph, list(rows)))
        self.served.add(graph)
        return len(self.attached[-1][1])

    def detach(self, graph):
        self.detached.append(graph)
        self.served.discard(graph)

    def serving(self, graph):
        return graph in self.served

    def purge_tombstones(self, graph, hours):
        self.purged.append((graph, hours))
        return 3

    def connection(self):
        return {"host": "pg.lakebase.io", "database": "graphs", "schema": "ontobricks_graph"}


_FakeLakebase = type(
    "_FakeLakebase",
    (LakebaseBase,),
    {name: (lambda self, *a, **k: None) for name in LakebaseBase.__abstractmethods__},
)


class _Store(_FakeLakebase):
    is_synced = True

    def __init__(self):  # no pool, no auth: a test double
        self.live = _Live()
        self.views: list = []

    def live_table(self):
        return self.live

    def ensure_synced_union_view(self, graph, synced_phy_override=""):
        self.views.append(graph)


#: The newest commit of the fake ``_slots``: the seed's version and replay start.
_SLOTS_COMMIT = {"version": 12, "timestamp": "2026-11-02T10:00:00+00:00"}
_REPLAY_FROM_MS = 1793613600000 - 600000  # 2026-11-02T10:00:00Z less the 10-minute margin


class _Warehouse:
    def __init__(self, rows=()):
        self.rows, self.queries = list(rows), []

    def execute_query(self, sql):
        self.queries.append(sql)
        return [dict(_SLOTS_COMMIT)] if sql.startswith("DESCRIBE HISTORY") else []

    def iter_rows(self, sql, batch_size=5000):
        self.queries.append(sql)
        return iter(self.rows)


class _Jobs:
    def __init__(self, names=("ontobricks-030-triple-rtm",), runs=()):
        self.names, self.runs = list(names), list(runs)
        self.started: list = []
        self.cancelled: list = []

    def list(self, name=None):
        jobs = [SimpleNamespace(job_id=10 + i, settings=SimpleNamespace(name=n)) for i, n in enumerate(self.names)]
        return [j for j in jobs if name is None or j.settings.name == name]

    def list_runs(self, job_id, active_only):
        assert active_only is True
        return self.runs

    def run_now(self, job_id, job_parameters):
        self.started.append((job_id, job_parameters))
        return SimpleNamespace(run_id=777)

    def cancel_run(self, run_id):
        self.cancelled.append(run_id)


class _Files:
    def __init__(self):
        self.uploads: dict = {}
        self.folders: list = []

    def create_directory(self, folder):
        self.folders.append(folder)

    def upload(self, path, contents, overwrite=False):
        assert overwrite is True
        self.uploads[path] = json.loads(contents.read())


def _client(jobs=None):
    return SimpleNamespace(
        jobs=jobs or _Jobs(),
        files=_Files(),
    )


def _run(domain="sales", version="3", state="RUNNING", run_id=55):
    params = [SimpleNamespace(name="domain", value=domain), SimpleNamespace(name="version", value=version)]
    return SimpleNamespace(
        run_id=run_id, job_parameters=params, state=SimpleNamespace(life_cycle_state=state)
    )


@pytest.fixture
def mappings(monkeypatch):
    monkeypatch.setattr(
        TwinIncrementalBuild,
        "mappings_for",
        staticmethod(lambda d: ({"C": {"table": "main.sales.customers"}}, [], "http://ex.org/")),
    )


def _stream(client, **settings):
    return TwinRealtimeStream(_settings(**settings), client_factory=lambda: client)


_SLOT_ROW = {"map_id": "m", "source_key": "1", "predicate": "p", "subject": "s", "object": "a"}


def _start(client, store=None, warehouse=None, domain=None, **settings):
    store = store or _Store()
    warehouse = warehouse or _Warehouse([_SLOT_ROW])
    started = _stream(client, **settings).start(domain or _domain(), store=store, client=warehouse)
    return started, store, warehouse


class TestStart:
    def test_the_job_is_started_with_every_parameter_and_no_secret_value(self, mappings):
        client = _client()

        started, _store, _wh = _start(client)

        assert started == {
            "run_id": 777,
            "state": "PENDING",
            "reused": False,
            "seeded": 1,
            "slots_version": 12,
        }
        [(job_id, params)] = client.jobs.started
        assert job_id == 10
        assert params == {
            "domain": "sales",
            "version": "3",
            "source_kind": "kafka",
            "bootstrap_servers": "b:9092",
            "topic": "sales.customers",
            "secret_scope": "ontobricks",
            "secret_key": "kafka-jaas",
            "slots_table": "main.kg.triplestore_sales_V3_slots",
            "mappings_json_path": "/Volumes/main/ob/registry/realtime/_mappings/ontobricks-rtm-sales-v3.json",
            "lakebase_host": "pg.lakebase.io",
            "lakebase_db": "graphs",
            "lakebase_user": "ontobricks_rtm",
            "lakebase_secret": "ontobricks/pg-password",
            "live_table": "ontobricks_graph.sales_v3_live",
            "checkpoint": "/Volumes/main/ob/registry/realtime/ontobricks-rtm-sales-v3",
            "state_ttl_hours": "48",
            "slots_version": "12",
            "start_timestamp_ms": str(_REPLAY_FROM_MS),
        }

    def test_live_is_granted_seeded_from_the_slots_and_served_before_the_run(self, mappings):
        client = _client()

        _started, store, warehouse = _start(client)

        assert store.live.ensured == [("sales_V3", "ontobricks_rtm")]
        assert warehouse.queries == [
            "DESCRIBE HISTORY main.kg.triplestore_sales_V3_slots LIMIT 1",
            "SELECT map_id, source_key, predicate, subject, object"
            " FROM main.kg.triplestore_sales_V3_slots VERSION AS OF 12",
        ]
        assert store.live.attached == [("sales_V3", [_SLOT_ROW])]
        assert store.views == ["sales_V3"]
        assert client.files.folders == ["/Volumes/main/ob/registry/realtime/_mappings"]
        uploaded = client.files.uploads[
            "/Volumes/main/ob/registry/realtime/_mappings/ontobricks-rtm-sales-v3.json"
        ]
        assert uploaded["base_uri"] == "http://ex.org/" and "C" in uploaded["entity_mappings"]

    def test_a_running_stream_of_the_version_is_reused_without_reseeding(self, mappings):
        client = _client(_Jobs(runs=[_run(run_id=56)]))

        started, store, _wh = _start(client)

        assert started == {
            "run_id": 56,
            "state": "RUNNING",
            "reused": True,
            "seeded": 0,
            "slots_version": None,
        }
        assert client.jobs.started == [] and store.live.attached == []

    def test_a_stream_of_another_version_is_cancelled_and_its_live_retired_first(self, mappings):
        client = _client(_Jobs(runs=[_run(version="2", run_id=41)]))
        store = _Store()
        store.live.served = {"sales_V2"}

        started, store, _wh = _start(client, store=store)

        assert client.jobs.cancelled == [41]
        assert store.live.detached == ["sales_V2"] and store.live.served == {"sales_V3"}
        assert started["run_id"] == 777 and started["reused"] is False

    @pytest.mark.parametrize("failing", ["upload", "run_now"])
    def test_a_failure_after_the_seed_retires_live_before_raising(self, mappings, failing):
        client = _client()
        if failing == "upload":
            client.files.upload = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no WRITE VOLUME"))
        else:
            client.jobs.run_now = lambda **kw: (_ for _ in ()).throw(RuntimeError("PERMISSION_DENIED"))
        store = _Store()

        with pytest.raises(InfrastructureError):
            _start(client, store=store)

        assert store.live.attached and store.live.detached == ["sales_V3"]
        assert store.live.served == set()

    def test_a_dev_mode_job_name_is_found(self, mappings):
        client = _client(_Jobs(names=("other", "[dev someone] ontobricks-030-triple-rtm")))

        _start(client)

        assert client.jobs.started[0][0] == 11

    @pytest.mark.parametrize(
        ("domain", "settings", "match"),
        [
            (_domain(), {"realtime_enabled": False}, "disabled"),
            (_domain(status="DRAFT"), {}, "published"),
            (_domain(source={**_SOURCE, "topic": ""}), {}, "incomplete"),
            (_domain(), {"realtime_lakebase_secret": ""}, "ONTOBRICKS_REALTIME_LAKEBASE_SECRET"),
            (_domain(), {"realtime_checkpoint_root": " "}, "ONTOBRICKS_REALTIME_CHECKPOINT_ROOT"),
            (_domain(), {"realtime_checkpoint_root": "dbfs:/rtm"}, "volume path"),
        ],
        ids=["disabled", "draft", "no-topic", "no-sink-secret", "no-checkpoint", "not-a-volume"],
    )
    def test_a_missing_prerequisite_is_refused_before_any_api_call(self, domain, settings, match):
        def no_client():
            raise AssertionError("no API call expected")

        with pytest.raises(ValidationError, match=match):
            TwinRealtimeStream(_settings(**settings), client_factory=no_client).start(
                domain, store=_Store(), client=_Warehouse()
            )

    def test_a_store_without_live_serving_is_refused(self, mappings):
        with pytest.raises(ValidationError, match="Lakebase"):
            _stream(_client()).start(_domain(), store=SimpleNamespace(), client=_Warehouse())

    def test_an_undeployed_job_is_not_found(self, mappings):
        with pytest.raises(NotFoundError, match="triple-rtm"):
            _start(_client(_Jobs(names=())))

    def test_an_sdk_failure_is_an_infrastructure_error(self, mappings):
        jobs = _Jobs()
        jobs.run_now = lambda **kw: (_ for _ in ()).throw(RuntimeError("PERMISSION_DENIED"))

        with pytest.raises(InfrastructureError):
            _start(_client(jobs))


class TestStopAndStatus:
    def test_stop_cancels_the_version_s_run_and_retires_live(self):
        client, store = _client(_Jobs(runs=[_run(run_id=56)])), _Store()
        store.live.served = {"sales_V3"}

        assert _stream(client).stop(_domain(), store=store) == {"run_id": 56, "state": "CANCELLING"}
        assert client.jobs.cancelled == [56]
        assert store.live.detached == ["sales_V3"] and store.views == ["sales_V3"]

    def test_stop_without_a_run_still_retires_a_served_live(self):
        store = _Store()
        store.live.served = {"sales_V3"}

        assert _stream(_client()).stop(_domain(), store=store) == {"run_id": None, "state": "not_running"}
        assert store.live.detached == ["sales_V3"]

    def test_status_reads_the_active_run(self):
        client = _client(_Jobs(runs=[_run(state="PENDING", run_id=9)]))

        assert _stream(client).status(_domain()) == {"run_id": 9, "state": "PENDING"}

    def test_status_without_a_job_name(self):
        stream = TwinRealtimeStream(_settings(ontobricks_app_name=""), client_factory=_client)

        assert stream.status(_domain())["state"] == "not_configured"

    def test_status_with_an_unreachable_api_is_unknown(self):
        def broken():
            raise RuntimeError("no host")

        status = TwinRealtimeStream(_settings(), client_factory=broken).status(_domain())

        assert status["state"] == "unknown" and "error" in status


class TestAfterTick:
    def test_a_dead_stream_s_live_is_retired(self):
        store = _Store()
        store.live.served = {"sales_V3"}

        facts = _stream(_client()).after_tick(_domain(), store)

        assert facts == {"realtime_state": "retired"} and store.live.detached == ["sales_V3"]

    def test_a_running_stream_s_tombstones_are_purged_after_the_state_ttl(self):
        store = _Store()
        store.live.served = {"sales_V3"}

        facts = _stream(_client(_Jobs(runs=[_run()]))).after_tick(_domain(), store)

        assert facts == {"live_tombstones_purged": 3}
        assert store.live.purged == [("sales_V3", 48)]

    def test_an_unserved_live_needs_nothing(self):
        assert _stream(_client()).after_tick(_domain(), _Store()) == {}


class _Fake:
    """A ``TwinRealtimeStream`` double for the build hook."""

    log: list = []

    def __init__(self, settings):
        self.settings = settings

    def start(self, domain, store=None, client=None):
        _Fake.log.append(("start", client))
        return {"run_id": 777, "state": "PENDING", "reused": False, "seeded": 1, "slots_version": 12}

    def stop_quietly(self, domain, store=None):
        _Fake.log.append(("stop", None))
        return {"run_id": 55, "state": "CANCELLING"}


def _pipe(effective, *, store=None, enabled=True):
    return SimpleNamespace(
        settings=SimpleNamespace(realtime_enabled=enabled),
        mode_decision=SimpleNamespace(effective=effective),
        domain=_domain(),
        store=store if store is not None else _Store(),
        source_client="warehouse",
        task_id="t-1",
        realtime_run_id="",
        realtime_state="",
    )


class TestAfterBuild:
    def setup_method(self):
        _Fake.log = []

    def test_a_realtime_build_starts_the_stream_on_its_warehouse_and_records_it(self):
        pipe = _pipe("realtime")

        TwinRealtimeStream.after_build(pipe, factory=_Fake)

        assert _Fake.log == [("start", "warehouse")]
        assert (pipe.realtime_run_id, pipe.realtime_state) == ("777", "PENDING")
        assert pipe.slots_seed_version == 12

    def test_a_build_that_is_not_realtime_retires_a_served_live(self):
        store = _Store()
        store.live.served = {"sales_V3"}
        pipe = _pipe("microbatch", store=store)

        TwinRealtimeStream.after_build(pipe, factory=_Fake)

        assert _Fake.log == [("stop", None)] and pipe.realtime_state == "CANCELLING"

    def test_realtime_off_makes_no_postgres_query(self):
        store = _Store()
        store.live.serving = lambda graph: pytest.fail("no Postgres query expected")

        TwinRealtimeStream.after_build(_pipe("microbatch", store=store, enabled=False), factory=_Fake)

        assert _Fake.log == []

    def test_a_failing_serving_query_is_recorded_not_raised(self):
        store = _Store()
        store.live.serving = lambda graph: (_ for _ in ()).throw(RuntimeError("connection refused"))
        pipe = _pipe("microbatch", store=store)

        TwinRealtimeStream.after_build(pipe, factory=_Fake)

        assert pipe.realtime_state == "failed:RuntimeError" and _Fake.log == []

    def test_a_build_without_a_served_live_calls_nothing(self):
        TwinRealtimeStream.after_build(_pipe("microbatch"), factory=_Fake)

        assert _Fake.log == []

    def test_a_non_lakebase_store_calls_nothing(self):
        TwinRealtimeStream.after_build(_pipe("incremental", store=SimpleNamespace()), factory=_Fake)

        assert _Fake.log == []

    def test_a_stream_that_cannot_start_is_recorded_not_raised(self):
        class _Refusing(_Fake):
            def start(self, domain, store=None, client=None):
                raise ValidationError("Only a published domain version streams in realtime")

        pipe = _pipe("realtime")

        TwinRealtimeStream.after_build(pipe, factory=_Refusing)

        assert pipe.realtime_state == "failed:ValidationError"

    def test_an_unreachable_job_still_retires_live(self):
        store = _Store()
        store.live.served = {"sales_V3"}
        stream = TwinRealtimeStream(_settings(ontobricks_app_name=""), client_factory=_client)

        stopped = stream.stop_quietly(_domain(), store=store)

        assert stopped["state"] == "retired" and store.live.detached == ["sales_V3"]


def test_the_job_name_defaults_to_the_bundle_name():
    assert TwinRealtimeJob.job_name(_settings()) == "ontobricks-030-triple-rtm"
    assert TwinRealtimeJob.job_name(_settings(realtime_job_name="custom")) == "custom"


def test_live_serving_needs_a_lakebase_store():
    with pytest.raises(ValidationError):
        TwinLiveServing.live(SimpleNamespace(live_table=lambda: None))
